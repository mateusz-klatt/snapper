"""Tests for the `plan_recovery_auto_settle_enabled` bootstrap and app setting."""

import pytest

from snapper.config.app import AppSettings
from snapper.config.bootstrap import BootstrapSettingsLoader


def test_plan_recovery_auto_settle_defaults_false(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify automatic startup plan settlement ships disabled by default.

    Given: No `PLAN_RECOVERY_AUTO_SETTLE_ENABLED` environment variable is set.
    When: `BootstrapSettingsLoader` and `AppSettings` are instantiated.
    Then: Both expose `plan_recovery_auto_settle_enabled` as `False`.
    """
    monkeypatch.delenv("PLAN_RECOVERY_AUTO_SETTLE_ENABLED", raising=False)
    bootstrap = BootstrapSettingsLoader(_env_file=None)
    settings = AppSettings(bootstrap)
    assert bootstrap.plan_recovery_auto_settle_enabled is False
    assert settings.plan_recovery_auto_settle_enabled is False


def test_plan_recovery_auto_settle_honors_truthy_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify `PLAN_RECOVERY_AUTO_SETTLE_ENABLED=true` enables startup settlement.

    Given: `PLAN_RECOVERY_AUTO_SETTLE_ENABLED` is set to `"true"` in the environment.
    When: `BootstrapSettingsLoader` and `AppSettings` are instantiated.
    Then: Both expose `plan_recovery_auto_settle_enabled` as `True`.
    """
    monkeypatch.setenv("PLAN_RECOVERY_AUTO_SETTLE_ENABLED", "true")
    bootstrap = BootstrapSettingsLoader(_env_file=None)
    settings = AppSettings(bootstrap)
    assert bootstrap.plan_recovery_auto_settle_enabled is True
    assert settings.plan_recovery_auto_settle_enabled is True
