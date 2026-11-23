"""Tests for the ``awaiting_target_stale_threshold_s`` bootstrap and app setting."""

import pytest
from pydantic import ValidationError

from snapper.config.app import AppSettings
from snapper.config.bootstrap import BootstrapSettingsLoader


def test_awaiting_target_stale_threshold_defaults_15_minutes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify stale awaiting_target threshold defaults to 900 seconds (15 minutes).

    Given: No ``AWAITING_TARGET_STALE_THRESHOLD_S`` environment variable is set.
    When: ``BootstrapSettingsLoader`` and ``AppSettings`` are instantiated.
    Then: Both expose ``awaiting_target_stale_threshold_s`` as ``900.0``.
    """
    monkeypatch.delenv("AWAITING_TARGET_STALE_THRESHOLD_S", raising=False)
    bootstrap = BootstrapSettingsLoader(_env_file=None)
    settings = AppSettings(bootstrap)
    assert bootstrap.awaiting_target_stale_threshold_s == 900.0
    assert settings.awaiting_target_stale_threshold_s == 900.0


def test_awaiting_target_stale_threshold_honors_env_and_rejects_nonpositive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify positive env overrides apply and non-positive values fail validation.

    Given: Valid positive and invalid non-positive env values for
        ``AWAITING_TARGET_STALE_THRESHOLD_S``.
    When: ``BootstrapSettingsLoader`` parses each value.
    Then: Positive values configure ``AppSettings`` and zero or negative values
        raise ``ValidationError``.
    """
    monkeypatch.setenv("AWAITING_TARGET_STALE_THRESHOLD_S", "120.5")
    bootstrap = BootstrapSettingsLoader(_env_file=None)
    settings = AppSettings(bootstrap)
    assert settings.awaiting_target_stale_threshold_s == 120.5

    for bad in ("0", "-10"):
        monkeypatch.setenv("AWAITING_TARGET_STALE_THRESHOLD_S", bad)
        with pytest.raises(ValidationError):
            BootstrapSettingsLoader(_env_file=None)
