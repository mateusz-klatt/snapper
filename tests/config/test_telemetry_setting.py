"""Tests for telemetry_recording_enabled setting."""

import pytest

from snapper.config.app import AppSettings
from snapper.config.bootstrap import BootstrapSettingsLoader


class TestTelemetryRecordingSetting:
    """Tests for the telemetry recording bootstrap setting."""

    def test_default_is_false(self) -> None:
        """Telemetry recording is disabled by default."""
        loader = BootstrapSettingsLoader()
        assert loader.telemetry_recording_enabled is False

    def test_app_settings_exposes_property(self) -> None:
        """AppSettings facade exposes telemetry_recording_enabled."""
        loader = BootstrapSettingsLoader()
        app_settings = AppSettings(bootstrap_settings=loader)
        assert app_settings.telemetry_recording_enabled is False

    def test_enabled_via_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Setting TELEMETRY_RECORDING_ENABLED=true enables recording."""
        monkeypatch.setenv("TELEMETRY_RECORDING_ENABLED", "true")
        loader = BootstrapSettingsLoader()
        assert loader.telemetry_recording_enabled is True
