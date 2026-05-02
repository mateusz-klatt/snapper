"""Tests for partitioning fields on :class:`BootstrapSettingsLoader`.

Bootstrap fields verify env-var round-trip only. The parsing contract
for ``coordinator_outbox_max_scan_rows`` (``str | None`` on bootstrap →
``int | None`` on ``AppSettings``) lives in
``tests/config/test_app_settings.py``.
"""

import pytest

from snapper.config.bootstrap import BootstrapSettingsLoader


class TestCoordinatorInstanceId:
    """``SNAPPER_COORDINATOR_INSTANCE_ID`` env round-trip."""

    def test_default_is_zero(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Default ``coordinator_instance_id`` is ``0`` (single-instance)."""
        monkeypatch.delenv("SNAPPER_COORDINATOR_INSTANCE_ID", raising=False)
        loader = BootstrapSettingsLoader()
        assert loader.coordinator_instance_id == 0

    def test_env_overrides_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Setting the env var changes the loaded value."""
        monkeypatch.setenv("SNAPPER_COORDINATOR_INSTANCE_ID", "3")
        loader = BootstrapSettingsLoader()
        assert loader.coordinator_instance_id == 3


class TestCoordinatorInstanceCount:
    """``SNAPPER_COORDINATOR_INSTANCE_COUNT`` env round-trip."""

    def test_default_is_one(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Default ``coordinator_instance_count`` is ``1``."""
        monkeypatch.delenv("SNAPPER_COORDINATOR_INSTANCE_COUNT", raising=False)
        loader = BootstrapSettingsLoader()
        assert loader.coordinator_instance_count == 1

    def test_env_overrides_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Setting the env var changes the loaded value."""
        monkeypatch.setenv("SNAPPER_COORDINATOR_INSTANCE_COUNT", "4")
        loader = BootstrapSettingsLoader()
        assert loader.coordinator_instance_count == 4


class TestCoordinatorOutboxMaxScanRowsRaw:
    """Raw ``str | None`` round-trip on the bootstrap field.

    Parsing to ``int | None`` + invalid-value ``ValueError`` lives on
    ``AppSettings.coordinator_outbox_max_scan_rows`` — see
    :mod:`tests.config.test_app_settings`.
    """

    def test_default_is_string_1000(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The bootstrap default is the string literal ``"1000"``."""
        monkeypatch.delenv("SNAPPER_COORDINATOR_OUTBOX_MAX_SCAN_ROWS", raising=False)
        loader = BootstrapSettingsLoader()
        assert loader.coordinator_outbox_max_scan_rows == "1000"

    def test_env_passes_through_unbounded_sentinel(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``"unbounded"`` is accepted verbatim (parsed by AppSettings)."""
        monkeypatch.setenv("SNAPPER_COORDINATOR_OUTBOX_MAX_SCAN_ROWS", "unbounded")
        loader = BootstrapSettingsLoader()
        assert loader.coordinator_outbox_max_scan_rows == "unbounded"

    def test_env_passes_through_empty_sentinel(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``""`` is accepted verbatim (parsed by AppSettings)."""
        monkeypatch.setenv("SNAPPER_COORDINATOR_OUTBOX_MAX_SCAN_ROWS", "")
        loader = BootstrapSettingsLoader()
        assert loader.coordinator_outbox_max_scan_rows == ""

    def test_env_passes_through_none_sentinel(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``"none"`` is accepted verbatim (parsed by AppSettings)."""
        monkeypatch.setenv("SNAPPER_COORDINATOR_OUTBOX_MAX_SCAN_ROWS", "none")
        loader = BootstrapSettingsLoader()
        assert loader.coordinator_outbox_max_scan_rows == "none"

    def test_env_passes_through_custom_int_string(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Custom numeric strings survive round-trip unchanged."""
        monkeypatch.setenv("SNAPPER_COORDINATOR_OUTBOX_MAX_SCAN_ROWS", "50")
        loader = BootstrapSettingsLoader()
        assert loader.coordinator_outbox_max_scan_rows == "50"
