"""Tests for the Phase-5B snapshotter env-var contract module."""

import logging

import pytest

from snapper.application.portfolio import pnl_snapshotter_config
from snapper.application.portfolio.pnl_snapshotter_config import CHECKPOINT_MODE_ENV_VAR
from snapper.application.portfolio.pnl_snapshotter_config import CHECKPOINT_MODES_ENABLED
from snapper.application.portfolio.pnl_snapshotter_config import DEFAULT_INTERVAL_SECONDS
from snapper.application.portfolio.pnl_snapshotter_config import ENABLED_ENV_VAR
from snapper.application.portfolio.pnl_snapshotter_config import ENV_VARS
from snapper.application.portfolio.pnl_snapshotter_config import FX_SHADOW_PINNING_ENV_VAR
from snapper.application.portfolio.pnl_snapshotter_config import INTERVAL_ENV_VAR
from snapper.application.portfolio.pnl_snapshotter_config import ResolvedCheckpointMode
from snapper.application.portfolio.pnl_snapshotter_config import resolve_checkpoint_modes
from snapper.application.portfolio.pnl_snapshotter_config import resolve_enabled
from snapper.application.portfolio.pnl_snapshotter_config import resolve_interval


class TestResolveInterval:
    """Interval coercion and range enforcement."""

    @pytest.mark.parametrize("raw", [None, "", "   "])
    def test_unset_returns_default(self, raw: str | None) -> None:
        """An empty or unset value falls back to the default interval."""
        assert resolve_interval(raw) == DEFAULT_INTERVAL_SECONDS

    def test_in_range_value_passes_through(self) -> None:
        """A valid in-range integer is returned as an int."""
        assert resolve_interval(" 120 ") == 120

    def test_unparseable_raises(self) -> None:
        """A non-integer value fails loud."""
        with pytest.raises(ValueError, match="not an integer"):
            resolve_interval("abc")

    @pytest.mark.parametrize("raw", ["5", "5000"])
    def test_out_of_range_raises(self, raw: str) -> None:
        """A value outside ``[10, 3600]`` fails loud."""
        with pytest.raises(ValueError, match="out of range"):
            resolve_interval(raw)


class TestResolveEnabled:
    """Enabled-flag parsing, defaulting to disabled."""

    @pytest.mark.parametrize("raw", ["1", "true", "TRUE", " yes "])
    def test_truthy_values_enable(self, raw: str) -> None:
        """Any truthy token enables the snapshotter."""
        assert resolve_enabled(raw) is True

    @pytest.mark.parametrize("raw", [None, "", "0", "false", "no"])
    def test_falsy_values_stay_disabled(self, raw: str | None) -> None:
        """Everything else, including unset, leaves it disabled."""
        assert resolve_enabled(raw) is False


class TestResolveCheckpointMode:
    """Opening-checkpoint rollout mode parsing, kill switch, and fail-loud validation."""

    @pytest.fixture(autouse=True)
    def _reset_warning_latch(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Reset the process-wide disabled-mode warning latch around each test."""
        monkeypatch.setattr(pnl_snapshotter_config._KILL_SWITCH_WARNING_STATE, "logged", False)

    def test_checkpoint_modes_enabled_includes_shadow_and_active(self) -> None:
        """All three checkpoint modes are enabled in production configuration.

        Given: Production module defaults in ``pnl_snapshotter_config``.
        When: Inspecting ``CHECKPOINT_MODES_ENABLED``.
        Then: It contains ``"off"``, ``"shadow"``, and ``"active"``.
        """
        assert frozenset({"off", "shadow", "active"}) == CHECKPOINT_MODES_ENABLED

    @pytest.mark.parametrize("raw", [None, "", "   ", "off", "OFF", " Off "])
    def test_unset_blank_or_off_resolves_to_off_without_warning(
        self,
        caplog: pytest.LogCaptureFixture,
        raw: str | None,
    ) -> None:
        """Unset, empty, whitespace, or ``off`` tokens resolve to ``off`` quietly.

        Given: An unset, blank, whitespace, or case-varied ``off`` string.
        When: ``resolve_checkpoint_modes`` parses it.
        Then: Both configured and effective modes are ``"off"`` and no warning logs.
        """
        with caplog.at_level(logging.WARNING, logger=pnl_snapshotter_config.__name__):
            assert resolve_checkpoint_modes(raw) == ResolvedCheckpointMode("off", "off")
        assert caplog.records == []

    def test_shadow_under_kill_switch_resolves_to_effective_off_and_logs_warning_once(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Disabled ``shadow`` under a narrowed kill switch resolves to ``off`` and warns once.

        Given: ``CHECKPOINT_MODES_ENABLED`` narrowed to ``{"off"}`` and ``shadow`` values.
        When: Resolved twice in the same process under the kill switch.
        Then: Effective mode is ``"off"``, configured is ``"shadow"``, and one WARNING logs.
        """
        monkeypatch.setattr(
            pnl_snapshotter_config,
            "CHECKPOINT_MODES_ENABLED",
            frozenset({"off"}),
        )
        with caplog.at_level(logging.WARNING, logger=pnl_snapshotter_config.__name__):
            first = resolve_checkpoint_modes(" Shadow\t")
            second = resolve_checkpoint_modes("shadow")
        assert first == ResolvedCheckpointMode(configured="shadow", effective="off")
        assert second == ResolvedCheckpointMode(configured="shadow", effective="off")
        warnings = [record for record in caplog.records if record.levelname == "WARNING"]
        assert len(warnings) == 1
        message = warnings[0].getMessage()
        assert "shadow" in message
        assert "off" in message
        assert "checkpoint modes are disabled until capture is enabled" in message

    @pytest.mark.parametrize("raw", ["active", "ACTIVE", " active "])
    def test_active_under_kill_switch_resolves_to_effective_off(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
        raw: str,
    ) -> None:
        """Disabled ``active`` under a narrowed kill switch resolves to effective ``off``.

        Given: ``CHECKPOINT_MODES_ENABLED`` narrowed to ``{"off"}`` and mixed-case ``active``.
        When: Resolved under the kill switch.
        Then: Configured is ``"active"``, effective is ``"off"``, and one warning logs.
        """
        monkeypatch.setattr(
            pnl_snapshotter_config,
            "CHECKPOINT_MODES_ENABLED",
            frozenset({"off"}),
        )
        with caplog.at_level(logging.WARNING, logger=pnl_snapshotter_config.__name__):
            resolved = resolve_checkpoint_modes(raw)
        assert resolved == ResolvedCheckpointMode(configured="active", effective="off")
        assert len(caplog.records) == 1

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("shadow", "shadow"),
            ("SHADOW", "shadow"),
            (" Shadow\t", "shadow"),
            ("active", "active"),
            ("ACTIVE", "active"),
            (" active ", "active"),
        ],
    )
    def test_shadow_and_active_accept_case_and_whitespace_unpatched(
        self,
        caplog: pytest.LogCaptureFixture,
        raw: str,
        expected: str,
    ) -> None:
        """Enabled ``shadow`` and ``active`` values strip whitespace and match case-insensitively.

        Given: Production ``CHECKPOINT_MODES_ENABLED`` and mixed-case mode tokens.
        When: ``resolve_checkpoint_modes`` parses the setting value.
        Then: It returns the canonical lowercase literal with no kill-switch warning.
        """
        with caplog.at_level(logging.WARNING, logger=pnl_snapshotter_config.__name__):
            assert resolve_checkpoint_modes(raw) == ResolvedCheckpointMode(
                configured=expected,
                effective=expected,
            )
        assert caplog.records == []

    @pytest.mark.parametrize("raw", ["on", "1", "true", "enabled", "unknown"])
    def test_invalid_mode_raises_naming_env_var(self, raw: str) -> None:
        """Any unrecognised token fails loud at startup and names the env var.

        Given: An unrecognised checkpoint mode token.
        When: ``resolve_checkpoint_modes`` validates it.
        Then: It raises ``ValueError`` naming ``PNL_SNAPSHOTTER_CHECKPOINT_MODE``.
        """
        with pytest.raises(ValueError, match=CHECKPOINT_MODE_ENV_VAR):
            resolve_checkpoint_modes(raw)


class TestEnvVarsContract:
    """The exported allowlist names every snapshotter contract key."""

    def test_env_vars_lists_all_keys(self) -> None:
        """``ENV_VARS`` includes interval, enabled, shadow pinning, and checkpoint mode.

        Given: The snapshotter config module constants.
        When: ``ENV_VARS`` is compared with the exported env-var names.
        Then: Every snapshotter environment key is registered in ``ENV_VARS``.
        """
        assert (
            frozenset(
                {
                    INTERVAL_ENV_VAR,
                    ENABLED_ENV_VAR,
                    FX_SHADOW_PINNING_ENV_VAR,
                    CHECKPOINT_MODE_ENV_VAR,
                }
            )
            == ENV_VARS
        )
