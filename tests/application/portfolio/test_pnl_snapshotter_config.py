"""Tests for the Phase-5B snapshotter env-var contract module."""

import pytest

from snapper.application.portfolio.pnl_snapshotter_config import CHECKPOINT_MODE_ENV_VAR
from snapper.application.portfolio.pnl_snapshotter_config import DEFAULT_INTERVAL_SECONDS
from snapper.application.portfolio.pnl_snapshotter_config import ENABLED_ENV_VAR
from snapper.application.portfolio.pnl_snapshotter_config import ENV_VARS
from snapper.application.portfolio.pnl_snapshotter_config import FX_SHADOW_PINNING_ENV_VAR
from snapper.application.portfolio.pnl_snapshotter_config import INTERVAL_ENV_VAR
from snapper.application.portfolio.pnl_snapshotter_config import resolve_checkpoint_mode
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
    """Opening-checkpoint rollout mode parsing and fail-loud validation."""

    @pytest.mark.parametrize("raw", [None, "", "   ", "off", "OFF", " Off "])
    def test_unset_blank_or_off_resolves_to_off(self, raw: str | None) -> None:
        """Unset, empty, whitespace, or ``off`` tokens resolve to ``off``.

        Given: An unset, blank, whitespace, or case-varied ``off`` string.
        When: ``resolve_checkpoint_mode`` parses the setting value.
        Then: It returns ``"off"``.
        """
        assert resolve_checkpoint_mode(raw) == "off"

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
    def test_shadow_and_active_accept_case_and_surrounding_whitespace(
        self, raw: str, expected: str
    ) -> None:
        """Valid ``shadow`` and ``active`` values strip whitespace and match case-insensitively.

        Given: Whitespace-padded or mixed-case ``shadow`` or ``active`` tokens.
        When: ``resolve_checkpoint_mode`` parses the setting value.
        Then: It returns the lowercase canonical literal ``"shadow"`` or ``"active"``.
        """
        assert resolve_checkpoint_mode(raw) == expected

    @pytest.mark.parametrize("raw", ["on", "1", "true", "enabled", "unknown"])
    def test_invalid_mode_raises_naming_env_var(self, raw: str) -> None:
        """Any unrecognised token fails loud at startup and names the env var.

        Given: An unrecognised checkpoint mode token.
        When: ``resolve_checkpoint_mode`` validates it.
        Then: It raises ``ValueError`` naming ``PNL_SNAPSHOTTER_CHECKPOINT_MODE``.
        """
        with pytest.raises(ValueError, match=CHECKPOINT_MODE_ENV_VAR):
            resolve_checkpoint_mode(raw)


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
