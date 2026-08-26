"""Tests for the Phase-5B snapshotter env-var contract module."""

import pytest

from snapper.application.portfolio.pnl_snapshotter_config import DEFAULT_INTERVAL_SECONDS
from snapper.application.portfolio.pnl_snapshotter_config import ENABLED_ENV_VAR
from snapper.application.portfolio.pnl_snapshotter_config import ENV_VARS
from snapper.application.portfolio.pnl_snapshotter_config import FX_SHADOW_PINNING_ENV_VAR
from snapper.application.portfolio.pnl_snapshotter_config import INTERVAL_ENV_VAR
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


class TestEnvVarsContract:
    """The exported allowlist names every snapshotter contract key."""

    def test_env_vars_lists_both_keys(self) -> None:
        """``ENV_VARS`` includes interval, snapshotting, and shadow pinning."""
        assert frozenset({INTERVAL_ENV_VAR, ENABLED_ENV_VAR, FX_SHADOW_PINNING_ENV_VAR}) == ENV_VARS
