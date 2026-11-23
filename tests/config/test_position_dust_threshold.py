"""Tests for the position dust notional threshold setting."""

from decimal import Decimal

import pytest
from pydantic import ValidationError

from snapper.config.app import AppSettings
from snapper.config.bootstrap import DEFAULT_POSITION_DUST_NOTIONAL_THRESHOLD
from snapper.config.bootstrap import BootstrapSettingsLoader


def test_position_dust_threshold_defaults_to_half_a_quote_unit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The dust notional threshold ships at 0.50 quote-currency units.

    Given: no POSITION_DUST_NOTIONAL_THRESHOLD environment variable,
    When: bootstrap and application settings are loaded,
    Then: both expose Decimal('0.50').
    """
    monkeypatch.delenv("POSITION_DUST_NOTIONAL_THRESHOLD", raising=False)
    bootstrap = BootstrapSettingsLoader(_env_file=None)
    settings = AppSettings(bootstrap)
    assert bootstrap.position_dust_notional_threshold == DEFAULT_POSITION_DUST_NOTIONAL_THRESHOLD
    assert settings.position_dust_notional_threshold == Decimal("0.50")


def test_position_dust_threshold_reads_a_decimal_env_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Operators can replace the default with an exact decimal string.

    Given: POSITION_DUST_NOTIONAL_THRESHOLD=1.25,
    When: bootstrap and application settings are loaded,
    Then: both expose Decimal('1.25').
    """
    monkeypatch.setenv("POSITION_DUST_NOTIONAL_THRESHOLD", "1.25")
    bootstrap = BootstrapSettingsLoader(_env_file=None)
    settings = AppSettings(bootstrap)
    assert bootstrap.position_dust_notional_threshold == Decimal("1.25")
    assert settings.position_dust_notional_threshold == Decimal("1.25")


def test_position_dust_threshold_accepts_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    """Zero is the explicit way to disable the notional rule.

    Given: POSITION_DUST_NOTIONAL_THRESHOLD=0,
    When: bootstrap settings are loaded,
    Then: the threshold is Decimal('0').
    """
    monkeypatch.setenv("POSITION_DUST_NOTIONAL_THRESHOLD", "0")
    bootstrap = BootstrapSettingsLoader(_env_file=None)
    assert bootstrap.position_dust_notional_threshold == Decimal("0")


@pytest.mark.parametrize("raw", ["-0.01", "NaN", "Infinity", "not-a-number"])
def test_position_dust_threshold_rejects_unusable_values(
    monkeypatch: pytest.MonkeyPatch,
    raw: str,
) -> None:
    """Negative, non-finite, and non-decimal thresholds fail closed.

    Given: an unusable POSITION_DUST_NOTIONAL_THRESHOLD value,
    When: bootstrap settings are loaded,
    Then: validation fails.
    """
    monkeypatch.setenv("POSITION_DUST_NOTIONAL_THRESHOLD", raw)
    with pytest.raises(ValidationError):
        BootstrapSettingsLoader(_env_file=None)
