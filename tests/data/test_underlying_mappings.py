"""Tests for underlying mapping description normalization."""

import pytest
from pydantic import ValidationError

from snapper.application.updaters.underlying_updater import UnderlyingDefinition
from snapper.application.updaters.underlying_updater import UnderlyingMappingConfig


def _config_with_description(description: object) -> UnderlyingMappingConfig:
    """Build a minimal mapping config carrying one description value."""
    return UnderlyingMappingConfig.model_validate(
        {
            "underlyings": [
                {
                    "ticker": "SPX",
                    "name": "S&P 500",
                    "asset_class": "index",
                    "description": description,
                    "patterns": [],
                }
            ]
        }
    )


def test_legacy_string_description_normalized_to_en_dict() -> None:
    """Legacy scalar descriptions become English locale maps."""
    config = _config_with_description("S&P 500 exposure.")
    assert config.underlyings[0].description == {"en": "S&P 500 exposure."}


def test_dict_description_preserved() -> None:
    """Locale-keyed descriptions validate without changing content."""
    config = _config_with_description(
        {
            "en": "Gold exposure.",
            "pl": "Ekspozycja na zloto.",
        }
    )
    assert config.underlyings[0].description == {
        "en": "Gold exposure.",
        "pl": "Ekspozycja na zloto.",
    }


def test_description_null_remains_null() -> None:
    """Null descriptions stay absent."""
    config = _config_with_description(None)
    assert config.underlyings[0].description is None


def test_normalize_description_passes_through_non_dict_input() -> None:
    """Non-dict raw input falls through normalization to standard validation.

    When ``model_validate`` receives a non-mapping value, the ``mode="before"``
    validator must not rewrite it; the canonical Pydantic type-mismatch error
    must surface from downstream field validation instead of a custom one.
    """
    with pytest.raises(ValidationError) as exc_info:
        UnderlyingDefinition.model_validate([1, 2, 3])
    assert "dict" in str(exc_info.value).lower() or "model" in str(exc_info.value).lower()
