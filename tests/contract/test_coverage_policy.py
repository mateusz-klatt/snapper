"""Contract tests for the repository coverage boundary."""

import tomllib
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_coverage_omit_is_not_configured() -> None:
    """The coverage boundary includes every configured production source.

    Given: The repository's authoritative coverage configuration,
    When: Its run configuration is inspected,
    Then: No source file can be omitted from coverage measurement.
    """
    configuration = tomllib.loads((_PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert "omit" not in configuration["tool"]["coverage"]["run"]
