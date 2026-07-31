"""Contract tests for the repository coverage boundary."""

import tomllib
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_coverage_omit_allowlist_is_exact() -> None:
    """The coverage boundary cannot grow without an explicit contract change.

    Given: The repository's authoritative coverage configuration,
    When: Its complete omit list is inspected,
    Then: Only empty package files and migrations are outside the
        unit-testable scope.
    """
    configuration = tomllib.loads((_PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert configuration["tool"]["coverage"]["run"]["omit"] == [
        "*/__init__.py",
        "src/snapper/data/migrations/*",
    ]
