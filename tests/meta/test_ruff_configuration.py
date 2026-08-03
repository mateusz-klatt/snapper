"""Pin local Ruff enforcement that mirrors actionable Sonar findings."""

import tomllib
from pathlib import Path
from typing import cast

_REPO_ROOT = Path(__file__).resolve().parents[2]


def test_ruff_rejects_empty_pytest_decorator_parentheses() -> None:
    """Require argument-free pytest decorators to omit parentheses.

    Given: The repository-wide Ruff lint configuration,
    When: Its pytest-style selectors and conventions are inspected,
    Then: Empty fixture and mark parentheses remain rejected locally.
    """
    configuration = tomllib.loads((_REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    tool_configuration = cast(dict[str, object], configuration["tool"])
    ruff_configuration = cast(dict[str, object], tool_configuration["ruff"])
    lint_configuration = cast(dict[str, object], ruff_configuration["lint"])
    selected_rules = cast(list[str], lint_configuration["select"])
    pytest_style = cast(dict[str, object], lint_configuration["flake8-pytest-style"])

    assert {"PT001", "PT023"}.issubset(selected_rules)
    assert pytest_style == {
        "fixture-parentheses": False,
        "mark-parentheses": False,
    }
