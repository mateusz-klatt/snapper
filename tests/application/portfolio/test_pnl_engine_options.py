"""Tests for the P&L engine valuation options resolver and single-producer AST guard."""

import ast
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from snapper.application.portfolio import pnl_engine_options
from snapper.application.portfolio.pnl_engine_options import PNL_ENGINE_OPTIONS_OFF
from snapper.application.portfolio.pnl_engine_options import PnlEngineOptions
from snapper.application.portfolio.pnl_engine_options import pnl_engine_options_for

_REPO_ROOT = Path(__file__).resolve().parents[3]
_SRC_ROOT = _REPO_ROOT / "src"
_RESOLVER_PATH = _SRC_ROOT / "snapper" / "application" / "portfolio" / "pnl_engine_options.py"
_OPTIONS_MODULE = "snapper.application.portfolio.pnl_engine_options"


def _imported_option_aliases(tree: ast.Module) -> set[str]:
    """Collect local aliases bound to ``PnlEngineOptions`` or ``_engine_options_table``.

    Args:
        tree: Parsed module AST.

    Returns:
        Set of local names aliasing ``PnlEngineOptions``.
    """
    aliases: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == _OPTIONS_MODULE:
            for alias in node.names:
                if alias.name == "PnlEngineOptions":
                    aliases.add(alias.asname or alias.name)
    return aliases


def _imported_table_aliases(tree: ast.Module) -> set[str]:
    """Collect local aliases bound to ``_engine_options_table``.

    Args:
        tree: Parsed module AST.

    Returns:
        Set of local names aliasing ``_engine_options_table``.
    """
    aliases: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == _OPTIONS_MODULE:
            for alias in node.names:
                if alias.name == "_engine_options_table":
                    aliases.add(alias.asname or alias.name)
    return aliases


class _EngineOptionsViolationScanner(ast.NodeVisitor):
    """AST visitor detecting direct ``PnlEngineOptions`` construction or seam calls."""

    def __init__(self, option_aliases: set[str], table_aliases: set[str]) -> None:
        """Initialize scanner with resolved module import aliases.

        Args:
            option_aliases: Local aliases bound to ``PnlEngineOptions``.
            table_aliases: Local aliases bound to ``_engine_options_table``.
        """
        self._option_names = {"PnlEngineOptions", *option_aliases}
        self._table_names = {"_engine_options_table", *table_aliases}
        self.violations: list[str] = []

    def visit_Call(self, node: ast.Call) -> None:
        """Record any call constructing ``PnlEngineOptions`` or invoking the test seam.

        Args:
            node: Call expression AST node.
        """
        target = node.func
        if isinstance(target, ast.Name) and target.id in self._option_names:
            self.violations.append(f"line {node.lineno}: direct PnlEngineOptions({target.id})")
        elif isinstance(target, ast.Attribute) and target.attr == "PnlEngineOptions":
            self.violations.append(f"line {node.lineno}: attribute PnlEngineOptions")
        elif isinstance(target, ast.Name) and target.id in self._table_names:
            self.violations.append(f"line {node.lineno}: direct _engine_options_table({target.id})")
        elif isinstance(target, ast.Attribute) and target.attr == "_engine_options_table":
            self.violations.append(f"line {node.lineno}: attribute _engine_options_table")
        self.generic_visit(node)


def _find_engine_options_violations(source: str) -> list[str]:
    """Scan Python source for disallowed ``PnlEngineOptions`` constructions or seam calls.

    Args:
        source: Python source text to parse and inspect.

    Returns:
        Human-readable violation descriptions with line numbers.
    """
    tree = ast.parse(source)
    scanner = _EngineOptionsViolationScanner(
        _imported_option_aliases(tree),
        _imported_table_aliases(tree),
    )
    scanner.visit(tree)
    return scanner.violations


def _find_private_table_references(source: str) -> list[str]:
    """Find every reference to the private options table or its accessor.

    Args:
        source: Python source text to parse and inspect.

    Returns:
        Human-readable reference descriptions with line numbers.
    """
    private_names = {"_engine_options_table", "_ENGINE_OPTIONS_BY_CALC_VERSION"}
    violations: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Name) and node.id in private_names:
            violations.append(f"line {node.lineno}: private table reference {node.id}")
        elif isinstance(node, ast.Attribute) and node.attr in private_names:
            violations.append(f"line {node.lineno}: private table attribute {node.attr}")
        elif isinstance(node, ast.ImportFrom) and node.module == _OPTIONS_MODULE:
            violations.extend(
                f"line {node.lineno}: private table import {alias.name}"
                for alias in node.names
                if alias.name in private_names
            )
    return violations


def test_pnl_engine_options_for_5c5_returns_expected_flags() -> None:
    """Resolve the registered ``"5C.5"`` calculation version with capture disabled.

    Given: The explicit all-off options value for non-pointwise reads.
    When: ``pnl_engine_options_for("5C.5")`` is called.
    Then: It returns the shared ``PNL_ENGINE_OPTIONS_OFF`` instance.
    """
    options = pnl_engine_options_for("5C.5")
    assert options is PNL_ENGINE_OPTIONS_OFF
    assert options == PnlEngineOptions(
        capture_permitted=False,
        compensated_unrealized_fallback=False,
        request_all_conversion_minutes=False,
    )


def test_pnl_engine_options_for_5c6_returns_expected_flags() -> None:
    """Resolve ``"5C.6"`` with capture permitted and valuation completeness flags enabled.

    Given: The ``"5C.6"`` calculation version with checkpoint capture permitted.
    When: ``pnl_engine_options_for("5C.6")`` is called.
    Then: It returns ``PnlEngineOptions(capture_permitted=True,
        compensated_unrealized_fallback=True, request_all_conversion_minutes=True)``.
    """
    assert pnl_engine_options_for("5C.6") == PnlEngineOptions(
        capture_permitted=True,
        compensated_unrealized_fallback=True,
        request_all_conversion_minutes=True,
    )


@pytest.mark.parametrize("calc_version", ["", "5C.4", "5C.7", "5A.16"])
def test_pnl_engine_options_for_unknown_version_raises(calc_version: str) -> None:
    """Reject unknown calculation versions rather than guessing engine flags.

    Given: An unregistered calculation version string.
    When: ``pnl_engine_options_for`` is called with that version.
    Then: A ``ValueError`` is raised naming the unsupported version.
    """
    with pytest.raises(ValueError, match="Unsupported"):
        pnl_engine_options_for(calc_version)


def test_pnl_engine_options_is_frozen_immutable() -> None:
    """Enforce frozen immutability on resolved ``PnlEngineOptions`` instances.

    Given: The ``PnlEngineOptions`` returned for ``"5C.5"``.
    When: A caller attempts to mutate ``capture_permitted`` in place.
    Then: ``FrozenInstanceError`` is raised.
    """
    options = pnl_engine_options_for("5C.5")
    with pytest.raises(FrozenInstanceError):
        options.capture_permitted = True


def test_pnl_engine_options_table_seam_supports_monkeypatching(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Route resolver lookups through ``_engine_options_table`` for test overrides.

    Given: ``_engine_options_table`` monkeypatched with a synthetic candidate version.
    When: ``pnl_engine_options_for`` resolves that synthetic version and ``"5C.5"``.
    Then: The synthetic entry is returned and unregistered versions still raise ``ValueError``.
    """
    candidate = PnlEngineOptions(
        capture_permitted=False,
        compensated_unrealized_fallback=True,
        request_all_conversion_minutes=False,
    )
    monkeypatch.setattr(
        pnl_engine_options,
        "_engine_options_table",
        lambda: {"5C.6": candidate},
    )
    assert pnl_engine_options_for("5C.6") == candidate
    with pytest.raises(ValueError, match="Unsupported"):
        pnl_engine_options_for("5C.5")


def test_ast_guard_forbids_direct_construction_outside_resolver() -> None:
    """Ensure every ``src/`` file respects ``pnl_engine_options.py`` as single producer.

    Given: Every Python source file under ``src/`` except ``pnl_engine_options.py``.
    When: The AST guard scans for direct construction and private table references.
    Then: Zero violations are found across the entire source tree.
    """
    assert _RESOLVER_PATH.is_file()
    violations: dict[str, list[str]] = {}
    for py_file in sorted(_SRC_ROOT.rglob("*.py")):
        if py_file == _RESOLVER_PATH:
            continue
        source = py_file.read_text(encoding="utf-8")
        found = [
            *_find_engine_options_violations(source),
            *_find_private_table_references(source),
        ]
        if found:
            violations[str(py_file.relative_to(_REPO_ROOT))] = found
    assert violations == {}


def test_ast_guard_catches_planted_violations_and_allows_type_annotations() -> None:
    """Verify the AST guard catches direct, attribute, and aliased constructions.

    Given: Synthetic Python source snippets containing type annotations, resolver
        calls, direct construction, attribute construction, aliased construction,
        and a production call to ``_engine_options_table``.
    When: ``_find_engine_options_violations`` inspects each snippet.
    Then: Type annotations and ``pnl_engine_options_for`` pass cleanly while every
        planted violation is detected.
    """
    allowed = """
from snapper.application.portfolio.pnl_engine_options import (
    PnlEngineOptions,
    pnl_engine_options_for,
)

def resolve(version: str) -> PnlEngineOptions:
    opts: PnlEngineOptions = pnl_engine_options_for(version)
    return opts
"""
    assert _find_engine_options_violations(allowed) == []

    planted = """
from snapper.application.portfolio import pnl_engine_options
from snapper.application.portfolio.pnl_engine_options import (
    PnlEngineOptions as OptsAlias,
    _engine_options_table as table_alias,
)

def rogue() -> None:
    first = PnlEngineOptions(True, False, False)
    second = pnl_engine_options.PnlEngineOptions(False, True, False)
    third = OptsAlias(False, False, True)
    fourth = table_alias()
    fifth = pnl_engine_options._engine_options_table()
    sixth = pnl_engine_options._ENGINE_OPTIONS_BY_CALC_VERSION
    return None
"""
    violations = _find_engine_options_violations(planted)
    assert len(violations) == 5
    references = _find_private_table_references(planted)
    assert len(references) == 3
    assert any("_ENGINE_OPTIONS_BY_CALC_VERSION" in item for item in references)
    assert any("_engine_options_table" in item for item in references)
