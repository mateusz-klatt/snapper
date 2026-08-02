"""Enforce the delegate agent-plane import boundary.

Agent-plane code under ``integrations/snapper-delegate/src/snapper_delegate/``
must never reach the message bus or the ORM/data layer, and may touch first
party ``snapper.*`` only through a tiny per-file allowlist. Direct ``zmq`` or
``snapper.messaging`` access would bypass the authenticated wake path; direct
``snapper.data`` access would bypass caps, scope grants, and audit entirely.

This is a development-time architectural honesty check on first-party code, not
a runtime control against a hostile model. It fails closed on files it cannot
parse and on a missing source root, and reports each violation as
``path:line:module``.
"""

import ast
import sys
from pathlib import Path
from typing import Final

SCAN_ROOT: Final[str] = "integrations/snapper-delegate/src/snapper_delegate"

FORBIDDEN_ROOTS: Final[tuple[tuple[str, ...], ...]] = (
    ("zmq",),
    ("snapper", "messaging"),
    ("snapper", "data"),
)

_REGISTRATION_ALLOWLIST: Final[frozenset[str]] = frozenset(
    {
        "snapper.application.process_manager.models",
        "snapper.application.process_manager.registry",
        "snapper.application.process_manager.process_parameters",
        "snapper.core.json_types",
        "snapper.core.types",
    }
)
_DEFAULT_ALLOWLIST: Final[frozenset[str]] = frozenset({"snapper.core.json_types"})
_PER_FILE_ALLOWLIST: Final[dict[str, frozenset[str]]] = {"registration.py": _REGISTRATION_ALLOWLIST}


def allowlist_for(filename: str) -> frozenset[str]:
    """Return the allowed first-party modules for one agent-plane file.

    Args:
        filename: Base name of the delegate module being scanned.

    Returns:
        The frozenset of exact ``snapper.*`` module paths that file may import.
    """
    return _PER_FILE_ALLOWLIST.get(filename, _DEFAULT_ALLOWLIST)


def _components(dotted: str) -> tuple[str, ...]:
    """Split a dotted import path into its component tuple."""
    return tuple(dotted.split("."))


def _starts_with(path: tuple[str, ...], root: tuple[str, ...]) -> bool:
    """Return whether a component path begins with a forbidden root path."""
    return path[: len(root)] == root


def _is_forbidden(dotted: str) -> bool:
    """Return whether one dotted module path hits a forbidden root."""
    components = _components(dotted)
    return any(_starts_with(components, root) for root in FORBIDDEN_ROOTS)


def _forbidden_candidates(node: ast.Import | ast.ImportFrom) -> list[str]:
    """Collect every dotted path a statement could route to a forbidden root.

    ``from snapper import data`` imports the ``snapper.data`` submodule, so the
    module-plus-name combination is checked alongside the base module.

    Args:
        node: An ``import`` or ``from ... import`` AST node.

    Returns:
        Dotted module paths to test against the forbidden roots.
    """
    if isinstance(node, ast.Import):
        return [alias.name for alias in node.names]
    if node.module is None:
        return []
    candidates = [node.module]
    candidates.extend(f"{node.module}.{alias.name}" for alias in node.names)
    return candidates


def _first_party_modules(node: ast.Import | ast.ImportFrom) -> list[str]:
    """Collect the ``snapper.*`` module(s) a statement imports for allowlisting.

    Args:
        node: An ``import`` or ``from ... import`` AST node.

    Returns:
        Dotted ``snapper`` module paths subject to the per-file allowlist.
    """
    if isinstance(node, ast.Import):
        return [alias.name for alias in node.names if _components(alias.name)[0] == "snapper"]
    if node.module is not None and _components(node.module)[0] == "snapper":
        return [node.module]
    return []


def _import_violations(
    node: ast.Import | ast.ImportFrom,
    allowlist: frozenset[str],
) -> list[str]:
    """Return forbidden or non-allowlisted modules imported by one node."""
    violations = [
        candidate for candidate in _forbidden_candidates(node) if _is_forbidden(candidate)
    ]
    violations.extend(
        module
        for module in _first_party_modules(node)
        if not _is_forbidden(module) and module not in allowlist
    )
    return violations


def check_delegate_boundary(filepath: Path, allowlist: frozenset[str]) -> list[tuple[int, str]]:
    """Check one agent-plane file for boundary violations.

    Args:
        filepath: Python source file to scan.
        allowlist: Exact ``snapper.*`` modules this file may import.

    Returns:
        List of ``(line_number, module)`` violations. An unreadable or
        unparseable file yields a single synthetic violation so the caller
        fails closed rather than skipping it.
    """
    try:
        source = filepath.read_text(encoding="utf-8")
    except OSError:
        return [(0, "unreadable source (fail closed)")]
    try:
        tree = ast.parse(source, filename=str(filepath))
    except SyntaxError:
        return [(0, "unparseable source (fail closed)")]
    violations: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Import | ast.ImportFrom):
            continue
        violations.extend((node.lineno, module) for module in _import_violations(node, allowlist))
    return violations


def scan_files(root: Path) -> dict[Path, list[tuple[int, str]]]:
    """Scan every agent-plane file under the source root.

    Args:
        root: Project root directory.

    Returns:
        Mapping of file path to its violations. A missing source root yields a
        single synthetic entry so the caller fails closed.
    """
    search_root = root / SCAN_ROOT
    if not search_root.exists():
        return {search_root: [(0, "missing source root (fail closed)")]}
    results: dict[Path, list[tuple[int, str]]] = {}
    for python_file in sorted(search_root.rglob("*.py")):
        if "__pycache__" in python_file.parts:
            continue
        findings = check_delegate_boundary(python_file, allowlist_for(python_file.name))
        if findings:
            results[python_file] = findings
    return results


def run_scan(root: Path, strict_mode: bool = False) -> int:
    """Run the boundary scan and return the exit code.

    Args:
        root: Root directory of the project to scan.
        strict_mode: When True, return 1 if any violation is found.

    Returns:
        Exit code (0 for clean, 1 for violations in strict mode).
    """
    print("=" * 70)
    print("Delegate Boundary Scanner")
    print("=" * 70)
    print(f"\nScanning: {root / SCAN_ROOT}")
    print(f"Mode: {'STRICT (will fail on findings)' if strict_mode else 'Report only'}")
    results = scan_files(root)
    print("\n" + "-" * 70)
    total = 0
    if not results:
        print("  No delegate boundary violations found")
    else:
        for filepath, findings in sorted(results.items()):
            print(f"\n  {filepath}")
            for line_num, module in findings:
                print(f"     {filepath}:{line_num}:{module}")
                total += 1
    print("\n" + "=" * 70)
    print(f"SUMMARY: {total} violation(s)")
    print("=" * 70)
    if strict_mode and total:
        return 1
    return 0


def main() -> int:
    """Entry point for the delegate boundary checker.

    Returns:
        Process exit code from :func:`run_scan`.
    """
    strict_mode = "--strict" in sys.argv
    root = Path(__file__).resolve().parent.parent
    return run_scan(root, strict_mode=strict_mode)


if __name__ == "__main__":
    raise SystemExit(main())
