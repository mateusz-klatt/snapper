"""Scan __init__.py files to enforce docstring-only policy.

This project requires that ``__init__.py`` files contain at most a single
module-level docstring.  No imports, assignments, class or function definitions,
or any other executable code is allowed.  This script enforces that rule by
parsing every ``__init__.py`` with the ``ast`` module and flagging violations.
"""

import ast
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Final

DEFAULT_RELATIVE_ROOTS: Final[tuple[str, ...]] = (
    "src",
    "tests",
    "scripts",
    "proprietary/src",
    "proprietary/tests",
)
SKIP_DIRS: Final[set[str]] = {
    ".venv",
    "node_modules",
    "__pycache__",
    ".git",
    "dist",
    "build",
    "coverage",
    ".pytest_cache",
    "data",
    "ios",
    "notebooks",
}

_NODE_LABELS: Final[dict[type[ast.stmt], str]] = {
    ast.Import: "import",
    ast.ImportFrom: "import",
    ast.Assign: "assignment",
    ast.AnnAssign: "assignment",
    ast.AugAssign: "assignment",
    ast.FunctionDef: "function definition",
    ast.AsyncFunctionDef: "async function definition",
    ast.ClassDef: "class definition",
    ast.If: "if statement",
    ast.For: "for loop",
    ast.While: "while loop",
    ast.Try: "try block",
}


def should_skip_path(path: Path) -> bool:
    """Return True when the path is in a skipped directory.

    Args:
        path: Candidate file path.

    Returns:
        True when any part of the path matches a skipped directory name.
    """
    return any(part in SKIP_DIRS for part in path.parts)


def iter_init_files(
    root: Path,
    relative_roots: Sequence[str] = DEFAULT_RELATIVE_ROOTS,
) -> list[Path]:
    """Collect ``__init__.py`` files under the configured roots.

    Args:
        root: Project root directory.
        relative_roots: Relative directories (from root) to scan.

    Returns:
        Sorted list of ``__init__.py`` file paths.
    """
    init_files: list[Path] = []
    for relative_root in relative_roots:
        search_root = root / relative_root
        if not search_root.exists():
            continue
        for init_file in search_root.rglob("__init__.py"):
            if should_skip_path(init_file):
                continue
            init_files.append(init_file)
    return sorted(init_files)


def _node_label(node: ast.stmt) -> str:
    """Return a human-readable label for an AST statement node.

    Args:
        node: An AST statement node.

    Returns:
        Descriptive label string.
    """
    return _NODE_LABELS.get(type(node), type(node).__name__)


def _is_docstring_only(body: list[ast.stmt]) -> bool:
    """Return True when body contains exactly one string-constant expression.

    Args:
        body: List of AST statement nodes from a module body.

    Returns:
        True when the body is a single docstring expression.
    """
    if len(body) != 1:
        return False
    node = body[0]
    return (
        isinstance(node, ast.Expr)
        and isinstance(node.value, ast.Constant)
        and isinstance(node.value.value, str)
    )


def check_init_file(filepath: Path) -> list[tuple[int, str]]:
    """Check a single ``__init__.py`` for non-docstring content.

    Args:
        filepath: Path to the ``__init__.py`` file.

    Returns:
        List of ``(line_number, description)`` tuples for each violation.
    """
    try:
        source = filepath.read_text(encoding="utf-8")
    except OSError:
        return []
    try:
        tree = ast.parse(source, filename=str(filepath))
    except SyntaxError:
        return []
    if not tree.body or _is_docstring_only(tree.body):
        return []
    violations: list[tuple[int, str]] = []
    for node in tree.body:
        if (
            isinstance(node, ast.Expr)
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
        ):
            continue
        violations.append((node.lineno, _node_label(node)))
    return violations


def scan_init_files(
    root: Path,
    relative_roots: Sequence[str] = DEFAULT_RELATIVE_ROOTS,
) -> dict[Path, list[tuple[int, str]]]:
    """Scan ``__init__.py`` files for violations.

    Args:
        root: Project root directory.
        relative_roots: Relative directories (from root) to scan.

    Returns:
        Mapping of file paths to violation findings.
    """
    results: dict[Path, list[tuple[int, str]]] = {}
    for init_file in iter_init_files(root, relative_roots):
        findings = check_init_file(init_file)
        if findings:
            results[init_file] = findings
    return results


def print_results(
    results: dict[Path, list[tuple[int, str]]],
    root: Path,
) -> int:
    """Print scan results and return total count of violations.

    Args:
        results: Dictionary mapping file paths to their violations.
        root: Root directory for computing relative paths.

    Returns:
        Total count of violations.
    """
    if not results:
        print("  No __init__.py violations found")
        return 0
    total = 0
    for filepath, findings in sorted(results.items()):
        rel_path = filepath.relative_to(root)
        print(f"\n  {rel_path}")
        for line_num, description in findings:
            print(f"     L{line_num}: {description}")
            total += 1
    return total


def run_scan(
    root: Path,
    strict_mode: bool = False,
    relative_roots: Sequence[str] = DEFAULT_RELATIVE_ROOTS,
) -> int:
    """Run the __init__.py scan and return exit code.

    Args:
        root: Root directory of the project to scan.
        strict_mode: If True, return exit code 1 when violations are found.
        relative_roots: Relative directories (from root) to scan.

    Returns:
        Exit code (0 for success, 1 for failure in strict mode with findings).
    """
    print("=" * 70)
    print("Init File Scanner")
    print("=" * 70)
    print(f"\nScanning: {root}")
    print(f"Mode: {'STRICT (will fail on findings)' if strict_mode else 'Report only'}")
    results = scan_init_files(root, relative_roots)
    print("\n" + "-" * 70)
    print("__init__.py FILES")
    print("-" * 70)
    total = print_results(results, root)
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"\n  Init file violations: {total}")
    if total > 0:
        print("\nFound __init__.py files with code. They must contain only a docstring.")
        if strict_mode:
            print("\nSTRICT MODE: Failing due to violations found.")
            return 1
    else:
        print("\nAll __init__.py files are clean. Docstring-only policy enforced!")
    return 0


def main() -> int:
    """Entry point for check_init_files script.

    Returns:
        Exit code from the scan operation.
    """
    strict_mode = "--strict" in sys.argv
    script_dir = Path(__file__).parent
    root = script_dir.parent
    return run_scan(root, strict_mode)


if __name__ == "__main__":
    raise SystemExit(main())
