"""Validate that ``if __name__ == "__main__":`` blocks use the canonical form.

The only allowed body is ``raise SystemExit(main())``.  This keeps every
entry-point consistent, ensures exit-code propagation to the shell, and
avoids coverage exclusion surprises.
"""

import ast
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Final

DEFAULT_RELATIVE_ROOTS: Final[tuple[str, ...]] = (
    "src",
    "scripts",
    "proprietary/src",
    "integrations/snapper-delegate/src",
)
SKIP_DIRS: Final[set[str]] = {
    ".venv",
    "node_modules",
    "__pycache__",
    ".git",
    "dist",
    "build",
    "data",
}
CANONICAL_FORM: Final[str] = "raise SystemExit(main())"


def should_skip_path(path: Path) -> bool:
    """Return True when the path is in a skipped directory.

    Args:
        path: Candidate file path.

    Returns:
        True when any part of the path matches a skipped directory name.
    """
    return any(part in SKIP_DIRS for part in path.parts)


def iter_python_files(
    root: Path,
    relative_roots: Sequence[str] = DEFAULT_RELATIVE_ROOTS,
) -> list[Path]:
    """Collect project Python files under the configured roots.

    Args:
        root: Project root directory.
        relative_roots: Relative directories (from root) to scan.

    Returns:
        Sorted list of Python file paths.
    """
    python_files: list[Path] = []
    for relative_root in relative_roots:
        search_root = root / relative_root
        if not search_root.exists():
            continue
        for python_file in search_root.rglob("*.py"):
            if should_skip_path(python_file):
                continue
            python_files.append(python_file)
    return sorted(python_files)


def _is_main_guard(node: ast.If) -> bool:
    """Return True when the If node is ``if __name__ == "__main__":``."""
    test = node.test
    if not isinstance(test, ast.Compare):
        return False
    if len(test.ops) != 1 or not isinstance(test.ops[0], ast.Eq):
        return False
    left = test.left
    right = test.comparators[0]
    if isinstance(left, ast.Name) and left.id == "__name__":
        return isinstance(right, ast.Constant) and right.value == "__main__"
    if isinstance(right, ast.Name) and right.id == "__name__":
        return isinstance(left, ast.Constant) and left.value == "__main__"
    return False


def _is_raise_system_exit_main(stmt: ast.stmt) -> bool:
    """Return True when the statement is ``raise SystemExit(main())``."""
    if not isinstance(stmt, ast.Raise):
        return False
    exc = stmt.exc
    if not isinstance(exc, ast.Call):
        return False
    func = exc.func
    if not (isinstance(func, ast.Name) and func.id == "SystemExit"):
        return False
    if len(exc.args) != 1 or exc.keywords:
        return False
    arg = exc.args[0]
    if not isinstance(arg, ast.Call):
        return False
    return (
        isinstance(arg.func, ast.Name)
        and arg.func.id == "main"
        and not arg.args
        and not arg.keywords
    )


def check_main_guard(filepath: Path) -> list[tuple[int, str]]:
    """Check ``if __name__`` blocks in a single file.

    Args:
        filepath: Python source file to check.

    Returns:
        List of (line_number, description) for each violation.
    """
    try:
        source = filepath.read_text(encoding="utf-8")
    except OSError:
        return []
    try:
        tree = ast.parse(source, filename=str(filepath))
    except SyntaxError:
        return []
    violations: list[tuple[int, str]] = []
    for node in ast.iter_child_nodes(tree):
        if not isinstance(node, ast.If):
            continue
        if not _is_main_guard(node):
            continue
        if node.orelse:
            violations.append((node.lineno, "main guard must not have an else branch"))
            continue
        if len(node.body) != 1:
            violations.append((node.lineno, f"main guard body must be exactly `{CANONICAL_FORM}`"))
            continue
        if not _is_raise_system_exit_main(node.body[0]):
            violations.append((node.lineno, f"main guard body must be `{CANONICAL_FORM}`"))
    return violations


def scan_files(
    root: Path,
    relative_roots: Sequence[str] = DEFAULT_RELATIVE_ROOTS,
) -> dict[Path, list[tuple[int, str]]]:
    """Scan all Python files for main guard violations.

    Args:
        root: Project root directory.
        relative_roots: Relative directories (from root) to scan.

    Returns:
        Mapping of file paths to violation findings.
    """
    results: dict[Path, list[tuple[int, str]]] = {}
    for python_file in iter_python_files(root, relative_roots):
        findings = check_main_guard(python_file)
        if findings:
            results[python_file] = findings
    return results


def run_scan(
    root: Path,
    strict_mode: bool = False,
    relative_roots: Sequence[str] = DEFAULT_RELATIVE_ROOTS,
) -> int:
    """Run the main guard scan and return exit code.

    Args:
        root: Root directory of the project to scan.
        strict_mode: If True, return exit code 1 when violations are found.
        relative_roots: Relative directories (from root) to scan.

    Returns:
        Exit code (0 for success, 1 for failure in strict mode with findings).
    """
    print("=" * 70)
    print("Main Guard Scanner")
    print("=" * 70)
    print(f"\nScanning: {root}")
    print(f"Mode: {'STRICT (will fail on findings)' if strict_mode else 'Report only'}")
    print(f"Required form: {CANONICAL_FORM}")
    results = scan_files(root, relative_roots)
    print("\n" + "-" * 70)
    print("PYTHON FILES (.py)")
    print("-" * 70)
    total = 0
    if not results:
        print("  No main guard violations found")
    else:
        for filepath, findings in sorted(results.items()):
            rel_path = filepath.relative_to(root)
            print(f"\n  {rel_path}")
            for line_num, description in findings:
                print(f"     L{line_num}: {description}")
                total += 1
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"\n  Main guard violations: {total}")
    if total > 0:
        print(f"\nAll `if __name__` blocks must use: {CANONICAL_FORM}")
        if strict_mode:
            print("\nSTRICT MODE: Failing due to violations found.")
            return 1
    else:
        print("\nAll main guards are canonical. Clean codebase!")
    return 0


def main() -> int:
    """Entry point for check_main_guard script.

    Returns:
        Exit code from the scan operation.
    """
    strict_mode = "--strict" in sys.argv
    script_dir = Path(__file__).parent
    root = script_dir.parent
    return run_scan(root, strict_mode)


if __name__ == "__main__":
    raise SystemExit(main())
