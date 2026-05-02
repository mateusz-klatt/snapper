"""Scan Python source files for comment tokens.

This project treats docstrings as the canonical place for rationale and guidance.
This script enforces that by failing when Python `COMMENT` tokens are present.
Hashes inside strings and docstrings are allowed and are not treated as comments.
"""

import io
import sys
import tokenize
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
}


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


def find_comment_tokens(filepath: Path) -> list[tuple[int, str]]:
    """Return comment tokens found in a Python source file.

    Args:
        filepath: Path to the Python file.

    Returns:
        A list of (line_number, comment_text) tuples.
    """
    try:
        content = filepath.read_bytes()
    except OSError:
        return []
    findings: list[tuple[int, str]] = []
    try:
        for token in tokenize.tokenize(io.BytesIO(content).readline):
            if token.type != tokenize.COMMENT:
                continue
            findings.append((token.start[0], token.string))
    except tokenize.TokenError:
        return []
    return findings


def scan_python_files(
    root: Path,
    relative_roots: Sequence[str] = DEFAULT_RELATIVE_ROOTS,
) -> dict[Path, list[tuple[int, str]]]:
    """Scan project Python files for comment tokens.

    Args:
        root: Project root directory.
        relative_roots: Relative directories (from root) to scan.

    Returns:
        Mapping of file paths to comment findings.
    """
    results: dict[Path, list[tuple[int, str]]] = {}
    for python_file in iter_python_files(root, relative_roots):
        findings = find_comment_tokens(python_file)
        if findings:
            results[python_file] = findings
    return results


def print_results(
    results: dict[Path, list[tuple[int, str]]],
    root: Path,
    file_type: str = "Python",
) -> int:
    """Print scan results and return total count of findings.

    Args:
        results: Dictionary mapping file paths to their findings.
        root: Root directory for computing relative paths.
        file_type: Type of files being reported (for display purposes).

    Returns:
        Total count of comment findings.
    """
    if not results:
        print(f"  No {file_type} comments found")
        return 0
    total = 0
    for filepath, findings in sorted(results.items()):
        rel_path = filepath.relative_to(root)
        print(f"\n  {rel_path}")
        for line_num, comment_text in findings:
            display_comment = comment_text[:80] + "..." if len(comment_text) > 80 else comment_text
            print(f"     L{line_num}: {display_comment}")
            total += 1
    return total


def run_scan(
    root: Path,
    strict_mode: bool = False,
    relative_roots: Sequence[str] = DEFAULT_RELATIVE_ROOTS,
) -> int:
    """Run the no-comment scan and return exit code.

    Args:
        root: Root directory of the project to scan.
        strict_mode: If True, return exit code 1 when comments are found.
        relative_roots: Relative directories (from root) to scan.

    Returns:
        Exit code (0 for success, 1 for failure in strict mode with findings).
    """
    print("=" * 70)
    print("Python Comment Scanner")
    print("=" * 70)
    print(f"\nScanning: {root}")
    print(f"Mode: {'STRICT (will fail on findings)' if strict_mode else 'Report only'}")
    results = scan_python_files(root, relative_roots)
    print("\n" + "-" * 70)
    print("PYTHON FILES (.py)")
    print("-" * 70)
    total = print_results(results, root, "Python")
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"\n  Python comments: {total}")
    if total > 0:
        print("\nFound Python comments. Move rationale and guidance into docstrings.")
        if strict_mode:
            print("\nSTRICT MODE: Failing due to comments found.")
            return 1
    else:
        print("\nNo Python comments found. Clean docstring-first codebase!")
    return 0


def main() -> int:
    """Entry point for check_no_comments script.

    Returns:
        Exit code from the scan operation.
    """
    strict_mode = "--strict" in sys.argv
    script_dir = Path(__file__).parent
    root = script_dir.parent
    return run_scan(root, strict_mode)


if __name__ == "__main__":
    raise SystemExit(main())
