"""Scan Python source files for forbidden in-place mutations on temporal tables.

The bitemporal rule allows only one UPDATE pattern on temporal tables: setting
``known_to`` to close the current version.  All other attribute mutations must
use the close-and-insert pattern.  Physical DELETE is also forbidden.  This
script enforces those constraints by regex-scanning source files and flagging
violations.

A secondary check detects ``KNOWN_TO_MAX`` used in SELECT/WHERE contexts.
Active-row queries should use the ``timestamp <= t, known_to > t`` range
pattern instead of comparing ``known_to`` to the sentinel value.
"""

import re
import sys
from pathlib import Path
from typing import Final

SCAN_ROOT: Final[str] = "src/snapper"

SKIP_DIRS: Final[set[str]] = {
    "__pycache__",
    "migrations",
}

WHITELIST_FILES: Final[set[str]] = {
    "models.py",
    "repository.py",
}

Violation = tuple[int, str, str]

FORBIDDEN_ATTR_PATTERNS: Final[list[tuple[re.Pattern[str], str]]] = [
    (re.compile(r"\.\bvalue\s*=(?!=)"), "Setting.value"),
    (re.compile(r"\.\bpassword_hash\s*=(?!=)"), "User.password_hash"),
    (re.compile(r"\.\bemail\s*=(?!=)"), "User.email"),
    (re.compile(r"\.\brole\s*=(?!=)"), "User.role"),
    (re.compile(r"\.\bis_active\s*=(?!=)"), "User.is_active"),
    (re.compile(r"\.\bexchange_symbol\s*=(?!=)"), "SymbolAlias.exchange_symbol"),
    (re.compile(r"\.\bcan_trade\s*=(?!=)"), "SymbolExchangeCapability.can_trade"),
    (re.compile(r"\.\bcan_market_data\s*=(?!=)"), "SymbolExchangeCapability.can_market_data"),
    (re.compile(r"\.\bsource\s*=(?!=)"), "SymbolExchangeCapability.source"),
    (re.compile(r"\.\breason\s*=(?!=)"), "Signal.reason / SymbolExchangeCapability.reason"),
    (re.compile(r"\.\bbase\s*=(?!=)"), "Instrument.base / Symbol.base"),
    (re.compile(r"\.\bquote\s*=(?!=)"), "Instrument.quote / Symbol.quote"),
    (re.compile(r"\.\basset_type\s*=(?!=)"), "Symbol.asset_type"),
]

DELETE_PATTERN: Final[re.Pattern[str]] = re.compile(r"session\.delete\(")

KNOWN_TO_MAX_SELECT_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"[!=]=\s*KNOWN_TO_MAX|KNOWN_TO_MAX\s*[!=]="
)

KNOWN_TO_MAX_INSERT_WHITELIST: Final[list[re.Pattern[str]]] = [
    re.compile(r'\["known_to"\]\s*=\s*KNOWN_TO_MAX'),
    re.compile(r'"known_to":\s*KNOWN_TO_MAX'),
    re.compile(r'\["known_to"\]\s*=\s*_KNOWN_TO_MAX'),
    re.compile(r'"known_to":\s*_KNOWN_TO_MAX'),
    re.compile(r"known_to[^#]*?default[^#]*?KNOWN_TO_MAX"),
    re.compile(r"KNOWN_TO_MAX[^#]*?default"),
]

WHITELIST_LINE_PATTERNS: Final[list[re.Pattern[str]]] = [
    re.compile(r"Mapped\["),
    re.compile(r"mapped_column\("),
    re.compile(r"\[.*\]\s*="),
    re.compile(r"known_to"),
    re.compile(r"default="),
    re.compile(r"server_default="),
    re.compile(r"^\s*def\b"),
    re.compile(r"^\s*class\b"),
    re.compile(r'"""'),
]


def _path_suffix(path: Path) -> str:
    """Return the POSIX suffix fragment for path-specific whitelist matching.

    Args:
        path: File path to extract suffix from.

    Returns:
        Slash-separated suffix string suitable for dict lookup.
    """
    return path.as_posix()


def should_skip_path(path: Path) -> bool:
    """Return True when the path is in a skipped directory or whitelisted file.

    Args:
        path: Candidate file path.

    Returns:
        True when the file name is in WHITELIST_FILES or any part of the path
        matches a SKIP_DIRS entry.
    """
    if path.name in WHITELIST_FILES:
        return True
    return any(part in SKIP_DIRS for part in path.parts)


def iter_python_files(root: Path) -> list[Path]:
    """Collect Python files under the scan root.

    Args:
        root: Project root directory.

    Returns:
        Sorted list of Python file paths under the scan root.
    """
    search_root = root / SCAN_ROOT
    if not search_root.exists():
        return []
    python_files: list[Path] = []
    for python_file in search_root.rglob("*.py"):
        if should_skip_path(python_file):
            continue
        python_files.append(python_file)
    return sorted(python_files)


def _is_whitelisted_line(line: str, filepath: Path | None = None) -> bool:
    """Return True when the line matches a whitelisted pattern.

    Checks global whitelist patterns only. The ``filepath`` parameter
    is reserved for future path-specific whitelist extensions.

    Args:
        line: Source code line to check.
        filepath: Optional file path (reserved for future use).

    Returns:
        True when the line is safe to ignore.
    """
    _ = filepath
    return any(pattern.search(line) for pattern in WHITELIST_LINE_PATTERNS)


def _is_known_to_max_select_whitelisted(line: str) -> bool:
    """Return True when a KNOWN_TO_MAX usage is in an INSERT/default context.

    Args:
        line: Source code line to check.

    Returns:
        True when the line is a safe INSERT-default usage of KNOWN_TO_MAX.
    """
    return any(p.search(line) for p in KNOWN_TO_MAX_INSERT_WHITELIST)


def _check_line(
    line_num: int,
    line: str,
    filepath: Path,
    violations: list[Violation],
) -> None:
    """Check a single source line for forbidden temporal patterns.

    The KNOWN_TO_MAX SELECT check runs independently of the attribute
    mutation whitelist because the ``known_to`` whitelist pattern is
    meant for close-operation assignments, not SELECT comparisons.

    Args:
        line_num: 1-based line number in the file.
        line: Raw source line text.
        filepath: Path to the file being checked.
        violations: Accumulator list for discovered violations.
    """
    if KNOWN_TO_MAX_SELECT_PATTERN.search(line) and not _is_known_to_max_select_whitelisted(line):
        violations.append((line_num, "KNOWN_TO_MAX in SELECT/WHERE", line.strip()))
    if _is_whitelisted_line(line, filepath):
        return
    for pattern, description in FORBIDDEN_ATTR_PATTERNS:
        if pattern.search(line):
            violations.append((line_num, description, line.strip()))
            break
    if DELETE_PATTERN.search(line):
        violations.append((line_num, "session.delete()", line.strip()))


def check_file(filepath: Path) -> list[Violation]:
    """Check a single Python file for forbidden temporal mutation patterns.

    Also detects ``KNOWN_TO_MAX`` used in SELECT/WHERE contexts (equality
    or inequality comparisons) that should use the temporal range pattern.

    Args:
        filepath: Path to the Python file.

    Returns:
        List of (line_number, description, line_text) tuples for each violation.
    """
    try:
        lines = filepath.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    violations: list[Violation] = []
    for line_num, line in enumerate(lines, start=1):
        _check_line(line_num, line, filepath, violations)
    return violations


def scan_files(root: Path) -> dict[Path, list[Violation]]:
    """Scan source files for forbidden temporal mutation patterns.

    Args:
        root: Project root directory.

    Returns:
        Mapping of file paths to violation findings.
    """
    results: dict[Path, list[Violation]] = {}
    for python_file in iter_python_files(root):
        findings = check_file(python_file)
        if findings:
            results[python_file] = findings
    return results


def print_results(
    results: dict[Path, list[Violation]],
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
        print("  No temporal mutation violations found")
        return 0
    total = 0
    for filepath, findings in sorted(results.items()):
        rel_path = filepath.relative_to(root)
        print(f"\n  {rel_path}")
        for line_num, description, line_text in findings:
            display_line = line_text[:80] + "..." if len(line_text) > 80 else line_text
            print(f"     L{line_num}: [{description}] {display_line}")
            total += 1
    return total


def run_scan(
    root: Path,
    strict_mode: bool = False,
) -> int:
    """Run the temporal mutation scan and return exit code.

    Args:
        root: Root directory of the project to scan.
        strict_mode: If True, return exit code 1 when violations are found.

    Returns:
        Exit code (0 for success, 1 for failure in strict mode with findings).
    """
    print("=" * 70)
    print("Temporal Mutation Scanner")
    print("=" * 70)
    print(f"\nScanning: {root}")
    print(f"Mode: {'STRICT (will fail on findings)' if strict_mode else 'Report only'}")
    results = scan_files(root)
    print("\n" + "-" * 70)
    print("TEMPORAL MUTATION CHECKS")
    print("-" * 70)
    total = print_results(results, root)
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"\n  Temporal mutation violations: {total}")
    if total > 0:
        print("\nForbidden in-place mutations found. Use close-and-insert instead.")
        if strict_mode:
            print("\nSTRICT MODE: Failing due to violations found.")
            return 1
    else:
        print("\nNo forbidden temporal mutations found.")
    return 0


def main() -> int:
    """Entry point for check_temporal_mutations script.

    Returns:
        Exit code from the scan operation.
    """
    strict_mode = "--strict" in sys.argv
    script_dir = Path(__file__).parent
    root = script_dir.parent
    return run_scan(root, strict_mode)


if __name__ == "__main__":
    raise SystemExit(main())
