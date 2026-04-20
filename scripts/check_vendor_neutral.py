"""Scan Python source files for vendor-specific names that leak into core.

Phase A §3.11 mandates that `src/snapper/` stays vendor-neutral so the MCP
surface cannot assume any single AI client (Claude Desktop, Cursor, Windsurf,
ChatGPT, Gemini, Copilot, ...).  Vendor-specific wrappers belong in adjacent
public repos (e.g. `integrations/snapper-mcp/`), not the core engine.

The check regex-scans every `*.py` file under `src/snapper/` for the terms
listed in `VENDOR_PATTERN`.  A line with a trailing `# vendor-neutral-ok`
comment is treated as an intentional, reviewed exemption.
"""

import io
import re
import sys
import tokenize
from pathlib import Path
from typing import Final

SCAN_ROOT: Final[str] = "src/snapper"

SKIP_DIRS: Final[set[str]] = {
    "__pycache__",
    "migrations",
}

Violation = tuple[int, str, str]

VENDOR_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(?i)"
    r"(?:"
    r"anthropic"
    r"|openai"
    r"|claude[._ \-]?(?:code|desktop|opus|sonnet|haiku)"
    r"|gemini[._ \-]?api"
    r"|copilot[._ \-]?api"
    r"|channel[._ \-]?plugin"
    r")"
)

ALLOWLIST_COMMENT: Final[str] = "vendor-neutral-ok"
"""Allowlist marker that must appear inside a real Python ``#`` comment.

A line is exempted only when :func:`_is_allowlisted` finds this string
inside a token the Python tokenizer classifies as
:data:`tokenize.COMMENT`. Substring-based checks were retired in the
R2 review fix-up because they exempted any line containing the text,
including string literals that happen to embed a ``#`` — e.g.
``NAME = "Claude Desktop # vendor-neutral-ok"`` — which let authors
silently bypass the scanner.
"""


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
        if any(part in SKIP_DIRS for part in python_file.parts):
            continue
        python_files.append(python_file)
    return sorted(python_files)


def _collect_allowlisted_lines(source: str) -> set[int]:
    """Return the line numbers whose marker sits inside a real ``#`` comment.

    The scanner tokenizes the *entire* file once so multi-line
    structures (triple-quoted docstrings, implicit line
    continuations, f-string spans) are classified correctly.
    Per-line tokenization was previously tried and retired: a line
    like ``Claude Desktop # vendor-neutral-ok`` sitting *inside* a
    docstring tokenizes in isolation as a COMMENT even though at the
    file level it is STRING, which let authors bypass the scanner
    by hiding the vendor reference inside a triple-quoted block.

    Tokenizer errors on the whole file (``tokenize.TokenError`` or
    ``SyntaxError``) fall through to an empty set so nothing gets
    exempted — the allowlist fails closed.

    Args:
        source: Full file text.

    Returns:
        1-based line numbers where a :data:`tokenize.COMMENT` token
        contains :data:`ALLOWLIST_COMMENT`.
    """
    try:
        tokens = tokenize.generate_tokens(io.StringIO(source).readline)
        allowlisted: set[int] = set()
        for tok in tokens:
            if tok.type == tokenize.COMMENT and ALLOWLIST_COMMENT in tok.string:
                allowlisted.add(tok.start[0])
        return allowlisted
    except (tokenize.TokenError, SyntaxError):
        return set()


def check_file(filepath: Path) -> list[Violation]:
    """Scan a single file and return vendor-string violations.

    The function reads the file once, derives the set of lines whose
    trailing ``#`` comment carries the allowlist marker via
    :func:`_collect_allowlisted_lines` (file-level tokenization so a
    docstring body cannot bypass), then regex-scans every line for
    vendor names. Lines in the allowlist set are skipped.

    Args:
        filepath: Path to the Python source file.

    Returns:
        List of (line_number, matched_token, stripped_line) tuples.
    """
    try:
        source = filepath.read_text(encoding="utf-8")
    except OSError:
        return []
    allowlisted_lines = _collect_allowlisted_lines(source)
    violations: list[Violation] = []
    for line_num, line in enumerate(source.splitlines(), start=1):
        match = VENDOR_PATTERN.search(line)
        if match is None:
            continue
        if line_num in allowlisted_lines:
            continue
        violations.append((line_num, match.group(0), line.strip()))
    return violations


def scan_files(root: Path) -> dict[Path, list[Violation]]:
    """Scan every source file and collect vendor-string violations.

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
    """Print violation findings and return the total count.

    Args:
        results: Dictionary mapping file paths to their violations.
        root: Root directory for computing relative paths.

    Returns:
        Total count of violations across all files.
    """
    if not results:
        print("  No vendor-specific names found")
        return 0
    total = 0
    for filepath, findings in sorted(results.items()):
        rel_path = filepath.relative_to(root)
        print(f"\n  {rel_path}")
        for line_num, token, line_text in findings:
            display_line = line_text[:80] + "..." if len(line_text) > 80 else line_text
            print(f"     L{line_num}: [{token}] {display_line}")
            total += 1
    return total


def run_scan(root: Path, strict_mode: bool) -> int:
    """Execute the vendor-neutrality scan.

    Args:
        root: Project root directory.
        strict_mode: If True, return exit code 1 when findings exist.

    Returns:
        Exit code (0 on clean scan, 1 in strict mode with findings).
    """
    print("=" * 70)
    print("Vendor-Neutrality Scanner")
    print("=" * 70)
    print(f"\nScanning: {root / SCAN_ROOT}")
    print(f"Mode: {'STRICT (will fail on findings)' if strict_mode else 'Report only'}")
    results = scan_files(root)
    print("\n" + "-" * 70)
    print("VENDOR-NEUTRALITY CHECKS")
    print("-" * 70)
    total = print_results(results, root)
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"\n  Vendor-specific-name violations: {total}")
    if total > 0:
        print(
            "\nAdd a trailing '# vendor-neutral-ok' comment to intentional "
            "references, or move vendor-specific code to adjacent public repos."
        )
        if strict_mode:
            print("\nSTRICT MODE: Failing due to violations found.")
            return 1
    else:
        print("\nCore stays vendor-neutral.")
    return 0


def main() -> int:
    """Entry point for the vendor-neutrality script.

    Returns:
        Exit code from the scan operation.
    """
    strict_mode = "--strict" in sys.argv
    script_dir = Path(__file__).parent
    root = script_dir.parent
    return run_scan(root, strict_mode)


if __name__ == "__main__":
    raise SystemExit(main())
