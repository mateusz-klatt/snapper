"""Verify the bridge wire-contract working-tree file matches the current backend schemas.

The script regenerates the wire-contract TS to a temporary path using
the same code path ``make ts-bridge`` runs, then byte-compares it
against the working-tree file at
``integrations/snapper-mcp/src/generated/wire-contract.ts``.

The comparison is against the **working tree**, NOT against ``git
HEAD``. This keeps ``make bridge-check`` correct in all three
developer states:

    just-regenerated  : working-tree == regen output → pass
    just-committed    : working-tree == committed output == regen → pass
    stale (schema edit without regen) : working-tree != regen → fail

Exit codes:

    0 — working-tree wire-contract matches current schemas
    1 — drift detected; the script prints the unified diff
    2 — generator or filesystem error before comparison
"""

import argparse
import difflib
import sys
import tempfile
from pathlib import Path

from scripts.generate_types import _BRIDGE_OUTPUT_DEFAULT
from scripts.generate_types import generate_bridge_wire_contract


def _project_root() -> Path:
    """Return the repository root (two levels above this script)."""
    return Path(__file__).resolve().parent.parent.parent


def compute_drift(target_path: Path) -> tuple[bool, str]:
    """Regenerate the bridge wire-contract to a temp file and compare bytes.

    Args:
        target_path: Working-tree path to compare against. Usually
            ``integrations/snapper-mcp/src/generated/wire-contract.ts``.

    Returns:
        A pair of ``(matches, diff_text)``. ``matches`` is True when
        the regenerated content equals the target file's content;
        ``diff_text`` is the empty string in that case, or a unified
        diff suitable for printing when the two differ.

    Raises:
        FileNotFoundError: When ``target_path`` does not exist.
    """
    if not target_path.is_file():
        raise FileNotFoundError(
            f"Bridge wire-contract not found at {target_path}; "
            "run `make ts-bridge` to generate it before checking drift."
        )

    target_content = target_path.read_text(encoding="utf-8")

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir) / "wire-contract.ts"
        regenerated = generate_bridge_wire_contract(tmp_path)

    if regenerated == target_content:
        return (True, "")

    diff = difflib.unified_diff(
        target_content.splitlines(keepends=True),
        regenerated.splitlines(keepends=True),
        fromfile=str(target_path),
        tofile="<regenerated>",
        n=3,
    )
    return (False, "".join(diff))


def main(argv: list[str] | None = None) -> int:
    """CLI entry point invoked by ``make bridge-check``.

    Args:
        argv: Optional CLI arguments (defaults to ``sys.argv[1:]``).

    Returns:
        Process exit code (0/1/2 — see module docstring).
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--target",
        default=None,
        help=(
            "Override the working-tree wire-contract path "
            "(default: integrations/snapper-mcp/src/generated/wire-contract.ts)"
        ),
    )
    args = parser.parse_args(argv)

    if args.target is not None:
        target_path = Path(args.target)
        if not target_path.is_absolute():
            target_path = (_project_root() / target_path).resolve()
    else:
        target_path = (_project_root() / _BRIDGE_OUTPUT_DEFAULT).resolve()

    try:
        matches, diff_text = compute_drift(target_path)
    except FileNotFoundError as exc:
        print(f"bridge-check drift: {exc}", file=sys.stderr)
        return 2

    if matches:
        print(f"bridge-check drift: OK — {target_path} matches current schemas")
        return 0

    print(
        "bridge-check drift: FAIL — working-tree wire-contract is stale.\n"
        "Run `make ts-bridge` to regenerate, then commit the result.",
        file=sys.stderr,
    )
    print(diff_text, file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
