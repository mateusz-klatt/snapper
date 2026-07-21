"""Refresh Python dependency constraints without fighting exact upstream pins.

``poetry up --latest`` raises every direct constraint's floor to the newest
release. That breaks whenever one of our dependencies pins another of our direct
dependencies to an EXACT version: ccxt 4.5.67 requires ``aiohttp==3.14.1``, so
bumping our own ``aiohttp`` floor to ``^3.14.2`` makes the solver unsatisfiable::

    because snapper depends on both aiohttp (^3.14.2) and ccxt (^4.5.67),
    version solving failed

Worse, ``poetry up`` has already rewritten ``pyproject.toml`` by the time the
solve fails, leaving the tree wedged.

This script DERIVES the set of constraints that must not be auto-bumped instead
of hard-coding package names: it reads the lock file, finds every dependency that
some other package pins exactly, intersects that with our own direct
dependencies, and excludes exactly those from the bump. Nothing here names a
specific library, so the exclusion set follows the dependency graph as it changes
— when ccxt relaxes its pin, aiohttp starts being bumped again with no edit here
and none in the Makefile, which stays free of dependency detail.
"""

import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Final

_LOCKFILE_NAME: Final = "poetry.lock"
_PROJECT_FILE_NAME: Final = "pyproject.toml"
_EXACT_PREFIXES: Final[tuple[str, ...]] = (">=", "<=", "^", "~", ">", "<", "!=", "*")


def _default_root() -> Path:
    """Return the repository root (this script lives in ``<root>/scripts``)."""
    return Path(__file__).resolve().parent.parent


def _is_exact(constraint: object) -> bool:
    """Report whether a lock constraint pins a single version.

    Poetry writes an exact requirement either bare (``"3.14.1"``) or with an
    equality operator (``"==3.14.1"``). Anything carrying a range operator, and
    any structured (dict or list) requirement, is treated as flexible.

    Args:
        constraint: The raw requirement value from a lock dependency table.

    Returns:
        ``True`` when the requirement admits exactly one version.
    """
    if not isinstance(constraint, str):
        return False
    text = constraint.strip()
    if not text or "," in text or "||" in text:
        return False
    if text.startswith("=="):
        return True
    return not text.startswith(_EXACT_PREFIXES)


def direct_dependency_names(project_text: str) -> set[str]:
    """Return the normalized names of our own direct dependencies.

    Args:
        project_text: Full text of ``pyproject.toml``.

    Returns:
        Normalized (lower-case, dash-separated) direct dependency names,
        excluding the ``python`` marker entry.
    """
    data = tomllib.loads(project_text)
    poetry_table = data.get("tool", {}).get("poetry", {})
    names: set[str] = set()
    for group in (poetry_table.get("dependencies", {}), *_group_tables(poetry_table)):
        names.update(str(name).lower().replace("_", "-") for name in group)
    names.discard("python")
    return names


def _group_tables(poetry_table: dict[str, object]) -> list[dict[str, object]]:
    """Return every optional dependency-group table declared for the project."""
    groups = poetry_table.get("group")
    if not isinstance(groups, dict):
        return []
    tables: list[dict[str, object]] = []
    for group in groups.values():
        if isinstance(group, dict):
            dependencies = group.get("dependencies")
            if isinstance(dependencies, dict):
                tables.append(dependencies)
    return tables


def exactly_pinned_dependencies(lock_text: str, direct_names: set[str]) -> list[str]:
    """Return our direct dependencies that some locked package pins exactly.

    Args:
        lock_text: Full text of ``poetry.lock``.
        direct_names: Normalized names of our own direct dependencies.

    Returns:
        Sorted normalized names whose floor must not be raised, because another
        package in the graph admits exactly one version of them.
    """
    data = tomllib.loads(lock_text)
    packages = data.get("package", [])
    pinned: set[str] = set()
    if not isinstance(packages, list):
        return []
    for package in packages:
        if not isinstance(package, dict):
            continue
        dependencies = package.get("dependencies")
        if not isinstance(dependencies, dict):
            continue
        for name, constraint in dependencies.items():
            normalized = str(name).lower().replace("_", "-")
            if normalized in direct_names and _is_exact(constraint):
                pinned.add(normalized)
    return sorted(pinned)


def build_command(excluded: list[str]) -> list[str]:
    """Build the ``poetry up`` invocation for the derived exclusion set.

    Args:
        excluded: Normalized dependency names to leave at their current floor.

    Returns:
        The argument vector to execute.
    """
    command = [sys.executable, "-m", "poetry", "up", "--latest"]
    for name in excluded:
        command.extend(["--exclude", name])
    return command


def main() -> int:
    """Bump direct constraints, skipping those an upstream package pins exactly.

    Returns:
        Process exit status; the bump itself is advisory, so a failure there is
        reported without failing the refresh, matching the tolerant step this
        replaces.
    """
    root = _default_root()
    lock_path = root / _LOCKFILE_NAME
    project_path = root / _PROJECT_FILE_NAME
    if not lock_path.is_file() or not project_path.is_file():
        print("refresh-python-deps: no lock or project file; nothing to bump")
        return 0
    direct_names = direct_dependency_names(project_path.read_text(encoding="utf-8"))
    excluded = exactly_pinned_dependencies(lock_path.read_text(encoding="utf-8"), direct_names)
    if excluded:
        print(f"refresh-python-deps: holding exactly-pinned constraints: {', '.join(excluded)}")
    completed = subprocess.run(build_command(excluded), cwd=root, check=False)
    if completed.returncode != 0:
        print(f"refresh-python-deps: constraint bump reported {completed.returncode}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
