"""Refresh frontend dependencies.

Upgrades direct dependencies to their latest versions (updating package.json),
while keeping selected protected dependencies at their existing version ranges,
then removes node_modules and lock file and performs a clean install.

This keeps the UI refresh behavior consistent with the backend refresh target
which upgrades dependencies to the latest available versions.
"""

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any
from typing import cast

IS_WINDOWS = sys.platform == "win32"
COREPACK_PACKAGE = "corepack"
PNPM_PACKAGE = "pnpm"
_VERSION_RE = re.compile(r"(\d++)\.(\d++)\.(\d++)(?:-([0-9A-Za-z.-]++))?")

os.environ.setdefault("COREPACK_ENABLE_DOWNLOAD_PROMPT", "0")


def read_package_json(package_json: Path) -> dict[str, Any]:
    """Read package.json into a Python dictionary.

    Args:
        package_json: Path to package.json.

    Returns:
        Parsed JSON data.
    """
    raw_data = json.loads(package_json.read_text(encoding="utf-8"))
    if not isinstance(raw_data, dict):
        raise ValueError("package.json must be a JSON object")
    return cast(dict[str, Any], raw_data)


def write_package_json(package_json: Path, data: dict[str, Any]) -> None:
    """Write package.json with stable formatting.

    Args:
        package_json: Path to package.json.
        data: JSON data to write.
    """
    package_json.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def get_dependency_spec(package_data: dict[str, Any], name: str) -> tuple[str | None, str | None]:
    """Find a dependency spec in package.json sections.

    Args:
        package_data: Parsed package.json data.
        name: Dependency name to search for.

    Returns:
        Tuple of (section_name, version_spec). If not found, (None, None).
    """
    for section in ("dependencies", "devDependencies", "optionalDependencies", "peerDependencies"):
        section_data = package_data.get(section)
        if isinstance(section_data, dict) and name in section_data:
            spec = section_data.get(name)
            if isinstance(spec, str):
                return section, spec
            return section, None
    return None, None


def restore_dependency_spec(
    package_data: dict[str, Any],
    name: str,
    section: str,
    spec: str,
) -> bool:
    """Restore a dependency spec into a specific section.

    If the dependency exists in a different section, it is removed there.

    Args:
        package_data: Parsed package.json data.
        name: Dependency name to restore.
        section: Target section name.
        spec: Version spec to enforce.

    Returns:
        True if package_data was modified, False otherwise.
    """
    modified = False
    current_section, _ = get_dependency_spec(package_data, name)

    if current_section is not None and current_section != section:
        current_section_data = package_data.get(current_section)
        if isinstance(current_section_data, dict) and name in current_section_data:
            del current_section_data[name]
            modified = True

    target_section_data = package_data.get(section)
    if not isinstance(target_section_data, dict):
        package_data[section] = {}
        target_section_data = package_data[section]
        modified = True

    if isinstance(target_section_data, dict) and target_section_data.get(name) != spec:
        target_section_data[name] = spec
        modified = True

    return modified


def run_cmd(
    args: list[str],
    check: bool = False,
    **kwargs: Any,
) -> subprocess.CompletedProcess[str]:
    """Run a command, using shell on Windows.

    Args:
        args: Command and arguments to execute.
        check: If True, raise CalledProcessError on non-zero exit.
        **kwargs: Additional arguments passed to subprocess.run.

    Returns:
        Completed process instance with execution results.
    """
    if IS_WINDOWS:
        return subprocess.run(args, shell=True, check=check, **kwargs)
    return subprocess.run(args, check=check, **kwargs)


def remove_lock_file(ui_dir: Path) -> bool:
    """Remove pnpm-lock.yaml if it exists.

    Args:
        ui_dir: Path to the UI directory containing the lock file.

    Returns:
        True if lock file was removed, False otherwise.
    """
    lock_file = ui_dir / "pnpm-lock.yaml"
    if lock_file.exists():
        print("Removing pnpm-lock.yaml")
        lock_file.unlink()
        return True
    return False


def remove_node_modules(ui_dir: Path) -> bool:
    """Remove node_modules directory if it exists.

    Args:
        ui_dir: Path to the UI directory containing node_modules.

    Returns:
        True if node_modules was removed, False otherwise.
    """
    node_modules = ui_dir / "node_modules"
    if node_modules.exists():
        print("Removing node_modules")
        shutil.rmtree(node_modules, ignore_errors=True)
        return True
    return False


def ensure_corepack_installed() -> None:
    """Ensure corepack is available, installing via npm if needed."""
    try:
        run_cmd(["corepack", "--version"], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("Installing corepack...")
        run_cmd(
            ["npm", "install", "-g", "--ignore-scripts", "corepack"],
            check=True,
        )
    run_cmd(["corepack", "enable"], check=True)


def _latest_npm_version(package_name: str) -> str | None:
    """Return the latest published npm version for a package."""
    result = run_cmd(
        ["npm", "view", package_name, "version"],
        capture_output=True,
        text=True,
    )
    latest = result.stdout.strip()
    if not latest:
        return None
    return latest


def _current_corepack_version() -> str | None:
    """Return the locally installed corepack version, or None if unavailable."""
    try:
        result = run_cmd(
            ["corepack", "--version"],
            capture_output=True,
            text=True,
            check=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None
    current = result.stdout.strip()
    return current or None


def upgrade_corepack() -> None:
    """Upgrade corepack to its latest npm-published version and enable it.

    Skips the global ``npm install`` when the local corepack already matches
    the latest published version. If the upgrade install fails (commonly an
    ``EACCES`` from ``npm install -g`` against a root-owned global prefix),
    logs a warning and continues with the current corepack so the broader
    refresh pipeline is not blocked on tooling already capable of running.
    """
    latest = _latest_npm_version(COREPACK_PACKAGE)
    if latest is None:
        print("Could not determine latest corepack version, skipping upgrade")
        run_cmd(["corepack", "enable"], check=True)
        return
    current = _current_corepack_version()
    if current == latest:
        print(f"corepack already at {latest}, skipping upgrade")
        run_cmd(["corepack", "enable"], check=True)
        return
    print(f"Upgrading corepack to {latest} (current: {current or 'unknown'})")
    try:
        run_cmd(
            ["npm", "install", "-g", "--ignore-scripts", f"{COREPACK_PACKAGE}@{latest}"],
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        print(
            f"Warning: corepack upgrade failed (exit {exc.returncode}); "
            f"continuing with current version {current or 'unknown'}. "
            "Re-run with sudo or fix the npm global prefix to upgrade."
        )
    run_cmd(["corepack", "enable"], check=True)


def upgrade_package_manager(ui_dir: Path) -> None:
    """Upgrade pnpm to latest and update packageManager field in package.json.

    Args:
        ui_dir: Path to the UI directory containing package.json.
    """
    latest = _latest_npm_version(PNPM_PACKAGE)
    if latest is None:
        print("Could not determine latest pnpm version, skipping packageManager update")
        return

    package_json = ui_dir / "package.json"
    package_data = read_package_json(package_json)
    new_value = f"pnpm@{latest}"
    if package_data.get("packageManager") != new_value:
        print(f"Updating packageManager to {new_value}")
        package_data["packageManager"] = new_value
        write_package_json(package_json, package_data)


def _next_decimal_run(spec: str, search_from: int) -> tuple[int, int] | None:
    """Find the next contiguous run of decimal digits.

    Args:
        spec: Version spec to search.
        search_from: Offset where scanning begins.

    Returns:
        Start and exclusive end offsets, or None when no run remains.
    """
    run_start = search_from
    while run_start < len(spec) and not spec[run_start].isdecimal():
        run_start += 1
    if run_start == len(spec):
        return None
    run_end = run_start + 1
    while run_end < len(spec) and spec[run_end].isdecimal():
        run_end += 1
    return run_start, run_end


def _version_sort_key(spec: str) -> tuple[int, int, int] | None:
    """Return a comparable (major, minor, patch) key for a stable version spec.

    Strips any leading range operator and extracts the first concrete
    ``major.minor.patch``. Returns None when the spec carries no stable triple
    (ranges such as ``*``, ``workspace:*``, ``1.2.x``, git URLs) or when it pins
    a prerelease, signalling that the downgrade guard must skip the dependency
    rather than risk an unsafe comparison.

    Args:
        spec: A package.json version spec string.

    Returns:
        The (major, minor, patch) tuple, or None when not safely comparable.
    """
    search_from = 0
    while decimal_run := _next_decimal_run(spec, search_from):
        run_start, run_end = decimal_run
        match = _VERSION_RE.match(spec, run_start)
        if match is not None:
            if match.group(4) is not None:
                return None
            return (int(match.group(1)), int(match.group(2)), int(match.group(3)))
        search_from = run_end
    return None


def collect_dependency_specs(package_data: dict[str, Any]) -> dict[str, tuple[str, str]]:
    """Map every direct dependency name to its (section, version spec).

    Walks the four standard dependency sections and records string specs only;
    non-dict sections and non-string specs are ignored. Dependency names carry no
    type check of their own: the production callers all pass data decoded by
    ``read_package_json``, and ``json.loads`` yields ``str`` keys at every nesting
    level, so a section key coming from a real package.json can never be a
    non-string. A hand-built mapping with non-string keys would pass the
    annotation and land in the result unchecked, which is acceptable here because
    the only such callers are tests that construct their own fixtures.

    Args:
        package_data: Parsed package.json data.

    Returns:
        Mapping of dependency name to its (section, spec) pair.
    """
    collected: dict[str, tuple[str, str]] = {}
    for section in ("dependencies", "devDependencies", "optionalDependencies", "peerDependencies"):
        section_data = package_data.get(section)
        if not isinstance(section_data, dict):
            continue
        for name, spec in section_data.items():
            if isinstance(spec, str):
                collected[name] = (section, spec)
    return collected


def guard_against_downgrades(
    package_data: dict[str, Any],
    specs_before: dict[str, tuple[str, str]],
) -> bool:
    """Restore any dependency whose spec regressed below its prior version.

    When an upgrade tool selects versions against a mutable ``latest`` dist-tag
    (``pnpm up --latest`` or ``npm-check-updates`` with the default ``latest``
    target), a stale metadata cache or a dist-tag pointing at an older release
    can yield a spec below the committed one. For every dependency present both
    before and after, the prior (higher) spec is restored when the new spec
    resolves to a strictly lower stable version, keeping the refresh monotonic
    so it can never introduce a regression. Shared by the UI (pnpm) and
    snapper-mcp (npm) refresh paths.

    Args:
        package_data: Parsed package.json data after the latest upgrade.
        specs_before: Pre-upgrade (section, spec) per dependency name.

    Returns:
        True if any spec was restored, False otherwise.
    """
    modified = False
    specs_after = collect_dependency_specs(package_data)
    for name, (section_before, spec_before) in specs_before.items():
        after = specs_after.get(name)
        if after is None:
            continue
        spec_after = after[1]
        key_before = _version_sort_key(spec_before)
        key_after = _version_sort_key(spec_after)
        if key_before is None or key_after is None:
            continue
        if key_after < key_before:
            print(f"Preventing downgrade of {name}: keeping {spec_before} over {spec_after}")
            modified = (
                restore_dependency_spec(package_data, name, section_before, spec_before) or modified
            )
    return modified


def upgrade_dependencies(ui_dir: Path) -> None:
    """Upgrade direct UI dependencies to latest versions, never regressing.

    Runs ``pnpm up --latest``, restores protected dependency version ranges and
    any dependency that the upgrade lowered below its prior version, then runs
    ``pnpm up`` to re-resolve within the restored ranges. The downgrade guard
    makes the refresh deterministic against regressions: a stale registry cache
    can never push a dependency backward.

    ``typescript`` is protected alongside ``eslint``/``@eslint/js`` because the
    TypeScript 7 native compiler ships only a ``tsc`` binary and drops the
    JavaScript compiler API (``ts.factory``) that ``openapi-typescript`` (the
    API-types codegen) and ``typescript-eslint`` build on. Pinning its committed
    ``6.x`` range keeps ``make ui-refresh`` from silently upgrading it to 7 and
    breaking codegen; remove it from the protected list once the frontend
    toolchain supports the native compiler.

    Args:
        ui_dir: Path to the UI directory containing package.json.
    """
    package_json = ui_dir / "package.json"
    if not package_json.exists():
        print(f"Skipping dependency upgrade (missing {package_json})")
        return

    protected_dependency_names = ["eslint", "@eslint/js", "typescript"]
    package_data_before = read_package_json(package_json)
    protected_specs: dict[str, tuple[str, str]] = {}
    for dep_name in protected_dependency_names:
        section, spec = get_dependency_spec(package_data_before, dep_name)
        if section is not None and spec is not None:
            protected_specs[dep_name] = (section, spec)
    specs_before = collect_dependency_specs(package_data_before)

    print("Upgrading UI direct dependencies to latest...")
    run_cmd(["pnpm", "up", "--latest"], cwd=ui_dir, check=True)

    package_data_after = read_package_json(package_json)
    modified = False
    for dep_name, (section, spec) in protected_specs.items():
        modified = restore_dependency_spec(package_data_after, dep_name, section, spec) or modified
    modified = guard_against_downgrades(package_data_after, specs_before) or modified

    if not modified:
        return

    print("Restoring protected and non-regressing dependency version ranges...")
    write_package_json(package_json, package_data_after)
    print("Re-resolving restored dependencies within allowed ranges...")
    run_cmd(["pnpm", "up"], cwd=ui_dir, check=True)


def install_dependencies(ui_dir: Path) -> None:
    """Run pnpm install in the UI directory.

    Args:
        ui_dir: Path to the UI directory where dependencies will be installed.
    """
    print("Installing UI dependencies...")
    run_cmd(["pnpm", "install"], cwd=ui_dir, check=True)
    print("UI dependencies refreshed!")


def refresh_ui(root: Path | None = None) -> None:
    """Run full UI refresh - remove lock/node_modules and reinstall.

    Args:
        root: Project root directory. Defaults to parent of script directory.
    """
    if root is None:
        root = Path(__file__).parent.parent
    ui_dir = root / "frontend"

    ensure_corepack_installed()
    upgrade_corepack()
    upgrade_package_manager(ui_dir)
    upgrade_dependencies(ui_dir)
    remove_lock_file(ui_dir)
    remove_node_modules(ui_dir)
    install_dependencies(ui_dir)


def main() -> int:
    """Entry point for ui_refresh script.

    Returns:
        Exit code, 0 for success.
    """
    refresh_ui()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
