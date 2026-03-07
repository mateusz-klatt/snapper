"""Refresh frontend dependencies.

Upgrades direct dependencies to their latest versions (updating package.json),
while keeping selected protected dependencies at their existing version ranges,
then removes node_modules and lock file and performs a clean install.

This keeps the UI refresh behavior consistent with the backend refresh target
which upgrades dependencies to the latest available versions.
"""

import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any
from typing import cast

IS_WINDOWS = sys.platform == "win32"


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


def upgrade_package_manager(ui_dir: Path) -> None:
    """Upgrade pnpm to latest and update packageManager field in package.json.

    Args:
        ui_dir: Path to the UI directory containing package.json.
    """
    result = run_cmd(
        ["npm", "view", "pnpm", "version"],
        capture_output=True,
        text=True,
    )
    latest = result.stdout.strip()
    if not latest:
        print("Could not determine latest pnpm version, skipping packageManager update")
        return

    package_json = ui_dir / "package.json"
    package_data = read_package_json(package_json)
    new_value = f"pnpm@{latest}"
    if package_data.get("packageManager") != new_value:
        print(f"Updating packageManager to {new_value}")
        package_data["packageManager"] = new_value
        write_package_json(package_json, package_data)


def upgrade_dependencies(ui_dir: Path) -> None:
    """Upgrade direct UI dependencies to latest versions.

    Runs ``pnpm up --latest``, restores protected dependency version ranges,
    then runs ``pnpm up`` to resolve latest versions within those ranges.

    Args:
        ui_dir: Path to the UI directory containing package.json.
    """
    package_json = ui_dir / "package.json"
    if not package_json.exists():
        print(f"Skipping dependency upgrade (missing {package_json})")
        return

    protected_dependency_names = ["eslint", "@eslint/js"]
    package_data_before = read_package_json(package_json)
    protected_specs: dict[str, tuple[str, str]] = {}
    for dep_name in protected_dependency_names:
        section, spec = get_dependency_spec(package_data_before, dep_name)
        if section is not None and spec is not None:
            protected_specs[dep_name] = (section, spec)

    print("Upgrading UI direct dependencies to latest...")
    run_cmd(["pnpm", "up", "--latest"], cwd=ui_dir, check=True)

    if not protected_specs:
        return

    package_data_after = read_package_json(package_json)
    modified = False
    for dep_name, (section, spec) in protected_specs.items():
        modified = restore_dependency_spec(package_data_after, dep_name, section, spec) or modified

    if modified:
        print("Restoring protected dependency version ranges...")
        write_package_json(package_json, package_data_after)

    print("Updating protected dependencies within allowed ranges...")
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
