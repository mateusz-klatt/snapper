"""Refresh frontend dependencies.

Upgrades direct dependencies to their latest versions (updating package.json),
then removes node_modules and lock file and performs a clean install.

This keeps the UI refresh behavior consistent with the backend refresh target
which upgrades dependencies to the latest available versions.
"""

import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

IS_WINDOWS = sys.platform == "win32"


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


def ensure_pnpm_installed() -> None:
    """Ensure pnpm is installed, installing via corepack if needed."""
    try:
        run_cmd(["pnpm", "--version"], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("Installing pnpm via corepack...")
        run_cmd(["corepack", "enable"], check=True)
        run_cmd(["corepack", "prepare", "pnpm@latest", "--activate"], check=True)


def upgrade_dependencies(ui_dir: Path) -> None:
    """Upgrade direct UI dependencies to latest versions.

    Args:
        ui_dir: Path to the UI directory containing package.json.
    """
    package_json = ui_dir / "package.json"
    if not package_json.exists():
        print(f"Skipping dependency upgrade (missing {package_json})")
        return

    print("Upgrading UI direct dependencies to latest...")
    run_cmd(["pnpm", "up", "--latest"], cwd=ui_dir, check=True)


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

    ensure_pnpm_installed()
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
