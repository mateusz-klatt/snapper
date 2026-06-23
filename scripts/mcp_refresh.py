"""Refresh integrations/snapper-mcp dependencies.

Upgrades direct dependencies of the snapper-mcp integration package to their
latest versions while keeping selected protected dependencies at their existing
version ranges, then removes node_modules and the npm lock file and performs a
clean install.

Mirrors scripts/ui_refresh.py but uses npm (snapper-mcp's package manager of
record) and npm-check-updates for the latest-version bump, since snapper-mcp
ships its own ``package-lock.json`` and is published as a standalone npm
package.
"""

from pathlib import Path

from scripts.ui_refresh import collect_dependency_specs
from scripts.ui_refresh import guard_against_downgrades
from scripts.ui_refresh import read_package_json
from scripts.ui_refresh import remove_node_modules
from scripts.ui_refresh import run_cmd
from scripts.ui_refresh import write_package_json

MCP_DIR_NAME = "integrations/snapper-mcp"
NPM_LOCK_FILE = "package-lock.json"
PROTECTED_DEPENDENCIES: tuple[str, ...] = ("eslint", "@eslint/js")


def remove_npm_lock_file(mcp_dir: Path) -> bool:
    """Remove package-lock.json if it exists.

    Args:
        mcp_dir: Path to the snapper-mcp directory containing the lock file.

    Returns:
        True if the lock file was removed, False otherwise.
    """
    lock_file = mcp_dir / NPM_LOCK_FILE
    if lock_file.exists():
        print(f"Removing {NPM_LOCK_FILE}")
        lock_file.unlink()
        return True
    return False


def upgrade_dependencies_npm(mcp_dir: Path) -> None:
    """Upgrade direct snapper-mcp dependencies to latest versions, never regressing.

    Runs ``npx --yes npm-check-updates -u`` excluding protected dependency
    names, so the bumped package.json keeps the protected entries at their
    current major range while every other dependency is rewritten to the
    latest available version. npm-check-updates resolves its default ``latest``
    target against the mutable ``latest`` dist-tag, so a stale metadata cache or
    a dist-tag pointing at an older release can rewrite a dependency below the
    committed version. The same downgrade guard used by the UI refresh restores
    any such regression, keeping the snapper-mcp refresh monotonic. The
    subsequent clean ``npm install`` re-resolves the lock file from the
    corrected package.json, so no interim re-resolve step is needed.

    Args:
        mcp_dir: Path to the snapper-mcp directory containing package.json.
    """
    package_json = mcp_dir / "package.json"
    if not package_json.exists():
        print(f"Skipping dependency upgrade (missing {package_json})")
        return

    specs_before = collect_dependency_specs(read_package_json(package_json))

    print("Upgrading snapper-mcp direct dependencies to latest...")
    run_cmd(
        [
            "npx",
            "--yes",
            "npm-check-updates",
            "-u",
            "--reject",
            ",".join(PROTECTED_DEPENDENCIES),
        ],
        cwd=mcp_dir,
        check=True,
    )

    package_data_after = read_package_json(package_json)
    if guard_against_downgrades(package_data_after, specs_before):
        print("Restoring non-regressing snapper-mcp dependency version ranges...")
        write_package_json(package_json, package_data_after)


def install_dependencies_npm(mcp_dir: Path) -> None:
    """Run npm install in the snapper-mcp directory.

    Args:
        mcp_dir: Path to the snapper-mcp directory where dependencies will
            be installed.
    """
    print("Installing snapper-mcp dependencies...")
    run_cmd(["npm", "install"], cwd=mcp_dir, check=True)
    print("snapper-mcp dependencies refreshed!")


def refresh_mcp(root: Path | None = None) -> None:
    """Run full snapper-mcp refresh - bump deps, wipe lock + node_modules, reinstall.

    Args:
        root: Project root directory. Defaults to the parent of the script
            directory.
    """
    if root is None:
        root = Path(__file__).parent.parent
    mcp_dir = root / MCP_DIR_NAME

    if not mcp_dir.exists():
        print(f"Skipping snapper-mcp refresh (missing {mcp_dir})")
        return

    upgrade_dependencies_npm(mcp_dir)
    remove_npm_lock_file(mcp_dir)
    remove_node_modules(mcp_dir)
    install_dependencies_npm(mcp_dir)


def main() -> int:
    """Entry point for mcp_refresh script.

    Returns:
        Exit code, 0 for success.
    """
    refresh_mcp()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
