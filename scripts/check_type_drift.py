"""Check for uncommitted type drift in generated files.

Compares current generated TypeScript/Swift types against freshly
generated versions to detect drift between backend schemas and frontend types.
"""

import filecmp
import shutil
import subprocess
import tempfile
from pathlib import Path


def get_files_to_check(project_root: Path) -> list[Path]:
    """Return list of generated files to check for drift.

    Args:
        project_root: Root directory of the project.

    Returns:
        List of paths to generated TypeScript, Swift, and backend-i18n
        JSON files.
    """
    backend_i18n_catalog = project_root / "src" / "snapper" / "i18n" / "catalogs"
    catalog_files: list[Path] = []
    if backend_i18n_catalog.is_dir():
        catalog_files = sorted(backend_i18n_catalog.glob("*.json"))
    return [
        project_root / "frontend" / "src" / "types" / "api.generated.ts",
        project_root / "frontend" / "src" / "types" / "ws.generated.ts",
        project_root / "frontend" / "src" / "types" / "entities.generated.ts",
        project_root / "frontend" / "src" / "types" / "permissions.generated.ts",
        project_root / "frontend" / "src" / "lib" / "schemas" / "ws.generated.zod.ts",
        project_root / "frontend" / "src" / "lib" / "schemas" / "api.generated.zod.ts",
        project_root / "ios" / "Snapper" / "Models" / "Generated" / "WSMessages.swift",
        project_root / "ios" / "Snapper" / "Models" / "Generated" / "APITypes.swift",
        project_root / "ios" / "Snapper" / "Models" / "Generated" / "Permissions.swift",
        *catalog_files,
    ]


def backup_files(files: list[Path], backup_dir: Path) -> dict[Path, Path]:
    """Backup files to temporary directory.

    Args:
        files: List of file paths to backup.
        backup_dir: Directory to store backup copies.

    Returns:
        Mapping of original file paths to their backup paths.
    """
    backups: dict[Path, Path] = {}
    for f in files:
        if f.exists():
            backup = backup_dir / f.name
            shutil.copy2(f, backup)
            backups[f] = backup
    return backups


def restore_files(backups: dict[Path, Path]) -> None:
    """Restore files from backups.

    Args:
        backups: Mapping of original file paths to their backup paths.
    """
    for original, backup in backups.items():
        shutil.copy2(backup, original)


def regenerate_types(project_root: Path) -> subprocess.CompletedProcess[str]:
    """Run type generation commands.

    Args:
        project_root: Root directory of the project.

    Returns:
        Completed process result from the type generation command.
    """
    return subprocess.run(
        ["make", "ui-gen-types", "ios-gen-types", "gen-backend-i18n-catalog"],
        check=False,
        cwd=project_root,
        capture_output=True,
        text=True,
    )


def check_drift(backups: dict[Path, Path], project_root: Path) -> list[Path]:
    """Compare files with backups and return list of drifted files.

    Args:
        backups: Mapping of original file paths to their backup paths.
        project_root: Root directory of the project for relative path display.

    Returns:
        List of file paths that have drifted from their backups.
    """
    drifted: list[Path] = []
    for original, backup in backups.items():
        if not filecmp.cmp(original, backup, shallow=False):
            print(f"  Drift detected: {original.relative_to(project_root)}")
            drifted.append(original)
    return drifted


def main() -> int:
    """Entry point for check_type_drift script.

    Returns:
        Exit code: 0 if types are up to date, 1 if drift detected or error.
    """
    project_root = Path(__file__).parent.parent

    files_to_check = get_files_to_check(project_root)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        backups = backup_files(files_to_check, tmppath)

        result = regenerate_types(project_root)
        if result.returncode != 0:
            print("Failed to generate types:")
            print(result.stderr)
            restore_files(backups)
            return 1

        drifted = check_drift(backups, project_root)
        restore_files(backups)

    if drifted:
        print("Type drift detected! Run 'make ui-gen-types ios-gen-types' and commit the changes.")
        return 1
    else:
        print("Generated types are up to date")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
