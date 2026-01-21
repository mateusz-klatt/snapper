"""Clean build artifacts and caches from the project.

Removes virtual environments, pytest/mypy/ruff caches, coverage reports,
and frontend build artifacts.
"""

import shutil
from pathlib import Path


def get_dirs_to_clean(root: Path) -> list[Path]:
    """Return list of directories to remove during cleanup.

    Args:
        root: Project root directory path.

    Returns:
        List of directory paths to be cleaned.
    """
    return [
        root / ".venv",
        root / ".pytest_cache",
        root / ".mypy_cache",
        root / ".ruff_cache",
        root / "htmlcov",
        root / "frontend" / "dist",
        root / "frontend" / "node_modules",
        root / "frontend" / "coverage",
    ]


def get_files_to_clean(root: Path) -> list[Path]:
    """Return list of files to remove during cleanup.

    Args:
        root: Project root directory path.

    Returns:
        List of file paths to be cleaned.
    """
    return [
        root / ".coverage",
        root / "frontend" / ".eslintcache",
    ]


def remove_directory(dir_path: Path, root: Path) -> bool:
    """Remove a directory if it exists.

    Args:
        dir_path: Path to the directory to remove.
        root: Project root directory for relative path display.

    Returns:
        True if the directory was removed, False otherwise.
    """
    if dir_path.exists():
        print(f"Removing {dir_path.relative_to(root)}")
        shutil.rmtree(dir_path, ignore_errors=True)
        return True
    return False


def remove_file(file_path: Path, root: Path) -> bool:
    """Remove a file if it exists.

    Args:
        file_path: Path to the file to remove.
        root: Project root directory for relative path display.

    Returns:
        True if the file was removed, False otherwise.
    """
    if file_path.exists():
        print(f"Removing {file_path.relative_to(root)}")
        file_path.unlink(missing_ok=True)
        return True
    return False


def clean_pycache(root: Path) -> int:
    """Remove all __pycache__ directories.

    Args:
        root: Project root directory to search recursively.

    Returns:
        Number of __pycache__ directories removed.
    """
    count = 0
    for pycache in root.rglob("__pycache__"):
        if pycache.is_dir():
            shutil.rmtree(pycache, ignore_errors=True)
            count += 1
    return count


def clean_pyc_files(root: Path) -> int:
    """Remove all .pyc files.

    Args:
        root: Project root directory to search recursively.

    Returns:
        Number of .pyc files removed.
    """
    count = 0
    for pyc in root.rglob("*.pyc"):
        pyc.unlink(missing_ok=True)
        count += 1
    return count


def clean_egg_info(root: Path) -> int:
    """Remove all .egg-info directories.

    Args:
        root: Project root directory to search.

    Returns:
        Number of .egg-info directories removed.
    """
    count = 0
    for egg_info in root.glob("*.egg-info"):
        if egg_info.is_dir():
            print(f"Removing {egg_info.relative_to(root)}")
            shutil.rmtree(egg_info, ignore_errors=True)
            count += 1
    return count


def clean(root: Path | None = None) -> None:
    """Run full cleanup on the project.

    Args:
        root: Project root directory path. Defaults to parent of script directory.
    """
    if root is None:
        root = Path(__file__).parent.parent

    for dir_path in get_dirs_to_clean(root):
        remove_directory(dir_path, root)

    for file_path in get_files_to_clean(root):
        remove_file(file_path, root)

    clean_pycache(root)
    clean_pyc_files(root)
    clean_egg_info(root)

    print("Cleanup completed!")


def main() -> int:
    """Entry point for clean script.

    Returns:
        Exit code, always 0 on success.
    """
    clean()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
