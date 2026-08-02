"""Synchronize pre-commit hook versions with pyproject.toml.

Reads dependency versions from pyproject.toml and updates the corresponding
hook versions in .pre-commit-config.yaml to maintain consistency.
"""

import sys
import tomllib
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from typing import cast

PRECOMMIT_CONFIG_FILENAME = ".pre-commit-config.yaml"
DEFAULT_PYPROJECT_PATH = Path("pyproject.toml")
DEFAULT_CONFIG_PATH = Path(PRECOMMIT_CONFIG_FILENAME)


def extract_version(raw_value: object, prefix: str = "") -> str:
    """Extract version string from dependency specification.

    Handles both string versions ("^1.0.0") and dict versions ({"version": "^1.0.0"}).

    Args:
        raw_value: Dependency value from pyproject.toml (string or mapping).
        prefix: Optional prefix to prepend to the version string.

    Returns:
        Cleaned version string with optional prefix applied.
    """
    if isinstance(raw_value, Mapping):
        mapping = cast(Mapping[str, Any], raw_value)
        value = mapping.get("version", "")
        if isinstance(value, str):
            raw_value = value
        elif value is None:
            raw_value = ""
        else:
            raw_value = str(value)
    if not isinstance(raw_value, str):
        raise ValueError("Unsupported dependency version format")
    version = raw_value.lstrip("^")
    if prefix and not version.startswith(prefix):
        return f"{prefix}{version}"
    return version


def load_versions(pyproject_path: Path = DEFAULT_PYPROJECT_PATH) -> dict[str, str]:
    """Load tool versions from pyproject.toml.

    Args:
        pyproject_path: Path to the pyproject.toml file.

    Returns:
        Dictionary mapping tool names to their version strings.
    """
    data = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    dev_deps = data["tool"]["poetry"]["group"]["dev"]["dependencies"]
    return {
        "ruff": extract_version(dev_deps["ruff"], prefix="v"),
        "black": extract_version(dev_deps["black"]),
        "isort": extract_version(dev_deps["isort"]),
    }


def _validate_config_path(config_path: Path) -> None:
    """Validate that callers request the root pre-commit config file.

    Args:
        config_path: Path to the .pre-commit-config.yaml file.

    Raises:
        ValueError: If path contains traversal or targets wrong filename.
    """
    if ".." in config_path.parts:
        raise ValueError(f"Config path must not contain '..' components: {config_path}")
    if config_path.is_absolute() or len(config_path.parts) != 1:
        raise ValueError(f"Config path must stay in current working directory: {config_path}")
    if config_path.name != PRECOMMIT_CONFIG_FILENAME:
        raise ValueError(f"Config path must target {PRECOMMIT_CONFIG_FILENAME}: {config_path}")


def _precommit_config_path() -> Path:
    """Return the fixed config file path in the current working directory.

    Returns:
        The resolved absolute path.

    Raises:
        FileNotFoundError: If config file does not exist.
    """
    resolved = Path.cwd().resolve() / PRECOMMIT_CONFIG_FILENAME
    if not resolved.exists():
        raise FileNotFoundError(f"{PRECOMMIT_CONFIG_FILENAME} not found")
    return resolved


def _read_config(config_path: Path) -> tuple[Path, list[str]]:
    """Validate path, read .pre-commit-config.yaml, and return lines.

    Args:
        config_path: Path to the .pre-commit-config.yaml file.

    Returns:
        Tuple of (resolved path, list of lines from the file).

    Raises:
        FileNotFoundError: If config file does not exist.
        ValueError: If path contains traversal or targets wrong filename.
    """
    _validate_config_path(config_path)
    resolved = _precommit_config_path()
    return resolved, resolved.read_text(encoding="utf-8").splitlines()


def _write_config(content: str) -> None:
    """Write content to the fixed root .pre-commit-config.yaml file.

    Args:
        content: Full file content to write.

    Raises:
        FileNotFoundError: If config file does not exist.
    """
    _precommit_config_path().write_text(content, encoding="utf-8")


def update_config(
    expected_revs: dict[str, str],
    config_path: Path = DEFAULT_CONFIG_PATH,
) -> None:
    """Update pre-commit config with expected revisions.

    Args:
        expected_revs: Dictionary mapping tool names to expected revision strings.
        config_path: Path to the .pre-commit-config.yaml file.

    Raises:
        FileNotFoundError: If config file does not exist.
        ValueError: If path contains traversal or targets wrong filename.
    """
    _, lines = _read_config(config_path)
    updated_lines: list[str] = []
    active_repo: str | None = None
    for line in lines:
        stripped = line.strip()
        if stripped.startswith(("- repo:", "repo:")):
            updated_lines.append(line)
            if "ruff-pre-commit" in stripped:
                active_repo = "ruff"
            elif "psf/black" in stripped:
                active_repo = "black"
            elif "pycqa/isort" in stripped:
                active_repo = "isort"
            else:
                active_repo = None
        elif stripped.startswith("rev:") and active_repo:
            indent = line.split("rev:", 1)[0]
            updated_line = f"{indent}rev: {expected_revs[active_repo]}"
            updated_lines.append(updated_line)
            active_repo = None
        else:
            updated_lines.append(line)
    _write_config("\n".join(updated_lines) + "\n")


def main() -> int:
    """Entry point for sync_precommit script.

    Returns:
        Exit code: 0 on success, 1 on failure.
    """
    print("Synchronizing pre-commit hooks with pyproject.toml versions...")
    try:
        expected_revs = load_versions(DEFAULT_PYPROJECT_PATH)
        update_config(expected_revs, DEFAULT_CONFIG_PATH)
        print(f"Updated hooks: {', '.join(f'{k}={v}' for k, v in expected_revs.items())}")
        return 0
    except Exception as exc:
        print(f"Failed to synchronize pre-commit hooks: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
