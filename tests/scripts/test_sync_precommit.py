"""Tests for pre-commit config synchronizer."""

from pathlib import Path

import pytest

from scripts.sync_precommit import extract_version
from scripts.sync_precommit import load_versions
from scripts.sync_precommit import main
from scripts.sync_precommit import update_config


class TestExtractVersion:
    """Test suite for ExtractVersion functionality."""

    def test_extracts_from_string(self) -> None:
        """Verify extracts from string.

        Given: A version string with caret prefix "^1.2.3",
        When: extract_version is called with this string,
        Then: Returns the version "1.2.3" without caret.
        """
        result = extract_version("^1.2.3")

        assert result == "1.2.3"

    def test_extracts_from_string_without_caret(self) -> None:
        """Verify extracts from string without caret.

        Given: A plain version string "1.2.3" without any prefix,
        When: extract_version is called with this string,
        Then: Returns the same version string "1.2.3" unchanged.
        """
        result = extract_version("1.2.3")

        assert result == "1.2.3"

    def test_extracts_from_dict_with_version(self) -> None:
        """Verify extracts from dict with version.

        Given: A dict with "version" key containing "^2.0.0",
        When: extract_version is called with this dict,
        Then: Returns the version "2.0.0" extracted from the dict.
        """
        result = extract_version({"version": "^2.0.0"})

        assert result == "2.0.0"

    def test_extracts_from_dict_with_none_version(self) -> None:
        """Verify extracts from dict with none version.

        Given: A dict with "version" key set to None,
        When: extract_version is called with this dict,
        Then: Returns an empty string as fallback.
        """
        result = extract_version({"version": None})

        assert result == ""

    def test_extracts_from_dict_with_int_version(self) -> None:
        """Verify extracts from dict with int version.

        Given: A dict with "version" key containing integer 123,
        When: extract_version is called with this dict,
        Then: Returns the version as string "123".
        """
        result = extract_version({"version": 123})

        assert result == "123"

    def test_adds_prefix_when_not_present(self) -> None:
        """Verify adds prefix when not present.

        Given: A version string "^1.0.0" and prefix="v" parameter,
        When: extract_version is called with these arguments,
        Then: Returns "v1.0.0" with the prefix prepended.
        """
        result = extract_version("^1.0.0", prefix="v")

        assert result == "v1.0.0"

    def test_does_not_duplicate_prefix(self) -> None:
        """Verify does not duplicate prefix.

        Given: A version string "v1.0.0" already having "v" prefix,
        When: extract_version is called with prefix="v",
        Then: Returns "v1.0.0" without duplicating the prefix.
        """
        result = extract_version("v1.0.0", prefix="v")

        assert result == "v1.0.0"

    def test_raises_for_unsupported_format(self) -> None:
        """Verify raises for unsupported format.

        Given: An integer value 123 as version input,
        When: extract_version is called with this unsupported type,
        Then: Raises ValueError with "Unsupported dependency version format".
        """
        with pytest.raises(ValueError, match="Unsupported dependency version format"):
            extract_version(123)

    def test_raises_for_list(self) -> None:
        """Verify raises for list.

        Given: A list [1, 2, 3] as version input,
        When: extract_version is called with this unsupported type,
        Then: Raises ValueError with "Unsupported dependency version format".
        """
        with pytest.raises(ValueError, match="Unsupported dependency version format"):
            extract_version([1, 2, 3])


class TestLoadVersions:
    """Test suite for LoadVersions functionality."""

    def test_loads_versions_from_pyproject(self, tmp_path: Path) -> None:
        """Verify loads versions from pyproject.

        Given: A pyproject.toml with ruff, black, and isort dependencies,
        When: load_versions is called with this file path,
        Then: Returns dict with extracted versions (ruff with "v" prefix).
        """
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("""
[tool.poetry.group.dev.dependencies]
ruff = "^0.1.0"
black = "^23.0.0"
isort = "^5.12.0"
""")

        result = load_versions(pyproject)

        assert result == {
            "ruff": "v0.1.0",
            "black": "23.0.0",
            "isort": "5.12.0",
        }

    def test_loads_versions_with_extras(self, tmp_path: Path) -> None:
        """Verify loads versions with extras.

        Given: A pyproject.toml with black having extras dict format,
        When: load_versions is called with this file path,
        Then: Extracts versions from both string and dict formats.
        """
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("""
[tool.poetry.group.dev.dependencies]
ruff = "^0.2.0"
black = {extras = ["jupyter"], version = "^24.0.0"}
isort = "^5.13.0"
""")

        result = load_versions(pyproject)

        assert result == {
            "ruff": "v0.2.0",
            "black": "24.0.0",
            "isort": "5.13.0",
        }


class TestUpdateConfig:
    """Test suite for UpdateConfig functionality."""

    def test_updates_ruff_revision(self, tmp_path: Path) -> None:
        """Verify updates ruff revision.

        Given: A pre-commit config with ruff-pre-commit repo at v0.0.1,
        When: update_config is called with ruff version v0.5.0,
        Then: The config file is updated to rev: v0.5.0.
        """
        config = tmp_path / ".pre-commit-config.yaml"
        config.write_text("""repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.0.1
    hooks:
      - id: ruff
""")

        update_config({"ruff": "v0.5.0", "black": "24.0.0", "isort": "5.13.0"}, config)

        content = config.read_text()
        assert "rev: v0.5.0" in content

    def test_updates_black_revision(self, tmp_path: Path) -> None:
        """Verify updates black revision.

        Given: A pre-commit config with psf/black repo at rev 23.0.0,
        When: update_config is called with black version 24.0.0,
        Then: The config file is updated to rev: 24.0.0.
        """
        config = tmp_path / ".pre-commit-config.yaml"
        config.write_text("""repos:
  - repo: https://github.com/psf/black
    rev: 23.0.0
    hooks:
      - id: black
""")

        update_config({"ruff": "v0.5.0", "black": "24.0.0", "isort": "5.13.0"}, config)

        content = config.read_text()
        assert "rev: 24.0.0" in content

    def test_updates_isort_revision(self, tmp_path: Path) -> None:
        """Verify updates isort revision.

        Given: A pre-commit config with pycqa/isort repo at rev 5.12.0,
        When: update_config is called with isort version 5.13.0,
        Then: The config file is updated to rev: 5.13.0.
        """
        config = tmp_path / ".pre-commit-config.yaml"
        config.write_text("""repos:
  - repo: https://github.com/pycqa/isort
    rev: 5.12.0
    hooks:
      - id: isort
""")

        update_config({"ruff": "v0.5.0", "black": "24.0.0", "isort": "5.13.0"}, config)

        content = config.read_text()
        assert "rev: 5.13.0" in content

    def test_preserves_indentation(self, tmp_path: Path) -> None:
        """Verify preserves indentation.

        Given: A pre-commit config with 4-space indented rev line,
        When: update_config updates the ruff revision,
        Then: The indentation "    rev: v0.5.0" is preserved.
        """
        config = tmp_path / ".pre-commit-config.yaml"
        config.write_text("""repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.0.1
    hooks:
      - id: ruff
""")

        update_config({"ruff": "v0.5.0", "black": "24.0.0", "isort": "5.13.0"}, config)

        content = config.read_text()
        assert "    rev: v0.5.0" in content

    def test_ignores_unknown_repos(self, tmp_path: Path) -> None:
        """Verify ignores unknown repos.

        Given: A pre-commit config with pre-commit-hooks repo (not in mapping),
        When: update_config is called with version mappings,
        Then: The unknown repo's rev v4.0.0 remains unchanged.
        """
        config = tmp_path / ".pre-commit-config.yaml"
        config.write_text("""repos:
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.0.0
    hooks:
      - id: trailing-whitespace
""")

        update_config({"ruff": "v0.5.0", "black": "24.0.0", "isort": "5.13.0"}, config)

        content = config.read_text()
        assert "rev: v4.0.0" in content

    def test_raises_for_missing_config(self, tmp_path: Path) -> None:
        """Verify raises for missing config.

        Given: A path to a non-existent .pre-commit-config.yaml file,
        When: update_config is called with this path,
        Then: Raises FileNotFoundError with config file not found message.
        """
        config = tmp_path / ".pre-commit-config.yaml"

        with pytest.raises(FileNotFoundError, match=".pre-commit-config.yaml not found"):
            update_config({"ruff": "v0.5.0", "black": "24.0.0", "isort": "5.13.0"}, config)

    def test_raises_for_path_traversal(self, tmp_path: Path) -> None:
        """Verify raises for path with traversal components.

        Given: A config path containing '..' components,
        When: update_config is called with this path,
        Then: Raises ValueError indicating path traversal.
        """
        config = tmp_path / ".." / ".pre-commit-config.yaml"

        with pytest.raises(ValueError, match="must not contain"):
            update_config({"ruff": "v0.5.0", "black": "24.0.0", "isort": "5.13.0"}, config)

    def test_handles_repo_without_dash(self, tmp_path: Path) -> None:
        """Verify handles repo without dash.

        Given: A pre-commit config with malformed YAML (repo without list dash),
        When: update_config is called with version mappings,
        Then: Still updates the rev to v0.5.0 via regex pattern matching.
        """
        config = tmp_path / ".pre-commit-config.yaml"
        config.write_text("""repos:
  repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.0.1
""")

        update_config({"ruff": "v0.5.0", "black": "24.0.0", "isort": "5.13.0"}, config)

        content = config.read_text()
        assert "rev: v0.5.0" in content


class TestMain:
    """Test suite for Main functionality."""

    def test_successful_sync(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify successful sync.

        Given: Valid pyproject.toml and pre-commit config with outdated ruff,
        When: main is called with both file paths,
        Then: Returns 0 and updates config to match pyproject versions.
        """
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("""
[tool.poetry.group.dev.dependencies]
ruff = "^0.5.0"
black = "^24.0.0"
isort = "^5.13.0"
""")
        config = tmp_path / ".pre-commit-config.yaml"
        config.write_text("""repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.0.1
    hooks:
      - id: ruff
""")
        monkeypatch.setattr("scripts.sync_precommit.DEFAULT_PYPROJECT_PATH", pyproject)
        monkeypatch.setattr("scripts.sync_precommit.DEFAULT_CONFIG_PATH", config)

        result = main()

        assert result == 0
        content = config.read_text()
        assert "rev: v0.5.0" in content

    def test_raises_on_error(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify returns error code on invalid TOML.

        Given: A pyproject.toml with invalid TOML syntax,
        When: main is called attempting to parse this file,
        Then: Returns exit code 1 and prints failure message to stderr.
        """
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("invalid toml [[[")
        config = tmp_path / ".pre-commit-config.yaml"
        config.write_text("repos: []")
        monkeypatch.setattr("scripts.sync_precommit.DEFAULT_PYPROJECT_PATH", pyproject)
        monkeypatch.setattr("scripts.sync_precommit.DEFAULT_CONFIG_PATH", config)

        result = main()

        assert result == 1
        captured = capsys.readouterr()
        assert "Failed to synchronize" in captured.err
