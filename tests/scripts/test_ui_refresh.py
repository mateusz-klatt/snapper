"""Tests for UI dependency refresh script."""

import json
import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import call
from unittest.mock import patch

import pytest

from scripts.ui_refresh import ensure_corepack_installed
from scripts.ui_refresh import get_dependency_spec
from scripts.ui_refresh import install_dependencies
from scripts.ui_refresh import main
from scripts.ui_refresh import read_package_json
from scripts.ui_refresh import refresh_ui
from scripts.ui_refresh import remove_lock_file
from scripts.ui_refresh import remove_node_modules
from scripts.ui_refresh import restore_dependency_spec
from scripts.ui_refresh import run_cmd
from scripts.ui_refresh import upgrade_dependencies
from scripts.ui_refresh import upgrade_package_manager
from scripts.ui_refresh import write_package_json


class TestRunCmd:
    """Test suite for RunCmd functionality."""

    def test_runs_command_without_shell_on_unix(self) -> None:
        """Verify run_cmd executes without shell on Unix systems.

        Given: IS_WINDOWS is False (Unix environment),
        When: run_cmd is called with a command list,
        Then: subprocess.run is called without shell=True parameter.
        """
        with (
            patch("scripts.ui_refresh.IS_WINDOWS", False),
            patch("subprocess.run") as mock_run,
        ):
            mock_run.return_value = subprocess.CompletedProcess([], 0)

            run_cmd(["echo", "test"])

            mock_run.assert_called_once_with(["echo", "test"], check=False)

    def test_runs_command_with_shell_on_windows(self) -> None:
        """Verify run_cmd executes with shell on Windows systems.

        Given: IS_WINDOWS is True (Windows environment),
        When: run_cmd is called with a command list,
        Then: subprocess.run is called with shell=True parameter.
        """
        with (
            patch("scripts.ui_refresh.IS_WINDOWS", True),
            patch("subprocess.run") as mock_run,
        ):
            mock_run.return_value = subprocess.CompletedProcess([], 0)

            run_cmd(["echo", "test"])

            mock_run.assert_called_once_with(["echo", "test"], shell=True, check=False)

    def test_passes_check_parameter(self) -> None:
        """Verify run_cmd forwards the check parameter to subprocess.run.

        Given: Unix environment with mocked subprocess.run,
        When: run_cmd is called with check=True,
        Then: subprocess.run receives check=True in its arguments.
        """
        with (
            patch("scripts.ui_refresh.IS_WINDOWS", False),
            patch("subprocess.run") as mock_run,
        ):
            mock_run.return_value = subprocess.CompletedProcess([], 0)

            run_cmd(["echo", "test"], check=True)

            mock_run.assert_called_once_with(["echo", "test"], check=True)

    def test_passes_extra_kwargs(self) -> None:
        """Verify run_cmd forwards extra keyword arguments to subprocess.run.

        Given: Unix environment with mocked subprocess.run,
        When: run_cmd is called with capture_output=True and cwd='/tmp',
        Then: subprocess.run receives both extra kwargs along with check parameter.
        """
        with (
            patch("scripts.ui_refresh.IS_WINDOWS", False),
            patch("subprocess.run") as mock_run,
        ):
            mock_run.return_value = subprocess.CompletedProcess([], 0)

            run_cmd(["echo", "test"], capture_output=True, cwd="/tmp")

            mock_run.assert_called_once_with(
                ["echo", "test"], check=False, capture_output=True, cwd="/tmp"
            )


class TestRemoveLockFile:
    """Test suite for RemoveLockFile functionality."""

    def test_removes_existing_lock_file(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify remove_lock_file deletes existing pnpm-lock.yaml.

        Given: A pnpm-lock.yaml file exists in the target directory,
        When: remove_lock_file is called with that directory,
        Then: The file is deleted, True is returned, and removal message is printed.
        """
        lock_file = tmp_path / "pnpm-lock.yaml"
        lock_file.write_text("lockfile content")

        result = remove_lock_file(tmp_path)

        assert result is True
        assert not lock_file.exists()
        captured = capsys.readouterr()
        assert "Removing pnpm-lock.yaml" in captured.out

    def test_returns_false_when_no_lock_file(self, tmp_path: Path) -> None:
        """Verify remove_lock_file returns False when no lock file exists.

        Given: An empty directory with no pnpm-lock.yaml file,
        When: remove_lock_file is called with that directory,
        Then: False is returned indicating no file was removed.
        """
        result = remove_lock_file(tmp_path)

        assert result is False


class TestRemoveNodeModules:
    """Test suite for RemoveNodeModules functionality."""

    def test_removes_existing_node_modules(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify remove_node_modules deletes existing node_modules directory.

        Given: A node_modules directory with nested packages exists,
        When: remove_node_modules is called with the parent directory,
        Then: The entire node_modules tree is deleted, True is returned, and removal message is printed.
        """
        node_modules = tmp_path / "node_modules"
        node_modules.mkdir()
        (node_modules / "package").mkdir()
        (node_modules / "package" / "index.js").write_text("module.exports = {}")

        result = remove_node_modules(tmp_path)

        assert result is True
        assert not node_modules.exists()
        captured = capsys.readouterr()
        assert "Removing node_modules" in captured.out

    def test_returns_false_when_no_node_modules(self, tmp_path: Path) -> None:
        """Verify remove_node_modules returns False when no directory exists.

        Given: An empty directory with no node_modules subdirectory,
        When: remove_node_modules is called with that directory,
        Then: False is returned indicating no directory was removed.
        """
        result = remove_node_modules(tmp_path)

        assert result is False


class TestEnsureCorepackInstalled:
    """Test suite for EnsureCorepackInstalled functionality."""

    def test_skips_install_when_corepack_available(self) -> None:
        """Verify ensure_corepack_installed skips npm install when corepack exists.

        Given: corepack --version command succeeds (corepack is installed),
        When: ensure_corepack_installed is called,
        Then: Only the version check and corepack enable run, no npm install.
        """
        with patch("scripts.ui_refresh.run_cmd") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess([], 0)

            ensure_corepack_installed()

            assert mock_run.call_args_list == [
                call(["corepack", "--version"], capture_output=True, check=True),
                call(["corepack", "enable"], check=True),
            ]

    def test_installs_corepack_when_not_found(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify ensure_corepack_installed triggers npm install on FileNotFoundError.

        Given: corepack --version raises FileNotFoundError (corepack not installed),
        When: ensure_corepack_installed is called,
        Then: npm install -g corepack is run, followed by corepack enable.
        """
        call_count = 0

        def side_effect(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise FileNotFoundError()
            return subprocess.CompletedProcess(args, 0)

        with patch("scripts.ui_refresh.run_cmd", side_effect=side_effect):
            ensure_corepack_installed()

        captured = capsys.readouterr()
        assert "Installing corepack" in captured.out
        assert call_count == 3

    def test_installs_corepack_on_called_process_error(self) -> None:
        """Verify ensure_corepack_installed triggers npm install on CalledProcessError.

        Given: corepack --version raises CalledProcessError (corepack broken),
        When: ensure_corepack_installed is called,
        Then: Three commands run: version check, npm install, corepack enable.
        """
        call_count = 0

        def side_effect(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise subprocess.CalledProcessError(1, args)
            return subprocess.CompletedProcess(args, 0)

        with patch("scripts.ui_refresh.run_cmd", side_effect=side_effect):
            ensure_corepack_installed()

        assert call_count == 3


class TestUpgradePackageManager:
    """Test suite for UpgradePackageManager functionality."""

    def test_updates_package_manager_field(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify upgrade_package_manager updates packageManager to latest pnpm.

        Given: package.json with packageManager set to an older pnpm version,
        When: upgrade_package_manager is called and npm reports a newer version,
        Then: packageManager field is updated to the latest version.
        """
        ui_dir = tmp_path / "frontend"
        ui_dir.mkdir()
        package_json = ui_dir / "package.json"
        package_json.write_text(
            json.dumps({"packageManager": "pnpm@10.0.0"}, indent=2) + "\n",
            encoding="utf-8",
        )

        with patch("scripts.ui_refresh.run_cmd") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess([], 0, stdout="10.30.3\n")

            upgrade_package_manager(ui_dir)

        updated = json.loads(package_json.read_text(encoding="utf-8"))
        assert updated["packageManager"] == "pnpm@10.30.3"
        captured = capsys.readouterr()
        assert "Updating packageManager" in captured.out

    def test_skips_when_already_latest(self, tmp_path: Path) -> None:
        """Verify upgrade_package_manager is a no-op when version matches.

        Given: package.json with packageManager already at latest version,
        When: upgrade_package_manager is called,
        Then: package.json is not rewritten.
        """
        ui_dir = tmp_path / "frontend"
        ui_dir.mkdir()
        package_json = ui_dir / "package.json"
        content = json.dumps({"packageManager": "pnpm@10.30.3"}, indent=2) + "\n"
        package_json.write_text(content, encoding="utf-8")

        with (
            patch("scripts.ui_refresh.run_cmd") as mock_run,
            patch("scripts.ui_refresh.write_package_json") as mock_write,
        ):
            mock_run.return_value = subprocess.CompletedProcess([], 0, stdout="10.30.3\n")

            upgrade_package_manager(ui_dir)

            mock_write.assert_not_called()

    def test_skips_when_version_unavailable(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify upgrade_package_manager skips when npm view returns empty.

        Given: npm view pnpm version returns empty output,
        When: upgrade_package_manager is called,
        Then: Prints skip message and does not modify package.json.
        """
        ui_dir = tmp_path / "frontend"
        ui_dir.mkdir()

        with (
            patch("scripts.ui_refresh.run_cmd") as mock_run,
            patch("scripts.ui_refresh.write_package_json") as mock_write,
        ):
            mock_run.return_value = subprocess.CompletedProcess([], 0, stdout="")

            upgrade_package_manager(ui_dir)

            mock_write.assert_not_called()
            captured = capsys.readouterr()
            assert "Could not determine" in captured.out


class TestUpgradeDependencies:
    """Test suite for UpgradeDependencies functionality."""

    def test_skips_upgrade_when_no_package_json(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify upgrade_dependencies skips when package.json is missing.

        Given: A directory without package.json,
        When: upgrade_dependencies is called with that directory,
        Then: Prints skip message and returns without running pnpm.
        """
        with patch("scripts.ui_refresh.run_cmd") as mock_run:
            upgrade_dependencies(tmp_path)

            mock_run.assert_not_called()
            captured = capsys.readouterr()
            assert "Skipping dependency upgrade" in captured.out

    def test_runs_pnpm_upgrade_when_package_json_exists(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify upgrade_dependencies runs pnpm up when package.json exists.

        Given: A directory with package.json,
        When: upgrade_dependencies is called,
        Then: Runs 'pnpm up --latest' with cwd set to target directory.
        """
        package_json = tmp_path / "package.json"
        package_json.write_text("{}")

        with patch("scripts.ui_refresh.run_cmd") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess([], 0)

            upgrade_dependencies(tmp_path)

            mock_run.assert_called_once_with(["pnpm", "up", "--latest"], cwd=tmp_path, check=True)
            captured = capsys.readouterr()
            assert "Upgrading UI direct dependencies" in captured.out

    def test_restores_protected_deps_after_latest_upgrade(self, tmp_path: Path) -> None:
        """Verify upgrade_dependencies restores protected deps after pnpm up --latest.

        Given: A package.json with eslint and @eslint/js pinned to 9.x ranges,
        When: upgrade_dependencies runs and the upgrade step rewrites both to 10.x,
        Then: package.json is restored to the original version specs.
        """
        package_json = tmp_path / "package.json"
        package_json.write_text(
            json.dumps(
                {
                    "name": "snapper-ui",
                    "devDependencies": {
                        "eslint": "^9.39.2",
                        "@eslint/js": "^9.39.2",
                        "eslint-plugin-react": "^7.37.5",
                    },
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

        def side_effect(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
            _ = kwargs
            if args == ["pnpm", "up", "--latest"]:
                data = json.loads(package_json.read_text(encoding="utf-8"))
                data["devDependencies"]["eslint"] = "^10.0.0"
                data["devDependencies"]["@eslint/js"] = "^10.0.0"
                package_json.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
            return subprocess.CompletedProcess(args, 0)

        with patch("scripts.ui_refresh.run_cmd", side_effect=side_effect) as mock_run:
            upgrade_dependencies(tmp_path)

        updated = json.loads(package_json.read_text(encoding="utf-8"))
        assert updated["devDependencies"]["eslint"] == "^9.39.2"
        assert updated["devDependencies"]["@eslint/js"] == "^9.39.2"
        assert mock_run.call_args_list == [
            call(["pnpm", "up", "--latest"], cwd=tmp_path, check=True),
            call(["pnpm", "up"], cwd=tmp_path, check=True),
        ]

    def test_does_not_write_package_json_when_no_change_needed(self, tmp_path: Path) -> None:
        """Verify upgrade_dependencies does not rewrite package.json when nothing changes.

        Given: package.json includes eslint in a protected range,
        When: pnpm up --latest does not modify eslint spec,
        Then: write_package_json is not called.
        """
        package_json = tmp_path / "package.json"
        package_json.write_text(
            json.dumps(
                {"devDependencies": {"eslint": "^9.39.2", "@eslint/js": "^9.39.2"}},
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

        with (
            patch("scripts.ui_refresh.run_cmd") as mock_run,
            patch("scripts.ui_refresh.write_package_json") as mock_write,
        ):
            mock_run.return_value = subprocess.CompletedProcess([], 0)

            upgrade_dependencies(tmp_path)

            mock_write.assert_not_called()
            assert mock_run.call_count == 2
            assert mock_run.call_args_list[1] == call(["pnpm", "up"], cwd=tmp_path, check=True)


class TestPackageJsonHelpers:
    """Test suite for package.json helper functions."""

    def test_read_package_json_raises_on_non_object(self, tmp_path: Path) -> None:
        """Verify read_package_json rejects non-object JSON.

        Given: A package.json file containing a JSON array,
        When: read_package_json is called,
        Then: ValueError is raised.
        """
        package_json = tmp_path / "package.json"
        package_json.write_text("[]\n", encoding="utf-8")

        with pytest.raises(ValueError):
            read_package_json(package_json)

    def test_write_package_json_roundtrips(self, tmp_path: Path) -> None:
        """Verify write_package_json writes valid JSON that can be read back.

        Given: A dictionary to write,
        When: write_package_json writes it,
        Then: json.loads reads the same structure.
        """
        package_json = tmp_path / "package.json"
        data: dict[str, object] = {"name": "snapper-ui", "devDependencies": {"eslint": "^9.39.2"}}
        write_package_json(package_json, data)

        loaded = json.loads(package_json.read_text(encoding="utf-8"))
        assert loaded == data

    def test_restore_dependency_spec_moves_between_sections(self) -> None:
        """Verify restore_dependency_spec moves dependency to target section.

        Given: Dependency exists in dependencies,
        When: restoring it into devDependencies with a specific version,
        Then: it is removed from dependencies and present in devDependencies.
        """
        package_data: dict[str, Any] = {
            "dependencies": {"eslint": "^10.0.0"},
            "devDependencies": {},
        }

        modified = restore_dependency_spec(package_data, "eslint", "devDependencies", "^9.39.2")

        assert modified is True
        assert "eslint" not in package_data["dependencies"]
        assert package_data["devDependencies"]["eslint"] == "^9.39.2"

    def test_get_dependency_spec_returns_none_for_non_string_spec(self) -> None:
        """Verify get_dependency_spec returns a section with None for non-string values.

        Given: A dependency present with a non-string value,
        When: get_dependency_spec is called,
        Then: It returns the section name and a None spec.
        """
        package_data: dict[str, Any] = {"dependencies": {"eslint": 123}}
        section, spec = get_dependency_spec(package_data, "eslint")
        assert section == "dependencies"
        assert spec is None

    def test_restore_dependency_spec_creates_target_section_when_missing(self) -> None:
        """Verify restore_dependency_spec creates the target section when absent.

        Given: Target section does not exist,
        When: restoring a dependency into that section,
        Then: The section is created and dependency spec is set.
        """
        package_data: dict[str, Any] = {"dependencies": {}}
        modified = restore_dependency_spec(package_data, "eslint", "devDependencies", "^9.39.2")
        assert modified is True
        assert package_data["devDependencies"]["eslint"] == "^9.39.2"

    def test_restore_dependency_spec_is_noop_when_already_matches(self) -> None:
        """Verify restore_dependency_spec returns False when no change is needed.

        Given: Dependency already exists in the target section with the same spec,
        When: restore_dependency_spec is called,
        Then: It returns False and leaves data unchanged.
        """
        package_data: dict[str, Any] = {"devDependencies": {"eslint": "^9.39.2"}}
        modified = restore_dependency_spec(package_data, "eslint", "devDependencies", "^9.39.2")
        assert modified is False
        assert package_data["devDependencies"]["eslint"] == "^9.39.2"

    def test_restore_dependency_spec_does_not_delete_when_dependency_disappears(self) -> None:
        """Verify restore_dependency_spec handles missing key at delete time.

        Given: A mapping that reports a dependency for discovery but not for deletion,
        When: restore_dependency_spec attempts to move it,
        Then: It skips deletion safely and still applies the target spec.
        """

        class FlakyDict(dict[str, Any]):
            """Dictionary that changes get() behavior after first access."""

            def __init__(self, *args: object, **kwargs: object) -> None:
                super().__init__(*args, **kwargs)
                self._dependencies_reads = 0

            def get(self, key: str, default: Any = None) -> Any:
                if key == "dependencies":
                    self._dependencies_reads += 1
                    if self._dependencies_reads >= 2:
                        return default
                return super().get(key, default)

        package_data: dict[str, Any] = FlakyDict({"dependencies": {"eslint": "^10.0.0"}})

        modified = restore_dependency_spec(package_data, "eslint", "devDependencies", "^9.39.2")

        assert modified is True
        assert package_data["devDependencies"]["eslint"] == "^9.39.2"


class TestInstallDependencies:
    """Test suite for InstallDependencies functionality."""

    def test_runs_pnpm_install(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify install_dependencies runs pnpm install in target directory.

        Given: A target directory path and mocked run_cmd,
        When: install_dependencies is called with that directory,
        Then: 'pnpm install' is executed with cwd set to target and check=True, with progress messages printed.
        """
        with patch("scripts.ui_refresh.run_cmd") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess([], 0)

            install_dependencies(tmp_path)

            mock_run.assert_called_once_with(["pnpm", "install"], cwd=tmp_path, check=True)
            captured = capsys.readouterr()
            assert "Installing UI dependencies" in captured.out
            assert "UI dependencies refreshed" in captured.out


class TestRefreshUi:
    """Test suite for RefreshUi functionality."""

    def test_full_refresh_flow(self, tmp_path: Path) -> None:
        """Verify refresh_ui performs complete cleanup and reinstall.

        Given: A frontend directory with pnpm-lock.yaml and node_modules,
        When: refresh_ui is called with the project root,
        Then: Both lock file and node_modules are removed before reinstalling dependencies.
        """
        ui_dir = tmp_path / "frontend"
        ui_dir.mkdir()
        package_json = ui_dir / "package.json"
        package_json.write_text(
            json.dumps({"packageManager": "pnpm@10.30.3"}, indent=2) + "\n",
            encoding="utf-8",
        )
        lock_file = ui_dir / "pnpm-lock.yaml"
        lock_file.write_text("content")
        node_modules = ui_dir / "node_modules"
        node_modules.mkdir()

        with patch("scripts.ui_refresh.run_cmd") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess([], 0, stdout="10.30.3\n")

            refresh_ui(tmp_path)

            assert not lock_file.exists()
            assert not node_modules.exists()

    def test_refresh_with_default_root(self) -> None:
        """Verify refresh_ui works when called without explicit root path.

        Given: All refresh helper functions are mocked,
        When: refresh_ui is called without arguments,
        Then: The function executes successfully using default project root.
        """
        with (
            patch("scripts.ui_refresh.ensure_corepack_installed"),
            patch("scripts.ui_refresh.upgrade_package_manager"),
            patch("scripts.ui_refresh.upgrade_dependencies"),
            patch("scripts.ui_refresh.remove_lock_file"),
            patch("scripts.ui_refresh.remove_node_modules"),
            patch("scripts.ui_refresh.install_dependencies"),
        ):
            refresh_ui()


class TestMain:
    """Test suite for Main functionality."""

    def test_returns_zero(self) -> None:
        """Verify main returns exit code 0 on success.

        Given: refresh_ui function is mocked to succeed,
        When: main is called,
        Then: Exit code 0 is returned indicating successful execution.
        """
        with patch("scripts.ui_refresh.refresh_ui"):
            result = main()

        assert result == 0
