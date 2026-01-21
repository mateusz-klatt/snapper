"""Tests for UI dependency refresh script."""

import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from scripts.ui_refresh import ensure_pnpm_installed
from scripts.ui_refresh import install_dependencies
from scripts.ui_refresh import main
from scripts.ui_refresh import refresh_ui
from scripts.ui_refresh import remove_lock_file
from scripts.ui_refresh import remove_node_modules
from scripts.ui_refresh import run_cmd


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


class TestEnsurePnpmInstalled:
    """Test suite for EnsurePnpmInstalled functionality."""

    def test_does_nothing_when_pnpm_available(self) -> None:
        """Verify ensure_pnpm_installed skips installation when pnpm exists.

        Given: pnpm --version command succeeds (pnpm is installed),
        When: ensure_pnpm_installed is called,
        Then: Only the version check is performed, no installation commands run.
        """
        with patch("scripts.ui_refresh.run_cmd") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess([], 0)

            ensure_pnpm_installed()

            mock_run.assert_called_once_with(["pnpm", "--version"], capture_output=True, check=True)

    def test_installs_pnpm_when_not_found(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify ensure_pnpm_installed triggers installation on FileNotFoundError.

        Given: pnpm --version command raises FileNotFoundError (pnpm not installed),
        When: ensure_pnpm_installed is called,
        Then: Installation via corepack is triggered and info message is printed.
        """
        call_count = 0

        def side_effect(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise FileNotFoundError()
            return subprocess.CompletedProcess(args, 0)

        with patch("scripts.ui_refresh.run_cmd", side_effect=side_effect):
            ensure_pnpm_installed()

        captured = capsys.readouterr()
        assert "Installing pnpm via corepack" in captured.out

    def test_installs_pnpm_on_called_process_error(self) -> None:
        """Verify ensure_pnpm_installed triggers installation on CalledProcessError.

        Given: pnpm --version command raises CalledProcessError (pnpm broken),
        When: ensure_pnpm_installed is called,
        Then: Three commands run: version check, corepack enable, and corepack prepare.
        """
        call_count = 0

        def side_effect(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise subprocess.CalledProcessError(1, args)
            return subprocess.CompletedProcess(args, 0)

        with patch("scripts.ui_refresh.run_cmd", side_effect=side_effect):
            ensure_pnpm_installed()

        assert call_count == 3


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
        lock_file = ui_dir / "pnpm-lock.yaml"
        lock_file.write_text("content")
        node_modules = ui_dir / "node_modules"
        node_modules.mkdir()

        with patch("scripts.ui_refresh.run_cmd") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess([], 0)

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
            patch("scripts.ui_refresh.remove_lock_file"),
            patch("scripts.ui_refresh.remove_node_modules"),
            patch("scripts.ui_refresh.ensure_pnpm_installed"),
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
