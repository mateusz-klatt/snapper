"""Tests for snapper-mcp dependency refresh script."""

import json
import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import call
from unittest.mock import patch

import pytest

from scripts.mcp_refresh import PROTECTED_DEPENDENCIES
from scripts.mcp_refresh import install_dependencies_npm
from scripts.mcp_refresh import main
from scripts.mcp_refresh import refresh_mcp
from scripts.mcp_refresh import remove_npm_lock_file
from scripts.mcp_refresh import upgrade_dependencies_npm


class TestRemoveNpmLockFile:
    """Test suite for RemoveNpmLockFile functionality."""

    def test_removes_existing_lock_file(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify remove_npm_lock_file deletes existing package-lock.json.

        Given: A package-lock.json exists in the snapper-mcp directory,
        When: remove_npm_lock_file is called with that directory,
        Then: The file is deleted, True is returned, and removal message is printed.
        """
        lock_file = tmp_path / "package-lock.json"
        lock_file.write_text("{}")

        result = remove_npm_lock_file(tmp_path)

        assert result is True
        assert not lock_file.exists()
        captured = capsys.readouterr()
        assert "Removing package-lock.json" in captured.out

    def test_returns_false_when_no_lock_file(self, tmp_path: Path) -> None:
        """Verify remove_npm_lock_file returns False when no lock file exists.

        Given: An empty directory with no package-lock.json,
        When: remove_npm_lock_file is called with that directory,
        Then: False is returned indicating nothing was removed.
        """
        result = remove_npm_lock_file(tmp_path)

        assert result is False


class TestUpgradeDependenciesNpm:
    """Test suite for UpgradeDependenciesNpm functionality."""

    def test_skips_upgrade_when_no_package_json(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify upgrade_dependencies_npm skips when package.json is missing.

        Given: A directory without package.json,
        When: upgrade_dependencies_npm is called with that directory,
        Then: A skip message is printed and no commands run.
        """
        with patch("scripts.mcp_refresh.run_cmd") as mock_run:
            upgrade_dependencies_npm(tmp_path)

            mock_run.assert_not_called()
            captured = capsys.readouterr()
            assert "Skipping dependency upgrade" in captured.out

    def test_runs_npm_check_updates_when_package_json_exists(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify upgrade_dependencies_npm invokes npm-check-updates with protected reject list.

        Given: A directory with package.json,
        When: upgrade_dependencies_npm is called,
        Then: ``npx --yes npm-check-updates -u --reject <protected>`` runs in cwd.
        """
        package_json = tmp_path / "package.json"
        package_json.write_text("{}")

        with patch("scripts.mcp_refresh.run_cmd") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess([], 0)

            upgrade_dependencies_npm(tmp_path)

            mock_run.assert_called_once_with(
                [
                    "npx",
                    "--yes",
                    "npm-check-updates",
                    "-u",
                    "--reject",
                    ",".join(PROTECTED_DEPENDENCIES),
                ],
                cwd=tmp_path,
                check=True,
            )
            captured = capsys.readouterr()
            assert "Upgrading snapper-mcp direct dependencies" in captured.out

    def test_restores_downgraded_dep_after_ncu(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify upgrade_dependencies_npm reverts a dependency ncu lowered below the committed spec.

        Given: package.json pins knip ^6.18.0 and npm-check-updates rewrites it
            down to ^6.17.2 (a stale ``latest`` dist-tag),
        When: upgrade_dependencies_npm runs,
        Then: The downgrade guard restores ^6.18.0, the corrected package.json is
            written, and a prevention message is printed.
        """
        package_json = tmp_path / "package.json"
        package_json.write_text(json.dumps({"devDependencies": {"knip": "^6.18.0"}}) + "\n")

        def fake_ncu(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
            package_json.write_text(json.dumps({"devDependencies": {"knip": "^6.17.2"}}) + "\n")
            return subprocess.CompletedProcess([], 0)

        with patch("scripts.mcp_refresh.run_cmd", side_effect=fake_ncu) as mock_run:
            upgrade_dependencies_npm(tmp_path)

        restored = json.loads(package_json.read_text(encoding="utf-8"))
        assert restored["devDependencies"]["knip"] == "^6.18.0"
        mock_run.assert_called_once_with(
            [
                "npx",
                "--yes",
                "npm-check-updates",
                "-u",
                "--reject",
                ",".join(PROTECTED_DEPENDENCIES),
            ],
            cwd=tmp_path,
            check=True,
        )
        captured = capsys.readouterr()
        assert "Preventing downgrade of knip" in captured.out
        assert "Restoring non-regressing snapper-mcp dependency version ranges" in captured.out

    def test_keeps_upgraded_dep_when_ncu_advances(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify upgrade_dependencies_npm leaves a genuine ncu upgrade untouched.

        Given: package.json pins knip ^6.17.2 and npm-check-updates advances it
            to ^6.18.0,
        When: upgrade_dependencies_npm runs,
        Then: The guard keeps the higher version with no restore and no prevention message.
        """
        package_json = tmp_path / "package.json"
        package_json.write_text(json.dumps({"devDependencies": {"knip": "^6.17.2"}}) + "\n")

        def fake_ncu(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
            package_json.write_text(json.dumps({"devDependencies": {"knip": "^6.18.0"}}) + "\n")
            return subprocess.CompletedProcess([], 0)

        with patch("scripts.mcp_refresh.run_cmd", side_effect=fake_ncu):
            upgrade_dependencies_npm(tmp_path)

        upgraded = json.loads(package_json.read_text(encoding="utf-8"))
        assert upgraded["devDependencies"]["knip"] == "^6.18.0"
        captured = capsys.readouterr()
        assert "Preventing downgrade" not in captured.out
        assert "Restoring non-regressing" not in captured.out


class TestInstallDependenciesNpm:
    """Test suite for InstallDependenciesNpm functionality."""

    def test_runs_npm_install(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify install_dependencies_npm runs npm install in target directory.

        Given: A target directory and mocked run_cmd,
        When: install_dependencies_npm is called with that directory,
        Then: ``npm install`` is executed with cwd set to the target and check=True.
        """
        with patch("scripts.mcp_refresh.run_cmd") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess([], 0)

            install_dependencies_npm(tmp_path)

            mock_run.assert_called_once_with(["npm", "install"], cwd=tmp_path, check=True)
            captured = capsys.readouterr()
            assert "Installing snapper-mcp dependencies" in captured.out
            assert "snapper-mcp dependencies refreshed" in captured.out


class TestRefreshMcp:
    """Test suite for RefreshMcp functionality."""

    def test_skips_when_mcp_dir_missing(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify refresh_mcp prints a skip message when the integration is absent.

        Given: A project root with no integrations/snapper-mcp directory,
        When: refresh_mcp is called with that root,
        Then: A skip message is printed and no helpers run.
        """
        with (
            patch("scripts.mcp_refresh.upgrade_dependencies_npm") as mock_upgrade,
            patch("scripts.mcp_refresh.remove_npm_lock_file") as mock_lock,
            patch("scripts.mcp_refresh.remove_node_modules") as mock_nm,
            patch("scripts.mcp_refresh.install_dependencies_npm") as mock_install,
        ):
            refresh_mcp(tmp_path)

            mock_upgrade.assert_not_called()
            mock_lock.assert_not_called()
            mock_nm.assert_not_called()
            mock_install.assert_not_called()
        captured = capsys.readouterr()
        assert "Skipping snapper-mcp refresh" in captured.out

    def test_full_refresh_flow(self, tmp_path: Path) -> None:
        """Verify refresh_mcp runs all stages in order when the integration exists.

        Given: A project root with integrations/snapper-mcp/package.json + lock + node_modules,
        When: refresh_mcp is called with that root,
        Then: Lock file and node_modules are removed before npm install runs.
        """
        mcp_dir = tmp_path / "integrations" / "snapper-mcp"
        mcp_dir.mkdir(parents=True)
        (mcp_dir / "package.json").write_text("{}")
        lock_file = mcp_dir / "package-lock.json"
        lock_file.write_text("{}")
        node_modules = mcp_dir / "node_modules"
        node_modules.mkdir()

        with patch("scripts.mcp_refresh.run_cmd") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess([], 0)

            refresh_mcp(tmp_path)

            assert not lock_file.exists()
            assert not node_modules.exists()
            assert mock_run.call_args_list == [
                call(
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
                ),
                call(["npm", "install"], cwd=mcp_dir, check=True),
            ]

    def test_refresh_with_default_root(self) -> None:
        """Verify refresh_mcp resolves a default root when called without arguments.

        Given: All refresh helpers are mocked,
        When: refresh_mcp is called without arguments,
        Then: The function executes without error using the default project root.
        """
        with (
            patch("scripts.mcp_refresh.upgrade_dependencies_npm"),
            patch("scripts.mcp_refresh.remove_npm_lock_file"),
            patch("scripts.mcp_refresh.remove_node_modules"),
            patch("scripts.mcp_refresh.install_dependencies_npm"),
        ):
            refresh_mcp()


class TestMain:
    """Test suite for Main entry point."""

    def test_returns_zero(self) -> None:
        """Verify main returns exit code 0 on success.

        Given: refresh_mcp is mocked to succeed,
        When: main is called,
        Then: Exit code 0 is returned.
        """
        with patch("scripts.mcp_refresh.refresh_mcp"):
            result = main()

        assert result == 0
