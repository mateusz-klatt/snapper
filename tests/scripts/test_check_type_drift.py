"""Tests for type drift checker."""

import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from scripts.check_type_drift import backup_files
from scripts.check_type_drift import check_drift
from scripts.check_type_drift import get_files_to_check
from scripts.check_type_drift import main
from scripts.check_type_drift import regenerate_types
from scripts.check_type_drift import restore_files


class TestGetFilesToCheck:
    """Test suite for GetFilesToCheck functionality."""

    def test_returns_expected_files(self, tmp_path: Path) -> None:
        """Verify returns expected files.

        Given: A temporary path as project root,
        When: Getting files to check for type drift,
        Then: Returns paths for all generated type files (TS, Zod, Swift).
        """
        files = get_files_to_check(tmp_path)

        expected_names = [
            "api.generated.ts",
            "ws.generated.ts",
            "entities.generated.ts",
            "ws.generated.zod.ts",
            "api.generated.zod.ts",
            "WSMessages.swift",
            "APITypes.swift",
        ]
        for name in expected_names:
            assert any(f.name == name for f in files), f"Missing {name}"

    def test_returns_list_of_paths(self, tmp_path: Path) -> None:
        """Verify returns list of paths.

        Given: A temporary path as project root,
        When: Getting files to check for type drift,
        Then: Returns a list where all elements are Path objects.
        """
        files = get_files_to_check(tmp_path)

        assert isinstance(files, list)
        assert all(isinstance(f, Path) for f in files)


class TestBackupFiles:
    """Test suite for BackupFiles functionality."""

    def test_backs_up_existing_files(self, tmp_path: Path) -> None:
        """Verify backs up existing files.

        Given: A source file with content and an empty backup directory,
        When: Backing up the source file,
        Then: Creates backup copy with original content and returns mapping.
        """
        source_file = tmp_path / "source.txt"
        source_file.write_text("content")
        backup_dir = tmp_path / "backup"
        backup_dir.mkdir()

        backups = backup_files([source_file], backup_dir)

        assert source_file in backups
        assert backups[source_file].exists()
        assert backups[source_file].read_text() == "content"

    def test_skips_nonexistent_files(self, tmp_path: Path) -> None:
        """Verify skips nonexistent files.

        Given: A path to a file that does not exist,
        When: Attempting to back up the nonexistent file,
        Then: The file is not included in the backups dictionary.
        """
        nonexistent = tmp_path / "nonexistent.txt"
        backup_dir = tmp_path / "backup"
        backup_dir.mkdir()

        backups = backup_files([nonexistent], backup_dir)

        assert nonexistent not in backups

    def test_returns_empty_dict_for_empty_list(self, tmp_path: Path) -> None:
        """Verify returns empty dict for empty list.

        Given: An empty list of files to back up,
        When: Calling backup_files with no files,
        Then: Returns an empty dictionary.
        """
        backup_dir = tmp_path / "backup"
        backup_dir.mkdir()

        backups = backup_files([], backup_dir)

        assert backups == {}


class TestRestoreFiles:
    """Test suite for RestoreFiles functionality."""

    def test_restores_files_from_backup(self, tmp_path: Path) -> None:
        """Verify restores files from backup.

        Given: A modified file and its backup with original content,
        When: Restoring files from the backup mapping,
        Then: The file content is restored to the backup version.
        """
        original = tmp_path / "original.txt"
        backup = tmp_path / "backup.txt"
        original.write_text("modified")
        backup.write_text("original content")

        restore_files({original: backup})

        assert original.read_text() == "original content"

    def test_handles_empty_dict(self) -> None:
        """Verify handles empty dict.

        Given: An empty backups dictionary,
        When: Calling restore_files with no mappings,
        Then: Completes without raising any exception.
        """
        restore_files({})


class TestRegenerateTypes:
    """Test suite for RegenerateTypes functionality."""

    def test_runs_make_command(self, tmp_path: Path) -> None:
        """Verify runs make command.

        Given: A mocked subprocess.run,
        When: Regenerating types for a project root,
        Then: Executes 'make ui-gen-types ios-gen-types' with correct options.
        """
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess([], 0)

            regenerate_types(tmp_path)

            mock_run.assert_called_once_with(
                ["make", "ui-gen-types", "ios-gen-types"],
                check=False,
                cwd=tmp_path,
                capture_output=True,
                text=True,
            )

    def test_returns_completed_process(self, tmp_path: Path) -> None:
        """Verify returns completed process.

        Given: A mocked subprocess.run returning a CompletedProcess,
        When: Regenerating types,
        Then: Returns the CompletedProcess from the subprocess call.
        """
        with patch("subprocess.run") as mock_run:
            expected = subprocess.CompletedProcess([], 0, stdout="", stderr="")
            mock_run.return_value = expected

            result = regenerate_types(tmp_path)

            assert result == expected


class TestCheckDrift:
    """Test suite for CheckDrift functionality."""

    def test_detects_drift(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify detects drift.

        Given: A file with content different from its backup,
        When: Checking for drift between current and backed up files,
        Then: Returns the drifted file and prints drift detection message.
        """
        original = tmp_path / "original.txt"
        backup = tmp_path / "backup.txt"
        original.write_text("modified content")
        backup.write_text("original content")

        drifted = check_drift({original: backup}, tmp_path)

        assert original in drifted
        captured = capsys.readouterr()
        assert "Drift detected" in captured.out

    def test_no_drift_when_files_match(self, tmp_path: Path) -> None:
        """Verify no drift when files match.

        Given: A file with content identical to its backup,
        When: Checking for drift between current and backed up files,
        Then: Returns an empty list indicating no drift.
        """
        original = tmp_path / "original.txt"
        backup = tmp_path / "backup.txt"
        original.write_text("same content")
        backup.write_text("same content")

        drifted = check_drift({original: backup}, tmp_path)

        assert drifted == []

    def test_returns_empty_for_empty_backups(self, tmp_path: Path) -> None:
        """Verify returns empty for empty backups.

        Given: An empty backups dictionary,
        When: Checking for drift with no files to compare,
        Then: Returns an empty list.
        """
        drifted = check_drift({}, tmp_path)

        assert drifted == []


class TestMain:
    """Test suite for Main functionality."""

    def test_success_when_no_drift(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify success when no drift.

        Given: No files to check and successful type regeneration,
        When: Running the main type drift check,
        Then: Returns 0 and prints success message about types being up to date.
        """
        with (
            patch("scripts.check_type_drift.get_files_to_check", return_value=[]),
            patch("scripts.check_type_drift.regenerate_types") as mock_regen,
        ):
            mock_regen.return_value = subprocess.CompletedProcess([], 0)

            result = main()

        assert result == 0
        captured = capsys.readouterr()
        assert "Generated types are up to date" in captured.out

    def test_failure_when_drift_detected(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify failure when drift detected.

        Given: A file that gets modified during type regeneration,
        When: Running the main type drift check,
        Then: Returns 1 and prints type drift detected message.
        """
        test_file = tmp_path / "test.txt"
        test_file.write_text("original")

        with (
            patch("scripts.check_type_drift.get_files_to_check", return_value=[test_file]),
            patch("scripts.check_type_drift.regenerate_types") as mock_regen,
            patch("scripts.check_type_drift.check_drift") as mock_check,
        ):
            mock_regen.return_value = subprocess.CompletedProcess([], 0)
            mock_check.return_value = [test_file]

            result = main()

        assert result == 1
        captured = capsys.readouterr()
        assert "Type drift detected" in captured.out

    def test_failure_when_generation_fails(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify failure when generation fails.

        Given: Type regeneration returns non-zero exit code with error,
        When: Running the main type drift check,
        Then: Returns 1 and prints failed to generate types message.
        """
        with (
            patch("scripts.check_type_drift.get_files_to_check", return_value=[]),
            patch("scripts.check_type_drift.regenerate_types") as mock_regen,
        ):
            mock_regen.return_value = subprocess.CompletedProcess(
                [], 1, stdout="", stderr="Error message"
            )

            result = main()

        assert result == 1
        captured = capsys.readouterr()
        assert "Failed to generate types" in captured.out

    def test_restores_files_on_generation_failure(self, tmp_path: Path) -> None:
        """Verify restores files on generation failure.

        Given: A file that gets modified during failing type regeneration,
        When: Running main and regeneration fails after modifying file,
        Then: The restore_files function is called to restore from backups.
        """
        test_file = tmp_path / "test.txt"

        with (
            patch("scripts.check_type_drift.get_files_to_check", return_value=[test_file]),
            patch("scripts.check_type_drift.backup_files") as mock_backup,
            patch("scripts.check_type_drift.regenerate_types") as mock_regen,
            patch("scripts.check_type_drift.restore_files") as mock_restore,
        ):
            mock_backup.return_value = {test_file: tmp_path / "backup.txt"}
            mock_regen.return_value = subprocess.CompletedProcess([], 1, stderr="error")

            main()

            mock_restore.assert_called_once()

    def test_uses_script_parent_as_default_root(self) -> None:
        """Verify uses script parent as default root.

        Given: No project_root argument provided to main,
        When: Calling main without arguments,
        Then: Uses script's parent directory as Path for project root.
        """
        with (
            patch("scripts.check_type_drift.get_files_to_check", return_value=[]) as mock_files,
            patch("scripts.check_type_drift.regenerate_types") as mock_regen,
        ):
            mock_regen.return_value = subprocess.CompletedProcess([], 0)

            main()

            call_args = mock_files.call_args[0][0]
            assert isinstance(call_args, Path)
