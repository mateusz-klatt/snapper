"""Tests for project cleanup script."""

from pathlib import Path
from unittest.mock import patch

import pytest

from scripts.clean import clean
from scripts.clean import clean_egg_info
from scripts.clean import clean_pyc_files
from scripts.clean import clean_pycache
from scripts.clean import get_dirs_to_clean
from scripts.clean import get_files_to_clean
from scripts.clean import main
from scripts.clean import remove_directory
from scripts.clean import remove_file


class TestGetDirsToClean:
    """Test suite for GetDirsToClean functionality."""

    def test_returns_expected_directories(self, tmp_path: Path) -> None:
        """Verify returns expected directories.

        Given: A temporary directory as the project root,
        When: get_dirs_to_clean is called with the root path,
        Then: Returns list containing all expected cache and build directories
            (.venv, .pytest_cache, .mypy_cache, .ruff_cache, htmlcov,
            frontend/dist, frontend/node_modules, frontend/coverage).
        """
        dirs = get_dirs_to_clean(tmp_path)

        assert tmp_path / ".venv" in dirs
        assert tmp_path / ".pytest_cache" in dirs
        assert tmp_path / ".mypy_cache" in dirs
        assert tmp_path / ".ruff_cache" in dirs
        assert tmp_path / "htmlcov" in dirs
        assert tmp_path / "frontend" / "dist" in dirs
        assert tmp_path / "frontend" / "node_modules" in dirs
        assert tmp_path / "frontend" / "coverage" in dirs

    def test_returns_list_of_paths(self, tmp_path: Path) -> None:
        """Verify returns list of paths.

        Given: A temporary directory as the project root,
        When: get_dirs_to_clean is called,
        Then: Returns a list where all elements are Path objects.
        """
        dirs = get_dirs_to_clean(tmp_path)

        assert isinstance(dirs, list)
        assert all(isinstance(d, Path) for d in dirs)


class TestGetFilesToClean:
    """Test suite for GetFilesToClean functionality."""

    def test_returns_expected_files(self, tmp_path: Path) -> None:
        """Verify returns expected files.

        Given: A temporary directory as the project root,
        When: get_files_to_clean is called with the root path,
        Then: Returns list containing expected files to clean
            (.coverage, frontend/.eslintcache).
        """
        files = get_files_to_clean(tmp_path)

        assert tmp_path / ".coverage" in files
        assert tmp_path / "frontend" / ".eslintcache" in files

    def test_returns_list_of_paths(self, tmp_path: Path) -> None:
        """Verify returns list of paths.

        Given: A temporary directory as the project root,
        When: get_files_to_clean is called,
        Then: Returns a list where all elements are Path objects.
        """
        files = get_files_to_clean(tmp_path)

        assert isinstance(files, list)
        assert all(isinstance(f, Path) for f in files)


class TestRemoveDirectory:
    """Test suite for RemoveDirectory functionality."""

    def test_removes_existing_directory(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify removes existing directory.

        Given: An existing directory with a file inside it,
        When: remove_directory is called on that directory,
        Then: Returns True, directory is deleted recursively,
            and prints message showing relative path removal.
        """
        target = tmp_path / "target_dir"
        target.mkdir()
        (target / "file.txt").write_text("content")

        result = remove_directory(target, tmp_path)

        assert result is True
        assert not target.exists()
        captured = capsys.readouterr()
        assert "Removing target_dir" in captured.out

    def test_returns_false_for_nonexistent_directory(self, tmp_path: Path) -> None:
        """Verify returns false for nonexistent directory.

        Given: A path pointing to a non-existent directory,
        When: remove_directory is called on that path,
        Then: Returns False without raising an error.
        """
        target = tmp_path / "nonexistent"

        result = remove_directory(target, tmp_path)

        assert result is False


class TestRemoveFile:
    """Test suite for RemoveFile functionality."""

    def test_removes_existing_file(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify removes existing file.

        Given: An existing file with content,
        When: remove_file is called on that file,
        Then: Returns True, file is deleted,
            and prints message showing filename removal.
        """
        target = tmp_path / "target_file.txt"
        target.write_text("content")

        result = remove_file(target, tmp_path)

        assert result is True
        assert not target.exists()
        captured = capsys.readouterr()
        assert "Removing target_file.txt" in captured.out

    def test_returns_false_for_nonexistent_file(self, tmp_path: Path) -> None:
        """Verify returns false for nonexistent file.

        Given: A path pointing to a non-existent file,
        When: remove_file is called on that path,
        Then: Returns False without raising an error.
        """
        target = tmp_path / "nonexistent.txt"

        result = remove_file(target, tmp_path)

        assert result is False


class TestCleanPycache:
    """Test suite for CleanPycache functionality."""

    def test_removes_pycache_directories(self, tmp_path: Path) -> None:
        """Verify removes pycache directories.

        Given: Multiple __pycache__ directories at root and nested levels,
        When: clean_pycache is called on the root path,
        Then: Returns count of 2, and all __pycache__ directories are removed.
        """
        cache1 = tmp_path / "__pycache__"
        cache2 = tmp_path / "subdir" / "__pycache__"
        cache1.mkdir()
        cache2.mkdir(parents=True)

        count = clean_pycache(tmp_path)

        assert count == 2
        assert not cache1.exists()
        assert not cache2.exists()

    def test_returns_zero_when_no_pycache(self, tmp_path: Path) -> None:
        """Verify returns zero when no pycache.

        Given: An empty directory with no __pycache__ directories,
        When: clean_pycache is called,
        Then: Returns 0 indicating no directories were removed.
        """
        count = clean_pycache(tmp_path)

        assert count == 0

    def test_skips_files_named_pycache(self, tmp_path: Path) -> None:
        """Verify skips files named pycache.

        Given: A regular file named __pycache__ (not a directory),
        When: clean_pycache is called,
        Then: Returns 0 and the file remains untouched.
        """
        pycache_file = tmp_path / "__pycache__"
        pycache_file.write_text("not a directory")

        count = clean_pycache(tmp_path)

        assert count == 0
        assert pycache_file.exists()


class TestCleanPycFiles:
    """Test suite for CleanPycFiles functionality."""

    def test_removes_pyc_files(self, tmp_path: Path) -> None:
        """Verify removes pyc files.

        Given: Multiple .pyc files at root and in subdirectory,
        When: clean_pyc_files is called on the root path,
        Then: Returns count of 2, and all .pyc files are deleted.
        """
        pyc1 = tmp_path / "file1.pyc"
        pyc2 = tmp_path / "subdir" / "file2.pyc"
        pyc1.write_text("")
        pyc2.parent.mkdir()
        pyc2.write_text("")

        count = clean_pyc_files(tmp_path)

        assert count == 2
        assert not pyc1.exists()
        assert not pyc2.exists()

    def test_returns_zero_when_no_pyc_files(self, tmp_path: Path) -> None:
        """Verify returns zero when no pyc files.

        Given: An empty directory with no .pyc files,
        When: clean_pyc_files is called,
        Then: Returns 0 indicating no files were removed.
        """
        count = clean_pyc_files(tmp_path)

        assert count == 0


class TestCleanEggInfo:
    """Test suite for CleanEggInfo functionality."""

    def test_removes_egg_info_directories(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify removes egg info directories.

        Given: A .egg-info directory exists in the project,
        When: clean_egg_info is called,
        Then: Returns count of 1, directory is removed,
            and prints message showing the removed directory name.
        """
        egg = tmp_path / "package.egg-info"
        egg.mkdir()

        count = clean_egg_info(tmp_path)

        assert count == 1
        assert not egg.exists()
        captured = capsys.readouterr()
        assert "package.egg-info" in captured.out

    def test_returns_zero_when_no_egg_info(self, tmp_path: Path) -> None:
        """Verify returns zero when no egg info.

        Given: A directory with no .egg-info directories,
        When: clean_egg_info is called,
        Then: Returns 0 indicating no directories were removed.
        """
        count = clean_egg_info(tmp_path)

        assert count == 0

    def test_ignores_egg_info_files(self, tmp_path: Path) -> None:
        """Verify ignores egg info files.

        Given: A regular file named with .egg-info suffix (not a directory),
        When: clean_egg_info is called,
        Then: Returns 0 and the file remains untouched.
        """
        egg_file = tmp_path / "package.egg-info"
        egg_file.write_text("not a directory")

        count = clean_egg_info(tmp_path)

        assert count == 0
        assert egg_file.exists()


class TestClean:
    """Test suite for Clean functionality."""

    def test_full_cleanup(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify full cleanup.

        Given: Directory contains .pytest_cache, .coverage, __pycache__,
            .pyc file, and .egg-info directory,
        When: clean() is called on the root path,
        Then: All artifacts are removed and "Cleanup completed!" is printed.
        """
        (tmp_path / ".pytest_cache").mkdir()
        (tmp_path / ".coverage").write_text("")
        (tmp_path / "__pycache__").mkdir()
        (tmp_path / "test.pyc").write_text("")
        (tmp_path / "pkg.egg-info").mkdir()

        clean(tmp_path)

        assert not (tmp_path / ".pytest_cache").exists()
        assert not (tmp_path / ".coverage").exists()
        assert not (tmp_path / "__pycache__").exists()
        assert not (tmp_path / "test.pyc").exists()
        assert not (tmp_path / "pkg.egg-info").exists()
        captured = capsys.readouterr()
        assert "Cleanup completed!" in captured.out

    def test_clean_with_default_root_uses_script_parent(self) -> None:
        """Verify clean with default root uses script parent.

        Given: All clean functions are mocked to return empty lists,
        When: clean() is called without arguments,
        Then: get_dirs_to_clean is called with the script's parent.parent
            path (default project root).
        """
        with (
            patch("scripts.clean.get_dirs_to_clean") as mock_dirs,
            patch("scripts.clean.get_files_to_clean") as mock_files,
            patch("scripts.clean.clean_pycache"),
            patch("scripts.clean.clean_pyc_files"),
            patch("scripts.clean.clean_egg_info"),
        ):
            mock_dirs.return_value = []
            mock_files.return_value = []

            clean()

            mock_dirs.assert_called_once()
            call_arg = mock_dirs.call_args[0][0]
            assert isinstance(call_arg, Path)


class TestMain:
    """Test suite for Main functionality."""

    def test_returns_zero(self, tmp_path: Path) -> None:
        """Verify returns zero.

        Given: The clean function is mocked,
        When: main() is called,
        Then: Returns 0 exit code and calls clean() exactly once.
        """
        with patch("scripts.clean.clean") as mock_clean:
            result = main()

        assert result == 0
        mock_clean.assert_called_once()
