"""Tests for no-comment checker script."""

from pathlib import Path
from unittest.mock import patch

import pytest

import scripts.check_no_comments as check_no_comments


class TestShouldSkipPath:
    """Test suite for should_skip_path functionality."""

    def test_returns_true_for_skipped_directory(self, tmp_path: Path) -> None:
        """Verify should_skip_path returns True for skipped directories.

        Given: A file path within a skipped directory,
        When: should_skip_path is called,
        Then: It returns True.
        """
        python_file = tmp_path / "src" / "__pycache__" / "x.py"
        assert check_no_comments.should_skip_path(python_file) is True

    def test_returns_false_for_normal_directory(self, tmp_path: Path) -> None:
        """Verify should_skip_path returns False for normal directories.

        Given: A file path outside of skipped directories,
        When: should_skip_path is called,
        Then: It returns False.
        """
        python_file = tmp_path / "src" / "x.py"
        assert check_no_comments.should_skip_path(python_file) is False


class TestIterPythonFiles:
    """Test suite for iter_python_files functionality."""

    def test_collects_python_files_and_skips_dirs(self, tmp_path: Path) -> None:
        """Verify iter_python_files collects files and skips ignored paths.

        Given: Python files under src/tests plus a file under __pycache__,
        When: iter_python_files is called,
        Then: It returns only the non-skipped Python files in sorted order.
        """
        (tmp_path / "src").mkdir()
        (tmp_path / "tests").mkdir()
        (tmp_path / "src" / "a.py").write_text('"""A."""\n')
        (tmp_path / "tests" / "b.py").write_text('"""B."""\n')
        (tmp_path / "src" / "__pycache__").mkdir()
        (tmp_path / "src" / "__pycache__" / "c.py").write_text("x = 1\n")

        files = check_no_comments.iter_python_files(
            tmp_path, relative_roots=("src", "tests", "missing")
        )

        assert files == [
            tmp_path / "src" / "a.py",
            tmp_path / "tests" / "b.py",
        ]


class TestFindCommentTokens:
    """Test suite for find_comment_tokens functionality."""

    def test_detects_real_comments_and_ignores_hash_in_strings(self, tmp_path: Path) -> None:
        """Verify tokenize-based comment detection.

        Given: A file with a trailing comment and hashes inside strings/docstrings,
        When: find_comment_tokens is called,
        Then: Only the real comment token is returned.
        """
        python_file = tmp_path / "example.py"
        python_file.write_text(
            'x = "# not a comment"\n'
            "y = 1  # real comment\n"
            '"""\n'
            "# not a comment inside docstring\n"
            '"""\n'
        )

        findings = check_no_comments.find_comment_tokens(python_file)

        assert findings == [(2, "# real comment")]

    def test_returns_empty_for_missing_file(self, tmp_path: Path) -> None:
        """Verify missing file is handled gracefully.

        Given: A file path that does not exist,
        When: find_comment_tokens is called,
        Then: It returns an empty list.
        """
        missing = tmp_path / "missing.py"
        assert check_no_comments.find_comment_tokens(missing) == []

    def test_returns_empty_on_tokenize_error(self, tmp_path: Path) -> None:
        """Verify tokenization errors do not crash the scan.

        Given: A file with an unterminated triple-quoted string,
        When: find_comment_tokens is called,
        Then: It returns an empty list.
        """
        broken = tmp_path / "broken.py"
        broken.write_text('"""unterminated\n')
        assert check_no_comments.find_comment_tokens(broken) == []


class TestScanPythonFiles:
    """Test suite for scan_python_files functionality."""

    def test_scans_only_configured_roots(self, tmp_path: Path) -> None:
        """Verify scan_python_files only scans configured roots.

        Given: A commented file under src and another under an unrelated directory,
        When: scan_python_files is called,
        Then: Only the file under src is reported.
        """
        (tmp_path / "src").mkdir()
        (tmp_path / "other").mkdir()
        src_file = tmp_path / "src" / "a.py"
        other_file = tmp_path / "other" / "b.py"
        src_file.write_text("x = 1  # comment\n")
        other_file.write_text("x = 1  # comment\n")

        results = check_no_comments.scan_python_files(tmp_path)

        assert src_file in results
        assert other_file not in results


class TestPrintResults:
    """Test suite for print_results functionality."""

    def test_returns_zero_when_no_results(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify print_results returns 0 for empty results.

        Given: An empty results mapping,
        When: print_results is called,
        Then: It returns 0 and prints a clean message.
        """
        count = check_no_comments.print_results({}, tmp_path)

        assert count == 0
        captured = capsys.readouterr()
        assert "No Python comments found" in captured.out

    def test_counts_findings_and_truncates_long_comments(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Verify print_results counts findings and truncates long output.

        Given: A results mapping with multiple findings including a long comment,
        When: print_results is called,
        Then: It returns the total count and prints an ellipsis for long comments.
        """
        python_file = tmp_path / "src" / "a.py"
        python_file.parent.mkdir(parents=True)
        long_comment = "#" + ("x" * 200)
        results = {python_file: [(1, "# short"), (2, long_comment)]}

        count = check_no_comments.print_results(results, tmp_path)

        assert count == 2
        captured = capsys.readouterr()
        assert "L1: # short" in captured.out
        assert "..." in captured.out


class TestRunScan:
    """Test suite for run_scan functionality."""

    def test_returns_zero_in_strict_mode_when_clean(self, tmp_path: Path) -> None:
        """Verify strict mode passes on a clean project tree.

        Given: A project root with Python files containing no comments,
        When: run_scan is executed in strict mode,
        Then: It returns 0.
        """
        (tmp_path / "src").mkdir()
        (tmp_path / "src" / "clean.py").write_text('"""Clean."""\n')

        result = check_no_comments.run_scan(tmp_path, strict_mode=True)

        assert result == 0

    def test_returns_one_in_strict_mode_when_comments_found(self, tmp_path: Path) -> None:
        """Verify strict mode fails when comments are present.

        Given: A project root with a Python file containing a comment token,
        When: run_scan is executed in strict mode,
        Then: It returns 1.
        """
        (tmp_path / "src").mkdir()
        (tmp_path / "src" / "has_comments.py").write_text("x = 1  # comment\n")

        result = check_no_comments.run_scan(tmp_path, strict_mode=True)

        assert result == 1

    def test_returns_zero_in_report_mode_when_comments_found(self, tmp_path: Path) -> None:
        """Verify report mode does not fail when comments are present.

        Given: A project root with a Python file containing a comment token,
        When: run_scan is executed without strict mode,
        Then: It returns 0.
        """
        (tmp_path / "src").mkdir()
        (tmp_path / "src" / "has_comments.py").write_text("x = 1  # comment\n")

        result = check_no_comments.run_scan(tmp_path, strict_mode=False)

        assert result == 0


class TestMain:
    """Test suite for main entry point."""

    def test_passes_strict_flag_to_run_scan(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify main passes strict flag to run_scan.

        Given: sys.argv contains '--strict',
        When: main is called,
        Then: run_scan is invoked with strict_mode=True.
        """
        monkeypatch.setattr(check_no_comments.sys, "argv", ["prog", "--strict"])
        with patch("scripts.check_no_comments.run_scan", return_value=0) as mock_run:
            result = check_no_comments.main()

        assert result == 0
        assert mock_run.call_args.args[1] is True

    def test_defaults_to_report_mode(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify main defaults to report mode when no flag is provided.

        Given: sys.argv without '--strict',
        When: main is called,
        Then: run_scan is invoked with strict_mode=False.
        """
        monkeypatch.setattr(check_no_comments.sys, "argv", ["prog"])
        with patch("scripts.check_no_comments.run_scan", return_value=0) as mock_run:
            result = check_no_comments.main()

        assert result == 0
        assert mock_run.call_args.args[1] is False
