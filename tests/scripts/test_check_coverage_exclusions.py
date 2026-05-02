"""Tests for coverage exclusion scanner."""

from pathlib import Path
from unittest.mock import patch

import pytest

from scripts.check_coverage_exclusions import PYTHON_PATTERNS
from scripts.check_coverage_exclusions import TS_PATTERNS
from scripts.check_coverage_exclusions import find_exclusions_in_file
from scripts.check_coverage_exclusions import main
from scripts.check_coverage_exclusions import print_results
from scripts.check_coverage_exclusions import run_scan
from scripts.check_coverage_exclusions import scan_python_files
from scripts.check_coverage_exclusions import scan_typescript_files


class TestPatterns:
    """Test suite for Patterns functionality."""

    def test_python_pragma_no_cover(self) -> None:
        """Verify pragma no cover pattern matches Python coverage exclusion comments.

        Given: The PYTHON_PATTERNS regex for 'pragma: no cover',
        When: Searching lines with various pragma comment formats,
        Then: Matches valid pragma comments (with/without spaces, case-insensitive)
              and rejects malformed ones.
        """
        pattern = PYTHON_PATTERNS["pragma: no cover"]

        assert pattern.search("# pragma: no cover")
        assert pattern.search("#pragma:no cover")
        assert pattern.search("# PRAGMA: NO COVER")
        assert not pattern.search("# pragma cover")

    def test_python_noqa(self) -> None:
        """Verify noqa pattern matches Python lint suppression comments.

        Given: The PYTHON_PATTERNS regex for 'noqa',
        When: Searching lines with various noqa comment formats,
        Then: Matches valid noqa comments (with/without error codes, spaces)
              and rejects invalid variations like 'no qa'.
        """
        pattern = PYTHON_PATTERNS["noqa"]

        assert pattern.search("# noqa")
        assert pattern.search("# noqa: E501")
        assert pattern.search("#noqa")
        assert not pattern.search("# no qa")

    def test_python_type_ignore(self) -> None:
        """Verify type ignore pattern matches Python mypy suppression comments.

        Given: The PYTHON_PATTERNS regex for 'type: ignore',
        When: Searching lines with various type ignore comment formats,
        Then: Matches valid type ignore comments (with/without error codes)
              and rejects malformed ones missing the colon.
        """
        pattern = PYTHON_PATTERNS["type: ignore"]

        assert pattern.search("# type: ignore")
        assert pattern.search("# type: ignore[arg-type]")
        assert pattern.search("# type: ignore[arg-type, return-value]")
        assert not pattern.search("# type ignore")

    def test_ts_istanbul_ignore(self) -> None:
        """Verify istanbul ignore pattern matches TypeScript coverage exclusion comments.

        Given: The TS_PATTERNS regex for 'istanbul ignore',
        When: Searching lines with various istanbul ignore comment formats,
        Then: Matches valid istanbul comments (next, else, file, case-insensitive)
              and rejects incomplete istanbul comments.
        """
        pattern = TS_PATTERNS["istanbul ignore"]

        assert pattern.search("/* istanbul ignore next */")
        assert pattern.search("/* istanbul ignore else */")
        assert pattern.search("/* ISTANBUL IGNORE FILE */")
        assert not pattern.search("/* istanbul */")

    def test_ts_eslint_disable(self) -> None:
        """Verify eslint-disable pattern matches TypeScript lint suppression comments.

        Given: The TS_PATTERNS regex for 'eslint-disable',
        When: Searching lines with various eslint-disable comment formats,
        Then: Matches valid eslint-disable comments (base, next-line, line)
              and rejects unrelated eslint comments like 'eslint enable'.
        """
        pattern = TS_PATTERNS["eslint-disable"]

        assert pattern.search("// eslint-disable")
        assert pattern.search("// eslint-disable-next-line")
        assert pattern.search("// eslint-disable-line")
        assert not pattern.search("// eslint enable")

    def test_ts_ts_ignore(self) -> None:
        """Verify @ts-ignore pattern matches TypeScript type suppression comments.

        Given: The TS_PATTERNS regex for '@ts-ignore',
        When: Searching lines with @ts-ignore comments,
        Then: Matches valid @ts-ignore comments with or without leading space.
        """
        pattern = TS_PATTERNS["@ts-ignore"]

        assert pattern.search("// @ts-ignore")
        assert pattern.search("//@ts-ignore")


class TestFindExclusionsInFile:
    """Test suite for FindExclusionsInFile functionality."""

    def test_finds_python_exclusions(self, tmp_path: Path) -> None:
        """Verify find_exclusions_in_file detects Python exclusion patterns.

        Given: A Python file containing noqa and type: ignore comments,
        When: Scanning the file with PYTHON_PATTERNS,
        Then: Returns list of findings with line numbers, pattern names, and content.
        """
        test_file = tmp_path / "test.py"
        test_file.write_text("x = 1  # noqa\ny = 2  # type: ignore\n")

        findings = find_exclusions_in_file(test_file, PYTHON_PATTERNS)

        assert len(findings) == 2
        assert findings[0] == (1, "noqa", "x = 1  # noqa")
        assert findings[1] == (2, "type: ignore", "y = 2  # type: ignore")

    def test_finds_ts_exclusions(self, tmp_path: Path) -> None:
        """Verify find_exclusions_in_file detects TypeScript exclusion patterns.

        Given: A TypeScript file containing @ts-ignore comment,
        When: Scanning the file with TS_PATTERNS,
        Then: Returns finding with line number 1, pattern '@ts-ignore', and line content.
        """
        test_file = tmp_path / "test.ts"
        test_file.write_text("// @ts-ignore\nconst x = 1;\n")

        findings = find_exclusions_in_file(test_file, TS_PATTERNS)

        assert len(findings) == 1
        assert findings[0] == (1, "@ts-ignore", "// @ts-ignore")

    def test_returns_empty_for_clean_file(self, tmp_path: Path) -> None:
        """Verify find_exclusions_in_file returns empty list for clean files.

        Given: A Python file with no exclusion patterns,
        When: Scanning the file with PYTHON_PATTERNS,
        Then: Returns an empty list.
        """
        test_file = tmp_path / "clean.py"
        test_file.write_text("x = 1\ny = 2\n")

        findings = find_exclusions_in_file(test_file, PYTHON_PATTERNS)

        assert findings == []

    def test_handles_unreadable_file(self, tmp_path: Path) -> None:
        """Verify find_exclusions_in_file handles non-existent files gracefully.

        Given: A file path that does not exist,
        When: Scanning the non-existent file,
        Then: Returns an empty list instead of raising an exception.
        """
        test_file = tmp_path / "unreadable.py"

        findings = find_exclusions_in_file(test_file, PYTHON_PATTERNS)

        assert findings == []

    def test_handles_unicode_error(self, tmp_path: Path) -> None:
        """Verify find_exclusions_in_file handles files with invalid encoding.

        Given: A file containing invalid UTF-8 bytes,
        When: Scanning the file with PYTHON_PATTERNS,
        Then: Returns an empty list instead of raising UnicodeDecodeError.
        """
        test_file = tmp_path / "binary.py"
        test_file.write_bytes(b"\xff\xfe invalid")

        findings = find_exclusions_in_file(test_file, PYTHON_PATTERNS)

        assert findings == []


class TestScanPythonFiles:
    """Test suite for ScanPythonFiles functionality."""

    def test_scans_python_files(self, tmp_path: Path) -> None:
        """Verify scan_python_files finds exclusions in .py files.

        Given: A directory with a Python file containing noqa comment,
        When: Scanning the directory for Python exclusions,
        Then: Returns results dict with the file and its findings.
        """
        py_file = tmp_path / "test.py"
        py_file.write_text("x = 1  # noqa\n")

        results = scan_python_files(tmp_path)

        assert py_file in results
        assert len(results[py_file]) == 1

    def test_skips_venv_directory(self, tmp_path: Path) -> None:
        """Verify scan_python_files excludes virtual environment directories.

        Given: A .venv directory containing a Python file with noqa comment,
        When: Scanning the parent directory for Python exclusions,
        Then: The file inside .venv is not included in results.
        """
        venv = tmp_path / ".venv"
        venv.mkdir()
        py_file = venv / "test.py"
        py_file.write_text("x = 1  # noqa\n")

        results = scan_python_files(tmp_path)

        assert py_file not in results

    def test_skips_pycache(self, tmp_path: Path) -> None:
        """Verify scan_python_files excludes __pycache__ directories.

        Given: A __pycache__ directory containing a Python file with noqa comment,
        When: Scanning the parent directory for Python exclusions,
        Then: The file inside __pycache__ is not included in results.
        """
        pycache = tmp_path / "__pycache__"
        pycache.mkdir()
        py_file = pycache / "test.py"
        py_file.write_text("x = 1  # noqa\n")

        results = scan_python_files(tmp_path)

        assert py_file not in results

    def test_skips_files_in_skip_files_list(self, tmp_path: Path) -> None:
        """Verify scan_python_files excludes files in the SKIP_FILES list.

        Given: A file named 'test_check_coverage_exclusions.py' with noqa patterns,
        When: Scanning the directory for Python exclusions,
        Then: The file is skipped because its name is in SKIP_FILES.
        """
        py_file = tmp_path / "test_check_coverage_exclusions.py"
        py_file.write_text("x = 1  # noqa\n")

        results = scan_python_files(tmp_path)

        assert py_file not in results

    def test_skips_clean_files(self, tmp_path: Path) -> None:
        """Verify scan_python_files excludes files without exclusion patterns.

        Given: A Python file with no exclusion patterns,
        When: Scanning the directory for Python exclusions,
        Then: The clean file is not included in results.
        """
        py_file = tmp_path / "clean.py"
        py_file.write_text("x = 1\n")

        results = scan_python_files(tmp_path)

        assert py_file not in results


class TestScanTypescriptFiles:
    """Test suite for ScanTypescriptFiles functionality."""

    def test_scans_ts_files(self, tmp_path: Path) -> None:
        """Verify scan_typescript_files finds exclusions in .ts files.

        Given: A directory with a TypeScript file containing @ts-ignore comment,
        When: Scanning the directory for TypeScript exclusions,
        Then: Returns results dict containing the .ts file.
        """
        ts_file = tmp_path / "test.ts"
        ts_file.write_text("// @ts-ignore\n")

        results = scan_typescript_files(tmp_path)

        assert ts_file in results

    def test_scans_tsx_files(self, tmp_path: Path) -> None:
        """Verify scan_typescript_files finds exclusions in .tsx files.

        Given: A directory with a TSX file containing @ts-ignore comment,
        When: Scanning the directory for TypeScript exclusions,
        Then: Returns results dict containing the .tsx file.
        """
        tsx_file = tmp_path / "test.tsx"
        tsx_file.write_text("// @ts-ignore\n")

        results = scan_typescript_files(tmp_path)

        assert tsx_file in results

    def test_skips_node_modules(self, tmp_path: Path) -> None:
        """Verify scan_typescript_files excludes node_modules directory.

        Given: A node_modules directory containing a TypeScript file with @ts-ignore,
        When: Scanning the parent directory for TypeScript exclusions,
        Then: The file inside node_modules is not included in results.
        """
        node_modules = tmp_path / "node_modules"
        node_modules.mkdir()
        ts_file = node_modules / "test.ts"
        ts_file.write_text("// @ts-ignore\n")

        results = scan_typescript_files(tmp_path)

        assert ts_file not in results

    def test_skips_generated_files(self, tmp_path: Path) -> None:
        """Verify scan_typescript_files excludes *.generated.ts files.

        Given: A generated TypeScript file (api.generated.ts) with @ts-ignore,
        When: Scanning the directory for TypeScript exclusions,
        Then: The generated file is not included in results.
        """
        ts_file = tmp_path / "api.generated.ts"
        ts_file.write_text("// @ts-ignore\n")

        results = scan_typescript_files(tmp_path)

        assert ts_file not in results

    def test_skips_clean_ts_files(self, tmp_path: Path) -> None:
        """Test that clean TS files without exclusion patterns are not included."""
        ts_file = tmp_path / "clean.ts"
        ts_file.write_text("const x = 1;\n")

        results = scan_typescript_files(tmp_path)

        assert ts_file not in results


class TestPrintResults:
    """Test suite for PrintResults functionality."""

    def test_prints_no_exclusions_message(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify print_results shows success message when no exclusions found.

        Given: An empty results dictionary,
        When: Printing results for Python files,
        Then: Returns count 0 and outputs 'No exclusions found' message.
        """
        count = print_results({}, tmp_path, "Python")

        assert count == 0
        captured = capsys.readouterr()
        assert "No exclusions found in Python files" in captured.out

    def test_prints_findings(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify print_results displays findings with file, line, and pattern info.

        Given: Results dict with one file containing a noqa finding at line 1,
        When: Printing results for Python files,
        Then: Returns count 1 and outputs filename, line number, and pattern name.
        """
        test_file = tmp_path / "test.py"
        results = {test_file: [(1, "noqa", "x = 1  # noqa")]}

        count = print_results(results, tmp_path, "Python")

        assert count == 1
        captured = capsys.readouterr()
        assert "test.py" in captured.out
        assert "L1:" in captured.out
        assert "[noqa]" in captured.out

    def test_truncates_long_lines(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify print_results truncates lines exceeding display width.

        Given: Results with a finding containing a 100+ character line,
        When: Printing results,
        Then: Output contains '...' indicating truncation.
        """
        test_file = tmp_path / "test.py"
        long_line = "x" * 100 + "  # noqa"
        results = {test_file: [(1, "noqa", long_line)]}

        print_results(results, tmp_path, "Python")

        captured = capsys.readouterr()
        assert "..." in captured.out


class TestRunScan:
    """Test suite for RunScan functionality."""

    def test_clean_codebase(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify run_scan returns success for codebase without exclusions.

        Given: A project structure with empty src, tests, scripts, frontend dirs,
        When: Running scan in non-strict mode,
        Then: Returns 0 and outputs 'No coverage/lint exclusions found' message.
        """
        (tmp_path / "src").mkdir()
        (tmp_path / "tests").mkdir()
        (tmp_path / "scripts").mkdir()
        (tmp_path / "frontend").mkdir()

        result = run_scan(tmp_path, strict_mode=False)

        assert result == 0
        captured = capsys.readouterr()
        assert "No coverage/lint exclusions found" in captured.out

    def test_strict_mode_fails_on_findings(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify run_scan fails in strict mode when exclusions are found.

        Given: A project with src/bad.py containing noqa comment,
        When: Running scan in strict mode,
        Then: Returns 1 (failure) and outputs 'STRICT MODE: Failing' message.
        """
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        (tmp_path / "tests").mkdir()
        (tmp_path / "scripts").mkdir()
        (tmp_path / "frontend").mkdir()
        bad_file = src_dir / "bad.py"
        bad_file.write_text("x = 1  # noqa\n")

        result = run_scan(tmp_path, strict_mode=True)

        assert result == 1
        captured = capsys.readouterr()
        assert "STRICT MODE: Failing" in captured.out

    def test_report_mode_returns_zero_on_findings(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify run_scan returns success in report mode even with findings.

        Given: A project with src/bad.py containing noqa comment,
        When: Running scan in non-strict (report) mode,
        Then: Returns 0 (success) despite findings being present.
        """
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        (tmp_path / "tests").mkdir()
        (tmp_path / "scripts").mkdir()
        (tmp_path / "frontend").mkdir()
        bad_file = src_dir / "bad.py"
        bad_file.write_text("x = 1  # noqa\n")

        result = run_scan(tmp_path, strict_mode=False)

        assert result == 0

    def test_scans_proprietary_subtrees_when_present(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify run_scan picks up findings under ``proprietary/src`` and ``proprietary/tests``.

        Given: A project layout that includes both ``proprietary/src`` and
            ``proprietary/tests`` subtrees, each with a file carrying a
            ``noqa`` exclusion,
        When: Running scan in strict mode,
        Then: Returns 1 (failure) AND surfaces both proprietary findings
            in the captured output, exercising the ``proprietary/*``
            optional-scan branches added so the scanner has parity with
            ``check_no_comments``.
        """
        (tmp_path / "src").mkdir()
        (tmp_path / "tests").mkdir()
        (tmp_path / "scripts").mkdir()
        (tmp_path / "frontend").mkdir()
        prop_src = tmp_path / "proprietary" / "src"
        prop_src.mkdir(parents=True)
        prop_tests = tmp_path / "proprietary" / "tests"
        prop_tests.mkdir(parents=True)
        (prop_src / "bad.py").write_text("x = 1  # noqa\n")
        (prop_tests / "bad.py").write_text("y = 2  # noqa\n")

        result = run_scan(tmp_path, strict_mode=True)

        assert result == 1
        captured = capsys.readouterr()
        assert "proprietary/src/bad.py" in captured.out
        assert "proprietary/tests/bad.py" in captured.out


class TestMain:
    """Test suite for Main functionality."""

    def test_uses_script_parent_as_root(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify main() uses script's parent directory as project root.

        Given: Command line with no arguments,
        When: Calling main(),
        Then: run_scan is called with strict_mode=False.
        """
        monkeypatch.setattr("sys.argv", ["check_coverage_exclusions.py"])

        with patch("scripts.check_coverage_exclusions.run_scan", return_value=0) as mock_scan:
            main()

            mock_scan.assert_called_once()
            call_args = mock_scan.call_args
            assert call_args[0][1] is False

    def test_strict_mode_from_argv(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify main() enables strict mode when --strict flag is passed.

        Given: Command line with --strict argument,
        When: Calling main(),
        Then: run_scan is called with strict_mode=True.
        """
        monkeypatch.setattr("sys.argv", ["check_coverage_exclusions.py", "--strict"])

        with patch("scripts.check_coverage_exclusions.run_scan", return_value=0) as mock_scan:
            main()

            call_args = mock_scan.call_args
            assert call_args[0][1] is True
