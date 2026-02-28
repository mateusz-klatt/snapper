"""Tests for __init__.py checker script."""

import ast
from pathlib import Path
from unittest.mock import patch

import pytest

import scripts.check_init_files as check_init_files


class TestShouldSkipPath:
    """Test suite for should_skip_path functionality."""

    def test_returns_true_for_skipped_directory(self, tmp_path: Path) -> None:
        """Verify should_skip_path returns True for skipped directories.

        Given: A file path within a skipped directory,
        When: should_skip_path is called,
        Then: It returns True.
        """
        init_file = tmp_path / "src" / "__pycache__" / "__init__.py"
        assert check_init_files.should_skip_path(init_file) is True

    def test_returns_false_for_normal_directory(self, tmp_path: Path) -> None:
        """Verify should_skip_path returns False for normal directories.

        Given: A file path outside of skipped directories,
        When: should_skip_path is called,
        Then: It returns False.
        """
        init_file = tmp_path / "src" / "pkg" / "__init__.py"
        assert check_init_files.should_skip_path(init_file) is False


class TestIterInitFiles:
    """Test suite for iter_init_files functionality."""

    def test_collects_init_files_and_skips_dirs(self, tmp_path: Path) -> None:
        """Verify iter_init_files collects files and skips ignored paths.

        Given: __init__.py files under src/tests plus one under __pycache__,
        When: iter_init_files is called,
        Then: It returns only the non-skipped files in sorted order.
        """
        (tmp_path / "src" / "pkg").mkdir(parents=True)
        (tmp_path / "tests" / "sub").mkdir(parents=True)
        (tmp_path / "src" / "pkg" / "__init__.py").write_text('"""Pkg."""\n')
        (tmp_path / "tests" / "sub" / "__init__.py").write_text('"""Sub."""\n')
        (tmp_path / "src" / "__pycache__").mkdir()
        (tmp_path / "src" / "__pycache__" / "__init__.py").write_text("")

        files = check_init_files.iter_init_files(
            tmp_path, relative_roots=("src", "tests", "missing")
        )

        assert files == [
            tmp_path / "src" / "pkg" / "__init__.py",
            tmp_path / "tests" / "sub" / "__init__.py",
        ]


class TestIsDocstringOnly:
    """Test suite for _is_docstring_only helper."""

    def test_single_docstring_returns_true(self) -> None:
        """Verify a body with one string constant is docstring-only.

        Given: A module body containing a single string expression,
        When: _is_docstring_only is called,
        Then: It returns True.
        """
        tree = ast.parse('"""Module docstring."""\n')
        assert check_init_files._is_docstring_only(tree.body) is True

    def test_empty_body_returns_false(self) -> None:
        """Verify an empty body does not match.

        Given: An empty module body,
        When: _is_docstring_only is called,
        Then: It returns False.
        """
        assert check_init_files._is_docstring_only([]) is False

    def test_multiple_nodes_returns_false(self) -> None:
        """Verify a body with more than one node returns False.

        Given: A module body with a docstring and an assignment,
        When: _is_docstring_only is called,
        Then: It returns False.
        """
        tree = ast.parse('"""Doc."""\nx = 1\n')
        assert check_init_files._is_docstring_only(tree.body) is False

    def test_non_string_constant_returns_false(self) -> None:
        """Verify a body with a non-string constant returns False.

        Given: A module body with a numeric constant expression,
        When: _is_docstring_only is called,
        Then: It returns False.
        """
        tree = ast.parse("42\n")
        assert check_init_files._is_docstring_only(tree.body) is False


class TestNodeLabel:
    """Test suite for _node_label helper."""

    def test_known_node_type(self) -> None:
        """Verify _node_label returns the mapped label for known types.

        Given: An ast.Import node,
        When: _node_label is called,
        Then: It returns 'import'.
        """
        node = ast.Import(names=[ast.alias(name="os")], lineno=1, col_offset=0)
        assert check_init_files._node_label(node) == "import"

    def test_unknown_node_type(self) -> None:
        """Verify _node_label falls back to class name for unknown types.

        Given: An AST node type not in the label mapping,
        When: _node_label is called,
        Then: It returns the class name.
        """
        node = ast.Pass(lineno=1, col_offset=0)
        assert check_init_files._node_label(node) == "Pass"


class TestCheckInitFile:
    """Test suite for check_init_file functionality."""

    def test_docstring_only_file_passes(self, tmp_path: Path) -> None:
        """Verify a docstring-only file produces no violations.

        Given: An __init__.py with only a docstring,
        When: check_init_file is called,
        Then: It returns an empty list.
        """
        init_file = tmp_path / "__init__.py"
        init_file.write_text('"""Package."""\n')
        assert check_init_files.check_init_file(init_file) == []

    def test_empty_file_passes(self, tmp_path: Path) -> None:
        """Verify an empty file produces no violations.

        Given: An empty __init__.py,
        When: check_init_file is called,
        Then: It returns an empty list.
        """
        init_file = tmp_path / "__init__.py"
        init_file.write_text("")
        assert check_init_files.check_init_file(init_file) == []

    def test_file_with_import_fails(self, tmp_path: Path) -> None:
        """Verify an __init__.py with imports produces violations.

        Given: An __init__.py with a docstring and an import,
        When: check_init_file is called,
        Then: It returns the import violation.
        """
        init_file = tmp_path / "__init__.py"
        init_file.write_text('"""Pkg."""\nimport os\n')
        result = check_init_files.check_init_file(init_file)
        assert len(result) == 1
        assert result[0] == (2, "import")

    def test_file_with_assignment_fails(self, tmp_path: Path) -> None:
        """Verify an __init__.py with assignments produces violations.

        Given: An __init__.py with an assignment,
        When: check_init_file is called,
        Then: It returns the assignment violation.
        """
        init_file = tmp_path / "__init__.py"
        init_file.write_text("x = 1\n")
        result = check_init_files.check_init_file(init_file)
        assert len(result) == 1
        assert result[0] == (1, "assignment")

    def test_file_with_class_definition_fails(self, tmp_path: Path) -> None:
        """Verify an __init__.py with class definitions produces violations.

        Given: An __init__.py with a class definition,
        When: check_init_file is called,
        Then: It returns the class definition violation.
        """
        init_file = tmp_path / "__init__.py"
        init_file.write_text("class Foo:\n    pass\n")
        result = check_init_files.check_init_file(init_file)
        assert len(result) == 1
        assert result[0] == (1, "class definition")

    def test_file_with_function_definition_fails(self, tmp_path: Path) -> None:
        """Verify an __init__.py with function definitions produces violations.

        Given: An __init__.py with a function definition,
        When: check_init_file is called,
        Then: It returns the function definition violation.
        """
        init_file = tmp_path / "__init__.py"
        init_file.write_text("def foo():\n    pass\n")
        result = check_init_files.check_init_file(init_file)
        assert len(result) == 1
        assert result[0] == (1, "function definition")

    def test_missing_file_returns_empty(self, tmp_path: Path) -> None:
        """Verify missing file is handled gracefully.

        Given: A file path that does not exist,
        When: check_init_file is called,
        Then: It returns an empty list.
        """
        missing = tmp_path / "__init__.py"
        assert check_init_files.check_init_file(missing) == []

    def test_syntax_error_returns_empty(self, tmp_path: Path) -> None:
        """Verify syntax errors are handled gracefully.

        Given: A file with invalid Python syntax,
        When: check_init_file is called,
        Then: It returns an empty list.
        """
        broken = tmp_path / "__init__.py"
        broken.write_text("def(\n")
        assert check_init_files.check_init_file(broken) == []

    def test_docstring_with_additional_code_reports_only_code(self, tmp_path: Path) -> None:
        """Verify that docstring nodes are skipped in violation reporting.

        Given: An __init__.py with a docstring and two code statements,
        When: check_init_file is called,
        Then: Only the code statements are reported as violations.
        """
        init_file = tmp_path / "__init__.py"
        init_file.write_text('"""Pkg."""\nimport os\nx = 1\n')
        result = check_init_files.check_init_file(init_file)
        assert len(result) == 2
        assert result[0] == (2, "import")
        assert result[1] == (3, "assignment")


class TestScanInitFiles:
    """Test suite for scan_init_files functionality."""

    def test_scans_only_configured_roots(self, tmp_path: Path) -> None:
        """Verify scan_init_files only scans configured roots.

        Given: A violating __init__.py under src and another under an unrelated dir,
        When: scan_init_files is called,
        Then: Only the file under src is reported.
        """
        (tmp_path / "src" / "pkg").mkdir(parents=True)
        (tmp_path / "other" / "pkg").mkdir(parents=True)
        src_file = tmp_path / "src" / "pkg" / "__init__.py"
        other_file = tmp_path / "other" / "pkg" / "__init__.py"
        src_file.write_text("import os\n")
        other_file.write_text("import os\n")

        results = check_init_files.scan_init_files(tmp_path)

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
        count = check_init_files.print_results({}, tmp_path)

        assert count == 0
        captured = capsys.readouterr()
        assert "No __init__.py violations found" in captured.out

    def test_counts_violations(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify print_results counts and displays violations.

        Given: A results mapping with multiple violations,
        When: print_results is called,
        Then: It returns the total count and prints each violation.
        """
        init_file = tmp_path / "src" / "pkg" / "__init__.py"
        init_file.parent.mkdir(parents=True)
        results: dict[Path, list[tuple[int, str]]] = {
            init_file: [(1, "import"), (2, "assignment")],
        }

        count = check_init_files.print_results(results, tmp_path)

        assert count == 2
        captured = capsys.readouterr()
        assert "L1: import" in captured.out
        assert "L2: assignment" in captured.out


class TestRunScan:
    """Test suite for run_scan functionality."""

    def test_returns_zero_in_strict_mode_when_clean(self, tmp_path: Path) -> None:
        """Verify strict mode passes on a clean project tree.

        Given: A project root with docstring-only __init__.py files,
        When: run_scan is executed in strict mode,
        Then: It returns 0.
        """
        (tmp_path / "src" / "pkg").mkdir(parents=True)
        (tmp_path / "src" / "pkg" / "__init__.py").write_text('"""Pkg."""\n')

        result = check_init_files.run_scan(tmp_path, strict_mode=True)

        assert result == 0

    def test_returns_one_in_strict_mode_when_violations_found(self, tmp_path: Path) -> None:
        """Verify strict mode fails when violations are present.

        Given: A project root with an __init__.py containing imports,
        When: run_scan is executed in strict mode,
        Then: It returns 1.
        """
        (tmp_path / "src" / "pkg").mkdir(parents=True)
        (tmp_path / "src" / "pkg" / "__init__.py").write_text("import os\n")

        result = check_init_files.run_scan(tmp_path, strict_mode=True)

        assert result == 1

    def test_returns_zero_in_report_mode_when_violations_found(self, tmp_path: Path) -> None:
        """Verify report mode does not fail when violations are present.

        Given: A project root with an __init__.py containing imports,
        When: run_scan is executed without strict mode,
        Then: It returns 0.
        """
        (tmp_path / "src" / "pkg").mkdir(parents=True)
        (tmp_path / "src" / "pkg" / "__init__.py").write_text("import os\n")

        result = check_init_files.run_scan(tmp_path, strict_mode=False)

        assert result == 0


class TestMain:
    """Test suite for main entry point."""

    def test_passes_strict_flag_to_run_scan(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify main passes strict flag to run_scan.

        Given: sys.argv contains '--strict',
        When: main is called,
        Then: run_scan is invoked with strict_mode=True.
        """
        monkeypatch.setattr(check_init_files.sys, "argv", ["prog", "--strict"])
        with patch("scripts.check_init_files.run_scan", return_value=0) as mock_run:
            result = check_init_files.main()

        assert result == 0
        assert mock_run.call_args.args[1] is True

    def test_defaults_to_report_mode(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify main defaults to report mode when no flag is provided.

        Given: sys.argv without '--strict',
        When: main is called,
        Then: run_scan is invoked with strict_mode=False.
        """
        monkeypatch.setattr(check_init_files.sys, "argv", ["prog"])
        with patch("scripts.check_init_files.run_scan", return_value=0) as mock_run:
            result = check_init_files.main()

        assert result == 0
        assert mock_run.call_args.args[1] is False
