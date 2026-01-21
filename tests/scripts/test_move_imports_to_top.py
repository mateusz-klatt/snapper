"""Tests for import mover script."""

import ast
from pathlib import Path
from unittest.mock import patch

import pytest

from scripts.move_imports_to_top import ImportMover
from scripts.move_imports_to_top import get_import_statement
from scripts.move_imports_to_top import main
from scripts.move_imports_to_top import move_imports_to_top


class TestImportMover:
    """Test suite for ImportMover functionality."""

    def test_detects_module_docstring(self) -> None:
        """Verify detects module docstring.

        Given: Python source code with a module-level docstring on line 1,
        When: ImportMover visits the parsed AST,
        Then: module_docstring_end is set to line 1 (docstring end position).
        """
        code = '"""Module docstring."""\nimport os\n'
        tree = ast.parse(code)
        visitor = ImportMover()

        visitor.visit(tree)

        assert visitor.module_docstring_end == 1

    def test_no_docstring(self) -> None:
        """Verify no docstring.

        Given: Python source code without a module-level docstring,
        When: ImportMover visits the parsed AST,
        Then: module_docstring_end remains 0 (no docstring detected).
        """
        code = "import os\nx = 1\n"
        tree = ast.parse(code)
        visitor = ImportMover()

        visitor.visit(tree)

        assert visitor.module_docstring_end == 0

    def test_collects_import_statements(self) -> None:
        """Verify collects import statements.

        Given: Python source code with two 'import' statements,
        When: ImportMover visits the parsed AST,
        Then: Both import nodes are collected in visitor.imports list.
        """
        code = "import os\nimport sys\n"
        tree = ast.parse(code)
        visitor = ImportMover()

        visitor.visit(tree)

        assert len(visitor.imports) == 2

    def test_collects_from_imports(self) -> None:
        """Verify collects from imports.

        Given: Python source code with two 'from X import Y' statements,
        When: ImportMover visits the parsed AST,
        Then: Both ImportFrom nodes are collected in visitor.imports list.
        """
        code = "from os import path\nfrom sys import argv\n"
        tree = ast.parse(code)
        visitor = ImportMover()

        visitor.visit(tree)

        assert len(visitor.imports) == 2

    def test_skips_try_blocks(self) -> None:
        """Verify skips try blocks.

        Given: Python source with an import inside a try/except block,
        When: ImportMover visits the parsed AST,
        Then: Import inside try block is not collected (len=0).
        """
        code = "try:\n    import optional\nexcept ImportError:\n    pass\n"
        tree = ast.parse(code)
        visitor = ImportMover()

        visitor.visit(tree)

        assert len(visitor.imports) == 0

    def test_tracks_first_non_import_line(self) -> None:
        """Verify tracks first non import line.

        Given: Python source with an import on line 1 and assignment on line 2,
        When: ImportMover visits the parsed AST,
        Then: first_non_import_line is set to 2 (the assignment line).
        """
        code = "import os\nx = 1\n"
        tree = ast.parse(code)
        visitor = ImportMover()

        visitor.visit(tree)

        assert visitor.first_non_import_line == 2


class TestGetImportStatement:
    """Test suite for GetImportStatement functionality."""

    def test_extracts_simple_import(self) -> None:
        r"""Verify extracts simple import.

        Given: Simple 'import os' statement parsed into AST node,
        When: get_import_statement is called with the node and source lines,
        Then: The exact import line 'import os\n' is returned.
        """
        code = "import os\n"
        tree = ast.parse(code)
        import_node = tree.body[0]
        lines = code.splitlines(keepends=True)

        result = get_import_statement(import_node, lines)

        assert result == "import os\n"

    def test_extracts_multiline_import(self) -> None:
        """Verify extracts multiline import.

        Given: Multi-line 'from os import (path, getcwd)' statement with parens,
        When: get_import_statement is called with the node and source lines,
        Then: The complete multi-line import text is extracted correctly.
        """
        code = "from os import (\n    path,\n    getcwd,\n)\n"
        tree = ast.parse(code)
        import_node = tree.body[0]
        lines = code.splitlines(keepends=True)

        result = get_import_statement(import_node, lines)

        assert "from os import" in result
        assert "path" in result

    def test_handles_indented_import(self) -> None:
        """Verify handles indented import.

        Given: An import statement inside an if-block (indented),
        When: get_import_statement is called with the node and source lines,
        Then: The import is extracted without the leading indentation.
        """
        code = "if True:\n    import os\n"
        tree = ast.parse(code)
        import_node = tree.body[0].body[0]
        lines = code.splitlines(keepends=True)

        result = get_import_statement(import_node, lines)

        assert result == "import os\n"

    def test_returns_empty_for_missing_lineno(self) -> None:
        """Verify returns empty for missing lineno.

        Given: A fake AST node without lineno attribute,
        When: get_import_statement is called with this node,
        Then: An empty string is returned (graceful fallback).
        """

        class FakeNode:
            pass

        node = FakeNode()
        result = get_import_statement(node, ["import os\n"])

        assert result == ""

    def test_handles_empty_lines_in_multiline_import(self) -> None:
        """Verify handles empty lines in multiline import.

        Given: Multi-line import inside if-block with empty line between parens,
        When: get_import_statement is called with the node and source lines,
        Then: The import is extracted correctly including the empty line.
        """
        code = "if True:\n    from os import (\n\n        path,\n    )\n"
        tree = ast.parse(code)
        if_stmt = tree.body[0]
        assert isinstance(if_stmt, ast.If)
        import_node = if_stmt.body[0]
        assert isinstance(import_node, (ast.Import, ast.ImportFrom))
        lines = list(code.splitlines(keepends=True))

        result = get_import_statement(import_node, lines)

        assert "from os import" in result

    def test_returns_empty_when_lines_empty(self) -> None:
        """Verify returns empty when lines empty.

        Given: A valid import node but an empty source lines list,
        When: get_import_statement is called with empty lines list,
        Then: An empty string is returned (early return guard).
        """
        code = "import os\n"
        tree = ast.parse(code)
        import_node = tree.body[0]
        assert isinstance(import_node, (ast.Import, ast.ImportFrom))

        result = get_import_statement(import_node, [])

        assert result == ""

    def test_handles_non_indented_multiline_import(self) -> None:
        """Verify handles non indented multiline import.

        Given: Multi-line import at top level (no indentation),
        When: get_import_statement is called with the node and source lines,
        Then: The complete multi-line import is extracted correctly.
        """
        code = "from os import (\npath,\n)\n"
        tree = ast.parse(code)
        import_node = tree.body[0]
        assert isinstance(import_node, (ast.Import, ast.ImportFrom))
        lines = list(code.splitlines(keepends=True))

        result = get_import_statement(import_node, lines)

        assert "from os import" in result


class TestMoveImportsToTop:
    """Test suite for MoveImportsToTop functionality."""

    def test_moves_scattered_imports(self, tmp_path: Path) -> None:
        """Verify moves scattered imports.

        Given: Python file with imports scattered (gap > 2 lines between them),
        When: move_imports_to_top is called on the file,
        Then: Returns True and all imports are moved to consecutive lines at top.
        """
        test_file = tmp_path / "test.py"
        test_file.write_text("import os\n\n\n\nx = 1\nimport sys\n")

        result = move_imports_to_top(test_file)

        assert result is True
        content = test_file.read_text()
        lines = content.split("\n")
        import_lines = [i for i, line in enumerate(lines) if line.startswith("import")]
        assert len(import_lines) >= 2
        assert max(import_lines) - min(import_lines) <= 1

    def test_no_change_when_imports_at_top(self, tmp_path: Path) -> None:
        """Verify no change when imports at top.

        Given: Python file with all imports already at top (consecutive),
        When: move_imports_to_top is called on the file,
        Then: Returns False (no modification needed).
        """
        test_file = tmp_path / "test.py"
        test_file.write_text("import os\nimport sys\n\nx = 1\n")

        result = move_imports_to_top(test_file)

        assert result is False

    def test_dry_run_does_not_modify(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify dry run does not modify.

        Given: Python file with scattered imports and dry_run=True,
        When: move_imports_to_top is called with dry_run flag,
        Then: Returns True but file is unchanged and 'Would modify' is printed.
        """
        test_file = tmp_path / "test.py"
        original = "import os\n\n\n\nx = 1\nimport sys\n"
        test_file.write_text(original)

        result = move_imports_to_top(test_file, dry_run=True)

        assert result is True
        assert test_file.read_text() == original
        captured = capsys.readouterr()
        assert "Would modify" in captured.out

    def test_handles_syntax_error(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify handles syntax error.

        Given: Python file with invalid syntax (incomplete def statement),
        When: move_imports_to_top is called on the file,
        Then: Returns False and 'Syntax error' message is printed.
        """
        test_file = tmp_path / "bad.py"
        test_file.write_text("def foo(\n")

        result = move_imports_to_top(test_file)

        assert result is False
        captured = capsys.readouterr()
        assert "Syntax error" in captured.out

    def test_returns_false_for_no_imports(self, tmp_path: Path) -> None:
        """Verify returns false for no imports.

        Given: Python file with no import statements at all,
        When: move_imports_to_top is called on the file,
        Then: Returns False (nothing to move).
        """
        test_file = tmp_path / "test.py"
        test_file.write_text("x = 1\ny = 2\n")

        result = move_imports_to_top(test_file)

        assert result is False

    def test_handles_exception(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify handles exception.

        Given: File path exists but read_text raises PermissionError,
        When: move_imports_to_top is called on the file,
        Then: Returns False and 'Error processing' message is printed.
        """
        test_file = tmp_path / "test.py"

        with patch(
            "scripts.move_imports_to_top.Path.read_text", side_effect=PermissionError("denied")
        ):
            result = move_imports_to_top(test_file)

        assert result is False
        captured = capsys.readouterr()
        assert "Error processing" in captured.out

    def test_preserves_docstring(self, tmp_path: Path) -> None:
        """Verify preserves docstring.

        Given: Python file starting with module docstring and scattered imports,
        When: move_imports_to_top is called on the file,
        Then: Docstring remains at the beginning, imports are placed after it.
        """
        test_file = tmp_path / "test.py"
        test_file.write_text('"""Docstring."""\nimport os\nx = 1\nimport sys\n')

        move_imports_to_top(test_file)

        content = test_file.read_text()
        assert content.startswith('"""Docstring."""')

    def test_removes_duplicate_imports(self, tmp_path: Path) -> None:
        """Verify removes duplicate imports.

        Given: Python file with same import appearing twice (scattered),
        When: move_imports_to_top is called on the file,
        Then: Duplicate import is removed, only one 'import os' remains.
        """
        test_file = tmp_path / "test.py"
        test_file.write_text("import os\n\n\n\nx = 1\nimport os\n")

        move_imports_to_top(test_file)

        content = test_file.read_text()
        assert content.count("import os") == 1

    def test_import_not_at_expected_start(self, tmp_path: Path) -> None:
        """Verify import not at expected start.

        Given: Python file with blank lines before the first import,
        When: move_imports_to_top is called on the file,
        Then: Returns True (import moved to proper top position).
        """
        test_file = tmp_path / "test.py"
        test_file.write_text("\n\n\nimport os\n")

        result = move_imports_to_top(test_file)

        assert result is True

    def test_handles_try_except_imports(self, tmp_path: Path) -> None:
        """Verify handles try except imports.

        Given: File with top-level import and import inside try/except block,
        When: move_imports_to_top is called on the file,
        Then: Returns False (try-block imports are ignored, main import at top).
        """
        test_file = tmp_path / "test.py"
        code = "import os\n\n\n\ntry:\n    import optional\nexcept ImportError:\n    pass\n"
        test_file.write_text(code)

        result = move_imports_to_top(test_file)

        assert result is False

    def test_skips_skip_until_lines(self, tmp_path: Path) -> None:
        """Verify skips skip until lines.

        Given: File with docstring, blank lines, then scattered imports,
        When: move_imports_to_top is called on the file,
        Then: Docstring is preserved, imports are consolidated after it.
        """
        test_file = tmp_path / "test.py"
        test_file.write_text('"""Docstring."""\n\n\n\nimport os\nx = 1\nimport sys\n')

        move_imports_to_top(test_file)

        content = test_file.read_text()
        assert content.startswith('"""Docstring."""')
        lines = content.split("\n")
        import_lines = [i for i, line in enumerate(lines) if line.startswith("import")]
        assert len(import_lines) >= 2

    def test_removes_consecutive_blank_lines(self, tmp_path: Path) -> None:
        """Verify removes consecutive blank lines.

        Given: File with docstring and multiple consecutive blank lines between imports,
        When: move_imports_to_top is called on the file,
        Then: Only one blank line after docstring, imports follow immediately.
        """
        test_file = tmp_path / "test.py"
        test_file.write_text('"""Doc."""\n\n\nimport os\n\n\n\n\n\n\nimport sys\n')

        move_imports_to_top(test_file)

        content = test_file.read_text()
        assert content.startswith('"""Doc."""\n')
        lines = content.split("\n")
        assert lines[1] == ""
        assert lines[2].startswith("import")

    def test_excludes_match_in_directory_iteration(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify excludes match in directory iteration.

        Given: Directory with .venv subfolder (excluded) and a normal Python file,
        When: main() processes the directory,
        Then: Only the normal file is processed (1/1), .venv is skipped.
        """
        excluded_dir = tmp_path / ".venv"
        excluded_dir.mkdir()
        excluded_file = excluded_dir / "test.py"
        excluded_file.write_text("import os\n\n\n\nimport sys\n")

        normal_file = tmp_path / "zzz_normal.py"
        normal_file.write_text("import os\n\n\n\nimport sys\n")

        with patch("sys.argv", ["move_imports", str(tmp_path)]):
            main()

        captured = capsys.readouterr()
        assert "1/1" in captured.out
        assert ".venv" not in captured.out

    def test_exclude_triggers_continue(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify exclude triggers continue.

        Given: Directory with multiple excluded folders (__pycache__, .venv, build),
        When: main() processes the directory,
        Then: Only the normal.py file is processed (1/1), excluded dirs skipped.
        """
        for exclude_name in ["__pycache__", ".venv", "build"]:
            excluded_dir = tmp_path / exclude_name
            excluded_dir.mkdir()
            excluded_file = excluded_dir / "test.py"
            excluded_file.write_text("import os\n\n\n\nimport sys\n")

        normal_file = tmp_path / "normal.py"
        normal_file.write_text("import os\n\n\n\nimport sys\n")

        with patch("sys.argv", ["move_imports", str(tmp_path)]):
            main()

        captured = capsys.readouterr()
        assert "1/1" in captured.out

    def test_directory_with_unmodified_file(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify directory with unmodified file.

        Given: Directory with one Python file that has imports already at top,
        When: main() processes the directory,
        Then: File is processed but not modified (Modified 0/1).
        """
        normal_file = tmp_path / "normal.py"
        normal_file.write_text("import os\nimport sys\n\nx = 1\n")

        with patch("sys.argv", ["move_imports", str(tmp_path)]):
            main()

        captured = capsys.readouterr()
        assert "Modified 0/1" in captured.out

    def test_get_import_statement_returns_empty(self, tmp_path: Path) -> None:
        """Verify get import statement returns empty.

        Given: File with scattered imports and mocked get_import_statement returning empty once,
        When: move_imports_to_top is called on the file,
        Then: Returns True, skipping the empty statement but processing the rest.
        """
        test_file = tmp_path / "test.py"
        test_file.write_text("import os\n\n\n\nimport sys\n")

        call_count = {"count": 0}

        original_func = __import__(
            "scripts.move_imports_to_top", fromlist=["get_import_statement"]
        ).get_import_statement

        def mock_get_import_statement(
            node: ast.Import | ast.ImportFrom, source_lines: list[str]
        ) -> str:
            call_count["count"] += 1
            if call_count["count"] == 1:
                return ""
            result: str = original_func(node, source_lines)
            return result

        with patch(
            "scripts.move_imports_to_top.get_import_statement",
            side_effect=mock_get_import_statement,
        ):
            result = move_imports_to_top(test_file)

        assert result is True

    def test_all_import_statements_empty(self, tmp_path: Path) -> None:
        """Verify all import statements empty.

        Given: File with scattered imports but get_import_statement mocked to always return empty,
        When: move_imports_to_top is called on the file,
        Then: Returns True (imports detected) but output has no import statements.
        """
        test_file = tmp_path / "test.py"
        test_file.write_text("import os\n\n\n\nimport sys\n")

        with patch(
            "scripts.move_imports_to_top.get_import_statement",
            return_value="",
        ):
            result = move_imports_to_top(test_file)

        assert result is True

    def test_handles_late_lines_after_docstring_region(self, tmp_path: Path) -> None:
        """Verify handles late lines after docstring region.

        Given: File with docstring and imports scattered far beyond line 10,
        When: move_imports_to_top is called on the file,
        Then: Returns True, lines beyond docstring region are processed differently.
        """
        test_file = tmp_path / "test.py"
        lines = ['"""Doc."""\n']
        lines.extend(["\n"] * 5)
        lines.append("import os\n")
        lines.extend(["\n"] * 10)
        lines.append("import sys\n")
        lines.append("x = 1\n")
        test_file.write_text("".join(lines))

        result = move_imports_to_top(test_file)

        assert result is True

    def test_handles_imports_with_gap(self, tmp_path: Path) -> None:
        """Verify handles imports with gap.

        Given: File with two imports separated by more than 2 blank lines,
        When: move_imports_to_top is called on the file,
        Then: Returns True (gap > 2 lines triggers scatter detection and fix).
        """
        test_file = tmp_path / "test.py"
        test_file.write_text("import os\n\n\n\nimport sys\nx = 1\n")

        result = move_imports_to_top(test_file)

        assert result is True

    def test_prints_modified_message(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify prints modified message.

        Given: File with scattered imports that needs modification,
        When: move_imports_to_top is called on the file,
        Then: 'Modified' message is printed to stdout.
        """
        test_file = tmp_path / "test.py"
        test_file.write_text("import os\n\n\n\nx = 1\nimport sys\n")

        move_imports_to_top(test_file)

        captured = capsys.readouterr()
        assert "Modified" in captured.out


class TestMain:
    """Test suite for Main functionality."""

    def test_processes_single_file(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify processes single file.

        Given: Single Python file path with scattered imports,
        When: main() is called with the file path argument,
        Then: File is modified and 'Modified 1/1' is printed.
        """
        test_file = tmp_path / "test.py"
        test_file.write_text("import os\n\n\n\nx = 1\nimport sys\n")

        with patch("sys.argv", ["move_imports", str(test_file)]):
            main()

        captured = capsys.readouterr()
        assert "Modified 1/1" in captured.out

    def test_processes_directory(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify processes directory.

        Given: Directory path containing one Python file with scattered imports,
        When: main() is called with the directory path argument,
        Then: All Python files are processed and 'Modified 1/1' is printed.
        """
        test_file = tmp_path / "test.py"
        test_file.write_text("import os\n\n\n\nx = 1\nimport sys\n")

        with patch("sys.argv", ["move_imports", str(tmp_path)]):
            main()

        captured = capsys.readouterr()
        assert "Modified 1/1" in captured.out

    def test_excludes_patterns(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify excludes patterns.

        Given: Directory with only a Python file inside .venv (default exclude),
        When: main() is called with the directory path argument,
        Then: No files are processed (0/0) because .venv is excluded.
        """
        venv = tmp_path / ".venv"
        venv.mkdir()
        test_file = venv / "test.py"
        test_file.write_text("import os\nx = 1\nimport sys\n")

        with patch("sys.argv", ["move_imports", str(tmp_path)]):
            main()

        captured = capsys.readouterr()
        assert "0/0" in captured.out or "Modified 0" in captured.out

    def test_custom_exclude(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify custom exclude.

        Given: Directory with Python file in 'custom_exclude' subfolder,
        When: main() is called with --exclude custom_exclude argument,
        Then: No files are processed (0/0) because custom dir is excluded.
        """
        custom_dir = tmp_path / "custom_exclude"
        custom_dir.mkdir()
        test_file = custom_dir / "test.py"
        test_file.write_text("import os\nx = 1\nimport sys\n")

        with patch("sys.argv", ["move_imports", str(tmp_path), "--exclude", "custom_exclude"]):
            main()

        captured = capsys.readouterr()
        assert "0/0" in captured.out or "Modified 0" in captured.out

    def test_dry_run_flag(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify dry run flag.

        Given: Python file with scattered imports,
        When: main() is called with --dry-run flag,
        Then: File content is unchanged and 'Would modify' is printed.
        """
        test_file = tmp_path / "test.py"
        original = "import os\n\n\n\nx = 1\nimport sys\n"
        test_file.write_text(original)

        with patch("sys.argv", ["move_imports", str(test_file), "--dry-run"]):
            main()

        assert test_file.read_text() == original
        captured = capsys.readouterr()
        assert "Would modify" in captured.out

    def test_dry_run_no_modifications(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify dry run no modifications.

        Given: Python file with imports already at top (no changes needed),
        When: main() is called with --dry-run flag,
        Then: 'Would modify 0/1' is printed (file scanned but no changes).
        """
        test_file = tmp_path / "test.py"
        test_file.write_text("import os\nimport sys\n\nx = 1\n")

        with patch("sys.argv", ["move_imports", str(test_file), "--dry-run"]):
            main()

        captured = capsys.readouterr()
        assert "Would modify 0/1" in captured.out

    def test_skips_non_python_files(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify skips non python files.

        Given: A .txt file (not .py) with import-like content,
        When: main() is called with the file path argument,
        Then: File is skipped (0/0) because it's not a Python file.
        """
        test_file = tmp_path / "test.txt"
        test_file.write_text("import os\nx = 1\nimport sys\n")

        with patch("sys.argv", ["move_imports", str(test_file)]):
            main()

        captured = capsys.readouterr()
        assert "0/0" in captured.out

    def test_reminder_message_after_modification(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify reminder message after modification.

        Given: Python file with scattered imports that will be modified,
        When: main() is called and file is modified,
        Then: 'Remember to run formatters' reminder message is printed.
        """
        test_file = tmp_path / "test.py"
        test_file.write_text("import os\n\n\n\nx = 1\nimport sys\n")

        with patch("sys.argv", ["move_imports", str(test_file)]):
            main()

        captured = capsys.readouterr()
        assert "Remember to run formatters" in captured.out

    def test_handles_nonexistent_path(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify handles nonexistent path.

        Given: A path to a file that does not exist,
        When: main() is called with the nonexistent path,
        Then: Completes without error with 0/0 files processed.
        """
        nonexistent = tmp_path / "nonexistent.py"

        with patch("sys.argv", ["move_imports", str(nonexistent)]):
            main()

        captured = capsys.readouterr()
        assert "0/0" in captured.out
