"""Tests for main guard checker script."""

import ast
from pathlib import Path
from unittest.mock import patch

import scripts.check_main_guard as check_main_guard


class TestShouldSkipPath:
    """Test suite for should_skip_path functionality."""

    def test_returns_true_for_skipped_directory(self, tmp_path: Path) -> None:
        """Verify should_skip_path returns True for skipped directories.

        Given: A file path within a skipped directory,
        When: should_skip_path is called,
        Then: It returns True.
        """
        python_file = tmp_path / "src" / "__pycache__" / "x.py"
        assert check_main_guard.should_skip_path(python_file) is True

    def test_returns_false_for_normal_directory(self, tmp_path: Path) -> None:
        """Verify should_skip_path returns False for normal directories.

        Given: A file path outside of skipped directories,
        When: should_skip_path is called,
        Then: It returns False.
        """
        python_file = tmp_path / "src" / "x.py"
        assert check_main_guard.should_skip_path(python_file) is False


class TestIterPythonFiles:
    """Test suite for iter_python_files functionality."""

    def test_collects_files_and_skips_dirs(self, tmp_path: Path) -> None:
        """Verify iter_python_files collects files and skips ignored paths.

        Given: Python files under src plus a file under __pycache__,
        When: iter_python_files is called,
        Then: It returns only the non-skipped Python files in sorted order.
        """
        (tmp_path / "src").mkdir()
        (tmp_path / "src" / "a.py").write_text('"""A."""\n')
        (tmp_path / "src" / "b.py").write_text('"""B."""\n')
        (tmp_path / "src" / "__pycache__").mkdir()
        (tmp_path / "src" / "__pycache__" / "c.py").write_text("x = 1\n")

        files = check_main_guard.iter_python_files(tmp_path, relative_roots=("src", "missing"))

        assert files == [
            tmp_path / "src" / "a.py",
            tmp_path / "src" / "b.py",
        ]


class TestIsMainGuard:
    """Test suite for _is_main_guard detection."""

    def test_detects_standard_main_guard(self) -> None:
        """Verify standard main guard is detected.

        Given: An if-node with ``__name__ == "__main__"``,
        When: _is_main_guard is called,
        Then: It returns True.
        """
        tree = ast.parse('if __name__ == "__main__":\n    pass\n')
        node = tree.body[0]
        assert check_main_guard._is_main_guard(node) is True

    def test_detects_reversed_main_guard(self) -> None:
        """Verify reversed comparison is detected.

        Given: An if-node with ``"__main__" == __name__``,
        When: _is_main_guard is called,
        Then: It returns True.
        """
        tree = ast.parse('if "__main__" == __name__:\n    pass\n')
        node = tree.body[0]
        assert check_main_guard._is_main_guard(node) is True

    def test_rejects_non_compare(self) -> None:
        """Verify non-Compare nodes are rejected.

        Given: An if-node with a boolean test,
        When: _is_main_guard is called,
        Then: It returns False.
        """
        tree = ast.parse("if True:\n    pass\n")
        node = tree.body[0]
        assert check_main_guard._is_main_guard(node) is False

    def test_rejects_wrong_variable(self) -> None:
        """Verify unrelated comparisons are rejected.

        Given: An if-node comparing a non-__name__ variable,
        When: _is_main_guard is called,
        Then: It returns False.
        """
        tree = ast.parse('if x == "__main__":\n    pass\n')
        node = tree.body[0]
        assert check_main_guard._is_main_guard(node) is False

    def test_rejects_chained_compare(self) -> None:
        """Verify chained comparisons are rejected.

        Given: An if-node with a chained comparison,
        When: _is_main_guard is called,
        Then: It returns False.
        """
        tree = ast.parse("if 1 < x < 10:\n    pass\n")
        node = tree.body[0]
        assert check_main_guard._is_main_guard(node) is False

    def test_rejects_not_eq_operator(self) -> None:
        """Verify non-equality operators are rejected.

        Given: An if-node using != instead of ==,
        When: _is_main_guard is called,
        Then: It returns False.
        """
        tree = ast.parse('if __name__ != "__main__":\n    pass\n')
        node = tree.body[0]
        assert check_main_guard._is_main_guard(node) is False

    def test_rejects_wrong_string_value(self) -> None:
        """Verify wrong string constant is rejected.

        Given: An if-node comparing __name__ to a wrong string,
        When: _is_main_guard is called,
        Then: It returns False.
        """
        tree = ast.parse('if __name__ == "other":\n    pass\n')
        node = tree.body[0]
        assert check_main_guard._is_main_guard(node) is False

    def test_rejects_reversed_with_wrong_value(self) -> None:
        """Verify reversed comparison with wrong value is rejected.

        Given: An if-node with reversed operands and wrong string,
        When: _is_main_guard is called,
        Then: It returns False.
        """
        tree = ast.parse('if "other" == __name__:\n    pass\n')
        node = tree.body[0]
        assert check_main_guard._is_main_guard(node) is False


class TestIsRaiseSystemExitMain:
    """Test suite for _is_raise_system_exit_main detection."""

    def test_accepts_canonical_form(self) -> None:
        """Verify canonical form is accepted.

        Given: A raise SystemExit(main()) statement,
        When: _is_raise_system_exit_main is called,
        Then: It returns True.
        """
        tree = ast.parse("raise SystemExit(main())")
        stmt = tree.body[0]
        assert check_main_guard._is_raise_system_exit_main(stmt) is True

    def test_rejects_bare_main_call(self) -> None:
        """Verify bare main() call is rejected.

        Given: A plain main() expression statement,
        When: _is_raise_system_exit_main is called,
        Then: It returns False.
        """
        tree = ast.parse("main()")
        stmt = tree.body[0]
        assert check_main_guard._is_raise_system_exit_main(stmt) is False

    def test_rejects_sys_exit(self) -> None:
        """Verify sys.exit(main()) is rejected.

        Given: A raise sys.exit(main()) statement,
        When: _is_raise_system_exit_main is called,
        Then: It returns False because func is an Attribute, not Name.
        """
        tree = ast.parse("raise sys.exit(main())")
        stmt = tree.body[0]
        assert check_main_guard._is_raise_system_exit_main(stmt) is False

    def test_rejects_system_exit_with_literal(self) -> None:
        """Verify SystemExit with a literal argument is rejected.

        Given: A raise SystemExit(0) statement,
        When: _is_raise_system_exit_main is called,
        Then: It returns False.
        """
        tree = ast.parse("raise SystemExit(0)")
        stmt = tree.body[0]
        assert check_main_guard._is_raise_system_exit_main(stmt) is False

    def test_rejects_system_exit_no_args(self) -> None:
        """Verify SystemExit with no arguments is rejected.

        Given: A raise SystemExit() statement,
        When: _is_raise_system_exit_main is called,
        Then: It returns False.
        """
        tree = ast.parse("raise SystemExit()")
        stmt = tree.body[0]
        assert check_main_guard._is_raise_system_exit_main(stmt) is False

    def test_rejects_system_exit_with_kwargs(self) -> None:
        """Verify SystemExit with keyword arguments is rejected.

        Given: A raise SystemExit(code=main()) statement,
        When: _is_raise_system_exit_main is called,
        Then: It returns False.
        """
        tree = ast.parse("raise SystemExit(code=main())")
        stmt = tree.body[0]
        assert check_main_guard._is_raise_system_exit_main(stmt) is False

    def test_rejects_wrong_function_name(self) -> None:
        """Verify SystemExit with wrong inner function is rejected.

        Given: A raise SystemExit(run()) statement,
        When: _is_raise_system_exit_main is called,
        Then: It returns False.
        """
        tree = ast.parse("raise SystemExit(run())")
        stmt = tree.body[0]
        assert check_main_guard._is_raise_system_exit_main(stmt) is False

    def test_rejects_main_with_args(self) -> None:
        """Verify main() called with arguments is rejected.

        Given: A raise SystemExit(main(1)) statement,
        When: _is_raise_system_exit_main is called,
        Then: It returns False.
        """
        tree = ast.parse("raise SystemExit(main(1))")
        stmt = tree.body[0]
        assert check_main_guard._is_raise_system_exit_main(stmt) is False

    def test_rejects_bare_raise(self) -> None:
        """Verify bare raise without exception is rejected.

        Given: A bare raise statement,
        When: _is_raise_system_exit_main is called,
        Then: It returns False.
        """
        tree = ast.parse("try:\n    pass\nexcept:\n    raise\n")
        raise_stmt = tree.body[0].handlers[0].body[0]
        assert check_main_guard._is_raise_system_exit_main(raise_stmt) is False

    def test_rejects_raise_non_call(self) -> None:
        """Verify raise of non-call expression is rejected.

        Given: A raise ValueError statement (no call),
        When: _is_raise_system_exit_main is called,
        Then: It returns False.
        """
        tree = ast.parse("raise ValueError")
        stmt = tree.body[0]
        assert check_main_guard._is_raise_system_exit_main(stmt) is False


class TestCheckMainGuard:
    """Test suite for check_main_guard file-level checks."""

    def test_passes_canonical_form(self, tmp_path: Path) -> None:
        """Verify canonical main guard passes validation.

        Given: A file with ``raise SystemExit(main())`` in the main guard,
        When: check_main_guard is called,
        Then: No violations are found.
        """
        f = tmp_path / "good.py"
        f.write_text(
            "def main() -> int:\n    return 0\n\n\n"
            'if __name__ == "__main__":\n    raise SystemExit(main())\n'
        )
        assert check_main_guard.check_main_guard(f) == []

    def test_rejects_bare_main(self, tmp_path: Path) -> None:
        """Verify bare main() in main guard is rejected.

        Given: A file with ``main()`` in the main guard,
        When: check_main_guard is called,
        Then: One violation is reported.
        """
        f = tmp_path / "bad.py"
        f.write_text('def main() -> None:\n    pass\n\n\nif __name__ == "__main__":\n    main()\n')
        violations = check_main_guard.check_main_guard(f)
        assert len(violations) == 1
        assert "raise SystemExit(main())" in violations[0][1]

    def test_rejects_multiple_statements(self, tmp_path: Path) -> None:
        """Verify multiple statements in main guard body are rejected.

        Given: A file with two statements in the main guard body,
        When: check_main_guard is called,
        Then: One violation is reported.
        """
        f = tmp_path / "multi.py"
        f.write_text(
            "def main() -> None:\n    pass\n\n\n"
            'if __name__ == "__main__":\n    print("hi")\n    main()\n'
        )
        violations = check_main_guard.check_main_guard(f)
        assert len(violations) == 1
        assert "exactly" in violations[0][1]

    def test_rejects_else_branch(self, tmp_path: Path) -> None:
        """Verify main guard with else branch is rejected.

        Given: A file with an else branch on the main guard,
        When: check_main_guard is called,
        Then: One violation about the else branch is reported.
        """
        f = tmp_path / "with_else.py"
        f.write_text(
            "def main() -> None:\n    pass\n\n\n"
            'if __name__ == "__main__":\n    raise SystemExit(main())\n'
            "else:\n    pass\n"
        )
        violations = check_main_guard.check_main_guard(f)
        assert len(violations) == 1
        assert "else" in violations[0][1]

    def test_ignores_non_main_guard_if(self, tmp_path: Path) -> None:
        """Verify non-main-guard if statements are ignored.

        Given: A file with regular if statements but no main guard,
        When: check_main_guard is called,
        Then: No violations are found.
        """
        f = tmp_path / "no_guard.py"
        f.write_text("if True:\n    pass\n")
        assert check_main_guard.check_main_guard(f) == []

    def test_handles_unreadable_file(self, tmp_path: Path) -> None:
        """Verify unreadable files return empty results.

        Given: A file path that does not exist,
        When: check_main_guard is called,
        Then: An empty list is returned.
        """
        f = tmp_path / "missing.py"
        assert check_main_guard.check_main_guard(f) == []

    def test_handles_syntax_error(self, tmp_path: Path) -> None:
        """Verify files with syntax errors return empty results.

        Given: A file with invalid Python syntax,
        When: check_main_guard is called,
        Then: An empty list is returned.
        """
        f = tmp_path / "broken.py"
        f.write_text("def :\n")
        assert check_main_guard.check_main_guard(f) == []

    def test_skips_non_if_top_level_nodes(self, tmp_path: Path) -> None:
        """Verify non-If top-level nodes are skipped.

        Given: A file with only function definitions,
        When: check_main_guard is called,
        Then: No violations are found.
        """
        f = tmp_path / "funcs_only.py"
        f.write_text("def foo() -> None:\n    pass\n\nx = 1\n")
        assert check_main_guard.check_main_guard(f) == []


class TestScanFiles:
    """Test suite for scan_files functionality."""

    def test_returns_violations_across_files(self, tmp_path: Path) -> None:
        """Verify scan_files aggregates violations from multiple files.

        Given: Two files, one good and one bad,
        When: scan_files is called,
        Then: Only the bad file appears in results.
        """
        src = tmp_path / "src"
        src.mkdir()
        good = src / "good.py"
        good.write_text(
            "def main() -> int:\n    return 0\n\n\n"
            'if __name__ == "__main__":\n    raise SystemExit(main())\n'
        )
        bad = src / "bad.py"
        bad.write_text(
            'def main() -> None:\n    pass\n\n\nif __name__ == "__main__":\n    main()\n'
        )
        results = check_main_guard.scan_files(tmp_path, relative_roots=("src",))
        assert bad in results
        assert good not in results


class TestRunScan:
    """Test suite for run_scan output and exit code."""

    def test_returns_zero_when_clean(self, tmp_path: Path) -> None:
        """Verify clean scan returns exit code 0.

        Given: A project with canonical main guards,
        When: run_scan is called in strict mode,
        Then: Exit code 0 is returned.
        """
        src = tmp_path / "src"
        src.mkdir()
        (src / "app.py").write_text(
            "def main() -> int:\n    return 0\n\n\n"
            'if __name__ == "__main__":\n    raise SystemExit(main())\n'
        )
        result = check_main_guard.run_scan(tmp_path, strict_mode=True, relative_roots=("src",))
        assert result == 0

    def test_returns_one_on_violation_strict(self, tmp_path: Path) -> None:
        """Verify violations cause exit code 1 in strict mode.

        Given: A project with a non-canonical main guard,
        When: run_scan is called in strict mode,
        Then: Exit code 1 is returned.
        """
        src = tmp_path / "src"
        src.mkdir()
        (src / "bad.py").write_text(
            'def main() -> None:\n    pass\n\n\nif __name__ == "__main__":\n    main()\n'
        )
        result = check_main_guard.run_scan(tmp_path, strict_mode=True, relative_roots=("src",))
        assert result == 1

    def test_returns_zero_on_violation_non_strict(self, tmp_path: Path) -> None:
        """Verify violations return exit code 0 in non-strict mode.

        Given: A project with a non-canonical main guard,
        When: run_scan is called without strict mode,
        Then: Exit code 0 is returned.
        """
        src = tmp_path / "src"
        src.mkdir()
        (src / "bad.py").write_text(
            'def main() -> None:\n    pass\n\n\nif __name__ == "__main__":\n    main()\n'
        )
        result = check_main_guard.run_scan(tmp_path, strict_mode=False, relative_roots=("src",))
        assert result == 0


class TestMain:
    """Test suite for main entry point."""

    def test_main_returns_exit_code(self) -> None:
        """Verify main returns the run_scan exit code.

        Given: A project with no violations,
        When: main is called,
        Then: It returns 0.
        """
        with patch("scripts.check_main_guard.run_scan", return_value=0) as mock_scan:
            result = check_main_guard.main()
        assert result == 0
        mock_scan.assert_called_once()

    def test_main_passes_strict_flag(self) -> None:
        """Verify main reads --strict from sys.argv.

        Given: sys.argv contains --strict,
        When: main is called,
        Then: run_scan receives strict_mode=True.
        """
        with (
            patch("sys.argv", ["check_main_guard.py", "--strict"]),
            patch("scripts.check_main_guard.run_scan", return_value=0) as mock_scan,
        ):
            check_main_guard.main()
        args = mock_scan.call_args[0]
        assert args[1] is True
