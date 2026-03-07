"""Tests for docstring checker script."""

import ast
from pathlib import Path

import pytest

import scripts.check_docstrings as check_docstrings


def test_is_test_file_detects_posix_and_windows_paths() -> None:
    """Verify test path detection.

    Given: POSIX and Windows-like paths,
    When: is_test_file is called,
    Then: It returns True only for paths containing a tests directory.
    """
    assert check_docstrings.is_test_file(Path("/tmp/tests/test_example.py")) is True
    assert check_docstrings.is_test_file(Path(r"C:\repo\tests\test_example.py")) is True
    assert check_docstrings.is_test_file(Path("/tmp/src/example.py")) is False


def test_extract_dunder_all_returns_set_for_assign() -> None:
    """Verify __all__ extraction from literal assignment.

    Given: A module with __all__ defined as a literal list,
    When: extract_dunder_all is called,
    Then: It returns a set of exported names.
    """
    tree = ast.parse('__all__ = ["A", "B"]\n')
    assert check_docstrings.extract_dunder_all(tree) == {"A", "B"}


def test_extract_dunder_all_returns_none_for_unresolvable_value() -> None:
    """Verify __all__ extraction returns None for unresolvable values.

    Given: A module with a non-literal __all__ definition,
    When: extract_dunder_all is called,
    Then: It returns None.
    """
    tree = ast.parse("__all__ = SOME_NAMES\nSOME_NAMES = ['A']\n")
    assert check_docstrings.extract_dunder_all(tree) is None


def test_extract_dunder_all_returns_none_for_non_string_elements() -> None:
    """Verify __all__ extraction rejects non-string elements.

    Given: A module with __all__ containing a non-string element,
    When: extract_dunder_all is called,
    Then: It returns None.
    """
    tree = ast.parse('__all__ = ["A", 1]\n')
    assert check_docstrings.extract_dunder_all(tree) is None


def test_extract_dunder_all_skips_non_all_assignments() -> None:
    """Verify __all__ extraction skips unrelated assignments.

    Given: A module with assignments and annotated assignments unrelated to __all__,
    When: extract_dunder_all is called,
    Then: It ignores unrelated nodes and still extracts __all__.
    """
    tree = ast.parse('x = 1\ny: int = 2\n__all__ = ["A"]\n')
    assert check_docstrings.extract_dunder_all(tree) == {"A"}


def test_extract_dunder_all_supports_annassign() -> None:
    """Verify __all__ extraction from annotated assignment.

    Given: A module with __all__ defined via annotated assignment,
    When: extract_dunder_all is called,
    Then: It returns a set of exported names.
    """
    tree = ast.parse('__all__: tuple[str, ...] = ("A",)\n')
    assert check_docstrings.extract_dunder_all(tree) == {"A"}


def test_validate_google_docstring_reports_missing_sections() -> None:
    """Verify Google docstring validation for args/returns sections.

    Given: A function with parameters and non-None return annotation,
    When: validate_google_docstring is called without Args/Returns sections,
    Then: It reports missing sections.
    """
    tree = ast.parse('def f(x: int) -> int:\n    """Summary."""\n    return x\n')
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef))
    errors = check_docstrings.validate_google_docstring("Summary.", node)
    assert any("Missing 'Args:'" in error for error in errors)
    assert any("Missing 'Returns:'" in error for error in errors)


def test_validate_google_docstring_skips_returns_when_no_annotation() -> None:
    """Verify Google docstring validation skips return section without annotation.

    Given: A function without a return annotation,
    When: validate_google_docstring is called,
    Then: It does not require a Returns section.
    """
    tree = ast.parse('def f():\n    """Summary."""\n    return\n')
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef))
    assert check_docstrings.validate_google_docstring("Summary.", node) == []


def test_validate_google_docstring_skips_returns_when_none_annotation() -> None:
    """Verify Google docstring validation skips returns when annotation is None.

    Given: A function annotated as returning None,
    When: validate_google_docstring is called,
    Then: It does not require a Returns section.
    """
    tree = ast.parse(
        'def f(x: int) -> None:\n    """Summary.\n\n    Args:\n        x: Value.\n    """\n    return\n'
    )
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef))
    assert check_docstrings.validate_google_docstring(ast.get_docstring(node) or "", node) == []


def test_validate_google_docstring_handles_missing_ast_unparse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify Google docstring validation without ast.unparse.

    Given: An environment without ast.unparse,
    When: validate_google_docstring is called,
    Then: It falls back to stringifying the annotation node.
    """
    tree = ast.parse(
        "def f(x: int) -> int:\n"
        '    """Summary.\n\nArgs:\n    x: Value.\n\nReturns:\n    Value.\n"""\n'
        "    return x\n"
    )
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef))
    monkeypatch.delattr(check_docstrings.ast, "unparse", raising=False)
    assert check_docstrings.validate_google_docstring(ast.get_docstring(node) or "", node) == []


def test_validate_bdd_docstring_success_and_errors() -> None:
    """Verify BDD docstring validation.

    Given: Valid and invalid BDD docstrings,
    When: validate_bdd_docstring is called,
    Then: It returns errors only for invalid docstrings.
    """
    valid = "Summary.\n\nGiven x,\nWhen y,\nThen z\n"
    assert check_docstrings.validate_bdd_docstring(valid) == []

    too_short = "Summary.\n"
    assert check_docstrings.validate_bdd_docstring(too_short) != []

    missing_given = "Summary.\n\nWhen x,\nThen y\n"
    errors = check_docstrings.validate_bdd_docstring(missing_given)
    assert any("Missing 'Given'" in error for error in errors)

    missing_when = "Summary.\n\nGiven x,\nThen y\n"
    errors_when = check_docstrings.validate_bdd_docstring(missing_when)
    assert any("Missing 'When'" in error for error in errors_when)

    missing_then = "Summary.\n\nGiven x,\nWhen y\n"
    errors_then = check_docstrings.validate_bdd_docstring(missing_then)
    assert any("Missing 'Then'" in error for error in errors_then)


def test_check_module_skips_init_and_reports_missing_docstring() -> None:
    """Verify module docstring checks.

    Given: An __init__.py file and a normal module without a docstring,
    When: check_module is called,
    Then: It skips __init__.py and reports missing docstring for normal module.
    """
    result = check_docstrings.ScanResult()

    init_tree = ast.parse("")
    check_docstrings.check_module(Path("pkg/__init__.py"), init_tree, result)
    assert result.modules_checked == 1
    assert result.issues == []

    mod_tree = ast.parse("x = 1\n")
    check_docstrings.check_module(Path("pkg/mod.py"), mod_tree, result)
    assert result.modules_checked == 2
    assert any(issue.issue_type == "missing_module_docstring" for issue in result.issues)


def test_check_class_reports_missing_docstring() -> None:
    """Verify class docstring checks.

    Given: A class without a docstring,
    When: check_class is called,
    Then: It reports a missing class docstring issue.
    """
    tree = ast.parse("class A:\n    pass\n")
    node = next(n for n in tree.body if isinstance(n, ast.ClassDef))
    result = check_docstrings.ScanResult()

    check_docstrings.check_class(Path("pkg/mod.py"), node, result)

    assert any(issue.issue_type == "missing_class_docstring" for issue in result.issues)


def test_check_class_with_docstring_reports_no_issues() -> None:
    """Verify check_class accepts class with valid docstring.

    Given: A class with a docstring,
    When: check_class is called,
    Then: No issues are reported.
    """
    tree = ast.parse('class A:\n    """A."""\n    pass\n')
    node = next(n for n in tree.body if isinstance(n, ast.ClassDef))
    result = check_docstrings.ScanResult()

    check_docstrings.check_class(Path("pkg/mod.py"), node, result)

    assert result.issues == []


def test_get_docstring_returns_none_for_other_nodes() -> None:
    """Verify get_docstring returns None for unsupported nodes.

    Given: A non-module/class/function AST node,
    When: get_docstring is called,
    Then: It returns None.
    """
    node = ast.parse("x = 1\n").body[0]
    assert check_docstrings.get_docstring(node) is None


def test_check_function_skips_private_and_dunder() -> None:
    """Verify function check skips private and dunder functions.

    Given: Private and dunder functions with/without docstrings,
    When: check_function is called,
    Then: It skips them without producing issues.
    """
    tree = ast.parse(
        'def _private() -> None:\n    """Private."""\n    return\n\n'
        "def __dunder__() -> None:\n    return\n"
    )
    private_node = tree.body[0]
    dunder_node = tree.body[1]
    assert isinstance(private_node, ast.FunctionDef)
    assert isinstance(dunder_node, ast.FunctionDef)

    result = check_docstrings.ScanResult()
    check_docstrings.check_function(Path("pkg/mod.py"), private_node, result)
    check_docstrings.check_function(Path("pkg/mod.py"), dunder_node, result)

    assert result.issues == []


def test_check_function_reports_missing_test_docstring() -> None:
    """Verify missing test docstring reporting.

    Given: A test function without a docstring,
    When: check_function is called for a tests path,
    Then: It reports a missing_test_docstring issue.
    """
    tree = ast.parse("def test_example() -> None:\n    return\n")
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef))
    result = check_docstrings.ScanResult()

    check_docstrings.check_function(Path("/tmp/tests/test_example.py"), node, result)

    assert any(issue.issue_type == "missing_test_docstring" for issue in result.issues)


def test_check_function_enforces_bdd_when_enabled() -> None:
    """Verify BDD enforcement for test docstrings.

    Given: A test function with a non-BDD docstring and BDD enforcement enabled,
    When: check_function is called,
    Then: It reports an invalid_bdd_docstring issue.
    """
    tree = ast.parse('def test_example() -> None:\n    """Not BDD."""\n    return\n')
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef))
    result = check_docstrings.ScanResult()

    check_docstrings.check_function(
        Path("/tmp/tests/test_example.py"),
        node,
        result,
        enforce_bdd=True,
    )
    assert any(issue.issue_type == "invalid_bdd_docstring" for issue in result.issues)


def test_check_function_enforces_google_sections_when_enabled() -> None:
    """Verify Google sections enforcement for function docstrings.

    Given: A function missing Args/Returns sections and enforcement enabled,
    When: check_function is called,
    Then: It reports invalid_google_docstring issues.
    """
    tree = ast.parse('def f(x: int) -> int:\n    """Summary."""\n    return x\n')
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef))
    result = check_docstrings.ScanResult()

    check_docstrings.check_function(
        Path("pkg/mod.py"),
        node,
        result,
        enforce_google_sections=True,
    )
    assert any(issue.issue_type == "invalid_google_docstring" for issue in result.issues)


def test_check_function_skips_validation_when_enforcement_disabled() -> None:
    """Verify check_function does not validate when enforcement is disabled.

    Given: A function with a docstring and all enforcement flags disabled,
    When: check_function is called,
    Then: It does not emit validation issues.
    """
    tree = ast.parse('def f() -> None:\n    """Summary."""\n    return\n')
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef))
    result = check_docstrings.ScanResult()

    check_docstrings.check_function(Path("pkg/mod.py"), node, result)

    assert result.issues == []


def test_scan_file_filters_by_dunder_all_and_tests(tmp_path: Path) -> None:
    """Verify scan_file respects __all__ and test filtering.

    Given: A module using __all__ and a tests module with helper functions,
    When: scan_file is executed,
    Then: Only exported names and test_* functions are checked.
    """
    module_path = tmp_path / "pkg" / "mod.py"
    module_path.parent.mkdir(parents=True)
    module_path.write_text(
        '"""Module docstring."""\n\n'
        '__all__ = ["public_fn", "PublicClass", "_private_fn", "_PrivateClass"]\n\n'
        "def public_fn() -> None:\n"
        "    return\n\n"
        "def _private_fn() -> None:\n"
        "    return\n\n"
        "def internal_fn() -> None:\n"
        "    return\n\n"
        "class PublicClass:\n"
        "    CONST = 1\n\n"
        "    def method(self) -> None:\n"
        "        return\n\n"
        "    def __repr__(self) -> str:\n"
        "        return 'PublicClass()'\n\n"
        "class _PrivateClass:\n"
        "    pass\n\n"
        "class InternalClass:\n"
        "    pass\n",
        encoding="utf-8",
    )

    result = check_docstrings.ScanResult()
    check_docstrings.scan_file(
        module_path,
        result,
        enforce_bdd=False,
        enforce_google_sections=False,
    )
    assert any(issue.issue_type == "missing_function_docstring" for issue in result.issues)
    assert any(issue.issue_type == "missing_class_docstring" for issue in result.issues)

    test_path = tmp_path / "tests" / "test_mod.py"
    test_path.parent.mkdir(parents=True, exist_ok=True)
    test_path.write_text(
        '"""Test module docstring."""\n\n'
        "def helper() -> None:\n"
        "    return\n\n"
        "class Helper:\n"
        "    pass\n\n"
        "def test_example() -> None:\n"
        "    return\n",
        encoding="utf-8",
    )
    test_result = check_docstrings.ScanResult()
    check_docstrings.scan_file(
        test_path,
        test_result,
        enforce_bdd=False,
        enforce_google_sections=False,
    )
    assert any(issue.issue_type == "missing_test_docstring" for issue in test_result.issues)
    assert all(issue.issue_type != "missing_function_docstring" for issue in test_result.issues)


def test_scan_file_reports_parse_errors(tmp_path: Path) -> None:
    """Verify scan_file reports parse errors.

    Given: A file with invalid syntax and a file with invalid UTF-8,
    When: scan_file is called,
    Then: It reports parse_error issues for both.
    """
    bad_syntax = tmp_path / "bad_syntax.py"
    bad_syntax.write_text("def broken(:\n", encoding="utf-8")
    result = check_docstrings.ScanResult()
    check_docstrings.scan_file(
        bad_syntax,
        result,
        enforce_bdd=False,
        enforce_google_sections=False,
    )
    assert any(issue.issue_type == "parse_error" for issue in result.issues)

    bad_bytes = tmp_path / "bad_bytes.py"
    bad_bytes.write_bytes(b"\xff")
    result_bytes = check_docstrings.ScanResult()
    check_docstrings.scan_file(
        bad_bytes,
        result_bytes,
        enforce_bdd=False,
        enforce_google_sections=False,
    )
    assert any(issue.issue_type == "parse_error" for issue in result_bytes.issues)


def test_scan_directory_skips_known_dirs(tmp_path: Path) -> None:
    """Verify scan_directory skips known directories.

    Given: A directory containing files in skipped and non-skipped paths,
    When: scan_directory is called,
    Then: Only non-skipped files are scanned.
    """
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "skipped.py").write_text('"""x"""\n', encoding="utf-8")

    (tmp_path / "ok.py").write_text('"""x"""\n', encoding="utf-8")

    result = check_docstrings.ScanResult()
    check_docstrings.scan_directory(
        tmp_path,
        result,
        enforce_bdd=False,
        enforce_google_sections=False,
    )

    assert result.files_scanned == 1


def test_print_results_renders_issue_groups(capsys: pytest.CaptureFixture[str]) -> None:
    """Verify print_results output groups issues.

    Given: A ScanResult with a single issue,
    When: print_results is called,
    Then: It renders the issue group header and details.
    """
    result = check_docstrings.ScanResult(
        issues=[
            check_docstrings.Issue(
                filepath=Path("x.py"),
                line=1,
                name="x.py",
                issue_type="missing_module_docstring",
                message="Module lacks docstring",
            )
        ],
        files_scanned=1,
        modules_checked=1,
        classes_checked=0,
        functions_checked=0,
    )
    check_docstrings.print_results(result, Path("."), verbose=False)
    output = capsys.readouterr().out
    assert "MISSING MODULE DOCSTRING" in output
    assert "x.py:1" in output


def test_main_covers_directory_exists_branches(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify main covers directory-exists branches.

    Given: A forced environment where scanned directories do not exist,
    When: main is called,
    Then: It skips scan_directory calls and returns success.
    """
    original_exists = check_docstrings.Path.exists

    def fake_exists(path: Path) -> bool:
        if path.name in {"src", "tests", "proprietary", "scripts"}:
            return False
        return original_exists(path)

    monkeypatch.setattr(check_docstrings.Path, "exists", fake_exists)
    monkeypatch.setattr(check_docstrings.sys, "argv", ["check_docstrings.py"])
    assert check_docstrings.main() == 0


def test_main_returns_non_strict_success_with_issues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify main behavior when issues exist and strict mode is disabled.

    Given: A scan producing issues and no --strict flag,
    When: main is called,
    Then: It returns success but prints the strict-mode hint.
    """
    recorded: list[tuple[bool, bool]] = []

    def fake_scan_directory(
        _root: Path,
        result: check_docstrings.ScanResult,
        *,
        enforce_bdd: bool,
        enforce_google_sections: bool,
    ) -> None:
        recorded.append((enforce_bdd, enforce_google_sections))
        root_dir = Path(check_docstrings.__file__).resolve().parent.parent
        result.issues.append(
            check_docstrings.Issue(
                filepath=root_dir / "x.py",
                line=1,
                name="x.py",
                issue_type="missing_module_docstring",
                message="Module lacks docstring",
            )
        )

    monkeypatch.setattr(check_docstrings, "scan_directory", fake_scan_directory)
    monkeypatch.setattr(check_docstrings.sys, "argv", ["check_docstrings.py"])
    assert check_docstrings.main() == 0
    assert recorded


def test_main_returns_strict_failure_with_issues(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify main behavior when issues exist in strict mode.

    Given: A scan producing issues and the --strict flag,
    When: main is called,
    Then: It returns failure.
    """

    def fake_scan_directory(
        _root: Path,
        result: check_docstrings.ScanResult,
        *,
        enforce_bdd: bool,
        enforce_google_sections: bool,
    ) -> None:
        assert enforce_bdd is True
        assert enforce_google_sections is True
        root_dir = Path(check_docstrings.__file__).resolve().parent.parent
        result.issues.append(
            check_docstrings.Issue(
                filepath=root_dir / "x.py",
                line=1,
                name="x.py",
                issue_type="missing_module_docstring",
                message="Module lacks docstring",
            )
        )

    monkeypatch.setattr(check_docstrings, "scan_directory", fake_scan_directory)
    monkeypatch.setattr(
        check_docstrings.sys,
        "argv",
        ["check_docstrings.py", "--strict", "--enforce-bdd", "--enforce-google-sections"],
    )
    assert check_docstrings.main() == 1


def test_is_pytest_fixture_detects_attribute_decorator() -> None:
    """Verify is_pytest_fixture detects @pytest.fixture decorator.

    Given: A function decorated with @pytest.fixture (attribute style),
    When: is_pytest_fixture is called,
    Then: It returns True.
    """
    tree = ast.parse("import pytest\n@pytest.fixture\ndef my_fixture():\n    pass\n")
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef))
    assert check_docstrings.is_pytest_fixture(node) is True


def test_is_pytest_fixture_detects_call_decorator() -> None:
    """Verify is_pytest_fixture detects @pytest.fixture() decorator.

    Given: A function decorated with @pytest.fixture() (call style),
    When: is_pytest_fixture is called,
    Then: It returns True.
    """
    tree = ast.parse("import pytest\n@pytest.fixture()\ndef my_fixture():\n    pass\n")
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef))
    assert check_docstrings.is_pytest_fixture(node) is True


def test_is_pytest_fixture_detects_direct_import_call() -> None:
    """Verify is_pytest_fixture detects @fixture() with direct import.

    Given: A function decorated with @fixture() after direct import,
    When: is_pytest_fixture is called,
    Then: It returns True.
    """
    tree = ast.parse("from pytest import fixture\n@fixture()\ndef my_fixture():\n    pass\n")
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef))
    assert check_docstrings.is_pytest_fixture(node) is True


def test_is_pytest_fixture_detects_direct_import_no_call() -> None:
    """Verify is_pytest_fixture detects @fixture without call parens.

    Given: A function decorated with @fixture (no parentheses) after direct import,
    When: is_pytest_fixture is called,
    Then: It returns True.
    """
    tree = ast.parse("from pytest import fixture\n@fixture\ndef my_fixture():\n    pass\n")
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef))
    assert check_docstrings.is_pytest_fixture(node) is True


def test_is_pytest_fixture_returns_false_for_regular_function() -> None:
    """Verify is_pytest_fixture returns False for non-fixtures.

    Given: A regular function without any fixture decorator,
    When: is_pytest_fixture is called,
    Then: It returns False.
    """
    tree = ast.parse("def regular_function():\n    pass\n")
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef))
    assert check_docstrings.is_pytest_fixture(node) is False


def test_is_pytest_fixture_returns_false_for_non_fixture_decorators() -> None:
    """Verify is_pytest_fixture returns False for non-fixture decorators.

    Given: A function with various non-fixture decorators,
    When: is_pytest_fixture is called,
    Then: It returns False for attribute, call, and name decorators that are not fixtures.
    """
    tree = ast.parse("import pytest\n@pytest.mark.skip\ndef f():\n    pass\n")
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef))
    assert check_docstrings.is_pytest_fixture(node) is False

    tree = ast.parse("import pytest\n@pytest.mark.parametrize('x', [1])\ndef f(x):\n    pass\n")
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef))
    assert check_docstrings.is_pytest_fixture(node) is False

    tree = ast.parse("from functools import wraps\n@wraps(None)\ndef f():\n    pass\n")
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef))
    assert check_docstrings.is_pytest_fixture(node) is False

    tree = ast.parse("@staticmethod\ndef f():\n    pass\n")
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef))
    assert check_docstrings.is_pytest_fixture(node) is False


def test_is_fixture_decorator_returns_false_for_call_with_subscript_func() -> None:
    """Verify _is_fixture_decorator returns False for Call wrapping Subscript.

    Given: A decorator that is a Call node whose func is a Subscript
        (e.g., ``@decorators[0]()``),
    When: _is_fixture_decorator is called,
    Then: It returns False because Subscript is not Attribute or Name.
    """
    tree = ast.parse("decorators = [None]\n@decorators[0]()\ndef f():\n    pass\n")
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef))
    assert check_docstrings._is_fixture_decorator(node.decorator_list[0]) is False


def test_is_fixture_decorator_returns_false_for_subscript_decorator() -> None:
    """Verify _is_fixture_decorator returns False for Subscript decorator.

    Given: A decorator that is a Subscript node (e.g., ``@decorators[0]``),
    When: _is_fixture_decorator is called,
    Then: It returns False because Subscript is not Attribute, Call, or Name.
    """
    tree = ast.parse("decorators = [None]\n@decorators[0]\ndef f():\n    pass\n")
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef))
    assert check_docstrings._is_fixture_decorator(node.decorator_list[0]) is False


def test_check_function_skips_fixtures_in_google_validation() -> None:
    """Verify fixtures are skipped in Google docstring validation.

    Given: A pytest fixture with a simple docstring (missing Args/Returns),
    When: check_function is called with enforce_google_sections=True,
    Then: No issues are reported because fixtures are exempt.
    """
    tree = ast.parse(
        "import pytest\n"
        "@pytest.fixture\n"
        "def my_fixture() -> str:\n"
        '    """Provide test data."""\n'
        "    return 'data'\n"
    )
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef))
    result = check_docstrings.ScanResult()

    check_docstrings.check_function(
        Path("pkg/mod.py"),
        node,
        result,
        enforce_google_sections=True,
    )

    assert result.issues == []
