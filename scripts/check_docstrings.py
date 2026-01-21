"""Docstring compliance checker for Python files.

Scans Python files and validates:
- Source files: Google-style docstrings for modules, classes, functions
- Test files: Docstrings for test_* functions (optionally BDD Given/When/Then)

Usage:
    python scripts/check_docstrings.py [--strict] [--verbose] [--enforce-bdd] [--enforce-google-sections]

Options:
    --strict   Exit with code 1 if any issues found
    --verbose  Show passing checks too
    --enforce-bdd             Require Given/When/Then docstrings for test_* functions
    --enforce-google-sections Require Args/Returns sections for functions
"""

import ast
import sys
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path

SKIP_DIRS = {
    ".venv",
    "node_modules",
    "__pycache__",
    ".git",
    "dist",
    "build",
    ".pytest_cache",
    "data",
    "migrations",
}

TEST_DIRS = {"tests", "proprietary/tests"}


@dataclass
class Issue:
    """A docstring compliance issue.

    Attributes:
        filepath: Path to the file with the issue.
        line: Line number where the issue occurs.
        name: Name of the module/class/function.
        issue_type: Category of the issue.
        message: Human-readable description.
    """

    filepath: Path
    line: int
    name: str
    issue_type: str
    message: str


@dataclass
class ScanResult:
    """Result of scanning all files.

    Attributes:
        issues: List of found issues.
        files_scanned: Number of files scanned.
        modules_checked: Number of modules checked.
        classes_checked: Number of classes checked.
        functions_checked: Number of functions checked.
    """

    issues: list[Issue] = field(default_factory=list)
    files_scanned: int = 0
    modules_checked: int = 0
    classes_checked: int = 0
    functions_checked: int = 0


def is_test_file(filepath: Path) -> bool:
    """Check if file is a test file based on path and name.

    Args:
        filepath: Path to the file to check.

    Returns:
        True if the file is in a tests directory.
    """
    path_str = str(filepath)
    return "/tests/" in path_str or "\\tests\\" in path_str


def is_test_function(name: str) -> bool:
    """Check if function name indicates a test function.

    Args:
        name: The function name to check.

    Returns:
        True if the name starts with 'test_'.
    """
    return name.startswith("test_")


def is_pytest_fixture(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    """Check if function is decorated with @pytest.fixture.

    Args:
        node: The function AST node.

    Returns:
        True if the function has a pytest.fixture decorator.
    """
    for decorator in node.decorator_list:
        if isinstance(decorator, ast.Attribute):
            if decorator.attr == "fixture":
                return True
        elif isinstance(decorator, ast.Call):
            func = decorator.func
            if isinstance(func, ast.Attribute) and func.attr == "fixture":
                return True
            if isinstance(func, ast.Name) and func.id == "fixture":
                return True
        elif isinstance(decorator, ast.Name) and decorator.id == "fixture":
            return True
    return False


def extract_dunder_all(tree: ast.Module) -> set[str] | None:
    """Extract `__all__` from a module if it is statically defined.

    Supports `__all__` assigned as a literal list/tuple/set of string literals.

    Args:
        tree: The parsed AST of a Python module.

    Returns:
        Set of exported names, or None if `__all__` is not present or cannot
        be resolved statically.
    """

    def extract_string_sequence(node: ast.AST) -> list[str] | None:
        if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
            values: list[str] = []
            for elt in node.elts:
                if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                    values.append(elt.value)
                else:
                    return None
            return values
        return None

    for node in tree.body:
        value: ast.AST | None = None
        if isinstance(node, ast.Assign):
            if any(
                isinstance(target, ast.Name) and target.id == "__all__" for target in node.targets
            ):
                value = node.value
        elif isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name) and node.target.id == "__all__":
                value = node.value

        if value is None:
            continue

        extracted = extract_string_sequence(value)
        if extracted is None:
            return None
        return set(extracted)

    return None


def get_docstring(node: ast.AST) -> str | None:
    """Extract docstring from AST node if present.

    Args:
        node: The AST node to extract the docstring from.

    Returns:
        The docstring if present, None otherwise.
    """
    if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
        return ast.get_docstring(node)
    return None


def validate_google_docstring(
    docstring: str,
    node: ast.FunctionDef | ast.AsyncFunctionDef,
) -> list[str]:
    """Validate Google-style docstring for a function.

    Args:
        docstring: The docstring to validate.
        node: The AST node of the function.

    Returns:
        List of validation error messages.
    """
    errors: list[str] = []

    params = [arg.arg for arg in node.args.args if arg.arg not in ("self", "cls")]
    params.extend(arg.arg for arg in node.args.kwonlyargs)

    if params and "Args:" not in docstring:
        errors.append(f"Missing 'Args:' section for params: {', '.join(params)}")

    if node.returns:
        return_annotation = (
            ast.unparse(node.returns) if hasattr(ast, "unparse") else str(node.returns)
        )
        if return_annotation not in ("None", "None:"):
            if "Returns:" not in docstring and "Yields:" not in docstring:
                errors.append("Missing 'Returns:' or 'Yields:' section")

    return errors


def validate_bdd_docstring(docstring: str) -> list[str]:
    """Validate BDD-style docstring for a test function.

    Expected format:
        Short description.

        Given ...,
        When ...,
        Then ...

    Args:
        docstring: The docstring to validate.

    Returns:
        List of validation error messages.
    """
    errors: list[str] = []
    lines = docstring.strip().split("\n")

    if len(lines) < 4:
        errors.append("BDD docstring too short (need: description, blank, Given, When, Then)")
        return errors

    content = docstring.lower()
    if "given" not in content:
        errors.append("Missing 'Given' clause")
    if "when" not in content:
        errors.append("Missing 'When' clause")
    if "then" not in content:
        errors.append("Missing 'Then' clause")

    return errors


def check_module(filepath: Path, tree: ast.Module, result: ScanResult) -> None:
    """Check module-level docstring.

    Args:
        filepath: Path to the file being checked.
        tree: Parsed AST of the module.
        result: ScanResult to accumulate findings.
    """
    result.modules_checked += 1

    if filepath.name == "__init__.py":
        return

    docstring = get_docstring(tree)
    if not docstring:
        result.issues.append(
            Issue(
                filepath=filepath,
                line=1,
                name=filepath.name,
                issue_type="missing_module_docstring",
                message="Module lacks docstring",
            )
        )


def check_class(
    filepath: Path,
    node: ast.ClassDef,
    result: ScanResult,
) -> None:
    """Check class docstring.

    Args:
        filepath: Path to the file being checked.
        node: The ClassDef AST node.
        result: ScanResult to accumulate findings.
    """
    result.classes_checked += 1

    docstring = get_docstring(node)
    if not docstring:
        result.issues.append(
            Issue(
                filepath=filepath,
                line=node.lineno,
                name=node.name,
                issue_type="missing_class_docstring",
                message=f"Class '{node.name}' lacks docstring",
            )
        )
        return

    for item in node.body:
        if isinstance(item, ast.FunctionDef) and item.name == "__init__":
            pass


def check_function(
    filepath: Path,
    node: ast.FunctionDef | ast.AsyncFunctionDef,
    result: ScanResult,
    is_method: bool = False,
    enforce_bdd: bool = False,
    enforce_google_sections: bool = False,
) -> None:
    """Check function/method docstring.

    Args:
        filepath: Path to the file being checked.
        node: The FunctionDef AST node.
        result: ScanResult to accumulate findings.
        is_method: Whether this is a class method.
        enforce_bdd: Whether to enforce Given/When/Then docstrings for tests.
        enforce_google_sections: Whether to enforce Args/Returns sections.
    """
    result.functions_checked += 1

    if node.name.startswith("_") and not node.name.startswith("__"):
        return

    if node.name.startswith("__"):
        return

    docstring = get_docstring(node)
    is_test = is_test_file(filepath) and is_test_function(node.name) and not is_pytest_fixture(node)

    if not docstring:
        if is_test:
            result.issues.append(
                Issue(
                    filepath=filepath,
                    line=node.lineno,
                    name=node.name,
                    issue_type="missing_test_docstring",
                    message=f"Test '{node.name}' lacks BDD docstring",
                )
            )
        else:
            result.issues.append(
                Issue(
                    filepath=filepath,
                    line=node.lineno,
                    name=node.name,
                    issue_type="missing_function_docstring",
                    message=f"Function '{node.name}' lacks docstring",
                )
            )
        return

    if is_test and enforce_bdd:
        errors = validate_bdd_docstring(docstring)
        for error in errors:
            result.issues.append(
                Issue(
                    filepath=filepath,
                    line=node.lineno,
                    name=node.name,
                    issue_type="invalid_bdd_docstring",
                    message=f"Test '{node.name}': {error}",
                )
            )
    elif not is_test and enforce_google_sections:
        if is_pytest_fixture(node):
            return
        errors = validate_google_docstring(docstring, node)
        for error in errors:
            result.issues.append(
                Issue(
                    filepath=filepath,
                    line=node.lineno,
                    name=node.name,
                    issue_type="invalid_google_docstring",
                    message=f"Function '{node.name}': {error}",
                )
            )


def scan_file(
    filepath: Path,
    result: ScanResult,
    *,
    enforce_bdd: bool,
    enforce_google_sections: bool,
) -> None:
    """Scan a single Python file for docstring issues.

    Args:
        filepath: Path to the Python file.
        result: ScanResult to accumulate findings.
        enforce_bdd: Whether to enforce Given/When/Then docstrings for tests.
        enforce_google_sections: Whether to enforce Args/Returns sections.
    """
    try:
        content = filepath.read_text(encoding="utf-8")
        tree = ast.parse(content, filename=str(filepath))
    except (SyntaxError, UnicodeDecodeError) as e:
        result.issues.append(
            Issue(
                filepath=filepath,
                line=1,
                name=filepath.name,
                issue_type="parse_error",
                message=f"Could not parse: {e}",
            )
        )
        return

    result.files_scanned += 1

    is_test_module = is_test_file(filepath)
    exported_names = extract_dunder_all(tree)

    check_module(filepath, tree, result)

    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            if is_test_module:
                continue
            if exported_names is not None and node.name not in exported_names:
                continue
            if node.name.startswith("_"):
                continue
            check_class(filepath, node, result)
            for item in node.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    check_function(
                        filepath,
                        item,
                        result,
                        is_method=True,
                        enforce_bdd=enforce_bdd,
                        enforce_google_sections=enforce_google_sections,
                    )
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if is_test_module and not is_test_function(node.name):
                continue
            if not is_test_module:
                if exported_names is not None and node.name not in exported_names:
                    continue
                if node.name.startswith("_"):
                    continue
            check_function(
                filepath,
                node,
                result,
                is_method=False,
                enforce_bdd=enforce_bdd,
                enforce_google_sections=enforce_google_sections,
            )


def scan_directory(
    root: Path,
    result: ScanResult,
    *,
    enforce_bdd: bool,
    enforce_google_sections: bool,
) -> None:
    """Recursively scan directory for Python files.

    Args:
        root: Root directory to scan.
        result: ScanResult to accumulate findings.
        enforce_bdd: Whether to enforce Given/When/Then docstrings for tests.
        enforce_google_sections: Whether to enforce Args/Returns sections.
    """
    for py_file in root.rglob("*.py"):
        if any(skip in py_file.parts for skip in SKIP_DIRS):
            continue
        scan_file(
            py_file,
            result,
            enforce_bdd=enforce_bdd,
            enforce_google_sections=enforce_google_sections,
        )


def print_results(result: ScanResult, root: Path, verbose: bool = False) -> None:
    """Print scan results to stdout.

    Args:
        result: The scan result to print.
        root: Root directory for relative path display.
        verbose: Whether to show verbose output.
    """
    print("=" * 70)
    print("Docstring Compliance Scanner")
    print("=" * 70)
    print(f"\nScanned: {result.files_scanned} files")
    print(
        f"Checked: {result.modules_checked} modules, "
        f"{result.classes_checked} classes, "
        f"{result.functions_checked} functions"
    )

    if not result.issues:
        print("\nNo issues found. All docstrings compliant!")
        return

    by_type: dict[str, list[Issue]] = {}
    for issue in result.issues:
        by_type.setdefault(issue.issue_type, []).append(issue)

    print(f"\nFound {len(result.issues)} issues:\n")

    for issue_type, issues in sorted(by_type.items()):
        print("-" * 70)
        print(f"{issue_type.upper().replace('_', ' ')} ({len(issues)})")
        print("-" * 70)
        for issue in sorted(issues, key=lambda i: (str(i.filepath), i.line)):
            rel_path = issue.filepath.relative_to(root)
            print(f"  {rel_path}:{issue.line}")
            print(f"    {issue.message}")
        print()


def main() -> int:
    """Entry point for check_docstrings script.

    Returns:
        Exit code (0 for success, 1 for issues in strict mode).
    """
    strict_mode = "--strict" in sys.argv
    verbose = "--verbose" in sys.argv
    enforce_bdd = "--enforce-bdd" in sys.argv
    enforce_google_sections = "--enforce-google-sections" in sys.argv

    script_dir = Path(__file__).parent
    root = script_dir.parent

    result = ScanResult()

    src_dir = root / "src"
    if src_dir.exists():
        scan_directory(
            src_dir,
            result,
            enforce_bdd=enforce_bdd,
            enforce_google_sections=enforce_google_sections,
        )

    tests_dir = root / "tests"
    if tests_dir.exists():
        scan_directory(
            tests_dir,
            result,
            enforce_bdd=enforce_bdd,
            enforce_google_sections=enforce_google_sections,
        )

    proprietary_dir = root / "proprietary"
    if proprietary_dir.exists():
        scan_directory(
            proprietary_dir,
            result,
            enforce_bdd=enforce_bdd,
            enforce_google_sections=enforce_google_sections,
        )

    scripts_dir = root / "scripts"
    if scripts_dir.exists():
        scan_directory(
            scripts_dir,
            result,
            enforce_bdd=enforce_bdd,
            enforce_google_sections=enforce_google_sections,
        )

    print_results(result, root, verbose)

    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"\n  Total issues: {len(result.issues)}")

    if result.issues:
        if strict_mode:
            print("\nSTRICT MODE: Failing due to issues found.")
            return 1
        print("\nRun with --strict to fail on issues.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
