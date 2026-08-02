"""Move import statements to the top of Python files.

Uses AST parsing to identify import statements scattered throughout files
and consolidates them at the module level, after any module docstring.
"""

import argparse
import ast
from pathlib import Path
from typing import Any


class ImportMover(ast.NodeVisitor):
    """AST visitor that collects import statements and their locations."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.imports: list[tuple[int, ast.Import | ast.ImportFrom]] = []
        self.first_non_import_line: int | None = None
        self.module_docstring_end: int = 0

    def visit_Module(self, node: ast.Module) -> None:
        """Track module docstring end position.

        Args:
            node: The module AST node being visited.
        """
        if (
            node.body
            and isinstance(node.body[0], ast.Expr)
            and isinstance(node.body[0].value, ast.Constant)
            and isinstance(node.body[0].value.value, str)
        ):
            self.module_docstring_end = node.body[0].end_lineno or 0
        self.generic_visit(node)

    def visit_Try(self, node: ast.Try) -> None:
        """Skip try blocks to avoid moving conditional imports.

        Args:
            node: The try statement AST node being visited.
        """
        pass

    def visit_Import(self, node: ast.Import) -> None:
        """Record import statement location.

        Args:
            node: The import statement AST node.
        """
        self.imports.append((node.lineno, node))

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        """Record from-import statement location.

        Args:
            node: The from-import statement AST node.
        """
        self.imports.append((node.lineno, node))

    def visit(self, node: ast.AST) -> Any:
        """Track first non-import line during traversal.

        Args:
            node: The AST node being visited.

        Returns:
            Result from the parent class visit method.
        """
        if (
            self.first_non_import_line is None
            and not isinstance(node, (ast.Module, ast.Import, ast.ImportFrom, ast.Expr))
            and hasattr(node, "lineno")
        ):
            self.first_non_import_line = node.lineno
        return super().visit(node)


def _dedent_lines(lines: list[str]) -> list[str]:
    """Remove common leading whitespace from non-empty lines.

    Args:
        lines: Source lines to dedent.

    Returns:
        Dedented lines, or original lines if no common indent found.
    """
    min_indent = float("inf")
    for line in lines:
        if line.strip():
            indent = len(line) - len(line.lstrip())
            min_indent = min(min_indent, indent)
    if min_indent == float("inf") or min_indent == 0:
        return lines
    return [line[int(min_indent) :] if line.strip() else line for line in lines]


def get_import_statement(node: ast.Import | ast.ImportFrom, source_lines: list[str]) -> str:
    """Extract the source code for an import statement.

    Args:
        node: The import or from-import AST node.
        source_lines: List of source code lines from the file.

    Returns:
        The source code text for the import statement.
    """
    if not hasattr(node, "lineno") or not hasattr(node, "end_lineno"):
        return ""
    start_line = node.lineno - 1
    end_line = node.end_lineno or node.lineno
    lines = source_lines[start_line:end_line]
    if lines:
        lines = _dedent_lines(lines)
    return "".join(lines)


def _imports_already_at_top(
    imports_sorted: list[tuple[int, ast.Import | ast.ImportFrom]],
    expected_start: int,
) -> bool:
    """Check whether all imports are already at the top of the file.

    Args:
        imports_sorted: Sorted list of (lineno, node) tuples.
        expected_start: Expected first import line number.

    Returns:
        True if all imports are contiguous at the top.
    """
    first_import_line = imports_sorted[0][0]
    if first_import_line > expected_start + 1:
        return False
    prev_end = None
    for lineno, node in imports_sorted:
        if prev_end is not None and lineno > prev_end + 2:
            return False
        prev_end = node.end_lineno or lineno
    return True


def _collect_import_statements(
    visitor: ImportMover, lines: list[str]
) -> tuple[list[str], set[int]]:
    """Collect import statement text and line indices to remove.

    Args:
        visitor: ImportMover with collected imports.
        lines: Source file lines.

    Returns:
        Tuple of (import_statements, lines_to_remove).
    """
    import_statements: list[str] = []
    import_lines_to_remove: set[int] = set()
    for _lineno, node in visitor.imports:
        stmt = get_import_statement(node, lines)
        if stmt:
            import_statements.append(stmt.rstrip() + "\n")
            start = node.lineno - 1
            end = node.end_lineno or node.lineno
            import_lines_to_remove.update(range(start, end))
    return import_statements, import_lines_to_remove


def _deduplicate_imports(import_statements: list[str]) -> list[str]:
    """Remove duplicate import statements while preserving order.

    Args:
        import_statements: List of import statement strings.

    Returns:
        Deduplicated list of import statements.
    """
    seen: set[str] = set()
    unique: list[str] = []
    for stmt in import_statements:
        if stmt not in seen:
            seen.add(stmt)
            unique.append(stmt)
    return unique


def _collect_body_lines(
    lines: list[str],
    docstring_end: int,
    import_lines_to_remove: set[int],
) -> list[str]:
    """Collect remaining body lines with blank-line deduplication near the top.

    Args:
        lines: Original source file lines.
        docstring_end: Line index where the module docstring ends.
        import_lines_to_remove: Line indices to skip.

    Returns:
        List of body lines after imports.
    """
    body: list[str] = []
    prev_was_blank = False
    near_top_boundary = docstring_end + 10
    for i, line in enumerate(lines):
        if i < docstring_end or i in import_lines_to_remove:
            continue
        if i < near_top_boundary and line.strip() == "":
            if prev_was_blank:
                continue
            prev_was_blank = True
        else:
            prev_was_blank = False
        body.append(line)
    return body


def _build_new_content(
    lines: list[str],
    visitor: ImportMover,
    import_statements: list[str],
    import_lines_to_remove: set[int],
) -> str:
    """Build new file content with imports moved to top.

    Args:
        lines: Original source file lines.
        visitor: ImportMover with docstring end info.
        import_statements: Collected import statement strings.
        import_lines_to_remove: Line indices to skip from original.

    Returns:
        Rebuilt file content string.
    """
    new_lines: list[str] = []
    if visitor.module_docstring_end > 0:
        new_lines.extend(lines[i] for i in range(visitor.module_docstring_end))
        new_lines.append("\n")
    if import_statements:
        new_lines.extend(_deduplicate_imports(import_statements))
        new_lines.append("\n")
    new_lines.extend(
        _collect_body_lines(lines, visitor.module_docstring_end, import_lines_to_remove)
    )
    return "".join(new_lines)


def move_imports_to_top(file_path: Path, dry_run: bool = False) -> bool:
    """Move all imports in a file to the top after the docstring.

    Args:
        file_path: Path to the Python file to process.
        dry_run: If True, only report changes without modifying the file.

    Returns:
        True if the file was modified (or would be in dry-run mode), False otherwise.
    """
    try:
        content = file_path.read_text(encoding="utf-8")
        lines = content.splitlines(keepends=True)
        try:
            tree = ast.parse(content, filename=str(file_path))
        except SyntaxError as e:
            print(f"  Syntax error in {file_path}: {e}")
            return False
        visitor = ImportMover()
        visitor.visit(tree)
        if not visitor.imports:
            return False
        imports_sorted = sorted(visitor.imports, key=lambda x: x[0])
        if _imports_already_at_top(imports_sorted, visitor.module_docstring_end + 1):
            return False
        import_statements, import_lines_to_remove = _collect_import_statements(visitor, lines)
        new_content = _build_new_content(lines, visitor, import_statements, import_lines_to_remove)
        if dry_run:
            print(f"  Would modify {file_path}")
            return True
        file_path.write_text(new_content, encoding="utf-8")
        print(f"  Modified {file_path}")
        return True
    except Exception as e:
        print(f"  Error processing {file_path}: {e}")
        return False


_DEFAULT_EXCLUDES = {
    "__pycache__",
    ".venv",
    "venv",
    ".git",
    ".pytest_cache",
    "build",
    "dist",
    "*.egg-info",
}


def _collect_python_files(
    paths: list[Path],
    excludes: set[str],
) -> list[Path]:
    """Collect all Python files from given paths, respecting excludes.

    Args:
        paths: File or directory paths to scan.
        excludes: Patterns to skip when scanning directories.

    Returns:
        List of Python file paths to process.
    """
    files: list[Path] = []
    for path in paths:
        if path.is_file() and path.suffix == ".py":
            files.append(path)
        elif path.is_dir():
            for py_file in path.rglob("*.py"):
                if not any(exclude in str(py_file) for exclude in excludes):
                    files.append(py_file)
    return files


def main() -> int:
    """Entry point for the import mover script.

    Returns:
        Exit code (0 for success).
    """
    print("Moving imports to top of Python files...")
    parser = argparse.ArgumentParser(description="Move all imports to the top of Python files")
    parser.add_argument(
        "paths",
        nargs="+",
        type=Path,
        help="Files or directories to process",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be changed without modifying files",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        help="Patterns to exclude (can be specified multiple times)",
    )
    args = parser.parse_args()
    excludes = _DEFAULT_EXCLUDES | set(args.exclude)
    files = _collect_python_files([Path(p) for p in args.paths], excludes)
    modified_count = sum(1 for f in files if move_imports_to_top(f, args.dry_run))
    print(f"\n{'Would modify' if args.dry_run else 'Modified'} {modified_count}/{len(files)} files")
    if modified_count > 0 and not args.dry_run:
        print("\nRemember to run formatters (black, isort) after this script!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
