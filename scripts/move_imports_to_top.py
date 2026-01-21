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
        min_indent = float("inf")
        for line in lines:
            if line.strip():
                indent = len(line) - len(line.lstrip())
                min_indent = min(min_indent, indent)
        if min_indent != float("inf") and min_indent > 0:
            dedented_lines = []
            for line in lines:
                if line.strip():
                    dedented_lines.append(line[int(min_indent) :])
                else:
                    dedented_lines.append(line)
            return "".join(dedented_lines)
    return "".join(lines)


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
        expected_start = visitor.module_docstring_end + 1
        first_import_line = imports_sorted[0][0]
        all_at_top = True
        if first_import_line > expected_start + 1:
            all_at_top = False
        else:
            prev_end = None
            for lineno, node in imports_sorted:
                if prev_end is not None and lineno > prev_end + 2:
                    all_at_top = False
                    break
                prev_end = node.end_lineno or lineno
        if all_at_top:
            return False
        import_statements: list[str] = []
        import_lines_to_remove: set[int] = set()
        for _lineno, node in visitor.imports:
            stmt = get_import_statement(node, lines)
            if stmt:
                import_statements.append(stmt.rstrip() + "\n")
                start = node.lineno - 1
                end = node.end_lineno or node.lineno
                for i in range(start, end):
                    import_lines_to_remove.add(i)
        new_lines: list[str] = []
        if visitor.module_docstring_end > 0:
            new_lines.extend(lines[i] for i in range(visitor.module_docstring_end))
            new_lines.append("\n")
        if import_statements:
            seen = set()
            unique_imports = []
            for stmt in import_statements:
                if stmt not in seen:
                    seen.add(stmt)
                    unique_imports.append(stmt)
            new_lines.extend(unique_imports)
            new_lines.append("\n")
        skip_until = visitor.module_docstring_end
        prev_was_blank = False
        for i, line in enumerate(lines):
            if i < skip_until or i in import_lines_to_remove:
                continue
            if i < (visitor.module_docstring_end + 10):
                if line.strip() == "":
                    if prev_was_blank:
                        continue
                    prev_was_blank = True
                else:
                    prev_was_blank = False
            new_lines.append(line)
        new_content = "".join(new_lines)
        if dry_run:
            print(f"  Would modify {file_path}")
            return True
        file_path.write_text(new_content, encoding="utf-8")
        print(f"  Modified {file_path}")
        return True
    except Exception as e:
        print(f"  Error processing {file_path}: {e}")
        return False


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
    default_excludes = {
        "__pycache__",
        ".venv",
        "venv",
        ".git",
        ".pytest_cache",
        "build",
        "dist",
        "*.egg-info",
    }
    excludes = default_excludes | set(args.exclude)
    modified_count = 0
    total_count = 0
    for path_arg in args.paths:
        path = Path(path_arg)
        if path.is_file():
            if path.suffix == ".py":
                total_count += 1
                if move_imports_to_top(path, args.dry_run):
                    modified_count += 1
        elif path.is_dir():
            for py_file in path.rglob("*.py"):
                if any(exclude in str(py_file) for exclude in excludes):
                    continue
                total_count += 1
                if move_imports_to_top(py_file, args.dry_run):
                    modified_count += 1
    print(
        f"\n{'Would modify' if args.dry_run else 'Modified'} {modified_count}/{total_count} files"
    )
    if modified_count > 0 and not args.dry_run:
        print("\nRemember to run formatters (black, isort) after this script!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
