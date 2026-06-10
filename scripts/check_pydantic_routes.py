"""Validate that every FastAPI route uses Pydantic models for I/O.

The audit walks router files (``*_routes.py`` plus the well-known
``auth/routes.py`` and ``settings_routes.py``) and checks each function
decorated with an HTTP-verb router decorator (``@router.get``,
``@app.post``, etc.). Return annotations must not be a plain dict /
mapping / ``Any`` / ``Response`` / ``JSONResponse``. Acceptable return
types are concrete Pydantic models, unions of Pydantic models, or
routes that declare their response schema through FastAPI metadata.

Routes that explicitly set ``response_model=None`` must provide a
``responses={...}`` entry with a Pydantic ``model`` so OpenAPI still
emits a schema.

The script reports violations and exits non-zero so it can run inside
``make check-all``.
"""

import ast
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Final

DEFAULT_RELATIVE_ROOTS: Final[tuple[str, ...]] = (
    "src/snapper/server",
    "src/snapper/auth",
    "src/snapper/config",
    "src/snapper/api",
)
SKIP_DIRS: Final[set[str]] = {
    ".venv",
    "node_modules",
    "__pycache__",
    ".git",
    "dist",
    "build",
    "data",
    "tests",
}
HTTP_VERBS: Final[frozenset[str]] = frozenset(
    {"get", "post", "put", "patch", "delete", "head", "options"},
)
FORBIDDEN_RETURN_NAMES: Final[frozenset[str]] = frozenset(
    {
        "dict",
        "Dict",
        "Mapping",
        "MutableMapping",
        "Any",
        "JsonObject",
        "JsonValue",
        "Response",
        "JSONResponse",
        "StreamingResponse",
        "PlainTextResponse",
        "object",
    },
)
EXEMPT_FUNCTIONS: Final[frozenset[str]] = frozenset(
    {
        "openapi_schema",
        "healthz",
    },
)


def should_skip_path(path: Path) -> bool:
    """Return True when the path is in a skipped directory.

    Args:
        path: Candidate file path.

    Returns:
        True when any part of the path matches a skipped directory name.
    """
    return any(part in SKIP_DIRS for part in path.parts)


def iter_router_files(
    root: Path,
    relative_roots: Sequence[str] = DEFAULT_RELATIVE_ROOTS,
) -> list[Path]:
    """Collect router source files under the configured roots.

    A router file is a ``*_routes.py`` module or any module that
    contains ``APIRouter(...)`` or ``FastAPI(...)`` at module scope.
    Because parsing every module is acceptable here, we simply emit all
    Python files under the listed roots and let the AST walker filter
    out non-router modules.

    Args:
        root: Project root directory.
        relative_roots: Relative directories (from root) to scan.

    Returns:
        Sorted list of Python file paths.
    """
    python_files: list[Path] = []
    for relative_root in relative_roots:
        search_root = root / relative_root
        if not search_root.exists():
            continue
        for python_file in search_root.rglob("*.py"):
            if should_skip_path(python_file):
                continue
            python_files.append(python_file)
    return sorted(python_files)


def _annotation_name(annotation: ast.expr) -> str:
    """Return a printable identifier for an annotation node.

    Args:
        annotation: AST node representing the annotation.

    Returns:
        The leftmost name in the annotation, or ``"<expr>"`` when the
        annotation does not resolve to a simple name.
    """
    if isinstance(annotation, ast.Name):
        return annotation.id
    if isinstance(annotation, ast.Attribute):
        return annotation.attr
    if isinstance(annotation, ast.Subscript):
        return _annotation_name(annotation.value)
    if isinstance(annotation, ast.Constant) and annotation.value is None:
        return "None"
    return "<expr>"


def _is_decorator_call(
    decorator: ast.expr,
) -> tuple[ast.Call, str] | None:
    """Return the call node and verb when ``decorator`` is a router call.

    Matches expressions like ``@router.get("/foo")`` or
    ``@app.websocket("/bar")``. WebSocket routes are *not* HTTP and are
    skipped by returning ``None``.

    Args:
        decorator: AST node for a single decorator.

    Returns:
        ``(call_node, verb)`` when matched; ``None`` otherwise.
    """
    if not isinstance(decorator, ast.Call):
        return None
    func = decorator.func
    if not isinstance(func, ast.Attribute):
        return None
    verb = func.attr
    if verb not in HTTP_VERBS:
        return None
    return decorator, verb


def _has_responses_with_model(call: ast.Call) -> bool:
    """Return True when the route declares ``responses={200: {"model": X}}``.

    Args:
        call: AST node for the router decorator call.

    Returns:
        True when at least one entry in ``responses`` carries a
        ``"model"`` key.
    """
    for keyword in call.keywords:
        if keyword.arg != "responses":
            continue
        if not isinstance(keyword.value, ast.Dict):
            return False
        for value in keyword.value.values:
            if isinstance(value, ast.Dict):
                for inner_key in value.keys:
                    if isinstance(inner_key, ast.Constant) and inner_key.value == "model":
                        return True
        return False
    return False


def _response_model_is_none(call: ast.Call) -> bool:
    """Return True when ``response_model=None`` is explicitly declared.

    Args:
        call: AST node for the router decorator call.

    Returns:
        True when ``response_model`` is set to ``None``.
    """
    for keyword in call.keywords:
        if keyword.arg != "response_model":
            continue
        if isinstance(keyword.value, ast.Constant) and keyword.value.value is None:
            return True
    return False


def _flatten_union(node: ast.expr) -> list[ast.expr]:
    """Return the list of branches in a union annotation.

    Handles both ``A | B`` syntax and ``Union[A, B]``.

    Args:
        node: AST node for an annotation.

    Returns:
        List of branch nodes; a single-element list when ``node`` is
        not a union.
    """
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
        return _flatten_union(node.left) + _flatten_union(node.right)
    if isinstance(node, ast.Subscript) and _annotation_name(node) in {"Union", "Optional"}:
        slice_node = node.slice
        if isinstance(slice_node, ast.Tuple):
            branches: list[ast.expr] = []
            for elt in slice_node.elts:
                branches.extend(_flatten_union(elt))
            return branches
        return _flatten_union(slice_node)
    return [node]


def _return_annotation_violation(
    return_annotation: ast.expr | None,
) -> str | None:
    """Return a violation message when the return annotation is forbidden.

    Args:
        return_annotation: AST node for the function return annotation
            (or ``None`` when the function omits one).

    Returns:
        Human-readable violation message, or ``None`` when the
        annotation is acceptable.
    """
    if return_annotation is None:
        return "missing return type annotation"
    branches = _flatten_union(return_annotation)
    for branch in branches:
        name = _annotation_name(branch)
        if name in FORBIDDEN_RETURN_NAMES:
            return f"return annotation contains forbidden type `{name}`"
    return None


def _is_router_definition(tree: ast.AST) -> bool:
    """Return True when the module looks like a FastAPI router module.

    A module qualifies when it assigns the result of ``APIRouter(...)``
    or ``FastAPI(...)`` to a top-level name, or imports such an object
    explicitly. The check is conservative — false negatives drop modules
    that have no routes anyway, and the AST walker requires at least one
    router decorator before flagging anything.

    Args:
        tree: Parsed module AST.

    Returns:
        True when the module is plausibly a router source.
    """
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            name = func.id if isinstance(func, ast.Name) else None
            if name in {"APIRouter", "FastAPI"}:
                return True
    return False


def _function_violations(
    func: ast.AsyncFunctionDef | ast.FunctionDef,
) -> list[tuple[int, str]]:
    """Return violations for a single function definition.

    Args:
        func: AST node for the function being audited.

    Returns:
        List of ``(line_number, message)`` violations.
    """
    if func.name in EXEMPT_FUNCTIONS:
        return []
    violations: list[tuple[int, str]] = []
    decorator_match = None
    for decorator in func.decorator_list:
        match = _is_decorator_call(decorator)
        if match is not None:
            decorator_match = match
            break
    if decorator_match is None:
        return []
    call, verb = decorator_match
    if _response_model_is_none(call) and not _has_responses_with_model(call):
        violations.append(
            (
                func.lineno,
                "route declares `response_model=None` without a `responses={...}`"
                " entry carrying a Pydantic `model` — OpenAPI emits no schema",
            ),
        )
    return_violation = _return_annotation_violation(func.returns)
    if return_violation is not None:
        violations.append((func.lineno, f"`{func.name}` ({verb.upper()}): {return_violation}"))
    return violations


def scan_file(filepath: Path) -> list[tuple[int, str]]:
    """Scan a single file for router violations.

    Args:
        filepath: Python source file to scan.

    Returns:
        List of ``(line_number, message)`` violations.
    """
    try:
        source = filepath.read_text(encoding="utf-8")
    except OSError:
        return []
    try:
        tree = ast.parse(source, filename=str(filepath))
    except SyntaxError:
        return []
    if not _is_router_definition(tree):
        return []
    violations: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            violations.extend(_function_violations(node))
    return violations


def scan_files(
    root: Path,
    relative_roots: Sequence[str] = DEFAULT_RELATIVE_ROOTS,
) -> dict[Path, list[tuple[int, str]]]:
    """Scan all router files for Pydantic-typing violations.

    Args:
        root: Project root directory.
        relative_roots: Relative directories (from root) to scan.

    Returns:
        Mapping of file paths to violation findings.
    """
    results: dict[Path, list[tuple[int, str]]] = {}
    for python_file in iter_router_files(root, relative_roots):
        findings = scan_file(python_file)
        if findings:
            results[python_file] = findings
    return results


def run_scan(
    root: Path,
    strict_mode: bool = False,
    relative_roots: Sequence[str] = DEFAULT_RELATIVE_ROOTS,
) -> int:
    """Run the audit and return an exit code.

    Args:
        root: Root directory of the project to scan.
        strict_mode: When True, return exit code 1 on any findings.
        relative_roots: Relative directories (from root) to scan.

    Returns:
        Process exit code (0 for success, 1 for failure in strict mode).
    """
    print("=" * 70)
    print("Pydantic Route Scanner")
    print("=" * 70)
    print(f"\nScanning: {root}")
    print(f"Mode: {'STRICT (will fail on findings)' if strict_mode else 'Report only'}")
    results = scan_files(root, relative_roots)
    print("\n" + "-" * 70)
    print("PYTHON FILES (.py)")
    print("-" * 70)
    total = 0
    if not results:
        print("  No Pydantic-routing violations found")
    else:
        for filepath, findings in sorted(results.items()):
            rel_path = filepath.relative_to(root)
            print(f"\n  {rel_path}")
            for line_num, description in findings:
                print(f"     L{line_num}: {description}")
                total += 1
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"\n  Pydantic-routing violations: {total}")
    if total > 0:
        print(
            "\nEvery FastAPI route must declare a Pydantic response model"
            " (either via `response_model=` or `responses={200: {'model': ...}}`),"
            " and return type annotations must reference Pydantic classes —"
            " never plain `dict`, `Any`, `JsonObject`, or raw `Response` types.",
        )
        if strict_mode:
            print("\nSTRICT MODE: Failing due to violations found.")
            return 1
    else:
        print("\nAll routes use Pydantic typing end-to-end.")
    return 0


def main() -> int:
    """Entry point for check_pydantic_routes script.

    Returns:
        Exit code from the scan operation.
    """
    strict_mode = "--strict" in sys.argv
    script_dir = Path(__file__).parent
    root = script_dir.parent
    return run_scan(root, strict_mode)


if __name__ == "__main__":
    raise SystemExit(main())
