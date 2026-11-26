"""Meta-audit — the sidecar reads wall-clock time only through its injectable clock.

Invariant: ``NotifySidecar`` never calls ``datetime.now(...)`` inline.
The single call lives in ``__init__`` as the default of the injectable
``self._clock``. Every timestamp is sampled through ``self._clock()``
so tests can drive time deterministically. Expiry, backoff and SCD2
writes sample a fresh instant immediately before each send or write
rather than reusing a tick-wide value that a long tick would make
stale. A helper that sneaks in an inline ``datetime.now(...)`` would
bypass the injected clock and is caught here at make-check time.
"""

import ast
from pathlib import Path

SIDECAR_FILE = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "snapper"
    / "application"
    / "notify"
    / "sidecar.py"
)

ALLOWED_ENTRY_BOUNDARIES: frozenset[str] = frozenset({"__init__"})


def _is_datetime_now_call(node: ast.AST) -> bool:
    """True when ``node`` is a call like ``datetime.now(...)`` or ``dt.datetime.now(...)``."""
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if isinstance(func, ast.Attribute) and func.attr == "now":
        value = func.value
        if isinstance(value, ast.Name) and value.id == "datetime":
            return True
        if (
            isinstance(value, ast.Attribute)
            and value.attr == "datetime"
            and isinstance(value.value, ast.Name)
        ):
            return True
    return False


def _enclosing_function_name(tree: ast.AST, target: ast.AST) -> str | None:
    """Return the name of the nearest enclosing function/method for ``target``."""
    for node in ast.walk(tree):
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)):
            for inner in ast.walk(node):
                if inner is target:
                    return node.name
    return None


def test_datetime_now_only_at_entry_boundaries() -> None:
    """Only the injectable clock default in ``__init__`` may call ``datetime.now``.

    Given: the sidecar source file,
    When: every ``datetime.now(...)`` call's enclosing function name
        is resolved via AST walk,
    Then: the enclosing function is always ``__init__``, where the
        default ``self._clock`` is built. Any other call site is a
        regression; the helper should sample ``self._clock()`` instead.
    """
    tree = ast.parse(SIDECAR_FILE.read_text(encoding="utf-8"))
    offending: list[str] = []
    for node in ast.walk(tree):
        if not _is_datetime_now_call(node):
            continue
        func_name = _enclosing_function_name(tree, node)
        if func_name is None:
            offending.append("<module-level>")
            continue
        if func_name not in ALLOWED_ENTRY_BOUNDARIES:
            offending.append(func_name)
    assert not offending, (
        "datetime.now(...) used outside the injectable clock default "
        f"({sorted(ALLOWED_ENTRY_BOUNDARIES)}): {sorted(set(offending))}. "
        "Sample self._clock() instead so the injected clock stays authoritative."
    )
