"""Meta-audit — ``datetime.now(UTC)`` confined to entry boundaries in sidecar.

Closes BE-3a R1 blocker B-7 (INV-5 per
``feedback_timestamp_discipline.md``): one ``now`` per logical
operation, minted at the entry boundary and threaded through every
helper / repository call. The sidecar's three entry boundaries are:

- ``NotifySidecar.start`` -> pre-drain + per-received-message now.
- ``NotifySidecar._process_retry_queue_loop`` -> per-tick now.

Every other helper takes ``now: datetime`` as an argument and must
not mint a fresh timestamp internally. A new caller that sneaks in
an inline ``datetime.now(...)`` inside a helper is caught here at
make-check time rather than during review.
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

ALLOWED_ENTRY_BOUNDARIES: frozenset[str] = frozenset(
    {
        "start",
        "_process_retry_queue_loop",
    }
)


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
    """Only ``start`` and ``_process_retry_queue_loop`` may call ``datetime.now``.

    Given: the sidecar source file,
    When: every ``datetime.now(...)`` call's enclosing function name
        is resolved via AST walk,
    Then: the enclosing function is always one of the allowed entry
        boundary methods. Any other call site is a B-7 regression —
        the helper should accept ``now: datetime`` as a parameter.
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
        "datetime.now(...) used outside the allowed entry boundaries "
        f"({sorted(ALLOWED_ENTRY_BOUNDARIES)}): {sorted(set(offending))}. "
        "Per feedback_timestamp_discipline.md, mint one now at the "
        "entry boundary and thread it through helpers as a parameter."
    )
