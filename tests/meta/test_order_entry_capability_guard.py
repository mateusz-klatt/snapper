"""Meta-audit — every order-entry submit route must call ``require_tradable``.

Wires the TradFi P3 Day 3 capability-guard contract from the AST side:
each REST handler that inserts a trade command must first invoke
``snapper.server._capability_guard.require_tradable``. This prevents a
future PR from adding a new submit route (or silently removing the
guard from an existing one) without also wiring the capability check.

Expected submit handlers are enumerated via the ``insert_trade_command``
canonical-sites matrix (``tests/meta/test_insert_trade_command_sites.py``)
— every HTTP-handler site there MUST also appear here. MCP tool sites
are out of scope (MCP uses its own ``validate_user_wallet_scope`` +
``is_tradeable`` pre-check path).
"""

import ast
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[2] / "src" / "snapper"

SUBMIT_HANDLER_SITES: set[tuple[str, str]] = {
    ("src/snapper/server/order_routes.py", "create_order"),
    ("src/snapper/server/execution_plan_routes.py", "create_bracket"),
    ("src/snapper/server/trailing_stop_routes.py", "create_trailing_stop"),
}

_GUARD_NAME = "require_tradable"


def _find_function(tree: ast.AST, name: str) -> ast.AsyncFunctionDef | ast.FunctionDef | None:
    """Return the first top-level function/async-function with the given name."""
    for node in ast.walk(tree):
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)) and node.name == name:
            return node
    return None


def _calls_guard(func: ast.AsyncFunctionDef | ast.FunctionDef) -> bool:
    """Return True when the function body contains any call to ``require_tradable``."""
    for node in ast.walk(func):
        if not isinstance(node, ast.Call):
            continue
        func_ref = node.func
        if isinstance(func_ref, ast.Name) and func_ref.id == _GUARD_NAME:
            return True
        if isinstance(func_ref, ast.Attribute) and func_ref.attr == _GUARD_NAME:
            return True
    return False


def test_every_submit_handler_calls_require_tradable() -> None:
    """Each enumerated submit handler contains a call to ``require_tradable``.

    Given: the SUBMIT_HANDLER_SITES matrix lists all REST submit
        handlers that insert trade commands,
    When: each handler's AST is walked for Call nodes,
    Then: every handler contains at least one call resolving to the
        ``require_tradable`` helper (either plain name or attribute
        access like ``_capability_guard.require_tradable``).
    """
    missing: list[str] = []
    for rel_path, func_name in SUBMIT_HANDLER_SITES:
        file_path = SRC_ROOT.parent.parent / rel_path
        tree = ast.parse(file_path.read_text(encoding="utf-8"))
        func = _find_function(tree, func_name)
        assert func is not None, f"handler {func_name} not found in {rel_path}"
        if not _calls_guard(func):
            missing.append(f"{rel_path}::{func_name}")
    assert not missing, (
        "Submit handlers missing require_tradable call: "
        + ", ".join(missing)
        + ". Every order-entry submit route must enforce "
        + "SymbolExchangeCapability.can_trade via the shared guard."
    )


def test_guard_import_present_in_each_route_module() -> None:
    """Each route module imports ``require_tradable`` from ``_capability_guard``.

    Given: the submit-handler files,
    When: the import graph is inspected,
    Then: every file contains
        ``from snapper.server._capability_guard import require_tradable``.
        Catches the case where a handler still textually calls a same-named
        function but the real guard module has been unimported.
    """
    missing: list[str] = []
    expected_module = "snapper.server._capability_guard"
    for rel_path, _ in SUBMIT_HANDLER_SITES:
        file_path = SRC_ROOT.parent.parent / rel_path
        tree = ast.parse(file_path.read_text(encoding="utf-8"))
        imports_guard = any(
            isinstance(node, ast.ImportFrom)
            and node.module == expected_module
            and any(alias.name == _GUARD_NAME for alias in node.names)
            for node in ast.walk(tree)
        )
        if not imports_guard:
            missing.append(rel_path)
    assert not missing, (
        "Route modules missing guard import: "
        + ", ".join(missing)
        + f". Expected: from {expected_module} import {_GUARD_NAME}"
    )
