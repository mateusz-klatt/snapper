"""Phase 4 meta-audit — ``insert_trade_command`` call-site ownership contract.

Every ``insert_trade_command`` call in ``src/snapper/**`` must pass
an explicit ``ownership=`` kwarg matching plan §1.8. This test is
repo-wide + forward-compatible: a future PR that adds an 8th call
site without updating §1.8 fails CI; a refactor that moves an
existing call to a different file line does NOT fail (the matrix
keys on ``(file, enclosing_function_name)``, not line numbers).

Value-shape policy (§1.8):
    - ``self._ownership`` (attribute access chain ending in
      ``_ownership``) — coordinator-bound site.
    - ``None`` literal — HTTP handler / plan service site (row
      propagates to the owning coordinator's outbox via §3.3 filter).

Positional ``ownership`` argument is rejected; the kwarg must be
visible at the call site so policy is auditable.
"""

import ast
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[2] / "src" / "snapper"

CANONICAL_SITES: set[tuple[str, str]] = {
    ("src/snapper/application/engine/service.py", "_send_order"),
    ("src/snapper/application/plans/service.py", "_reemit_single_stranded_cancel"),
    ("src/snapper/application/plans/service.py", "_dispatch_commands"),
    ("src/snapper/server/order_routes.py", "create_order"),
    ("src/snapper/server/order_routes.py", "_cancel_plan"),
    ("src/snapper/server/trailing_stop_routes.py", "cancel_trailing_stop"),
    ("src/snapper/server/execution_plan_routes.py", "cancel_bracket"),
}


SITE_POLICY: dict[tuple[str, str], str] = {
    ("src/snapper/application/engine/service.py", "_send_order"): "ownership",
    ("src/snapper/application/plans/service.py", "_reemit_single_stranded_cancel"): "none",
    ("src/snapper/application/plans/service.py", "_dispatch_commands"): "none",
    ("src/snapper/server/order_routes.py", "create_order"): "none",
    ("src/snapper/server/order_routes.py", "_cancel_plan"): "none",
    ("src/snapper/server/trailing_stop_routes.py", "cancel_trailing_stop"): "none",
    ("src/snapper/server/execution_plan_routes.py", "cancel_bracket"): "none",
}


def _is_ownership_value_policy(value: ast.expr) -> bool:
    """Accept either ``self._ownership`` attribute chain or ``None``."""
    if isinstance(value, ast.Constant) and value.value is None:
        return True
    if isinstance(value, ast.Attribute) and value.attr == "_ownership":
        base = value.value
        return isinstance(base, ast.Name) and base.id == "self"
    return False


def _classify_ownership_value(value: ast.expr) -> str | None:
    """Return ``"ownership"``, ``"none"``, or ``None`` for the given AST value.

    Used by :func:`test_insert_site_policy_matches_plan_section_1_8` to
    pin each canonical site to its expected policy value. Prevents a
    future accidental swap (e.g., ``_send_order`` changing to
    ``ownership=None``) from passing the weaker
    "any-known-policy-value" check.
    """
    if isinstance(value, ast.Constant) and value.value is None:
        return "none"
    if isinstance(value, ast.Attribute) and value.attr == "_ownership":
        base = value.value
        if isinstance(base, ast.Name) and base.id == "self":
            return "ownership"
    return None


def _find_enclosing_function(
    tree: ast.Module, target_line: int
) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
    """Return the innermost function definition whose body contains ``target_line``."""
    match: ast.FunctionDef | ast.AsyncFunctionDef | None = None
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            end = node.end_lineno if node.end_lineno is not None else node.lineno
            if node.lineno <= target_line <= end:
                if match is None or node.lineno >= match.lineno:
                    match = node
    return match


def _collect_insert_sites(
    path: Path,
) -> list[tuple[str, str, int, ast.Call]]:
    """Return ``(rel_path, fn_name, lineno, call_node)`` for each insert site.

    Only direct attribute-access calls are matched; indirect calls via
    ``getattr`` are out of scope because ``insert_trade_command`` is
    always called directly in this codebase.
    """
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    rel = path.relative_to(SRC_ROOT.parents[1]).as_posix()
    results: list[tuple[str, str, int, ast.Call]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not isinstance(func, ast.Attribute):
            continue
        if func.attr != "insert_trade_command":
            continue
        enclosing = _find_enclosing_function(tree, node.lineno)
        fn_name = enclosing.name if enclosing is not None else "<module>"
        results.append((rel, fn_name, node.lineno, node))
    return results


def test_every_insert_trade_command_call_has_explicit_ownership_kwarg() -> None:
    """Every call site passes ``ownership=`` with a known-policy value.

    Given: an AST walk over every ``.py`` file under
        ``src/snapper/**`` collecting ``insert_trade_command`` calls,
    When: each call is inspected for an explicit ``ownership=``
        kwarg with a policy-conformant value,
    Then: the only accepted values are ``self._ownership``
        (coordinator-bound) or the ``None`` literal (HTTP / plan
        site) — positional ownership is rejected so human review
        can always see the policy at the call site.
    """
    all_sites: list[tuple[str, str, int, ast.Call]] = []
    for py_file in SRC_ROOT.rglob("*.py"):
        all_sites.extend(_collect_insert_sites(py_file))

    violations: list[str] = []
    for rel, fn_name, lineno, call in all_sites:
        kwargs_by_name = {kw.arg: kw.value for kw in call.keywords if kw.arg is not None}
        if "ownership" not in kwargs_by_name:
            violations.append(f"{rel}:{lineno} in {fn_name}(): missing ownership= kwarg")
            continue
        value = kwargs_by_name["ownership"]
        if not _is_ownership_value_policy(value):
            violations.append(
                f"{rel}:{lineno} in {fn_name}(): ownership= value does not match "
                f"§1.8 policy (must be self._ownership OR None literal)"
            )
    assert not violations, "insert_trade_command call-site violations:\n" + "\n".join(violations)


def test_insert_site_policy_matches_plan_section_1_8() -> None:
    """Each canonical site uses its specific policy value.

    Given: the §1.8 ``SITE_POLICY`` dict pinning each
        ``(file, function)`` tuple to ``"ownership"`` or ``"none"``,
    When: every insert site collected via AST walk is classified,
    Then: the actual policy value matches the expected one — a
        future accidental swap (e.g., ``_send_order`` flipped to
        ``ownership=None``) fails CI instead of silently bypassing
        the defense-in-depth guard in production.
    """
    violations: list[str] = []
    for py_file in SRC_ROOT.rglob("*.py"):
        for rel, fn_name, lineno, call in _collect_insert_sites(py_file):
            site = (rel, fn_name)
            if site not in SITE_POLICY:
                continue
            kwargs_by_name = {kw.arg: kw.value for kw in call.keywords if kw.arg is not None}
            actual_value = kwargs_by_name.get("ownership")
            if actual_value is None:
                violations.append(f"{rel}:{lineno} in {fn_name}(): ownership= kwarg missing")
                continue
            actual_policy = _classify_ownership_value(actual_value)
            expected_policy = SITE_POLICY[site]
            if actual_policy != expected_policy:
                violations.append(
                    f"{rel}:{lineno} in {fn_name}(): expected "
                    f"ownership={expected_policy!r} per §1.8, got "
                    f"{actual_policy!r}"
                )
    assert not violations, "per-site policy violations (plan §1.8):\n" + "\n".join(violations)


def test_insert_site_matrix_matches_plan_section_1_8() -> None:
    """Collected ``(file, function)`` set matches §1.8 canonical matrix.

    Given: the :data:`CANONICAL_SITES` set enumerating the 7
        known insert sites per plan §1.8,
    When: every ``insert_trade_command`` call in
        ``src/snapper/**`` is collected via AST walk,
    Then: the collected set equals the canonical set — adding a
        new site without updating §1.8 fails CI, and removing a
        listed site without cleaning the canonical matrix also
        fails CI.
    """
    collected: set[tuple[str, str]] = set()
    for py_file in SRC_ROOT.rglob("*.py"):
        for rel, fn_name, _lineno, _call in _collect_insert_sites(py_file):
            collected.add((rel, fn_name))
    missing = CANONICAL_SITES - collected
    unexpected = collected - CANONICAL_SITES
    assert not missing, "expected §1.8 sites missing from src/:\n" + "\n".join(
        f"  {f}::{fn}" for f, fn in sorted(missing)
    )
    assert not unexpected, (
        "new insert_trade_command sites not listed in §1.8 — update "
        "plan_trade_runtime_phase4.md §1.8 AND CANONICAL_SITES:\n"
        + "\n".join(f"  {f}::{fn}" for f, fn in sorted(unexpected))
    )
