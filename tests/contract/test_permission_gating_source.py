"""Guard the permission-only capability boundary across every client.

Roles are labels for named permission sets. Handwritten capability code must
therefore consume an effective permission or a permission-based resource
requirement, never compare a user's role, traverse a role hierarchy, or call a
role-check helper. The narrow exceptions below cover role display, user CRUD,
development-user lookup, process topology, and the role-to-permission mapping
itself. Generated contracts, tests, fixtures, migrations, and the unrelated
``ProcessRoleEnum`` domain are outside this source-level contract.
"""

import ast
import re
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
_AUTH_ROLE_VALUES = frozenset(
    {"admin", "operator", "viewer", "ai_delegate", "ai_reviewer", "ai_researcher"}
)
_ROLE_HELPERS = frozenset(
    {
        "check_role",
        "has_role",
        "hasrole",
        "minimum_role",
        "require_role",
        "requirerole",
        "role_grants_permission",
        "role_allows",
        "role_at_least",
    }
)
_PYTHON_MAPPING_OWNERS: dict[str, str] = {
    "scripts/generate_types.py": "generated client role-set display contracts",
    "src/snapper/auth/domain/permissions.py": "canonical role-to-permission mapping",
    "src/snapper/auth/tokens.py": "role ceiling and legacy token migration",
    "src/snapper/data/repository.py": "permission-derived principal fan-out queries",
    "src/snapper/data/seed/loader.py": "permission-derived development memberships",
}


@dataclass(frozen=True)
class FindingSignature:
    """Stable identity for one forbidden construct or explicit exception."""

    repository: str
    path: str
    kind: str
    expression: str


@dataclass(frozen=True)
class Finding:
    """A source occurrence with a stable signature and diagnostic line."""

    signature: FindingSignature
    line: int


@dataclass(frozen=True)
class AllowedFinding:
    """One reviewed non-capability role use and its architectural reason."""

    signature: FindingSignature
    reason: str


_ALLOWED_FINDINGS: tuple[AllowedFinding, ...] = (
    AllowedFinding(
        FindingSignature(
            "backend",
            "src/snapper/cli/dev_pat.py",
            "role-comparison",
            'user.role == "admin"',
        ),
        "development credential lookup by the user-domain role field",
    ),
    AllowedFinding(
        FindingSignature(
            "backend",
            "src/snapper/application/ai_researchers/service.py",
            "role-comparison",
            "User.role == UserRole.AI_RESEARCHER.value",
        ),
        "AI researcher user-management ownership and quota query",
    ),
    AllowedFinding(
        FindingSignature(
            "backend",
            "src/snapper/application/ai_delegates/service.py",
            "role-query-membership",
            "User.role.in_(tuple(sorted(AI_REVIEW_PRINCIPAL_ROLES)))",
        ),
        "AI integration user-domain list query for shared delegate lifecycle roles",
    ),
    AllowedFinding(
        FindingSignature(
            "backend",
            "src/snapper/application/ai_delegates/service.py",
            "role-query-membership",
            "User.role.in_(tuple(sorted(AI_REVIEW_PRINCIPAL_ROLES)))",
        ),
        "AI integration user-domain quota query for shared delegate lifecycle roles",
    ),
    AllowedFinding(
        FindingSignature(
            "backend",
            "src/snapper/application/ai_delegates/service.py",
            "role-query-membership",
            "User.role.in_(tuple(sorted(AI_REVIEW_PRINCIPAL_ROLES)))",
        ),
        "AI integration user-domain owner lookup for shared delegate lifecycle roles",
    ),
    AllowedFinding(
        FindingSignature(
            "backend",
            "src/snapper/application/ai_delegates/service.py",
            "role-query-membership",
            "User.role.in_(tuple(sorted(AI_REVIEW_PRINCIPAL_ROLES)))",
        ),
        "AI integration user-domain membership lookup for shared delegate lifecycle roles",
    ),
    AllowedFinding(
        FindingSignature(
            "backend",
            "src/snapper/data/repository.py",
            "role-query-membership",
            "User.role.in_(matching_roles)",
        ),
        "principal fan-out query from a permission-derived role set",
    ),
    AllowedFinding(
        FindingSignature(
            "backend",
            "src/snapper/data/repository.py",
            "role-query-membership",
            "User.role.in_(_AI_REVIEW_DECISION_ROLE_VALUES)",
        ),
        "AI delegate state lookup from a permission-derived role set",
    ),
    AllowedFinding(
        FindingSignature(
            "backend",
            "src/snapper/data/repository.py",
            "role-query-membership",
            "User.role.in_(_AI_REVIEW_DECISION_ROLE_VALUES)",
        ),
        "scoped AI delegate lookup from a permission-derived role set",
    ),
    AllowedFinding(
        FindingSignature(
            "frontend",
            "src/components/auth/UserProfile.tsx",
            "role-switch",
            "switch (role) {",
        ),
        "role badge colour and icon display",
    ),
    AllowedFinding(
        FindingSignature(
            "frontend",
            "src/components/auth/UserProfile.tsx",
            "role-switch",
            "switch (role) {",
        ),
        "role badge colour and icon display",
    ),
    AllowedFinding(
        FindingSignature(
            "frontend",
            "src/features/admin/Admin.tsx",
            "role-permission-lookup",
            "const rolePermissions = ROLE_PERMISSIONS[role]",
        ),
        "read-only admin role-to-permission explanation matrix",
    ),
    AllowedFinding(
        FindingSignature(
            "frontend",
            "src/features/admin/UserManagement/UserForm.tsx",
            "role-comparison",
            "const isDelegate = user?.role === 'ai_delegate'",
        ),
        "role-domain form fields for AI delegate user CRUD",
    ),
    AllowedFinding(
        FindingSignature(
            "frontend",
            "src/features/admin/UserManagement/UserForm.tsx",
            "role-comparison",
            "formData.role !== user.role ||",
        ),
        "role-domain change detection for user CRUD",
    ),
    AllowedFinding(
        FindingSignature(
            "frontend",
            "src/features/admin/UserManagement/UserForm.tsx",
            "role-comparison",
            "formData.role === 'viewer'",
        ),
        "selected role description in the user-management form",
    ),
    AllowedFinding(
        FindingSignature(
            "frontend",
            "src/features/admin/UserManagement/UserForm.tsx",
            "role-comparison",
            ": formData.role === 'operator'",
        ),
        "selected role description in the user-management form",
    ),
    AllowedFinding(
        FindingSignature(
            "frontend",
            "src/features/admin/UserManagement/UserForm.tsx",
            "role-comparison",
            ": formData.role === 'admin'",
        ),
        "selected role description in the user-management form",
    ),
    AllowedFinding(
        FindingSignature(
            "frontend",
            "src/features/admin/UserManagement/UserList.tsx",
            "role-switch",
            "switch (role) {",
        ),
        "role badge colour display",
    ),
    AllowedFinding(
        FindingSignature(
            "frontend",
            "src/features/admin/UserManagement/UserList.tsx",
            "role-switch",
            "switch (role) {",
        ),
        "role badge icon display",
    ),
    AllowedFinding(
        FindingSignature(
            "frontend",
            "src/features/processes/Processes.tsx",
            "role-comparison",
            "process.role !== 'strategy' &&",
        ),
        "process topology uses the unrelated process role domain",
    ),
    AllowedFinding(
        FindingSignature(
            "frontend",
            "src/features/processes/Processes.tsx",
            "role-comparison",
            "process.role !== 'backtest' &&",
        ),
        "process topology uses the unrelated process role domain",
    ),
    AllowedFinding(
        FindingSignature(
            "frontend",
            "src/features/processes/Processes.tsx",
            "role-comparison",
            "process.role !== 'strategy' &&",
        ),
        "process topology uses the unrelated process role domain",
    ),
    AllowedFinding(
        FindingSignature(
            "frontend",
            "src/features/processes/Processes.tsx",
            "role-comparison",
            "process.role !== 'backtest'",
        ),
        "process topology uses the unrelated process role domain",
    ),
    AllowedFinding(
        FindingSignature(
            "frontend",
            "src/features/strategies/Strategies.tsx",
            "role-comparison",
            "return availableProcesses?.payload.filter(process => process.role === 'strategy') ?? []",
        ),
        "strategy filtering uses the unrelated process role domain",
    ),
    AllowedFinding(
        FindingSignature(
            "frontend",
            "src/features/strategies/StrategyLaunchModal.tsx",
            "role-comparison",
            "const isScoped = selectedRole === 'strategy' && referenceEntries.length > 0",
        ),
        "strategy template topology uses the unrelated process role domain",
    ),
    AllowedFinding(
        FindingSignature(
            "ios",
            "Snapper/Views/AdminView.swift",
            "role-switch",
            "switch user.role {",
        ),
        "role badge colour display in user management",
    ),
    AllowedFinding(
        FindingSignature(
            "mcp",
            "src/check.ts",
            "role-comparison",
            "if (role !== null) lines.push(` role: ${role}`);",
        ),
        "CLI diagnostic output displays the authenticated role",
    ),
)


_FOREIGN_ROLE_REFERENCE = (
    r"(?:\b[A-Za-z_$][\w$]*(?:[?!]?\.[A-Za-z_$][\w$]*)*[?!]?\.role\b"
    r"|\b(?:role|[A-Za-z_$][\w$]*(?:Role|_role))\b)"
)
_FOREIGN_COMPARATOR = r"(?:===|!==|==|!=|<=|>=|(?<![=])<(?!=)|(?<![=])>(?!=))"
_FOREIGN_RULES: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "role-check-helper",
        re.compile(r"\b(?:hasRole|has_role|requireRole|require_role|roleAtLeast)\s*\("),
    ),
    (
        "role-hierarchy",
        re.compile(
            r"\b(?:minimumRole|roleHierarchy|roleLevels|roleOrder|roleRanks"
            r"|ROLE_HIERARCHY|ROLE_LEVELS|ROLE_ORDER|ROLE_RANKS)\b"
        ),
    ),
    (
        "role-comparison",
        re.compile(
            rf"(?:{_FOREIGN_ROLE_REFERENCE}\s*{_FOREIGN_COMPARATOR}"
            rf"|{_FOREIGN_COMPARATOR}\s*{_FOREIGN_ROLE_REFERENCE})"
        ),
    ),
    (
        "role-membership",
        re.compile(rf"\b(?:contains|includes)\s*\(\s*{_FOREIGN_ROLE_REFERENCE}\s*\)"),
    ),
    (
        "role-switch",
        re.compile(rf"\bswitch\s*(?:\(\s*)?{_FOREIGN_ROLE_REFERENCE}(?:\s*\))?\s*\{{"),
    ),
    (
        "role-permission-lookup",
        re.compile(r"\b(?:ROLE_PERMISSIONS|rolePermissions)\s*\["),
    ),
    (
        "role-resource-map",
        re.compile(r"\b(?:RESOURCE_ACCESS|resourceAccess)\b"),
    ),
)


def _normalise_expression(expression: str) -> str:
    """Collapse source whitespace so allowlists survive formatting-only movement."""
    return " ".join(expression.split())


def _node_expression(source: str, node: ast.AST) -> str:
    """Return stable source text for an AST node."""
    segment = ast.get_source_segment(source, node)
    return _normalise_expression(segment if segment is not None else ast.unparse(node))


def _call_name(node: ast.Call) -> str:
    """Return the final identifier of a Python call target."""
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return ""


def _is_role_expression(node: ast.AST) -> bool:
    """Return whether an AST expression denotes an authentication role."""
    if isinstance(node, ast.Name):
        return node.id == "role" or node.id.endswith(("_role", "Role"))
    if isinstance(node, ast.Attribute):
        if node.attr == "role":
            return True
        return isinstance(node.value, ast.Name) and node.value.id == "UserRole"
    if isinstance(node, ast.Subscript):
        return isinstance(node.slice, ast.Constant) and node.slice.value == "role"
    if isinstance(node, ast.Call):
        return _call_name(node) in {"get_role", "resolve_role"}
    if isinstance(node, (ast.List, ast.Set, ast.Tuple)):
        return any(_is_role_expression(item) for item in node.elts)
    return False


def _is_principal_role_expression(node: ast.AST) -> bool:
    """Return whether an expression is a user or credential role field."""
    if not isinstance(node, ast.Attribute) or node.attr != "role":
        return False
    current: ast.AST = node.value
    while isinstance(current, ast.Attribute):
        current = current.value
    if not isinstance(current, ast.Name):
        return False
    root = current.id.lower()
    return any(marker in root for marker in ("claim", "owner", "principal", "token", "user"))


def _is_auth_role_literal(node: ast.AST) -> bool:
    """Return whether an expression contains a concrete authentication role."""
    if isinstance(node, ast.Constant):
        return isinstance(node.value, str) and node.value in _AUTH_ROLE_VALUES
    if isinstance(node, ast.Attribute):
        current: ast.AST = node
        while isinstance(current, ast.Attribute):
            current = current.value
        return isinstance(current, ast.Name) and current.id == "UserRole"
    if isinstance(node, (ast.List, ast.Set, ast.Tuple)):
        return any(_is_auth_role_literal(item) for item in node.elts)
    return False


def _comparison_is_auth_role_decision(node: ast.Compare) -> bool:
    """Return whether a comparison makes a decision from an auth role."""
    operands = [node.left, *node.comparators]
    if any(_is_auth_role_literal(operand) for operand in operands):
        return any(_is_role_expression(operand) for operand in operands)
    if any(isinstance(operator, (ast.In, ast.NotIn)) for operator in node.ops):
        return any(_is_role_expression(operand) for operand in operands)
    if any(_is_principal_role_expression(operand) for operand in operands):
        return True
    return sum(_is_role_expression(operand) for operand in operands) >= 2


def _is_role_query_membership(node: ast.Call) -> bool:
    """Return whether a SQL-style membership call targets a role column."""
    return (
        isinstance(node.func, ast.Attribute)
        and node.func.attr in {"in_", "not_in"}
        and isinstance(node.func.value, ast.Attribute)
        and node.func.value.attr == "role"
    )


def _is_role_permission_lookup(node: ast.AST) -> bool:
    """Return whether a node reads the canonical role-permission mapping."""
    if isinstance(node, ast.Subscript):
        return isinstance(node.value, ast.Name) and node.value.id in {
            "BACKEND_ROLE_PERMISSIONS",
            "ROLE_PERMISSIONS",
        }
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        return (
            isinstance(node.func.value, ast.Name)
            and node.func.value.id in {"BACKEND_ROLE_PERMISSIONS", "ROLE_PERMISSIONS"}
            and node.func.attr in {"get", "items", "keys", "values"}
        )
    return False


def _python_sources() -> Iterable[tuple[str, Path]]:
    """Yield handwritten backend Python sources from production roots."""
    for source_root in (_REPOSITORY_ROOT / "src" / "snapper", _REPOSITORY_ROOT / "scripts"):
        for path in sorted(source_root.rglob("*.py")):
            relative = path.relative_to(_REPOSITORY_ROOT)
            lowered_parts = {part.lower() for part in relative.parts}
            if lowered_parts.intersection({"fixtures", "generated", "migrations", "tests"}):
                continue
            yield relative.as_posix(), path


def _scan_python() -> list[Finding]:
    """Find role decisions in handwritten backend Python with AST precision."""
    findings: list[Finding] = []
    for relative, path in _python_sources():
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
        mapping_owner = relative in _PYTHON_MAPPING_OWNERS
        for node in ast.walk(tree):
            kind = ""
            if isinstance(node, ast.Call) and _call_name(node).lower() in _ROLE_HELPERS:
                helper_name = _call_name(node).lower()
                is_canonical_role_projection = (
                    relative == "src/snapper/auth/domain/permissions.py"
                    and helper_name == "role_grants_permission"
                )
                if not is_canonical_role_projection:
                    kind = "role-check-helper"
            elif isinstance(node, ast.Compare) and _comparison_is_auth_role_decision(node):
                kind = "role-comparison"
            elif isinstance(node, ast.Match) and _is_role_expression(node.subject):
                kind = "role-switch"
            elif isinstance(node, ast.Call) and _is_role_query_membership(node):
                kind = "role-query-membership"
            elif not mapping_owner and _is_role_permission_lookup(node):
                kind = "role-permission-lookup"
            if kind:
                findings.append(
                    Finding(
                        FindingSignature(
                            "backend",
                            relative,
                            kind,
                            _node_expression(source, node),
                        ),
                        node.lineno,
                    )
                )
    return findings


def _foreign_sources() -> Iterable[tuple[str, Path, Path]]:
    """Yield handwritten TypeScript and Swift production sources."""
    specifications = (
        ("frontend", _REPOSITORY_ROOT / "frontend" / "src", {".ts", ".tsx"}),
        ("ios", _REPOSITORY_ROOT / "ios" / "Snapper", {".swift"}),
        (
            "mcp",
            _REPOSITORY_ROOT / "integrations" / "snapper-mcp" / "src",
            {".ts", ".tsx"},
        ),
    )
    for repository, source_root, suffixes in specifications:
        for path in sorted(source_root.rglob("*")):
            if not path.is_file() or path.suffix not in suffixes:
                continue
            lowered_parts = {part.lower() for part in path.relative_to(source_root).parts}
            if lowered_parts.intersection({"__tests__", "fixtures", "generated", "tests"}):
                continue
            if ".generated." in path.name or ".test." in path.name or ".spec." in path.name:
                continue
            yield repository, source_root, path


def _line_without_comments(line: str, inside_block: bool) -> tuple[str, bool]:
    """Remove C-family comments from one line while retaining executable text."""
    remaining = line
    pieces: list[str] = []
    while remaining:
        if inside_block:
            block_end = remaining.find("*/")
            if block_end == -1:
                return "".join(pieces), True
            remaining = remaining[block_end + 2 :]
            inside_block = False
            continue
        block_start = remaining.find("/*")
        line_start = remaining.find("//")
        if line_start != -1 and (block_start == -1 or line_start < block_start):
            pieces.append(remaining[:line_start])
            return "".join(pieces), False
        if block_start == -1:
            pieces.append(remaining)
            return "".join(pieces), False
        pieces.append(remaining[:block_start])
        remaining = remaining[block_start + 2 :]
        inside_block = True
    return "".join(pieces), inside_block


def _scan_foreign_sources() -> list[Finding]:
    """Find role decisions in handwritten frontend, iOS, and MCP sources."""
    findings: list[Finding] = []
    for repository, source_root, path in _foreign_sources():
        inside_block = False
        prefix = "Snapper" if repository == "ios" else "src"
        relative = f"{prefix}/{path.relative_to(source_root).as_posix()}"
        for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            code, inside_block = _line_without_comments(raw_line, inside_block)
            expression = _normalise_expression(code)
            if not expression:
                continue
            for kind, pattern in _FOREIGN_RULES:
                if pattern.search(code):
                    findings.append(
                        Finding(
                            FindingSignature(repository, relative, kind, expression),
                            line_number,
                        )
                    )
    return findings


def _format_occurrences(findings: Iterable[Finding]) -> str:
    """Render actionable source diagnostics for a failed contract."""
    return "\n".join(
        f"{finding.signature.repository}/{finding.signature.path}:{finding.line} "
        f"[{finding.signature.kind}] {finding.signature.expression}"
        for finding in sorted(
            findings,
            key=lambda item: (
                item.signature.repository,
                item.signature.path,
                item.line,
                item.signature.kind,
            ),
        )
    )


def test_capability_decisions_never_branch_on_roles() -> None:
    """Reject capability decisions that branch on an authentication role.

    Given: Every handwritten backend, frontend, iOS, and MCP production source,
    When: Executable role decisions are compared with the reviewed exceptions,
    Then: Only mapping, display, role CRUD, and process-topology uses remain.
    """
    findings = [*_scan_python(), *_scan_foreign_sources()]
    observed = Counter(finding.signature for finding in findings)
    allowed = Counter(item.signature for item in _ALLOWED_FINDINGS)
    unexpected_signatures = observed - allowed
    stale_signatures = allowed - observed
    unexpected = [finding for finding in findings if unexpected_signatures[finding.signature] > 0]
    stale = [item for item in _ALLOWED_FINDINGS if stale_signatures[item.signature] > 0]
    details: list[str] = []
    if unexpected:
        details.append(
            "Unexpected role-based decision(s); gate the capability with effective permissions:\n"
            f"{_format_occurrences(unexpected)}"
        )
    if stale:
        details.append(
            "Stale role exception(s); delete them from the allowlist:\n"
            + "\n".join(
                f"{item.signature.repository}/{item.signature.path} "
                f"[{item.signature.kind}] {item.signature.expression}: {item.reason}"
                for item in stale
            )
        )
    assert not details, "\n\n".join(details)
