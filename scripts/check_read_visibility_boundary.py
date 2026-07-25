"""Enforce the read/trade wallet-visibility split in the REST server package.

``snapper.server.scoping`` deliberately exposes TWO primitives rather than
one function with an ``intent`` flag: ``resolve_readable_wallets`` (operator
scope grants UNION the user's personal ``wallet_user_read_grants``) and
``resolve_tradable_wallets`` (operator scope grants alone). The split only
buys anything while it holds, and convention alone does not hold it: the day
a viewer cannot open a detail page, the cheapest edit is to re-point one
mutation route at the wider primitive.

Six assertions, all AST-derived:

Verb linkage
    The transitive-caller closure of the read plane inside
    ``src/snapper/server`` is computed to a fixed point, and no function in
    it may carry a ``post``/``put``/``patch``/``delete`` route. A one-hop or
    name-only scan cannot prove this: the read plane is reached through
    shared helpers such as ``load_readable_execution_plan``, so the leak
    this guard exists to catch would appear several hops above the call
    site.

    The closure is seeded on ``resolve_readable_wallets`` AND on every call
    of ``Repository.list_readable_wallets_for_user``, because the
    repository method is a second door into the same wider set and
    ``wallet_routes.list_wallets`` already walks through it. Seeding on the
    REST primitive alone would leave a mutation route free to query the
    read plane directly.

    A function's verbs come from its verb decorator (``@router.post``),
    from a generic ``@router.api_route(..., methods=[...])`` decorator,
    from any ``add_api_route(..., methods=[...])`` registration naming it
    (a handler passed as a value carries no decorator at all, and
    ``app.py`` already registers six endpoints that way), and from a
    module-level alias of any of those three attributes (``_post =
    router.post`` then ``@_post(...)``). Its edges include the
    ``Depends``/``Security`` providers declared in its signature or route
    decorator, which run for the route without ever being called in its
    body.

Package fence
    The closure above is bounded to ``src/snapper/server``, which proves
    something only while every route-to-primitive path stays inside that
    package. So no module elsewhere under ``src/snapper`` may import the
    scoping module or name the read primitive, and a mutation route cannot
    reach the read plane by hopping out of the package and back.

Name integrity
    Neither the read primitive nor the repository read method may be
    re-bound under a second name -- by aliased import or by assignment --
    inside the server package. Both of the checks above match on a name:
    the fence exempts this package because it owns the primitive, so a
    renamed re-export here would be importable from anywhere under a name
    the fence cannot recognise, and a locally rebound
    ``list_readable_wallets_for_user`` would be called under a name the
    closure seed does not know.

Import containment
    The read primitive and ``Repository.list_readable_wallets_for_user`` are
    unreachable from the MCP tool surface, the plan cancel service, the core
    wallet resolver, and the process-manager strategy scope. MCP mirrors the
    TRADE plane on purpose (delegates are trade principals), so read grants
    must not leak into it.

Trade call-site pinning
    Every ``resolve_tradable_wallets`` call site is pinned by enclosing
    function. A new one fails until it is added deliberately, and a
    disappeared one fails too, so the pin cannot go stale while a trade gate
    is quietly deleted.

Trade claim-gate consumer pinning
    Every function that declares ``require_tradable_active_wallet`` as a
    dependency is pinned the same way, both directions. Pinning only the
    gate's own internal ``resolve_tradable_wallets`` call proves the helper
    still consults the trade plane, which stays true no matter how many
    routes stop using it: deleting the ``Depends`` line from a write route
    leaves the helper untouched and every other assertion green. The
    consumers are the thing worth pinning, because each one is a route that
    would otherwise fall back to the raw, read-plane-minted
    ``active_wallet_public_id`` claim.

Edges are resolved through each module's import table and same-module
definitions rather than by bare name, because bare-name matching links
``repo.get_execution_plan`` to a route handler that happens to share the
name and drowns the real signal. The import table keeps the ORIGINAL name
of an aliased symbol, so ``from .helpers import load_plan as load`` still
resolves to ``helpers.load_plan``. Calls routed through an object attribute
that is not an imported module alias form no edge -- except for the two
scoping primitives and the repository read method, whose names are unique in
the tree and always form an edge however they are spelled.

Working on the AST rather than on text is also what keeps the containment
check honest: ``snapper/mcp/auth.py`` and ``snapper/mcp/tools.py`` both
*name* ``resolve_readable_wallets`` in prose to record that MCP stays off
the read plane, and a regex scan would have to be weakened to tolerate
that documentation.

What this checker does NOT prove
This section is load-bearing. A guard that is read as proving more than it
proves is worse than no guard, because it retires the suspicion that would
have found the rest. Five classes are out of scope; the first is out of reach
of ANY AST guard, and the second is one this checker's verb model does not
merely miss but actively blesses:

Claim-mediated, cross-request escalation
    A value written into JWT claims under one authorization plane on one
    request and consumed as authority under a different plane on a LATER
    request. The two halves are separate requests, so there is no
    call-graph edge between them and nothing here can join them. This is
    not hypothetical: ``POST /auth/refresh`` validates
    ``active_wallet_public_id`` against the READ plane (deliberately -- a
    read-granted user must be able to pin the hint), and the backtest
    write routes then took that claim as the wallet to persist against.
    Every assertion below passed while that hole was open, and would pass
    again. The mechanism that covers this class is
    ``snapper.server.scoping.require_tradable_active_wallet``, a
    dependency that re-resolves the claim through
    ``resolve_tradable_wallets`` at consumption time; the trade-pin above
    is what keeps THAT gate from being deleted quietly. When a new claim
    starts carrying authority, a checker rule is not the remedy -- a
    consumption-time re-resolution is.

Side-effecting reads
    The verb model is the whole basis of the verb-linkage assertion, and it
    equates "safe for the read plane" with "not routed on POST/PUT/PATCH/
    DELETE". A GET that WRITES is therefore not merely unreported, it is
    actively blessed: routing it on ``get`` is exactly what makes it pass.
    The worked example is real. ``GET /api/portfolio/pnl/series`` and
    ``GET /api/portfolio/pnl/timeline`` reach
    ``Repository.record_portfolio_pnl_anchor_if_execution_prefix_matches``
    through ``build_wallet_pnl_series`` ->
    ``pnl_timeline_service._load_or_create_anchor``, and durably create the
    activation anchor that permanently defines where a scope's P&L history
    begins. When those routes moved to ``resolve_readable_wallets`` this
    checker stayed green, while the set of principals that could author an
    anchor silently grew by everyone holding a personal read grant. The
    remedy is not a verb rule: those endpoints derive anchor creation from
    ``resolve_tradable_wallets`` separately from response authorization, and
    the trade-call-site pin above is what keeps THAT consult from being
    deleted. Read the two planes at every GET that persists anything; this
    checker will not.

Non-literal ``methods=`` values
    Verbs are read only from string constants inside a literal sequence.
    ``methods=_MUTATING`` or a comprehension contributes no verbs, so a
    read-closure handler routed that way is not reported.

Dynamic dispatch and indirection
    ``getattr``, ``importlib.import_module``, a route registered on a
    router this scan cannot resolve, and function references handed to
    anything other than a route registration or a dependency marker
    (``functools.partial``, for instance).

Runtime scope
    First-party ``src/snapper`` sources only. Third-party middleware,
    generated code and the database's own row-level rules are not read.

The package fence and the name-integrity rule are what stop the last two
from becoming an open escape hatch: the primitive keeps one name and lives
in one package, so laundering it requires an edit this checker does see.

This is a development-time architectural honesty check on first-party code.
It fails closed on a missing scan root, on a missing contained path, on
sources it cannot read or parse, and when the read primitive itself cannot
be found. Every finding is reported as ``path:line:detail``.
"""

import ast
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Final

SCAN_ROOT: Final[str] = "src/snapper/server"
PRIMITIVE_FENCE_ROOT: Final[str] = "src/snapper"
SERVER_PACKAGE: Final[str] = "snapper.server"
SCOPING_MODULE: Final[str] = "snapper.server.scoping"

READ_PRIMITIVE: Final[str] = "resolve_readable_wallets"
TRADE_PRIMITIVE: Final[str] = "resolve_tradable_wallets"
TRADE_CLAIM_GATE: Final[str] = "require_tradable_active_wallet"
READ_REPOSITORY_METHOD: Final[str] = "list_readable_wallets_for_user"

PINNED_PRIMITIVES: Final[frozenset[str]] = frozenset({READ_PRIMITIVE, TRADE_PRIMITIVE})
FORBIDDEN_READ_SYMBOLS: Final[frozenset[str]] = frozenset({READ_PRIMITIVE, READ_REPOSITORY_METHOD})
FENCED_READ_SYMBOLS: Final[frozenset[str]] = frozenset({READ_PRIMITIVE})
MUTATION_VERBS: Final[frozenset[str]] = frozenset({"post", "put", "patch", "delete"})

ROUTE_REGISTRAR: Final[str] = "add_api_route"
ROUTE_DECORATOR: Final[str] = "api_route"
ROUTE_ATTRIBUTES: Final[frozenset[str]] = MUTATION_VERBS | {ROUTE_DECORATOR, ROUTE_REGISTRAR}
DEPENDENCY_MARKERS: Final[frozenset[str]] = frozenset({"Depends", "Security"})

CONTAINED_PATHS: Final[tuple[str, ...]] = (
    "src/snapper/mcp",
    "src/snapper/application/plans/cancel_service.py",
    "src/snapper/core/wallet_resolution.py",
    "src/snapper/application/process_manager/strategy_scope.py",
)

TRADE_CALL_SITES: Final[frozenset[tuple[str, str]]] = frozenset(
    {
        ("_plan_route_helpers", "load_open_accessible_cycle"),
        ("_plan_route_helpers", "load_tradable_execution_plan"),
        ("order_routes", "_resolve_create_order_wallet"),
        ("paired_execution_routes", "terminalize_paired_execution_group"),
        ("portfolio_timeline_routes", "_validate_timeline_request"),
        ("scoping", "require_tradable_active_wallet"),
    }
)
"""Enclosing functions permitted to consult the trade plane.

``portfolio_timeline_routes._validate_timeline_request`` is the one entry that
is NOT a mutation gate. Both P&L endpoints are GETs authorized on the read
plane, and they consult the trade plane for a second, narrower question: may
this caller PERSIST the activation anchor this read would otherwise create?
Pinning it here is what stops that consult being deleted, which would return
the endpoints to letting any read grant author a scope's P&L origin."""

TRADE_CLAIM_CONSUMERS: Final[frozenset[tuple[str, str]]] = frozenset(
    {
        ("backtest_routes", "cancel_backtest"),
        ("backtest_routes", "create_backtest"),
        ("backtest_routes", "create_comparison"),
        ("backtest_routes", "rerun_backtest"),
    }
)
"""Route handlers that must keep re-resolving the active-wallet claim.

Each declares ``require_tradable_active_wallet`` as a dependency and takes the
wallet it writes against from that gate's return value. Dropping the
declaration is a silent escalation: the handler falls back to the raw
``active_wallet_public_id`` claim, which ``POST /auth/refresh`` mints against
the READ plane."""

CallTarget = tuple[str | None, str]
FunctionId = tuple[str, str]
DefinitionIndex = dict[tuple[str, str], set[FunctionId]]
NameIndex = dict[str, set[FunctionId]]
ImportTable = dict[str, tuple[str, str]]


@dataclass(frozen=True)
class FunctionRecord:
    """One function definition found under the server scan root."""

    module: str
    qualname: str
    name: str
    lineno: int
    verbs: frozenset[str]
    callees: tuple[CallTarget, ...]


@dataclass(frozen=True)
class ResolutionIndex:
    """Name-resolution tables over every scanned server function."""

    by_module: DefinitionIndex
    by_name: NameIndex
    imports: dict[str, ImportTable]


@dataclass(frozen=True)
class Violation:
    """One boundary breach, rendered as ``path:line:detail``."""

    path: str
    line: int
    detail: str


def module_source_path(module: str) -> str:
    """Return the repo-relative source path of a scanned server module.

    Args:
        module: Dotted module name relative to the scan root.

    Returns:
        Repo-relative path string used when reporting a violation.
    """
    return f"{SCAN_ROOT}/{module.replace('.', '/')}.py"


def parse_source(filepath: Path) -> tuple[ast.Module | None, str]:
    """Parse one Python file, failing closed instead of raising.

    Args:
        filepath: Python source file to parse.

    Returns:
        Tuple of the parsed module (``None`` on failure) and a fail-closed
        reason string (empty when parsing succeeded).
    """
    try:
        source = filepath.read_text(encoding="utf-8")
    except OSError:
        return None, "unreadable source (fail closed)"
    try:
        return ast.parse(source, filename=str(filepath)), ""
    except SyntaxError:
        return None, "unparseable source (fail closed)"


def _listed_verbs(node: ast.expr) -> frozenset[str]:
    """Return the mutating verbs a ``methods=`` argument lists.

    Only string CONSTANTS inside a literal sequence are read. A verb list
    built from names, a module constant or a comprehension contributes
    nothing -- a deliberate narrow reading, recorded as a non-claim in the
    module docstring rather than papered over.
    """
    if not isinstance(node, ast.List | ast.Tuple | ast.Set):
        return frozenset()
    listed = {
        element.value.lower()
        for element in node.elts
        if isinstance(element, ast.Constant) and isinstance(element.value, str)
    }
    return frozenset(listed & MUTATION_VERBS)


def _methods_verbs(call: ast.Call) -> frozenset[str]:
    """Return the mutating verbs one call's ``methods=`` keyword lists."""
    for keyword in call.keywords:
        if keyword.arg == "methods":
            return _listed_verbs(keyword.value)
    return frozenset()


def route_alias_table(tree: ast.Module) -> dict[str, str]:
    """Map module-level names bound to a router's route attribute.

    ``_post = router.post`` followed by ``@_post("/x")`` routes exactly
    like the attribute it aliases, and the same holds for ``api_route``
    and ``add_api_route``. Only module-level, single-attribute bindings
    are followed: that is the form worth reading, and a deeper chase would
    trade real coverage for the illusion of it.

    Args:
        tree: Parsed module to inspect.

    Returns:
        Mapping of locally bound name to the route attribute it aliases.
    """
    aliases: dict[str, str] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign):
            targets: list[ast.expr] = list(node.targets)
            value = node.value
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            targets = [node.target]
            value = node.value
        else:
            continue
        if not isinstance(value, ast.Attribute) or value.attr not in ROUTE_ATTRIBUTES:
            continue
        for bound in targets:
            if isinstance(bound, ast.Name):
                aliases[bound.id] = value.attr
    return aliases


def _route_attribute(expression: ast.expr, aliases: dict[str, str]) -> str | None:
    """Return the route attribute an expression denotes, alias included."""
    if isinstance(expression, ast.Attribute):
        return expression.attr
    if isinstance(expression, ast.Name):
        return aliases.get(expression.id)
    return None


def _decorator_verbs(
    node: ast.FunctionDef | ast.AsyncFunctionDef,
    aliases: dict[str, str],
) -> frozenset[str]:
    """Return the mutating HTTP verbs a function is routed on by decorator.

    Covers the verb decorators (``@router.post``), the generic
    ``@router.api_route(..., methods=[...])`` form, and module-level
    aliases of either.

    Args:
        node: Function definition whose decorators are read.
        aliases: Module-level route-attribute aliases from
            :func:`route_alias_table`.

    Returns:
        The mutating verbs this definition is routed on by decorator.
    """
    verbs: set[str] = set()
    for decorator in node.decorator_list:
        call = decorator if isinstance(decorator, ast.Call) else None
        expression = call.func if call is not None else decorator
        attribute = _route_attribute(expression, aliases)
        if attribute is None:
            continue
        if attribute in MUTATION_VERBS:
            verbs.add(attribute)
        elif attribute == ROUTE_DECORATOR and call is not None:
            verbs.update(_methods_verbs(call))
    return frozenset(verbs)


def _reference_target(expression: ast.expr) -> CallTarget | None:
    """Return the ``(receiver, name)`` an expression denotes.

    The receiver is ``None`` for a bare name, the identifier for
    ``alias.name``, and the empty string for any deeper attribute chain,
    which can never match a module alias.
    """
    if isinstance(expression, ast.Name):
        return (None, expression.id)
    if isinstance(expression, ast.Attribute):
        receiver = expression.value.id if isinstance(expression.value, ast.Name) else ""
        return (receiver, expression.attr)
    return None


def _direct_call_targets(node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[CallTarget]:
    """Return the calls made directly by one function body.

    Nested definitions are not descended into. A router factory that merely
    *defines* a decorated handler does not call it, and treating it as a
    caller would smear every nested handler's edges onto the factory and
    from there onto ``create_app``.
    """
    targets: list[CallTarget] = []
    pending: list[ast.AST] = list(node.body)
    while pending:
        current = pending.pop()
        if isinstance(current, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            continue
        if isinstance(current, ast.Call):
            target = _reference_target(current.func)
            if target is not None:
                targets.append(target)
        pending.extend(ast.iter_child_nodes(current))
    return targets


def _dependency_provider(call: ast.Call) -> CallTarget | None:
    """Return the provider a ``Depends``/``Security`` marker wraps."""
    for keyword in call.keywords:
        if keyword.arg == "dependency":
            return _reference_target(keyword.value)
    if call.args:
        return _reference_target(call.args[0])
    return None


def _dependency_targets(node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[CallTarget]:
    """Return the FastAPI dependency providers one function declares.

    A provider named in a signature or in ``dependencies=[...]`` runs for
    every request the route serves, yet it is never called in the body, so
    a body-only scan would miss a mutation route wired to a read-plane
    provider.
    """
    targets: list[CallTarget] = []
    for source in [node.args, *node.decorator_list]:
        for child in ast.walk(source):
            if not isinstance(child, ast.Call):
                continue
            marker = _reference_target(child.func)
            if marker is None or marker[1] not in DEPENDENCY_MARKERS:
                continue
            provider = _dependency_provider(child)
            if provider is not None:
                targets.append(provider)
    return targets


def _walk_scope(
    node: ast.Module | ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef,
    qualifier: str,
    module: str,
    records: list[FunctionRecord],
    aliases: dict[str, str],
) -> None:
    """Recursively record every function defined under one AST scope."""
    for child in node.body:
        if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef):
            qualname = f"{qualifier}.{child.name}" if qualifier else child.name
            records.append(
                FunctionRecord(
                    module=module,
                    qualname=qualname,
                    name=child.name,
                    lineno=child.lineno,
                    verbs=_decorator_verbs(child, aliases),
                    callees=tuple(_direct_call_targets(child) + _dependency_targets(child)),
                )
            )
            _walk_scope(child, qualname, module, records, aliases)
        elif isinstance(child, ast.ClassDef):
            scope = f"{qualifier}.{child.name}" if qualifier else child.name
            _walk_scope(child, scope, module, records, aliases)


def _relative_submodule(module: str, level: int) -> str:
    """Return the submodule a relative import in ``module`` resolves against."""
    parts = module.split(".")[:-1]
    keep = max(len(parts) - (level - 1), 0)
    return ".".join(parts[:keep])


def _record_from_import(table: ImportTable, node: ast.ImportFrom, module: str) -> None:
    """Record the server submodule and original name each imported name binds."""
    prefix = f"{SERVER_PACKAGE}."
    imported = node.module or ""
    if node.level:
        base = _relative_submodule(module, node.level)
        submodule = ".".join(part for part in (base, imported) if part)
    elif imported == SERVER_PACKAGE:
        submodule = ""
    elif imported.startswith(prefix):
        submodule = imported[len(prefix) :]
    else:
        return
    for alias in node.names:
        local = alias.asname or alias.name
        table[local] = (submodule, alias.name) if submodule else (alias.name, alias.name)


def server_import_table(tree: ast.Module, module: str) -> ImportTable:
    """Map local names to the server definition they were imported from.

    An alias is stored under its local name but keeps the ORIGINAL symbol
    name, so a renamed import still resolves to the function it points at.

    Args:
        tree: Parsed module to inspect.
        module: Dotted name of that module, relative to the scan root,
            used to resolve package-relative imports.

    Returns:
        Mapping of locally bound name to ``(submodule, original name)``,
        with the submodule given relative to ``snapper.server``.
    """
    table: ImportTable = {}
    prefix = f"{SERVER_PACKAGE}."
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith(prefix):
                    local = alias.asname or alias.name.rsplit(".", 1)[-1]
                    target = alias.name[len(prefix) :]
                    table[local] = (target, target.rsplit(".", 1)[-1])
        elif isinstance(node, ast.ImportFrom):
            _record_from_import(table, node, module)
    return table


def build_records(trees: dict[str, ast.Module]) -> dict[FunctionId, FunctionRecord]:
    """Collect every function definition in the scanned server modules.

    Args:
        trees: Mapping of module name to its parsed AST.

    Returns:
        Mapping of ``(module, qualname)`` to its function record.
    """
    records: dict[FunctionId, FunctionRecord] = {}
    for module, tree in trees.items():
        found: list[FunctionRecord] = []
        _walk_scope(tree, "", module, found, route_alias_table(tree))
        for record in found:
            records[(module, record.qualname)] = record
    return records


def build_index(
    records: dict[FunctionId, FunctionRecord],
    trees: dict[str, ast.Module],
) -> ResolutionIndex:
    """Build the tables that resolve a call target to server functions.

    Args:
        records: Every function record found under the scan root.
        trees: Mapping of module name to its parsed AST.

    Returns:
        Index of definitions by module, definitions by bare name, and the
        per-module import tables.
    """
    by_module: DefinitionIndex = {}
    by_name: NameIndex = {}
    for function_id, record in records.items():
        by_module.setdefault((record.module, record.name), set()).add(function_id)
        by_name.setdefault(record.name, set()).add(function_id)
    return ResolutionIndex(
        by_module=by_module,
        by_name=by_name,
        imports={module: server_import_table(tree, module) for module, tree in trees.items()},
    )


def resolve_target(index: ResolutionIndex, module: str, target: CallTarget) -> set[FunctionId]:
    """Return the server functions one call or reference target can reach.

    Args:
        index: Name-resolution tables from :func:`build_index`.
        module: Dotted name of the module the target appears in.
        target: ``(receiver, name)`` pair to resolve.

    Returns:
        Set of function ids the target may denote, empty when it resolves
        to nothing defined under the scan root.
    """
    receiver, name = target
    if name in PINNED_PRIMITIVES:
        return set(index.by_name.get(name, set()))
    table = index.imports[module]
    if receiver is None:
        found = set(index.by_module.get((module, name), set()))
        binding = table.get(name)
        if binding is not None:
            found.update(index.by_module.get(binding, set()))
        return found
    imported = table.get(receiver)
    if imported is None:
        return set()
    return set(index.by_module.get((imported[0], name), set()))


def build_edges(
    records: dict[FunctionId, FunctionRecord],
    index: ResolutionIndex,
) -> dict[FunctionId, set[FunctionId]]:
    """Build the forward call graph over the scanned server functions.

    Args:
        records: Every function record found under the scan root.
        index: Name-resolution tables from :func:`build_index`.

    Returns:
        Mapping of caller id to the set of callee ids it reaches directly.
    """
    return {
        function_id: {
            callee
            for target in record.callees
            for callee in resolve_target(index, record.module, target)
        }
        for function_id, record in records.items()
    }


def _registration(
    call: ast.Call,
    aliases: dict[str, str],
) -> tuple[CallTarget, frozenset[str]] | None:
    """Return the handler and mutating verbs one ``add_api_route`` call registers."""
    if _route_attribute(call.func, aliases) != ROUTE_REGISTRAR:
        return None
    handler = call.args[1] if len(call.args) > 1 else None
    for keyword in call.keywords:
        if keyword.arg == "endpoint":
            handler = keyword.value
    verbs = _methods_verbs(call)
    if handler is None or not verbs:
        return None
    target = _reference_target(handler)
    return None if target is None else (target, verbs)


def registered_verbs(
    trees: dict[str, ast.Module],
    index: ResolutionIndex,
) -> dict[FunctionId, frozenset[str]]:
    """Return the mutating verbs attached by ``add_api_route`` registrations.

    FastAPI routes a handler either through a decorator or by passing it as
    a value to ``add_api_route``, where the verbs live in a ``methods=``
    list and the handler itself stays undecorated. ``server/app.py`` already
    registers six endpoints that way, so a verb read off decorators alone
    would leave that whole channel unguarded.

    Args:
        trees: Mapping of module name to its parsed AST.
        index: Name-resolution tables from :func:`build_index`.

    Returns:
        Mapping of function id to the mutating verbs it is registered on.
    """
    found: dict[FunctionId, set[str]] = {}
    for module, tree in trees.items():
        aliases = route_alias_table(tree)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            registration = _registration(node, aliases)
            if registration is None:
                continue
            target, verbs = registration
            for function_id in resolve_target(index, module, target):
                found.setdefault(function_id, set()).update(verbs)
    return {function_id: frozenset(verbs) for function_id, verbs in found.items()}


def compute_read_closure(
    records: dict[FunctionId, FunctionRecord],
    edges: dict[FunctionId, set[FunctionId]],
) -> set[FunctionId]:
    """Return every server function that transitively reaches the read plane.

    The closure runs over REVERSED edges to a fixed point: a function joins
    once any function it calls is already in the closure, so a leak stays
    visible however many helpers sit between a route and the read plane. It
    is seeded on the REST primitive AND on every direct caller of the
    repository read method, which is the second door into the same wider
    wallet set.

    Args:
        records: Every function record found under the scan root.
        edges: Forward call graph over those records.

    Returns:
        Set of function ids in the read closure, including its seeds.
    """
    closure = {
        function_id
        for function_id, record in records.items()
        if record.name == READ_PRIMITIVE
        or any(name == READ_REPOSITORY_METHOD for _, name in record.callees)
    }
    changed = True
    while changed:
        changed = False
        for function_id, targets in edges.items():
            if function_id not in closure and targets & closure:
                closure.add(function_id)
                changed = True
    return closure


def check_verb_linkage(
    records: dict[FunctionId, FunctionRecord],
    closure: set[FunctionId],
    registered: dict[FunctionId, frozenset[str]],
) -> list[Violation]:
    """Reject any read-closure function that is routed on a mutating verb.

    Args:
        records: Every function record found under the scan root.
        closure: Read-closure function ids from :func:`compute_read_closure`.
        registered: Verbs attached by ``add_api_route`` registrations.

    Returns:
        One violation per read-closure function routed on a mutating verb,
        whether it was decorated or registered as a value.
    """
    violations: list[Violation] = []
    for function_id in sorted(closure):
        record = records[function_id]
        routed = record.verbs | registered.get(function_id, frozenset())
        if routed:
            verbs = "/".join(sorted(routed)).upper()
            violations.append(
                Violation(
                    module_source_path(record.module),
                    record.lineno,
                    f"{record.qualname} is routed on {verbs} but reaches {READ_PRIMITIVE}",
                )
            )
    return violations


def _check_pinned_references(
    records: dict[FunctionId, FunctionRecord],
    symbol: str,
    pinned: frozenset[FunctionId],
    noun: str,
) -> list[Violation]:
    """Compare the functions referencing one symbol against a pinned set.

    Both directions are reported, and the second is the load-bearing one: a
    pin that only refuses NEW references would let a gate be deleted in
    silence. A reference is any call or declared dependency, since
    :func:`_walk_scope` records both in ``callees``.

    Args:
        records: Every function record found under the scan root.
        symbol: Bare name whose referencing functions are pinned.
        pinned: The ``(module, qualname)`` pairs allowed to reference it.
        noun: What one reference is called in the violation text.

    Returns:
        One violation per unpinned new reference and per pinned reference
        that has disappeared.
    """
    observed: set[FunctionId] = set()
    violations: list[Violation] = []
    for function_id in sorted(records):
        record = records[function_id]
        if not any(name == symbol for _, name in record.callees):
            continue
        observed.add(function_id)
        if function_id not in pinned:
            violations.append(
                Violation(
                    module_source_path(record.module),
                    record.lineno,
                    f"unpinned {symbol} {noun} in {record.qualname}",
                )
            )
    for module, qualname in sorted(pinned - observed):
        violations.append(
            Violation(
                module_source_path(module),
                0,
                f"pinned {symbol} {noun} {qualname} has disappeared",
            )
        )
    return violations


def check_trade_call_sites(records: dict[FunctionId, FunctionRecord]) -> list[Violation]:
    """Compare observed trade-primitive call sites against the pinned set.

    Args:
        records: Every function record found under the scan root.

    Returns:
        One violation per unpinned new call site and per pinned call site
        that has disappeared.
    """
    return _check_pinned_references(records, TRADE_PRIMITIVE, TRADE_CALL_SITES, "call site")


def check_trade_claim_consumers(records: dict[FunctionId, FunctionRecord]) -> list[Violation]:
    """Compare observed claim-gate consumers against the pinned set.

    The gate's own ``resolve_tradable_wallets`` call is already pinned by
    :func:`check_trade_call_sites`, but that only proves the helper still
    consults the trade plane -- which stays true after every route stops
    depending on it. Pinning the CONSUMERS is what makes deleting a
    ``Depends(require_tradable_active_wallet)`` line fail the gate.

    Args:
        records: Every function record found under the scan root.

    Returns:
        One violation per unpinned new consumer and per pinned consumer
        that has disappeared.
    """
    return _check_pinned_references(records, TRADE_CLAIM_GATE, TRADE_CLAIM_CONSUMERS, "consumer")


def _aliased_import_findings(
    node: ast.ImportFrom, symbols: frozenset[str]
) -> list[tuple[int, str]]:
    """Return the read-plane symbols one import binds under a different name."""
    return [
        (node.lineno, f"re-binds {alias.name} as {alias.asname}")
        for alias in node.names
        if alias.name in symbols and alias.asname is not None and alias.asname != alias.name
    ]


def _assignment_findings(node: ast.Assign, symbols: frozenset[str]) -> list[tuple[int, str]]:
    """Return the read-plane symbols one assignment re-binds."""
    source = _reference_target(node.value)
    if source is None or source[1] not in symbols:
        return []
    return [
        (node.lineno, f"re-binds {source[1]} as {target.id}")
        for target in node.targets
        if isinstance(target, ast.Name) and target.id != source[1]
    ]


def rebinding_findings(tree: ast.Module, symbols: frozenset[str]) -> list[tuple[int, str]]:
    """Return every re-binding of a read-plane symbol under a second name.

    The package fence exempts ``src/snapper/server`` because that package
    owns the primitive, and the exemption is exactly where laundering would
    happen: a server module that re-exports the primitive under another
    name lets any package import the new name, which matches neither the
    scoping module nor the fenced symbol. A rebound repository read method
    is the same evasion aimed at the closure seed, which likewise matches
    on a name. Aliased imports and plain assignments are both refused, so
    each read-plane entrance keeps exactly one name.

    Args:
        tree: Parsed module to inspect.
        symbols: Read-plane symbol names that must not be re-bound.

    Returns:
        List of ``(line_number, detail)`` findings.
    """
    findings: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            findings.extend(_aliased_import_findings(node, symbols))
        elif isinstance(node, ast.Assign):
            findings.extend(_assignment_findings(node, symbols))
    return findings


def check_read_symbol_names(trees: dict[str, ast.Module]) -> list[Violation]:
    """Reject any second name for a read-plane entrance in the server package.

    Args:
        trees: Mapping of module name to its parsed AST.

    Returns:
        One violation per aliased import or assignment that re-binds the
        read primitive or the repository read method.
    """
    violations: list[Violation] = []
    for module in sorted(trees):
        violations.extend(
            Violation(module_source_path(module), line, detail)
            for line, detail in rebinding_findings(trees[module], FORBIDDEN_READ_SYMBOLS)
        )
    return violations


def scan_server_package(root: Path) -> list[Violation]:
    """Run the server-package assertions over the scanned modules.

    Args:
        root: Project root directory to scan.

    Returns:
        Every violation found, including fail-closed entries for a missing
        scan root, unreadable sources, and an absent read primitive.
    """
    trees, violations = load_server_package(root)
    records = build_records(trees)
    if not any(record.name == READ_PRIMITIVE for record in records.values()):
        violations.append(
            Violation(SCAN_ROOT, 0, f"{READ_PRIMITIVE} is not defined here (fail closed)")
        )
        return violations
    index = build_index(records, trees)
    closure = compute_read_closure(records, build_edges(records, index))
    violations.extend(check_verb_linkage(records, closure, registered_verbs(trees, index)))
    violations.extend(check_trade_call_sites(records))
    violations.extend(check_trade_claim_consumers(records))
    violations.extend(check_read_symbol_names(trees))
    return violations


def load_server_package(root: Path) -> tuple[dict[str, ast.Module], list[Violation]]:
    """Parse every module under the server scan root.

    Args:
        root: Project root directory to scan.

    Returns:
        Tuple of the parsed module map and the fail-closed violations raised
        while loading it.
    """
    search_root = root / SCAN_ROOT
    if not search_root.is_dir():
        return {}, [Violation(SCAN_ROOT, 0, "missing scan root (fail closed)")]
    trees: dict[str, ast.Module] = {}
    violations: list[Violation] = []
    for python_file in sorted(search_root.rglob("*.py")):
        if "__pycache__" in python_file.parts:
            continue
        tree, reason = parse_source(python_file)
        if tree is None:
            violations.append(Violation(str(python_file), 0, reason))
            continue
        module = python_file.relative_to(search_root).with_suffix("").as_posix()
        trees[module.replace("/", ".")] = tree
    return trees, violations


def _forbidden_import_findings(
    node: ast.Import | ast.ImportFrom,
    symbols: frozenset[str],
) -> list[tuple[int, str]]:
    """Return the read-plane imports one import statement performs."""
    if isinstance(node, ast.Import):
        return [
            (node.lineno, f"imports {alias.name}")
            for alias in node.names
            if alias.name == SCOPING_MODULE or alias.name.startswith(f"{SCOPING_MODULE}.")
        ]
    module = node.module or ""
    if module == SCOPING_MODULE:
        return [(node.lineno, f"imports {module}")]
    return [
        (node.lineno, f"imports {module}.{alias.name}")
        for alias in node.names
        if alias.name in symbols
    ]


def containment_findings(tree: ast.Module, symbols: frozenset[str]) -> list[tuple[int, str]]:
    """Return every reference a module makes to the given read-plane symbols.

    Prose mentions of the read plane are invisible here by construction:
    docstrings are string constants, not name or attribute nodes.

    Args:
        tree: Parsed module to inspect.
        symbols: Read-plane symbol names this module may not reference.

    Returns:
        List of ``(line_number, detail)`` findings.
    """
    findings: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import | ast.ImportFrom):
            findings.extend(_forbidden_import_findings(node, symbols))
        elif isinstance(node, ast.Name) and node.id in symbols:
            findings.append((node.lineno, f"references {node.id}"))
        elif isinstance(node, ast.Attribute) and node.attr in symbols:
            findings.append((node.lineno, f"references {node.attr}"))
    return findings


def scan_primitive_fence(root: Path) -> list[Violation]:
    """Assert the read primitive is unreachable outside the server package.

    The verb-linkage closure is bounded to ``src/snapper/server``, which only
    proves anything while every path from a route to the read primitive stays
    inside that package. This fence is what makes that bound sound: no module
    elsewhere under ``src/snapper`` may import the scoping module or name the
    read primitive, so no mutation route can reach it by hopping out of the
    package and back. ``list_readable_wallets_for_user`` is deliberately NOT
    fenced here -- ``snapper.auth.routes`` legitimately calls it to resolve
    the wallet hint, and pinning that surface is the contained-path check's
    job, not this one's.

    Args:
        root: Project root directory to scan.

    Returns:
        Every violation found, including fail-closed entries for a missing
        fence root and unreadable sources.
    """
    fence_root = root / PRIMITIVE_FENCE_ROOT
    if not fence_root.is_dir():
        return [Violation(PRIMITIVE_FENCE_ROOT, 0, "missing fence root (fail closed)")]
    server_root = root / SCAN_ROOT
    violations: list[Violation] = []
    for python_file in sorted(fence_root.rglob("*.py")):
        if "__pycache__" in python_file.parts or server_root in python_file.parents:
            continue
        tree, reason = parse_source(python_file)
        if tree is None:
            violations.append(Violation(str(python_file), 0, reason))
            continue
        violations.extend(
            Violation(str(python_file), line, detail)
            for line, detail in containment_findings(tree, FENCED_READ_SYMBOLS)
        )
    return violations


def _contained_python_files(target: Path) -> list[Path]:
    """Return the Python files one contained path entry covers."""
    if target.is_dir():
        return [
            python_file
            for python_file in sorted(target.rglob("*.py"))
            if "__pycache__" not in python_file.parts
        ]
    return [target]


def scan_import_containment(root: Path) -> list[Violation]:
    """Assert the contained modules never reach the read plane.

    Args:
        root: Project root directory to scan.

    Returns:
        Every violation found, including a fail-closed entry per missing
        contained path and per unreadable source.
    """
    violations: list[Violation] = []
    for entry in CONTAINED_PATHS:
        target = root / entry
        if not target.exists():
            violations.append(Violation(entry, 0, "missing contained path (fail closed)"))
            continue
        for python_file in _contained_python_files(target):
            tree, reason = parse_source(python_file)
            if tree is None:
                violations.append(Violation(str(python_file), 0, reason))
                continue
            violations.extend(
                Violation(str(python_file), line, detail)
                for line, detail in containment_findings(tree, FORBIDDEN_READ_SYMBOLS)
            )
    return violations


def scan_files(root: Path) -> list[Violation]:
    """Run every read-visibility boundary assertion.

    Args:
        root: Project root directory to scan.

    Returns:
        The combined violation list from the server-package, package-fence
        and containment scans.
    """
    violations = scan_server_package(root)
    violations.extend(scan_primitive_fence(root))
    violations.extend(scan_import_containment(root))
    return violations


def run_scan(root: Path, strict_mode: bool = False) -> int:
    """Run the boundary scan and return the exit code.

    Args:
        root: Root directory of the project to scan.
        strict_mode: When True, return 1 if any violation is found.

    Returns:
        Exit code (0 for clean, 1 for violations in strict mode).
    """
    print("=" * 70)
    print("Read Visibility Boundary Scanner")
    print("=" * 70)
    print(f"\nScanning: {root / SCAN_ROOT}")
    print(f"Mode: {'STRICT (will fail on findings)' if strict_mode else 'Report only'}")
    violations = scan_files(root)
    print("\n" + "-" * 70)
    if not violations:
        print("  No read/trade visibility boundary violations found")
    else:
        for violation in violations:
            print(f"     {violation.path}:{violation.line}:{violation.detail}")
    print("\n" + "=" * 70)
    print(f"SUMMARY: {len(violations)} violation(s)")
    print("=" * 70)
    if strict_mode and violations:
        return 1
    return 0


def main() -> int:
    """Entry point for the read visibility boundary checker.

    Returns:
        Process exit code from :func:`run_scan`.
    """
    strict_mode = "--strict" in sys.argv
    root = Path(__file__).resolve().parent.parent
    return run_scan(root, strict_mode=strict_mode)


if __name__ == "__main__":
    raise SystemExit(main())
