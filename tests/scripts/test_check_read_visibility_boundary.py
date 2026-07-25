"""Tests for the read/trade visibility boundary checker script."""

import ast
from pathlib import Path

import pytest

import scripts.check_read_visibility_boundary as boundary

_SCOPING_STUB = '''"""Fixture scoping module."""


async def resolve_readable_wallets(principal, repo):
    """Fixture read primitive."""
    return None


async def resolve_tradable_wallets(principal, repo):
    """Fixture trade primitive."""
    return None
'''


def _server_module(tmp_path: Path, name: str, source: str) -> Path:
    package = tmp_path / boundary.SCAN_ROOT
    package.mkdir(parents=True, exist_ok=True)
    module = package / name
    module.write_text(source, encoding="utf-8")
    return module


def _scoping_package(tmp_path: Path) -> None:
    _server_module(tmp_path, "scoping.py", _SCOPING_STUB)


def _contained_stubs(tmp_path: Path) -> None:
    for entry in boundary.CONTAINED_PATHS:
        target = tmp_path / entry
        if target.suffix == ".py":
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text('"""Fixture contained module."""\n', encoding="utf-8")
        else:
            target.mkdir(parents=True, exist_ok=True)
            (target / "tools.py").write_text('"""Fixture tool."""\n', encoding="utf-8")


def _contained_module(tmp_path: Path, source: str) -> Path:
    _contained_stubs(tmp_path)
    target = tmp_path / "src/snapper/core/wallet_resolution.py"
    target.write_text(source, encoding="utf-8")
    return target


def _outside_module(tmp_path: Path, source: str) -> Path:
    target = tmp_path / "src/snapper/auth/routes.py"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(source, encoding="utf-8")
    return target


def _records(tmp_path: Path) -> dict[boundary.FunctionId, boundary.FunctionRecord]:
    trees, _ = boundary.load_server_package(tmp_path)
    return boundary.build_records(trees)


def _closure(tmp_path: Path) -> set[boundary.FunctionId]:
    trees, _ = boundary.load_server_package(tmp_path)
    records = boundary.build_records(trees)
    index = boundary.build_index(records, trees)
    edges = boundary.build_edges(records, index)
    return boundary.compute_read_closure(records, edges)


def _closure_names(tmp_path: Path) -> set[str]:
    return {qualname for _, qualname in _closure(tmp_path)}


def _table(source: str, module: str = "routes") -> boundary.ImportTable:
    return boundary.server_import_table(ast.parse(source), module)


def _registered(tmp_path: Path) -> dict[boundary.FunctionId, frozenset[str]]:
    trees, _ = boundary.load_server_package(tmp_path)
    records = boundary.build_records(trees)
    return boundary.registered_verbs(trees, boundary.build_index(records, trees))


def _details(violations: list[boundary.Violation]) -> list[str]:
    return [violation.detail for violation in violations]


class TestModuleSourcePath:
    """Test suite for violation path rendering."""

    def test_nested_module_renders_a_repo_relative_path(self) -> None:
        """A dotted module name renders as a real source path.

        Given: A module name nested inside a server subpackage,
        When: The source path is rendered,
        Then: The dots become directory separators under the scan root.
        """
        assert boundary.module_source_path("sub.routes") == "src/snapper/server/sub/routes.py"


class TestParseSource:
    """Test suite for fail-closed source parsing."""

    def test_valid_source_parses(self, tmp_path: Path) -> None:
        """A parseable file yields a module and no reason.

        Given: A syntactically valid Python file,
        When: The source is parsed,
        Then: A module is returned with an empty reason.
        """
        module = _server_module(tmp_path, "ok.py", '"""Fixture."""\n')
        tree, reason = boundary.parse_source(module)
        assert isinstance(tree, ast.Module)
        assert reason == ""

    def test_syntax_error_fails_closed(self, tmp_path: Path) -> None:
        """An unparseable file is reported rather than skipped.

        Given: A server file that does not parse,
        When: The source is parsed,
        Then: No module and the unparseable fail-closed reason are returned.
        """
        module = _server_module(tmp_path, "broken.py", "def broken(:\n")
        tree, reason = boundary.parse_source(module)
        assert tree is None
        assert reason == "unparseable source (fail closed)"

    def test_unreadable_path_fails_closed(self, tmp_path: Path) -> None:
        """A path that cannot be read is reported rather than skipped.

        Given: A path pointing at a directory instead of a file,
        When: The source is parsed,
        Then: No module and the unreadable fail-closed reason are returned.
        """
        tree, reason = boundary.parse_source(tmp_path)
        assert tree is None
        assert reason == "unreadable source (fail closed)"


class TestDecoratorVerbs:
    """Test suite for route-verb detection on function definitions."""

    @pytest.mark.parametrize(
        ("decorator", "expected"),
        [
            ('@router.post("/x")', frozenset({"post"})),
            ('@router.put("/x")', frozenset({"put"})),
            ('@router.patch("/x")', frozenset({"patch"})),
            ("@router.delete", frozenset({"delete"})),
            ('@router.get("/x")', frozenset()),
            ("@staticmethod", frozenset()),
            ('@router.api_route("/x", methods=["POST"])', frozenset({"post"})),
            (
                '@router.api_route("/x", methods=["PUT", "PATCH"])',
                frozenset({"put", "patch"}),
            ),
            ('@router.api_route("/x", methods=("DELETE",))', frozenset({"delete"})),
            ('@router.api_route("/x", methods=["GET"])', frozenset()),
            ('@router.api_route("/x")', frozenset()),
            ("@router.api_route", frozenset()),
            ('@router.api_route("/x", methods=_MUTATING)', frozenset()),
            ('@router.api_route("/x", methods="POST")', frozenset()),
        ],
    )
    def test_verbs_are_read_off_the_decorator(
        self, tmp_path: Path, decorator: str, expected: frozenset[str]
    ) -> None:
        """Mutating route decorators are detected in call and bare form.

        Given: A handler carrying one decorator, including the generic
            ``api_route`` form whose verbs live in a ``methods=`` list,
        When: The server package is recorded,
        Then: Only mutating HTTP verbs named by a string constant inside a
            literal sequence are attributed to the handler. A ``methods=``
            value the AST cannot read contributes nothing, which the module
            docstring records as an explicit non-claim rather than a
            silently narrow reading.
        """
        _server_module(
            tmp_path,
            "routes.py",
            f'"""Fixture."""\n\n\n{decorator}\ndef handler():\n    """Handler."""\n',
        )
        records = _records(tmp_path)
        assert records[("routes", "handler")].verbs == expected

    @pytest.mark.parametrize(
        ("alias", "decorator", "expected"),
        [
            ("_post = router.post", "@_post", frozenset({"post"})),
            ("_post = router.post", '@_post("/x")', frozenset({"post"})),
            ("_post: Final = router.post", '@_post("/x")', frozenset({"post"})),
            ("_get = router.get", '@_get("/x")', frozenset()),
            (
                "_route = router.api_route",
                '@_route("/x", methods=["DELETE"])',
                frozenset({"delete"}),
            ),
            ("_other = router.include_router", '@_other("/x")', frozenset()),
            ("_post = post", '@_post("/x")', frozenset()),
            ("_post = router.post()", '@_post("/x")', frozenset()),
            ("_post: Final[str]", '@_post("/x")', frozenset()),
        ],
    )
    def test_module_level_aliases_of_a_route_attribute_are_followed(
        self, tmp_path: Path, alias: str, decorator: str, expected: frozenset[str]
    ) -> None:
        """A route decorator kept behind a module-level name still routes.

        Given: A module-level binding of a router attribute and a handler
            decorated through that name,
        When: The server package is recorded,
        Then: The handler carries the verbs of the aliased attribute. Only
            a plain module-level attribute binding is followed — a bare
            name, a call result, and an annotation with no value bind
            nothing this scan will trust.
        """
        _server_module(
            tmp_path,
            "routes.py",
            f'"""Fixture."""\n\n{alias}\n\n\n{decorator}\ndef handler():\n    """Handler."""\n',
        )
        records = _records(tmp_path)
        assert records[("routes", "handler")].verbs == expected

    def test_function_local_alias_is_not_followed(self, tmp_path: Path) -> None:
        """An alias bound inside a function body is out of scope.

        Given: A router attribute bound to a name inside a function,
        When: The module's alias table is built,
        Then: The binding is absent, because only module-level assignments
            are read and chasing local flow would claim more than an AST
            walk can deliver.
        """
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n\ndef build():\n    """Build."""\n    _post = router.post\n'
            "    return _post\n",
        )
        trees, _ = boundary.load_server_package(tmp_path)
        assert boundary.route_alias_table(trees["routes"]) == {}

    def test_non_name_assignment_target_binds_nothing(self, tmp_path: Path) -> None:
        """A route attribute assigned to anything but a bare name is skipped.

        Given: A module binding ``router.post`` through a tuple target and
            through an attribute target,
        When: The module's alias table is built,
        Then: Neither form registers an alias, because only a bare module-level
            name can later appear as a decorator this scan resolves.
        """
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n'
            "_pair, _other = router.post, None\n"
            "holder.attr = router.post\n",
        )
        trees, _ = boundary.load_server_package(tmp_path)
        assert boundary.route_alias_table(trees["routes"]) == {}


class TestCallTargets:
    """Test suite for direct call-target extraction."""

    def test_nested_definitions_are_not_smeared_onto_the_factory(self, tmp_path: Path) -> None:
        """A factory does not inherit the calls of the handler it defines.

        Given: A factory whose nested handler calls a helper,
        When: The server package is recorded,
        Then: Only the nested handler records that call.
        """
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n\ndef factory():\n    """Factory."""\n\n'
            '    def handler():\n        """Handler."""\n        return helper()\n\n'
            "    return handler\n",
        )
        records = _records(tmp_path)
        assert (None, "helper") not in records[("routes", "factory")].callees
        assert (None, "helper") in records[("routes", "factory.handler")].callees

    def test_classes_inside_a_function_are_not_descended_into(self, tmp_path: Path) -> None:
        """A class body nested in a function contributes no calls to it.

        Given: A function defining a class whose method calls a helper,
        When: The server package is recorded,
        Then: The enclosing function records no call to that helper.
        """
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n\ndef factory():\n    """Factory."""\n\n'
            '    class Inner:\n        """Inner."""\n\n'
            '        def go(self):\n            """Go."""\n            return helper()\n\n'
            "    return Inner\n",
        )
        records = _records(tmp_path)
        assert (None, "helper") not in records[("routes", "factory")].callees

    def test_methods_are_recorded_under_their_class(self, tmp_path: Path) -> None:
        """Module-level classes are walked so their methods are recorded.

        Given: A module-level class with one method,
        When: The server package is recorded,
        Then: The method is present under its class qualname.
        """
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n\nclass Loader:\n    """Loader."""\n\n'
            '    def go(self):\n        """Go."""\n        return helper()\n',
        )
        records = _records(tmp_path)
        assert (None, "helper") in records[("routes", "Loader.go")].callees

    def test_call_of_a_call_expression_is_not_a_target(self, tmp_path: Path) -> None:
        """A call whose callee is itself a call yields no direct target.

        Given: A function invoking the result of another call,
        When: The server package is recorded,
        Then: Only the inner callable is recorded as a target.
        """
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n\ndef handler():\n    """Handler."""\n    return factory()()\n',
        )
        records = _records(tmp_path)
        assert records[("routes", "handler")].callees == ((None, "factory"),)

    def test_deep_attribute_chain_gets_an_unmatchable_receiver(self, tmp_path: Path) -> None:
        """A deep attribute chain cannot pose as a module alias.

        Given: A function calling a two-level attribute chain,
        When: The server package is recorded,
        Then: The receiver is the empty string, which matches no alias.
        """
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n\ndef handler():\n    """Handler."""\n    return a.b.c()\n',
        )
        records = _records(tmp_path)
        assert records[("routes", "handler")].callees == (("", "c"),)


class TestImportTable:
    """Test suite for server-module import resolution."""

    def test_aliased_module_import_is_resolved(self) -> None:
        """An aliased server-module import binds to its submodule.

        Given: ``import snapper.server.scoping as sc``,
        When: The import table is built,
        Then: The alias maps to the scoping submodule.
        """
        assert _table("import snapper.server.scoping as sc") == {"sc": ("scoping", "scoping")}

    def test_plain_module_import_binds_the_last_component(self) -> None:
        """A plain server-module import is bound generously.

        Given: ``import snapper.server.scoping``,
        When: The import table is built,
        Then: The trailing component maps to the scoping submodule.
        """
        assert _table("import snapper.server.scoping") == {"scoping": ("scoping", "scoping")}

    def test_dotted_module_import_keeps_the_full_submodule_path(self) -> None:
        """A nested server module keeps its dotted path in the table.

        Given: ``import snapper.server.sub.helpers``,
        When: The import table is built,
        Then: The bound name maps to the dotted submodule and its own name.
        """
        assert _table("import snapper.server.sub.helpers") == {
            "helpers": ("sub.helpers", "helpers")
        }

    def test_foreign_module_import_is_ignored(self) -> None:
        """Imports outside the server package are not tracked.

        Given: ``import os``,
        When: The import table is built,
        Then: The table stays empty.
        """
        assert _table("import os") == {}

    def test_submodule_import_from_the_package_is_resolved(self) -> None:
        """``from snapper.server import scoping`` binds the submodule name.

        Given: A submodule imported from the server package,
        When: The import table is built,
        Then: The name maps to itself as a submodule.
        """
        assert _table("from snapper.server import scoping") == {"scoping": ("scoping", "scoping")}

    def test_symbol_import_maps_to_its_defining_submodule(self) -> None:
        """A symbol imported from a server submodule maps to that submodule.

        Given: ``from snapper.server.scoping import resolve_readable_wallets``,
        When: The import table is built,
        Then: The symbol maps to the scoping submodule.
        """
        table = _table("from snapper.server.scoping import resolve_readable_wallets")
        assert table == {"resolve_readable_wallets": ("scoping", "resolve_readable_wallets")}

    def test_aliased_symbol_import_keeps_the_original_name(self) -> None:
        """An aliased symbol import records what it points at, not its alias.

        Given: A symbol imported from a server submodule under an alias,
        When: The import table is built,
        Then: The alias maps to the defining submodule AND the original
            symbol name, so the renamed call still resolves to the function
            it reaches.
        """
        table = _table("from snapper.server.scoping import resolve_readable_wallets as rrw")
        assert table == {"rrw": ("scoping", "resolve_readable_wallets")}

    def test_relative_submodule_import_is_resolved(self) -> None:
        """A package-relative symbol import resolves to its submodule.

        Given: ``from .scoping import resolve_readable_wallets``,
        When: The import table is built,
        Then: The symbol maps to the scoping submodule.
        """
        table = _table("from .scoping import resolve_readable_wallets")
        assert table == {"resolve_readable_wallets": ("scoping", "resolve_readable_wallets")}

    def test_relative_package_import_is_resolved(self) -> None:
        """``from . import scoping`` binds the submodule name.

        Given: A relative import of a sibling submodule,
        When: The import table is built,
        Then: The name maps to itself as a submodule.
        """
        assert _table("from . import scoping") == {"scoping": ("scoping", "scoping")}

    def test_relative_import_resolves_against_its_own_subpackage(self) -> None:
        """A relative import in a nested module is not read as a top-level one.

        Given: ``from .helpers import loader`` inside ``sub.routes``,
        When: The import table is built,
        Then: It resolves to ``sub.helpers``, so a nested subpackage cannot
            silently drop the edge by resolving against the scan root.
        """
        assert _table("from .helpers import loader", "sub.routes") == {
            "loader": ("sub.helpers", "loader")
        }

    def test_parent_relative_import_climbs_one_package(self) -> None:
        """A parent-relative import resolves against the enclosing package.

        Given: ``from .. import scoping`` inside ``sub.routes``,
        When: The import table is built,
        Then: The name resolves to the top-level scoping submodule.
        """
        assert _table("from .. import scoping", "sub.routes") == {"scoping": ("scoping", "scoping")}

    def test_relative_import_above_the_scan_root_is_clamped(self) -> None:
        """Climbing past the scan root does not produce a negative package.

        Given: ``from ... import scoping`` inside ``sub.routes``,
        When: The import table is built,
        Then: The base is clamped to the scan root rather than wrapping.
        """
        assert _table("from ... import scoping", "sub.routes") == {
            "scoping": ("scoping", "scoping")
        }

    def test_foreign_symbol_import_is_ignored(self) -> None:
        """Symbols from outside the server package are not tracked.

        Given: ``from fastapi import HTTPException``,
        When: The import table is built,
        Then: The table stays empty.
        """
        assert _table("from fastapi import HTTPException") == {}


class TestReadClosure:
    """Test suite for the transitive read-closure computation."""

    def test_closure_reaches_across_three_hops(self, tmp_path: Path) -> None:
        """The closure is a fixed point, not a single hop.

        Given: A handler calling a helper that calls a loader that reads,
        When: The read closure is computed,
        Then: Every function on the chain is in the closure.
        """
        _scoping_package(tmp_path)
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n'
            "from snapper.server.scoping import resolve_readable_wallets\n\n\n"
            'async def loader(principal, repo):\n    """Loader."""\n'
            "    return await resolve_readable_wallets(principal, repo)\n\n\n"
            'async def helper(principal, repo):\n    """Helper."""\n'
            "    return await loader(principal, repo)\n\n\n"
            'async def handler(principal, repo):\n    """Handler."""\n'
            "    return await helper(principal, repo)\n",
        )
        assert {"loader", "helper", "handler"} <= _closure_names(tmp_path)

    def test_unrelated_functions_stay_out_of_the_closure(self, tmp_path: Path) -> None:
        """Only functions that reach the read primitive join the closure.

        Given: A module with one reading and one non-reading function,
        When: The read closure is computed,
        Then: The non-reading function is absent.
        """
        _scoping_package(tmp_path)
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n'
            "from snapper.server.scoping import resolve_readable_wallets\n\n\n"
            'async def reader(principal, repo):\n    """Reader."""\n'
            "    return await resolve_readable_wallets(principal, repo)\n\n\n"
            'async def unrelated():\n    """Unrelated."""\n    return 1\n',
        )
        assert "unrelated" not in _closure_names(tmp_path)

    def test_trade_primitive_callers_stay_out_of_the_closure(self, tmp_path: Path) -> None:
        """The trade primitive never pulls its callers onto the read plane.

        Given: A mutation handler resolving through the trade primitive,
        When: The read closure is computed,
        Then: The handler is absent from the read closure.
        """
        _scoping_package(tmp_path)
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n'
            "from snapper.server.scoping import resolve_tradable_wallets\n\n\n"
            '@router.post("/cancel")\n'
            'async def cancel(principal, repo):\n    """Cancel."""\n'
            "    return await resolve_tradable_wallets(principal, repo)\n",
        )
        assert "cancel" not in _closure_names(tmp_path)

    def test_module_qualified_primitive_call_still_forms_an_edge(self, tmp_path: Path) -> None:
        """Spelling the primitive through a module alias does not evade the graph.

        Given: A handler calling ``scoping.resolve_readable_wallets``,
        When: The read closure is computed,
        Then: The handler is in the closure.
        """
        _scoping_package(tmp_path)
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n'
            "from snapper.server import scoping\n\n\n"
            'async def handler(principal, repo):\n    """Handler."""\n'
            "    return await scoping.resolve_readable_wallets(principal, repo)\n",
        )
        assert "handler" in _closure_names(tmp_path)

    def test_unresolvable_receiver_still_forms_a_primitive_edge(self, tmp_path: Path) -> None:
        """The primitive names are edges however the receiver is spelled.

        Given: A handler calling the primitive off an opaque attribute chain,
        When: The read closure is computed,
        Then: The handler is still in the closure.
        """
        _scoping_package(tmp_path)
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n\n'
            'async def handler(module, principal, repo):\n    """Handler."""\n'
            "    return await module.inner.resolve_readable_wallets(principal, repo)\n",
        )
        assert "handler" in _closure_names(tmp_path)

    def test_cross_module_alias_call_forms_an_edge(self, tmp_path: Path) -> None:
        """A helper reached through an imported module alias is an edge.

        Given: A handler calling ``helpers.loader`` where helpers is a server module,
        When: The read closure is computed,
        Then: The handler joins the closure through the loader.
        """
        _scoping_package(tmp_path)
        _server_module(
            tmp_path,
            "helpers.py",
            '"""Fixture."""\n\n'
            "from snapper.server.scoping import resolve_readable_wallets\n\n\n"
            'async def loader(principal, repo):\n    """Loader."""\n'
            "    return await resolve_readable_wallets(principal, repo)\n",
        )
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n'
            "from snapper.server import helpers\n\n\n"
            'async def handler(principal, repo):\n    """Handler."""\n'
            "    return await helpers.loader(principal, repo)\n",
        )
        assert "handler" in _closure_names(tmp_path)

    def test_cross_module_symbol_call_forms_an_edge(self, tmp_path: Path) -> None:
        """A helper imported by name from another server module is an edge.

        Given: A handler importing and calling a loader from a sibling module,
        When: The read closure is computed,
        Then: The handler joins the closure through the loader.
        """
        _scoping_package(tmp_path)
        _server_module(
            tmp_path,
            "helpers.py",
            '"""Fixture."""\n\n'
            "from snapper.server.scoping import resolve_readable_wallets\n\n\n"
            'async def loader(principal, repo):\n    """Loader."""\n'
            "    return await resolve_readable_wallets(principal, repo)\n",
        )
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n'
            "from snapper.server.helpers import loader\n\n\n"
            'async def handler(principal, repo):\n    """Handler."""\n'
            "    return await loader(principal, repo)\n",
        )
        assert "handler" in _closure_names(tmp_path)

    def test_object_method_call_is_not_an_edge(self, tmp_path: Path) -> None:
        """A repository method sharing a helper's name is not an edge.

        Given: A handler calling ``repo.loader`` while a server ``loader`` reads,
        When: The read closure is computed,
        Then: The handler stays out of the closure.
        """
        _scoping_package(tmp_path)
        _server_module(
            tmp_path,
            "helpers.py",
            '"""Fixture."""\n\n'
            "from snapper.server.scoping import resolve_readable_wallets\n\n\n"
            'async def loader(principal, repo):\n    """Loader."""\n'
            "    return await resolve_readable_wallets(principal, repo)\n",
        )
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n\n'
            'async def handler(repo):\n    """Handler."""\n    return await repo.loader()\n',
        )
        assert "handler" not in _closure_names(tmp_path)

    def test_repository_read_method_seeds_the_closure(self, tmp_path: Path) -> None:
        """The repository read method is a second door into the read plane.

        Given: A handler awaiting ``repo.list_readable_wallets_for_user``
            without touching the REST primitive,
        When: The read closure is computed,
        Then: The handler is in the closure, because seeding on the REST
            primitive alone would leave the wider wallet set reachable
            directly.
        """
        _scoping_package(tmp_path)
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n\n'
            'async def handler(principal, repo, now):\n    """Handler."""\n'
            "    return await repo.list_readable_wallets_for_user(principal, now)\n",
        )
        assert "handler" in _closure_names(tmp_path)

    def test_repository_trade_method_does_not_seed_the_closure(self, tmp_path: Path) -> None:
        """Only the read half of the repository seeds the closure.

        Given: A handler awaiting the operator-plane repository method,
        When: The read closure is computed,
        Then: The handler stays out of the closure.
        """
        _scoping_package(tmp_path)
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n\n'
            'async def handler(principal, repo, now):\n    """Handler."""\n'
            "    return await repo.list_accessible_wallets_for_operators(principal, now)\n",
        )
        assert "handler" not in _closure_names(tmp_path)

    def test_aliased_helper_import_still_forms_an_edge(self, tmp_path: Path) -> None:
        """Renaming a helper at import time does not break the call graph.

        Given: A handler importing a reading loader under a different local
            name and calling it,
        When: The read closure is computed,
        Then: The handler is in the closure, because the import table keeps
            the original symbol name rather than the alias.
        """
        _scoping_package(tmp_path)
        _server_module(
            tmp_path,
            "helpers.py",
            '"""Fixture."""\n\n'
            "from snapper.server.scoping import resolve_readable_wallets\n\n\n"
            'async def load_plan(principal, repo):\n    """Load."""\n'
            "    return await resolve_readable_wallets(principal, repo)\n",
        )
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n'
            "from snapper.server.helpers import load_plan as load\n\n\n"
            'async def handler(principal, repo):\n    """Handler."""\n'
            "    return await load(principal, repo)\n",
        )
        assert "handler" in _closure_names(tmp_path)

    def test_imported_name_absent_from_its_module_forms_no_edge(self, tmp_path: Path) -> None:
        """An import table entry with no matching definition adds nothing.

        Given: A handler importing a name that its source module does not define,
        When: The read closure is computed,
        Then: The handler stays out of the closure.
        """
        _scoping_package(tmp_path)
        _server_module(tmp_path, "helpers.py", '"""Fixture."""\n')
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n'
            "from snapper.server.helpers import loader\n\n\n"
            'async def handler(principal, repo):\n    """Handler."""\n'
            "    return await loader(principal, repo)\n",
        )
        assert "handler" not in _closure_names(tmp_path)


class TestVerbLinkage:
    """Test suite for the mutation-verb assertion over the read closure."""

    def test_post_route_reaching_the_read_plane_is_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A mutation route in the read closure is a violation.

        Given: A POST handler whose loader resolves through the read primitive,
        When: The server package is scanned,
        Then: The handler is reported with its verb and the primitive name.
        """
        monkeypatch.setattr(boundary, "TRADE_CALL_SITES", frozenset())
        monkeypatch.setattr(boundary, "TRADE_CLAIM_CONSUMERS", frozenset())
        _scoping_package(tmp_path)
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n'
            "from snapper.server.scoping import resolve_readable_wallets\n\n\n"
            'async def loader(principal, repo):\n    """Loader."""\n'
            "    return await resolve_readable_wallets(principal, repo)\n\n\n"
            '@router.post("/plans/cancel")\n'
            'async def cancel_plan(principal, repo):\n    """Cancel."""\n'
            "    return await loader(principal, repo)\n",
        )
        violations = boundary.scan_server_package(tmp_path)
        assert _details(violations) == [
            "cancel_plan is routed on POST but reaches resolve_readable_wallets"
        ]

    def test_api_route_mutation_handler_reaching_the_read_plane_is_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The generic ``api_route`` decorator is not a way around the guard.

        Given: A handler routed with ``@router.api_route(methods=["POST"])``
            that resolves through the read primitive,
        When: The server package is scanned,
        Then: It is reported exactly like a ``@router.post`` handler. Before
            this the decorator carried no verb at all, so the whole route
            form was an unguarded channel.
        """
        monkeypatch.setattr(boundary, "TRADE_CALL_SITES", frozenset())
        monkeypatch.setattr(boundary, "TRADE_CLAIM_CONSUMERS", frozenset())
        _scoping_package(tmp_path)
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n'
            "from snapper.server.scoping import resolve_readable_wallets\n\n\n"
            '@router.api_route("/plans/cancel", methods=["POST"])\n'
            'async def cancel_plan(principal, repo):\n    """Cancel."""\n'
            "    return await resolve_readable_wallets(principal, repo)\n",
        )
        assert _details(boundary.scan_server_package(tmp_path)) == [
            "cancel_plan is routed on POST but reaches resolve_readable_wallets"
        ]

    def test_aliased_mutation_decorator_reaching_the_read_plane_is_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An aliased verb decorator is not a way around the guard either.

        Given: ``router.post`` bound to a module-level name and used to
            route a handler that resolves through the read primitive,
        When: The server package is scanned,
        Then: The handler is reported on POST.
        """
        monkeypatch.setattr(boundary, "TRADE_CALL_SITES", frozenset())
        monkeypatch.setattr(boundary, "TRADE_CLAIM_CONSUMERS", frozenset())
        _scoping_package(tmp_path)
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n'
            "from snapper.server.scoping import resolve_readable_wallets\n\n"
            "_mutate = router.post\n\n\n"
            '@_mutate("/plans/cancel")\n'
            'async def cancel_plan(principal, repo):\n    """Cancel."""\n'
            "    return await resolve_readable_wallets(principal, repo)\n",
        )
        assert _details(boundary.scan_server_package(tmp_path)) == [
            "cancel_plan is routed on POST but reaches resolve_readable_wallets"
        ]

    def test_get_route_reaching_the_read_plane_is_clean(self, tmp_path: Path) -> None:
        """A read route in the read closure is exactly what is expected.

        Given: A GET handler resolving through the read primitive,
        When: The verb linkage is checked,
        Then: No violation is reported.
        """
        _scoping_package(tmp_path)
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n'
            "from snapper.server.scoping import resolve_readable_wallets\n\n\n"
            '@router.get("/plans")\n'
            'async def list_plans(principal, repo):\n    """List."""\n'
            "    return await resolve_readable_wallets(principal, repo)\n",
        )
        trees, _ = boundary.load_server_package(tmp_path)
        records = boundary.build_records(trees)
        index = boundary.build_index(records, trees)
        closure = boundary.compute_read_closure(records, boundary.build_edges(records, index))
        registered = boundary.registered_verbs(trees, index)
        assert boundary.check_verb_linkage(records, closure, registered) == []

    def test_shared_helper_repointed_at_the_read_plane_is_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Re-pointing a helper that a POST route shares is caught.

        Given: A loader used by both a GET and a POST route, resolving through
            the read primitive,
        When: The server package is scanned,
        Then: Only the POST route is reported.
        """
        monkeypatch.setattr(boundary, "TRADE_CALL_SITES", frozenset())
        monkeypatch.setattr(boundary, "TRADE_CLAIM_CONSUMERS", frozenset())
        _scoping_package(tmp_path)
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n'
            "from snapper.server.scoping import resolve_readable_wallets\n\n\n"
            'async def load_plan(principal, repo):\n    """Load."""\n'
            "    return await resolve_readable_wallets(principal, repo)\n\n\n"
            '@router.get("/plans/{plan_id}")\n'
            'async def get_plan(principal, repo):\n    """Get."""\n'
            "    return await load_plan(principal, repo)\n\n\n"
            '@router.post("/plans/{plan_id}/actions")\n'
            'async def act_on_plan(principal, repo):\n    """Act."""\n'
            "    return await load_plan(principal, repo)\n",
        )
        violations = boundary.scan_server_package(tmp_path)
        assert _details(violations) == [
            "act_on_plan is routed on POST but reaches resolve_readable_wallets"
        ]

    def test_nested_route_handler_is_reported(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A mutation route nested inside a router factory is still caught.

        Given: A factory whose nested POST handler resolves through the read plane,
        When: The server package is scanned,
        Then: The nested handler is reported under its qualified name.
        """
        monkeypatch.setattr(boundary, "TRADE_CALL_SITES", frozenset())
        monkeypatch.setattr(boundary, "TRADE_CLAIM_CONSUMERS", frozenset())
        _scoping_package(tmp_path)
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n'
            "from snapper.server.scoping import resolve_readable_wallets\n\n\n"
            'def make_router(principal, repo):\n    """Factory."""\n\n'
            '    @router.patch("/plans")\n'
            '    async def patch_plan():\n        """Patch."""\n'
            "        return await resolve_readable_wallets(principal, repo)\n\n"
            "    return patch_plan\n",
        )
        violations = boundary.scan_server_package(tmp_path)
        assert _details(violations) == [
            "make_router.patch_plan is routed on PATCH but reaches resolve_readable_wallets"
        ]

    def test_post_route_calling_the_repository_read_method_is_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A mutation route may not query the read plane through the repository.

        Given: A POST handler awaiting ``repo.list_readable_wallets_for_user``
            without touching the REST primitive,
        When: The server package is scanned,
        Then: The handler is reported, because the repository method reaches
            the same wider wallet set the REST primitive does.
        """
        monkeypatch.setattr(boundary, "TRADE_CALL_SITES", frozenset())
        monkeypatch.setattr(boundary, "TRADE_CLAIM_CONSUMERS", frozenset())
        _scoping_package(tmp_path)
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n\n'
            '@router.post("/wallets/refresh")\n'
            'async def refresh_wallets(principal, repo, now):\n    """Refresh."""\n'
            "    return await repo.list_readable_wallets_for_user(principal, now)\n",
        )
        assert _details(boundary.scan_server_package(tmp_path)) == [
            "refresh_wallets is routed on POST but reaches resolve_readable_wallets"
        ]

    def test_registered_mutation_handler_is_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A handler routed by ``add_api_route`` carries its verbs too.

        Given: An undecorated reading handler registered with
            ``methods=["POST"]``,
        When: The server package is scanned,
        Then: The handler is reported, because a route registered as a value
            never carries a decorator to read the verb off.
        """
        monkeypatch.setattr(boundary, "TRADE_CALL_SITES", frozenset())
        monkeypatch.setattr(boundary, "TRADE_CLAIM_CONSUMERS", frozenset())
        _scoping_package(tmp_path)
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n'
            "from snapper.server.scoping import resolve_readable_wallets\n\n\n"
            'async def cancel_plan(principal, repo):\n    """Cancel."""\n'
            "    return await resolve_readable_wallets(principal, repo)\n\n\n"
            'def build():\n    """Build."""\n'
            '    router.add_api_route("/plans/cancel", cancel_plan, methods=["POST"])\n',
        )
        assert _details(boundary.scan_server_package(tmp_path)) == [
            "cancel_plan is routed on POST but reaches resolve_readable_wallets"
        ]

    def test_registered_read_handler_is_clean(self, tmp_path: Path) -> None:
        """Registering a reading handler on GET stays legitimate.

        Given: The same handler registered with ``methods=["GET"]``,
        When: The server package is scanned,
        Then: No verb-linkage violation is reported.
        """
        _scoping_package(tmp_path)
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n'
            "from snapper.server.scoping import resolve_readable_wallets\n\n\n"
            'async def list_plans(principal, repo):\n    """List."""\n'
            "    return await resolve_readable_wallets(principal, repo)\n\n\n"
            'def build():\n    """Build."""\n'
            '    router.add_api_route("/plans", list_plans, methods=["GET"])\n',
        )
        details = _details(boundary.scan_server_package(tmp_path))
        assert all("is routed on" not in detail for detail in details)

    def test_dependency_provider_pulls_a_post_route_into_the_closure(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A read-plane dependency runs for the route that declares it.

        Given: A POST handler whose signature depends on a reading provider
            it never calls in its body,
        When: The server package is scanned,
        Then: The handler is reported, because FastAPI resolves the provider
            for every request the route serves.
        """
        monkeypatch.setattr(boundary, "TRADE_CALL_SITES", frozenset())
        monkeypatch.setattr(boundary, "TRADE_CLAIM_CONSUMERS", frozenset())
        _scoping_package(tmp_path)
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n'
            "from snapper.server.scoping import resolve_readable_wallets\n\n\n"
            'async def readable(principal, repo):\n    """Provider."""\n'
            "    return await resolve_readable_wallets(principal, repo)\n\n\n"
            '@router.post("/plans/cancel")\n'
            "async def cancel_plan(wallets: Annotated[list, Depends(readable)]):\n"
            '    """Cancel."""\n    return wallets\n',
        )
        assert _details(boundary.scan_server_package(tmp_path)) == [
            "cancel_plan is routed on POST but reaches resolve_readable_wallets"
        ]

    def test_route_decorator_dependency_pulls_a_post_route_into_the_closure(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A dependency declared on the route decorator counts as well.

        Given: A POST route declaring a reading provider in
            ``dependencies=[...]``,
        When: The server package is scanned,
        Then: The handler is reported.
        """
        monkeypatch.setattr(boundary, "TRADE_CALL_SITES", frozenset())
        monkeypatch.setattr(boundary, "TRADE_CLAIM_CONSUMERS", frozenset())
        _scoping_package(tmp_path)
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n'
            "from snapper.server.scoping import resolve_readable_wallets\n\n\n"
            'async def readable(principal, repo):\n    """Provider."""\n'
            "    return await resolve_readable_wallets(principal, repo)\n\n\n"
            '@router.post("/plans/cancel", dependencies=[Depends(readable)])\n'
            'async def cancel_plan():\n    """Cancel."""\n    return None\n',
        )
        assert _details(boundary.scan_server_package(tmp_path)) == [
            "cancel_plan is routed on POST but reaches resolve_readable_wallets"
        ]


class TestRouteRegistrations:
    """Test suite for verbs harvested from ``add_api_route`` registrations."""

    @pytest.mark.parametrize(
        ("registration", "expected"),
        [
            ('router.add_api_route("/x", handler, methods=["POST"])', frozenset({"post"})),
            ('router.add_api_route("/x", endpoint=handler, methods=["PUT"])', frozenset({"put"})),
            ('router.add_api_route("/x", handler, methods=("DELETE",))', frozenset({"delete"})),
            (
                'router.add_api_route("/x", handler, methods=["PATCH"], name="x")',
                frozenset({"patch"}),
            ),
            ('router.add_api_route("/x", handler, methods=[verb, 1, "POST"])', frozenset({"post"})),
            ('router.add_api_route("/x", handler, methods=["GET"])', frozenset()),
            ('router.add_api_route("/x", handler)', frozenset()),
            ('router.add_api_route("/x", handler, methods="POST")', frozenset()),
            ('router.add_api_route("/x", methods=["POST"])', frozenset()),
            ('router.add_api_route("/x", make(), methods=["POST"])', frozenset()),
            ('router.add_api_route("/x", missing, methods=["POST"])', frozenset()),
            ('router.include_router(handler, methods=["POST"])', frozenset()),
            ('factory()("/x", handler, methods=["POST"])', frozenset()),
            ('router.api_route("/x", handler, methods=["POST"])', frozenset()),
        ],
    )
    def test_registration_forms(
        self, tmp_path: Path, registration: str, expected: frozenset[str]
    ) -> None:
        """Only a resolvable handler on a mutating method picks up verbs.

        Given: One ``add_api_route`` registration form,
        When: The registered verbs are collected,
        Then: The handler carries exactly the mutating verbs it is routed on.
        """
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n\ndef handler():\n    """Handler."""\n\n\n' + registration + "\n",
        )
        assert _registered(tmp_path).get(("routes", "handler"), frozenset()) == expected

    def test_registrar_reached_through_a_module_level_alias(self, tmp_path: Path) -> None:
        """``add_api_route`` kept behind a name still registers its verbs.

        Given: ``add_api_route`` bound to a module-level name and a handler
            registered through it on POST,
        When: The registered verbs are collected,
        Then: The handler carries POST, so hiding the registrar behind a
            name is not a way past the verb-linkage assertion.
        """
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n_register = app.add_api_route\n\n\n'
            'def handler():\n    """Handler."""\n\n\n'
            '_register("/x", handler, methods=["POST"])\n',
        )
        assert _registered(tmp_path)[("routes", "handler")] == frozenset({"post"})


class TestDependencyTargets:
    """Test suite for FastAPI dependency providers as call-graph edges."""

    @pytest.mark.parametrize(
        ("declaration", "expected"),
        [
            ("dep: Annotated[int, Depends(provider)]", True),
            ("dep: int = Depends(provider)", True),
            ("dep: int = Depends(dependency=provider)", True),
            ("dep: int = Depends(provider, use_cache=False)", True),
            ("dep: int = Security(provider)", True),
            ("dep: int = Depends()", False),
            ("dep: int = Query(provider)", False),
            ("dep: int = Depends(factory())", False),
        ],
    )
    def test_declaration_forms(self, tmp_path: Path, declaration: str, expected: bool) -> None:
        """A dependency marker contributes its provider as an edge.

        Given: One dependency declaration in a handler signature,
        When: The server package is recorded,
        Then: The provider is a callee exactly when a marker really wraps it.
        """
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n\n'
            f"async def handler({declaration}):\n"
            '    """Handler."""\n    return None\n',
        )
        callees = _records(tmp_path)[("routes", "handler")].callees
        assert ((None, "provider") in callees) is expected


class TestReadSymbolNames:
    """Test suite for the one-name rule on the read primitive."""

    @pytest.mark.parametrize(
        ("statement", "expected"),
        [
            (
                "from snapper.server.scoping import resolve_readable_wallets as resolve_wallets",
                ["re-binds resolve_readable_wallets as resolve_wallets"],
            ),
            (
                "resolve_wallets = resolve_readable_wallets",
                ["re-binds resolve_readable_wallets as resolve_wallets"],
            ),
            (
                "resolve_wallets = scoping.resolve_readable_wallets",
                ["re-binds resolve_readable_wallets as resolve_wallets"],
            ),
            ("from snapper.server.scoping import resolve_readable_wallets", []),
            (
                (
                    "from snapper.server.scoping import "
                    "resolve_readable_wallets as resolve_readable_wallets"
                ),
                [],
            ),
            (
                "read_for_user = repo.list_readable_wallets_for_user",
                ["re-binds list_readable_wallets_for_user as read_for_user"],
            ),
            ("from snapper.server.helpers import load_plan as load", []),
            ("resolve_wallets = resolve_tradable_wallets", []),
            ("resolve_wallets = build_gate()", []),
            ("holder.gate = resolve_readable_wallets", []),
            ("resolve_readable_wallets = resolve_readable_wallets", []),
        ],
    )
    def test_rebinding_forms(self, tmp_path: Path, statement: str, expected: list[str]) -> None:
        """A read-plane entrance may not acquire a second name.

        Given: One binding statement in a server module,
        When: The server trees are checked for re-bindings,
        Then: Only a statement giving the read primitive or the repository
            read method another name is reported, because both the fence
            and the closure seed recognise those entrances by name.
        """
        _server_module(tmp_path, "routes.py", f'"""Fixture."""\n\n{statement}\n')
        trees, _ = boundary.load_server_package(tmp_path)
        assert _details(boundary.check_read_symbol_names(trees)) == expected

    def test_laundered_re_export_is_reported_by_the_package_scan(self, tmp_path: Path) -> None:
        """The scan refuses the laundering the fence cannot see.

        Given: A server module re-exporting the read primitive under a
            second name, which any package could then import,
        When: The server package is scanned,
        Then: The re-binding is reported with its source path and line.
        """
        _scoping_package(tmp_path)
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n'
            "from snapper.server.scoping import resolve_readable_wallets as resolve_wallets\n",
        )
        violations = [
            violation
            for violation in boundary.scan_server_package(tmp_path)
            if violation.detail.startswith("re-binds")
        ]
        assert violations == [
            boundary.Violation(
                "src/snapper/server/routes.py",
                3,
                "re-binds resolve_readable_wallets as resolve_wallets",
            )
        ]


class TestTradeCallSitePinning:
    """Test suite for the pinned trade-primitive call-site allowlist."""

    def test_unpinned_call_site_is_rejected(self, tmp_path: Path) -> None:
        """A new trade-primitive call site fails until it is pinned.

        Given: A module calling the trade primitive from an unpinned function,
        When: The call sites are checked against an empty allowlist,
        Then: The new call site is reported.
        """
        _scoping_package(tmp_path)
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n'
            "from snapper.server.scoping import resolve_tradable_wallets\n\n\n"
            'async def new_gate(principal, repo):\n    """Gate."""\n'
            "    return await resolve_tradable_wallets(principal, repo)\n",
        )
        records = _records(tmp_path)
        assert "unpinned resolve_tradable_wallets call site in new_gate" in _details(
            boundary.check_trade_call_sites(records)
        )

    def test_pinned_call_site_passes(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """A call site present in the allowlist is accepted.

        Given: A trade-primitive call site that the allowlist pins,
        When: The call sites are checked,
        Then: No violation is reported.
        """
        monkeypatch.setattr(boundary, "TRADE_CALL_SITES", frozenset({("routes", "gate")}))
        _scoping_package(tmp_path)
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n'
            "from snapper.server.scoping import resolve_tradable_wallets\n\n\n"
            'async def gate(principal, repo):\n    """Gate."""\n'
            "    return await resolve_tradable_wallets(principal, repo)\n",
        )
        assert boundary.check_trade_call_sites(_records(tmp_path)) == []

    def test_disappeared_call_site_is_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A pinned trade gate that vanishes fails rather than going stale.

        Given: An allowlist pinning a call site the tree no longer contains,
        When: The call sites are checked,
        Then: The disappearance is reported.
        """
        monkeypatch.setattr(boundary, "TRADE_CALL_SITES", frozenset({("routes", "gate")}))
        _scoping_package(tmp_path)
        _server_module(tmp_path, "routes.py", '"""Fixture."""\n')
        violations = boundary.check_trade_call_sites(_records(tmp_path))
        assert _details(violations) == [
            "pinned resolve_tradable_wallets call site gate has disappeared"
        ]


class TestTradeClaimConsumerPinning:
    """Test suite for the pinned ``require_tradable_active_wallet`` consumers.

    Pinning the gate's own trade-plane call only proves the helper still
    consults that plane, which stays true after every route stops using it.
    These tests pin the CONSUMERS, so deleting the dependency from a write
    route fails the gate instead of passing silently.
    """

    def test_unpinned_consumer_is_rejected(self, tmp_path: Path) -> None:
        """A new claim-gate consumer fails until it is pinned.

        Given: A write route declaring the claim gate as a dependency from an
            unpinned handler,
        When: The consumers are checked against the shipped allowlist,
        Then: The new consumer is reported.
        """
        _scoping_package(tmp_path)
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n'
            "from snapper.server.scoping import require_tradable_active_wallet\n\n\n"
            '@router.post("/x")\n'
            "async def new_write(wallet_id=Depends(require_tradable_active_wallet)):\n"
            '    """Write."""\n'
            "    return wallet_id\n",
        )
        assert "unpinned require_tradable_active_wallet consumer in new_write" in _details(
            boundary.check_trade_claim_consumers(_records(tmp_path))
        )

    def test_pinned_consumer_passes(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """A consumer present in the allowlist is accepted.

        Given: A claim-gate consumer the allowlist pins,
        When: The consumers are checked,
        Then: No violation is reported.
        """
        monkeypatch.setattr(boundary, "TRADE_CLAIM_CONSUMERS", frozenset({("routes", "write")}))
        _scoping_package(tmp_path)
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n'
            "from snapper.server.scoping import require_tradable_active_wallet\n\n\n"
            '@router.post("/x")\n'
            "async def write(wallet_id=Depends(require_tradable_active_wallet)):\n"
            '    """Write."""\n'
            "    return wallet_id\n",
        )
        assert boundary.check_trade_claim_consumers(_records(tmp_path)) == []

    def test_dropped_dependency_is_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Silently dropping the gate from a write route fails the check.

        Given: An allowlist pinning a consumer whose handler no longer
            declares the claim gate, having fallen back to the raw claim,
        When: The consumers are checked,
        Then: The disappearance is reported rather than passing.
        """
        monkeypatch.setattr(boundary, "TRADE_CLAIM_CONSUMERS", frozenset({("routes", "write")}))
        _scoping_package(tmp_path)
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n'
            '@router.post("/x")\n'
            "async def write(principal):\n"
            '    """Write."""\n'
            "    return principal.active_wallet_public_id\n",
        )
        assert _details(boundary.check_trade_claim_consumers(_records(tmp_path))) == [
            "pinned require_tradable_active_wallet consumer write has disappeared"
        ]


class TestImportContainment:
    """Test suite for read-plane containment outside the server package."""

    def test_read_primitive_import_is_rejected(self, tmp_path: Path) -> None:
        """A contained module may not import the read primitive.

        Given: The core wallet resolver importing the read primitive,
        When: Import containment is scanned,
        Then: The import is reported.
        """
        _contained_module(
            tmp_path,
            '"""Fixture."""\n\nfrom snapper.server.scoping import resolve_readable_wallets\n',
        )
        assert "imports snapper.server.scoping" in _details(
            boundary.scan_import_containment(tmp_path)
        )

    def test_trade_primitive_import_is_also_rejected(self, tmp_path: Path) -> None:
        """Contained modules may not reach the scoping module at all.

        Given: A contained module importing the trade primitive,
        When: Import containment is scanned,
        Then: The import is still reported, because MCP mirrors the trade
            plane locally rather than importing the REST resolver.
        """
        _contained_module(
            tmp_path,
            '"""Fixture."""\n\nfrom snapper.server.scoping import resolve_tradable_wallets\n',
        )
        assert "imports snapper.server.scoping" in _details(
            boundary.scan_import_containment(tmp_path)
        )

    def test_scoping_module_import_is_rejected(self, tmp_path: Path) -> None:
        """Importing the scoping module itself is a breach.

        Given: A contained module doing ``import snapper.server.scoping``,
        When: Import containment is scanned,
        Then: The import is reported.
        """
        _contained_module(tmp_path, '"""Fixture."""\n\nimport snapper.server.scoping\n')
        assert "imports snapper.server.scoping" in _details(
            boundary.scan_import_containment(tmp_path)
        )

    def test_scoping_submodule_import_is_rejected(self, tmp_path: Path) -> None:
        """A descendant of the scoping module is a breach too.

        Given: A contained module importing a scoping descendant,
        When: Import containment is scanned,
        Then: The import is reported.
        """
        _contained_module(tmp_path, '"""Fixture."""\n\nimport snapper.server.scoping.helpers\n')
        assert "imports snapper.server.scoping.helpers" in _details(
            boundary.scan_import_containment(tmp_path)
        )

    def test_repository_read_method_import_is_rejected(self, tmp_path: Path) -> None:
        """Importing the read repository method by name is a breach.

        Given: A contained module importing list_readable_wallets_for_user,
        When: Import containment is scanned,
        Then: The import is reported.
        """
        _contained_module(
            tmp_path,
            '"""Fixture."""\n\nfrom snapper.data.helpers import list_readable_wallets_for_user\n',
        )
        assert "imports snapper.data.helpers.list_readable_wallets_for_user" in _details(
            boundary.scan_import_containment(tmp_path)
        )

    def test_repository_read_method_call_is_rejected(self, tmp_path: Path) -> None:
        """Calling the read repository method off the repo is a breach.

        Given: A contained module awaiting repo.list_readable_wallets_for_user,
        When: Import containment is scanned,
        Then: The attribute reference is reported.
        """
        _contained_module(
            tmp_path,
            '"""Fixture."""\n\n\nasync def go(repo, user, ops, now):\n    """Go."""\n'
            "    return await repo.list_readable_wallets_for_user(user, ops, now)\n",
        )
        assert "references list_readable_wallets_for_user" in _details(
            boundary.scan_import_containment(tmp_path)
        )

    def test_bare_read_primitive_reference_is_rejected(self, tmp_path: Path) -> None:
        """A bare name reference to the read primitive is a breach.

        Given: A contained module referencing the read primitive by name,
        When: Import containment is scanned,
        Then: The reference is reported.
        """
        _contained_module(
            tmp_path,
            '"""Fixture."""\n\n\ndef go():\n    """Go."""\n    return resolve_readable_wallets\n',
        )
        assert "references resolve_readable_wallets" in _details(
            boundary.scan_import_containment(tmp_path)
        )

    def test_prose_mentions_of_the_read_plane_are_clean(self, tmp_path: Path) -> None:
        """Documenting the split is not the same as reaching across it.

        Given: A contained module naming both primitives only in its docstring,
        When: Import containment is scanned,
        Then: No violation is reported, which is why this check is AST-based.
        """
        _contained_module(
            tmp_path,
            '"""MCP mirrors resolve_tradable_wallets, never resolve_readable_wallets.\n\n'
            'It also never calls list_readable_wallets_for_user.\n"""\n',
        )
        assert boundary.scan_import_containment(tmp_path) == []

    def test_unrelated_first_party_import_is_clean(self, tmp_path: Path) -> None:
        """Contained modules keep their ordinary first-party imports.

        Given: A contained module importing the trade-plane repository method,
        When: Import containment is scanned,
        Then: No violation is reported.
        """
        _contained_module(
            tmp_path,
            '"""Fixture."""\n\n'
            "from snapper.data.repository import list_accessible_wallets_for_operators\n",
        )
        assert boundary.scan_import_containment(tmp_path) == []

    def test_missing_contained_path_fails_closed(self, tmp_path: Path) -> None:
        """A contained path that does not exist is reported, not skipped.

        Given: A project root with no contained paths at all,
        When: Import containment is scanned,
        Then: Every contained path is reported as fail-closed.
        """
        violations = boundary.scan_import_containment(tmp_path)
        assert _details(violations) == ["missing contained path (fail closed)"] * len(
            boundary.CONTAINED_PATHS
        )

    def test_unparseable_contained_file_fails_closed(self, tmp_path: Path) -> None:
        """An unparseable contained module is reported, not skipped.

        Given: A contained module that does not parse,
        When: Import containment is scanned,
        Then: The fail-closed reason is reported.
        """
        _contained_module(tmp_path, "def broken(:\n")
        assert "unparseable source (fail closed)" in _details(
            boundary.scan_import_containment(tmp_path)
        )

    def test_pycache_inside_a_contained_package_is_skipped(self, tmp_path: Path) -> None:
        """Compiled cache artifacts are never scanned.

        Given: A contained package whose __pycache__ holds a breaching module,
        When: Import containment is scanned,
        Then: No violation is reported.
        """
        _contained_stubs(tmp_path)
        cache = tmp_path / "src/snapper/mcp/__pycache__"
        cache.mkdir(parents=True, exist_ok=True)
        (cache / "tools.py").write_text(
            "from snapper.server.scoping import resolve_readable_wallets\n", encoding="utf-8"
        )
        assert boundary.scan_import_containment(tmp_path) == []


class TestPrimitiveFence:
    """Test suite for the package fence that bounds the verb-linkage closure."""

    def test_read_primitive_import_outside_the_server_package_is_rejected(
        self, tmp_path: Path
    ) -> None:
        """No module outside the server package may import the read primitive.

        Given: An auth module importing the read primitive,
        When: The package fence is scanned,
        Then: The import is reported, so the closure cannot be escaped by
            routing a mutation route through another package.
        """
        _outside_module(
            tmp_path,
            '"""Fixture."""\n\nfrom snapper.server.scoping import resolve_readable_wallets\n',
        )
        assert "imports snapper.server.scoping" in _details(boundary.scan_primitive_fence(tmp_path))

    def test_read_primitive_reference_outside_the_server_package_is_rejected(
        self, tmp_path: Path
    ) -> None:
        """Naming the read primitive outside the server package is a breach.

        Given: An auth module calling the read primitive,
        When: The package fence is scanned,
        Then: The reference is reported.
        """
        _outside_module(
            tmp_path,
            '"""Fixture."""\n\n\nasync def go(principal, repo):\n    """Go."""\n'
            "    return await resolve_readable_wallets(principal, repo)\n",
        )
        assert "references resolve_readable_wallets" in _details(
            boundary.scan_primitive_fence(tmp_path)
        )

    def test_repository_read_method_outside_the_server_package_is_allowed(
        self, tmp_path: Path
    ) -> None:
        """The read repository method stays usable outside the server package.

        Given: An auth module resolving the wallet hint through
            list_readable_wallets_for_user,
        When: The package fence is scanned,
        Then: No violation is reported, because the fence pins the REST
            primitive rather than the repository read plane.
        """
        _outside_module(
            tmp_path,
            '"""Fixture."""\n\n\nasync def go(repo, user, ops, now):\n    """Go."""\n'
            "    return await repo.list_readable_wallets_for_user(user, ops, now)\n",
        )
        assert boundary.scan_primitive_fence(tmp_path) == []

    def test_server_package_itself_is_exempt(self, tmp_path: Path) -> None:
        """The fence never fires on the package that owns the primitive.

        Given: A server module importing and calling the read primitive,
        When: The package fence is scanned,
        Then: No violation is reported.
        """
        _scoping_package(tmp_path)
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n'
            "from snapper.server.scoping import resolve_readable_wallets\n\n\n"
            'async def handler(principal, repo):\n    """Handler."""\n'
            "    return await resolve_readable_wallets(principal, repo)\n",
        )
        assert boundary.scan_primitive_fence(tmp_path) == []

    def test_missing_fence_root_fails_closed(self, tmp_path: Path) -> None:
        """A missing source tree is reported rather than passing silently.

        Given: A project root with no src/snapper tree,
        When: The package fence is scanned,
        Then: The fail-closed reason is reported.
        """
        assert _details(boundary.scan_primitive_fence(tmp_path)) == [
            "missing fence root (fail closed)"
        ]

    def test_unparseable_module_fails_closed(self, tmp_path: Path) -> None:
        """An unparseable module outside the server package is reported.

        Given: An auth module that does not parse,
        When: The package fence is scanned,
        Then: The fail-closed reason is reported.
        """
        _outside_module(tmp_path, "def broken(:\n")
        assert _details(boundary.scan_primitive_fence(tmp_path)) == [
            "unparseable source (fail closed)"
        ]

    def test_pycache_is_skipped(self, tmp_path: Path) -> None:
        """Compiled cache artifacts are never scanned by the fence.

        Given: A __pycache__ directory holding a breaching module,
        When: The package fence is scanned,
        Then: No violation is reported.
        """
        _outside_module(tmp_path, '"""Fixture."""\n')
        cache = tmp_path / "src/snapper/auth/__pycache__"
        cache.mkdir(parents=True, exist_ok=True)
        (cache / "routes.py").write_text(
            "from snapper.server.scoping import resolve_readable_wallets\n", encoding="utf-8"
        )
        assert boundary.scan_primitive_fence(tmp_path) == []


class TestServerPackageFailClosed:
    """Test suite for fail-closed behavior on the server scan root."""

    def test_missing_scan_root_fails_closed(self, tmp_path: Path) -> None:
        """A missing server package is reported rather than passing silently.

        Given: A project root without the server package,
        When: The server package is scanned,
        Then: Both the missing root and the absent primitive are reported.
        """
        assert _details(boundary.scan_server_package(tmp_path)) == [
            "missing scan root (fail closed)",
            "resolve_readable_wallets is not defined here (fail closed)",
        ]

    def test_absent_read_primitive_fails_closed(self, tmp_path: Path) -> None:
        """A renamed or deleted read primitive is reported, not ignored.

        Given: A server package that defines no read primitive,
        When: The server package is scanned,
        Then: The absent primitive is reported as fail-closed.
        """
        _server_module(tmp_path, "routes.py", '"""Fixture."""\n')
        assert _details(boundary.scan_server_package(tmp_path)) == [
            "resolve_readable_wallets is not defined here (fail closed)"
        ]

    def test_unparseable_server_module_fails_closed(self, tmp_path: Path) -> None:
        """An unparseable server module is reported rather than skipped.

        Given: A server package containing a file that does not parse,
        When: The server package is scanned,
        Then: The fail-closed reason is reported alongside the clean result.
        """
        _scoping_package(tmp_path)
        _server_module(tmp_path, "broken.py", "def broken(:\n")
        assert "unparseable source (fail closed)" in _details(
            boundary.scan_server_package(tmp_path)
        )

    def test_pycache_inside_the_server_package_is_skipped(self, tmp_path: Path) -> None:
        """Compiled cache artifacts under the scan root are never parsed.

        Given: A server package whose __pycache__ holds a leaking route,
        When: The server package is scanned,
        Then: Only the disappeared pinned call sites are reported.
        """
        _scoping_package(tmp_path)
        cache = tmp_path / boundary.SCAN_ROOT / "__pycache__"
        cache.mkdir(parents=True, exist_ok=True)
        (cache / "routes.py").write_text(
            '@router.post("/x")\nasync def leak(principal, repo):\n'
            "    return await resolve_readable_wallets(principal, repo)\n",
            encoding="utf-8",
        )
        details = _details(boundary.scan_server_package(tmp_path))
        assert all("is routed on" not in detail for detail in details)


class TestRunScan:
    """Test suite for the reporting entry points and exit codes."""

    def test_clean_project_returns_zero(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A project honoring the split passes strict mode.

        Given: A fixture project whose only reader is a GET route,
        When: run_scan executes in strict mode,
        Then: It returns exit code 0.
        """
        monkeypatch.setattr(boundary, "TRADE_CALL_SITES", frozenset())
        monkeypatch.setattr(boundary, "TRADE_CLAIM_CONSUMERS", frozenset())
        _contained_stubs(tmp_path)
        _scoping_package(tmp_path)
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n'
            "from snapper.server.scoping import resolve_readable_wallets\n\n\n"
            '@router.get("/plans")\n'
            'async def list_plans(principal, repo):\n    """List."""\n'
            "    return await resolve_readable_wallets(principal, repo)\n",
        )
        assert boundary.run_scan(tmp_path, strict_mode=True) == 0

    def test_leaking_project_fails_strict_mode(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A read-closure function carrying a POST decorator is rejected.

        Given: A fixture project whose POST cancel route reaches the read plane
            through a shared loader,
        When: run_scan executes in strict mode,
        Then: It returns exit code 1.
        """
        monkeypatch.setattr(boundary, "TRADE_CALL_SITES", frozenset())
        monkeypatch.setattr(boundary, "TRADE_CLAIM_CONSUMERS", frozenset())
        _contained_stubs(tmp_path)
        _scoping_package(tmp_path)
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n'
            "from snapper.server.scoping import resolve_readable_wallets\n\n\n"
            'async def load_plan(principal, repo):\n    """Load."""\n'
            "    return await resolve_readable_wallets(principal, repo)\n\n\n"
            '@router.post("/plans/{plan_id}/cancel")\n'
            'async def cancel_plan(principal, repo):\n    """Cancel."""\n'
            "    return await load_plan(principal, repo)\n",
        )
        assert boundary.run_scan(tmp_path, strict_mode=True) == 1

    def test_report_only_mode_returns_zero(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Report-only mode surfaces findings without failing.

        Given: The same leaking fixture project,
        When: run_scan executes without strict mode,
        Then: It returns exit code 0.
        """
        monkeypatch.setattr(boundary, "TRADE_CALL_SITES", frozenset())
        monkeypatch.setattr(boundary, "TRADE_CLAIM_CONSUMERS", frozenset())
        _contained_stubs(tmp_path)
        _scoping_package(tmp_path)
        _server_module(
            tmp_path,
            "routes.py",
            '"""Fixture."""\n\n'
            "from snapper.server.scoping import resolve_readable_wallets\n\n\n"
            '@router.post("/plans/{plan_id}/cancel")\n'
            'async def cancel_plan(principal, repo):\n    """Cancel."""\n'
            "    return await resolve_readable_wallets(principal, repo)\n",
        )
        assert boundary.run_scan(tmp_path, strict_mode=False) == 0

    def test_real_repository_passes_strict_mode(self) -> None:
        """The shipped tree honors its own read/trade split.

        Given: The real repository root,
        When: The boundary is scanned in strict mode,
        Then: It reports no violations.
        """
        root = Path(boundary.__file__).resolve().parent.parent
        assert boundary.run_scan(root, strict_mode=True) == 0

    def test_main_strict_passes_on_the_real_repository(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The strict CLI entry point returns success on the shipped tree.

        Given: The process invoked with the --strict flag,
        When: main resolves the repository root and scans it,
        Then: It returns exit code 0 because the real tree is clean.
        """
        monkeypatch.setattr(boundary.sys, "argv", ["check_read_visibility_boundary.py", "--strict"])
        assert boundary.main() == 0
