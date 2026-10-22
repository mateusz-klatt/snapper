"""Structural compatibility and conservative parsing for generated path sharing."""

import subprocess
from pathlib import Path

import pytest

from scripts.generate_types import _hoist_openapi_path_items
from scripts.generate_types import postprocess_openapi_typescript_file

_METHODS = ("get", "put", "post", "delete", "options", "head", "patch", "trace")
_PARAMETERS = """            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
"""


def _source(methods: tuple[str, ...] = ("get",), parameters: str = _PARAMETERS) -> str:
    """Build the generator's complete method inventory for a representative path."""
    properties = "\n".join(
        (
            f'        {method}: Operations["{method}"];'
            if method in methods
            else f"        {method}?: never;"
        )
        for method in _METHODS
    )
    return (
        'export interface Paths {\n    "/items/{id}": {\n'
        f"        parameters: {{\n{parameters}        }};\n{properties}\n    }};\n}}\n"
        "export interface Operations { get: { response: string }; post: { body: number }; }\n"
    )


@pytest.mark.parametrize("declaration", ["interface Paths", "type Paths ="])
def test_hoisted_paths_keep_mixed_operations_and_are_idempotent(declaration: str) -> None:
    """Share boilerplate without dropping supported verbs or duplicating helpers.

    Given: A generated path with required GET and POST operations.
    When: The path hoister runs twice for either supported root declaration.
    Then: Both operations remain required and the second pass is byte-identical.
    """
    source = _source(("get", "post")).replace("interface Paths", declaration)
    result = _hoist_openapi_path_items(source)
    assert result.count("type OpenApiPathItem<") == 1
    assert "get: OpenApiOperationGet;" in result
    assert "post: OpenApiOperationPost;" in result
    assert '    "/items/{id}": OpenApiPathItem<{' in result
    assert _hoist_openapi_path_items(result) == result


@pytest.mark.parametrize(
    "replacement",
    ["        custom?: never;", "        get?: never;", "        put: string;", ""],
)
def test_unfamiliar_or_incomplete_method_inventory_is_not_rewritten(replacement: str) -> None:
    """Reject lossy rewrites when upstream output differs from the recognized grammar.

    Given: An unknown, duplicate, differently typed or absent method property.
    When: The postprocessor considers the path entry.
    Then: The entire original file is retained verbatim.
    """
    source = _source().replace("        put?: never;", replacement)
    assert _hoist_openapi_path_items(source) == source


def test_unrelated_roots_and_conflicting_helper_names_are_preserved() -> None:
    """Avoid modifying unrelated contracts or colliding with an existing declaration.

    Given: A non-Paths root or a source that already defines the helper name.
    When: Path sharing runs.
    Then: Both sources remain unchanged.
    """
    unrelated = _source().replace("interface Paths", "interface Other")
    collision = "type OpenApiPathItem = string;\n" + _source()
    assert _hoist_openapi_path_items(unrelated) == unrelated
    assert _hoist_openapi_path_items(collision) == collision


def test_parameters_are_preserved_and_typescript_assignability_remains_exact(
    tmp_path: Path,
) -> None:
    """Compile independent structural proofs with required path parameters and mixed verbs.

    Given: Required nested path parameters and required GET/POST operations.
    When: The public postprocessor runs and TypeScript checks original and hoisted types.
    Then: Assignability, keys, requiredness and unsupported optional-never verbs agree.
    """
    parameters = _PARAMETERS.replace(
        "path?: never;", 'path: { id: string; nested?: { token: "}" }; };'
    )
    source = _source(("get", "post"), parameters)
    target = tmp_path / "paths.ts"
    target.write_text(source, encoding="utf-8")
    postprocess_openapi_typescript_file(target)
    result = target.read_text(encoding="utf-8")
    assert parameters in result
    original = source.replace("interface Paths", "interface Original").split(
        "export interface Operations"
    )[0]
    proofs = """
    type Assert<T extends true> = T;
    type Equal<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;
    type NewItem = Paths["/items/{id}"];
    type OldItem = Original["/items/{id}"];
    type Forward = Assert<Equal<NewItem, OldItem>>;
    type Keys = Assert<Equal<keyof NewItem, keyof OldItem>>;
    type Required = Assert<Equal<{} extends Pick<NewItem, "get" | "post" | "parameters"> ? true : false, false>>;
    type MissingMethod = Assert<Equal<NewItem["put"], undefined>>;
    type Unsupported = Assert<Equal<{ put: string } extends Pick<NewItem, "put"> ? true : false, false>>;
    type PathParameter = Assert<Equal<NewItem["parameters"]["path"]["id"], string>>;
    """
    target.write_text(result + original + proofs, encoding="utf-8")
    compiler = Path(__file__).resolve().parents[2] / "frontend/node_modules/typescript/bin/tsc"
    completed = subprocess.run(
        [
            "node",
            str(compiler),
            "--noEmit",
            "--strict",
            "--exactOptionalPropertyTypes",
            "--skipLibCheck",
            str(target),
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_empty_and_incomplete_paths_and_escaped_names_are_handled_conservatively() -> None:
    """Cover empty interfaces, omitted method lines and escaped path identifiers.

    Given: Empty, incomplete and correctly escaped generated path entries.
    When: The sharing pass parses each source.
    Then: Unrecognized shapes stay intact while valid escaped names retain their spelling.
    """
    empty = "export interface Paths {\n}\n"
    incomplete = _source().replace("        put?: never;\n", "")
    escaped = _source(()).replace("/items/{id}", '/items/\\"quoted\\"')
    assert _hoist_openapi_path_items(empty) == empty
    assert _hoist_openapi_path_items(incomplete) == incomplete
    assert '"/items/\\"quoted\\"": OpenApiPathItem<{' in _hoist_openapi_path_items(escaped)


@pytest.mark.parametrize("identifier", ["read-item", "123read", "read$Item", "__"])
def test_unsupported_operation_ids_preserve_original_contract(identifier: str) -> None:
    """Keep unfamiliar operation names intact rather than creating invalid aliases.

    Given: A path referencing a punctuation-bearing, numeric or empty-word identifier.
    When: Operation sharing attempts to derive a PascalCase alias.
    Then: The original file remains byte-identical with no partial declarations.
    """
    source = _source().replace('Operations["get"]', f'Operations["{identifier}"]')
    assert _hoist_openapi_path_items(source) == source


def test_existing_alias_identifier_cannot_be_shadowed() -> None:
    """Preserve the upstream type namespace when a generated alias would collide.

    Given: The source already declares the intended operation alias name.
    When: The path-sharing pass runs.
    Then: It retains the source without redefining or reusing that unrelated type.
    """
    source = "type OpenApiOperationGet = string;\n" + _source()
    assert _hoist_openapi_path_items(source) == source


def test_normalized_name_collisions_preserve_both_distinct_operation_references() -> None:
    """Detect all normalized aliases before rewriting the first affected path.

    Given: Distinct operation identifiers normalize to the same PascalCase name.
    When: Paths with both identifiers occur in either order.
    Then: Neither path is rewritten and neither operation is accidentally substituted.
    """
    first = _source().split("export interface Operations")[0]
    second = first.split("export interface Paths {\n", 1)[1].rsplit("}\n", 1)[0]
    second = second.replace('"/items/{id}"', '"/other/{id}"')
    for left, right in (("read_item", "read__item"), ("read__item", "read_item")):
        source = first.rsplit("}\n", 1)[0].replace('Operations["get"]', f'Operations["{left}"]')
        source += second.replace('Operations["get"]', f'Operations["{right}"]') + "}\n"
        assert _hoist_openapi_path_items(source) == source


def test_same_operation_is_shared_once_with_pascal_case_and_leading_underscores() -> None:
    """Reuse stable operation identity without duplicating declarations.

    Given: Two methods reference the same generated identifier with a leading underscore.
    When: Path boilerplate and operation references are shared.
    Then: One PascalCase alias serves both methods and a second pass is unchanged.
    """
    source = _source(("get", "post")).replace('Operations["get"]', 'Operations["_read_item"]')
    source = source.replace('Operations["post"]', 'Operations["_read_item"]')
    result = _hoist_openapi_path_items(source)
    assert result.count('type OpenApiOperationReadItem = Operations["_read_item"];') == 1
    assert "get: OpenApiOperationReadItem;" in result
    assert "post: OpenApiOperationReadItem;" in result
    assert _hoist_openapi_path_items(result) == result
