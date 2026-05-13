"""Tests for the Pydantic-route checker script."""

import ast
from pathlib import Path
from unittest.mock import patch

import scripts.check_pydantic_routes as check_pydantic_routes

_GOOD_ROUTER_SOURCE = '''"""Example router."""

from fastapi import APIRouter

router = APIRouter()


@router.get("/foo")
async def foo() -> "MyModel":
    """Return foo.

    Returns:
        MyModel: payload.
    """
    return MyModel()
'''


_DICT_RETURN_SOURCE = '''"""Bad router."""

from fastapi import APIRouter

router = APIRouter()


@router.get("/foo")
async def foo() -> dict[str, object]:
    """Return foo.

    Returns:
        Dict payload.
    """
    return {}
'''


_RESPONSE_NONE_SOURCE = '''"""Router with response_model=None."""

from fastapi import APIRouter

router = APIRouter()


@router.get("/foo", response_model=None)
async def foo() -> "MyModel":
    """Return foo.

    Returns:
        MyModel: payload.
    """
    return MyModel()
'''


_RESPONSE_NONE_WITH_RESPONSES_SOURCE = '''"""Router with response_model=None and responses dict."""

from fastapi import APIRouter

router = APIRouter()


@router.get("/foo", response_model=None, responses={200: {"model": "MyModel"}})
async def foo() -> "MyModel":
    """Return foo.

    Returns:
        MyModel: payload.
    """
    return MyModel()
'''


_NON_ROUTER_SOURCE = '''"""Helper module, not a router."""


def helper() -> dict[str, object]:
    """Return a dict.

    Returns:
        Empty dict.
    """
    return {}
'''


class TestShouldSkipPath:
    """Test suite for should_skip_path."""

    def test_skips_pycache(self, tmp_path: Path) -> None:
        """Skipped directory parts trigger a skip.

        Given: A path that traverses ``__pycache__``,
        When: should_skip_path is called,
        Then: It returns True.
        """
        path = tmp_path / "src" / "__pycache__" / "x.py"
        assert check_pydantic_routes.should_skip_path(path) is True

    def test_accepts_normal_path(self, tmp_path: Path) -> None:
        """Normal paths are not skipped.

        Given: A path that does not traverse any skipped directory,
        When: should_skip_path is called,
        Then: It returns False.
        """
        path = tmp_path / "src" / "snapper" / "server" / "x.py"
        assert check_pydantic_routes.should_skip_path(path) is False


class TestIterRouterFiles:
    """Test suite for iter_router_files."""

    def test_collects_python_files(self, tmp_path: Path) -> None:
        """Sorted Python files inside the configured roots are returned.

        Given: A project tree with files under and outside ``src/snapper``,
        When: iter_router_files is called with the matching relative root,
        Then: Only the in-scope sorted files are returned.
        """
        (tmp_path / "src" / "snapper" / "server").mkdir(parents=True)
        a = tmp_path / "src" / "snapper" / "server" / "a.py"
        b = tmp_path / "src" / "snapper" / "server" / "b.py"
        a.write_text("")
        b.write_text("")
        (tmp_path / "src" / "snapper" / "server" / "__pycache__").mkdir()
        (tmp_path / "src" / "snapper" / "server" / "__pycache__" / "skip.py").write_text("")

        files = check_pydantic_routes.iter_router_files(
            tmp_path, relative_roots=("src/snapper/server", "missing")
        )
        assert files == [a, b]


class TestAnnotationName:
    """Test suite for _annotation_name."""

    def test_resolves_simple_name(self) -> None:
        """Bare names resolve to their identifier.

        Given: A ``Name`` annotation node,
        When: _annotation_name is called,
        Then: The identifier is returned.
        """
        tree = ast.parse("def f() -> int: ...")
        func = tree.body[0]
        assert isinstance(func, ast.FunctionDef)
        annotation = func.returns
        assert annotation is not None
        assert check_pydantic_routes._annotation_name(annotation) == "int"

    def test_resolves_attribute(self) -> None:
        """Attribute access uses the rightmost segment.

        Given: A dotted attribute annotation,
        When: _annotation_name is called,
        Then: The attribute name is returned.
        """
        tree = ast.parse("def f() -> mod.Cls: ...")
        func = tree.body[0]
        assert isinstance(func, ast.FunctionDef)
        annotation = func.returns
        assert annotation is not None
        assert check_pydantic_routes._annotation_name(annotation) == "Cls"

    def test_resolves_subscript(self) -> None:
        """Subscript annotations resolve via the value node.

        Given: A subscripted generic annotation,
        When: _annotation_name is called,
        Then: The base name is returned.
        """
        tree = ast.parse("def f() -> list[int]: ...")
        func = tree.body[0]
        assert isinstance(func, ast.FunctionDef)
        annotation = func.returns
        assert annotation is not None
        assert check_pydantic_routes._annotation_name(annotation) == "list"

    def test_resolves_none_literal(self) -> None:
        """``None`` literal annotations resolve to ``"None"``.

        Given: A function with ``-> None`` return annotation,
        When: _annotation_name is called,
        Then: The literal ``"None"`` is returned.
        """
        tree = ast.parse("def f() -> None: ...")
        func = tree.body[0]
        assert isinstance(func, ast.FunctionDef)
        annotation = func.returns
        assert annotation is not None
        assert check_pydantic_routes._annotation_name(annotation) == "None"

    def test_resolves_unknown_to_expr_placeholder(self) -> None:
        """Unknown nodes fall back to the ``<expr>`` placeholder.

        Given: A binary operation used as an annotation node,
        When: _annotation_name is called,
        Then: ``"<expr>"`` is returned.
        """
        node = ast.BinOp(left=ast.Constant(value=1), op=ast.Add(), right=ast.Constant(value=2))
        assert check_pydantic_routes._annotation_name(node) == "<expr>"


class TestIsDecoratorCall:
    """Test suite for _is_decorator_call."""

    def test_matches_http_verb(self) -> None:
        """Router decorators with HTTP verbs are matched.

        Given: A decorator like ``@router.get("/x")``,
        When: _is_decorator_call is called,
        Then: The call node and the verb are returned.
        """
        tree = ast.parse("@router.get('/x')\nasync def f(): ...")
        func = tree.body[0]
        assert isinstance(func, ast.AsyncFunctionDef)
        result = check_pydantic_routes._is_decorator_call(func.decorator_list[0])
        assert result is not None
        _, verb = result
        assert verb == "get"

    def test_rejects_websocket(self) -> None:
        """WebSocket decorators are not HTTP routes.

        Given: A ``@router.websocket(...)`` decorator,
        When: _is_decorator_call is called,
        Then: ``None`` is returned.
        """
        tree = ast.parse("@router.websocket('/ws')\nasync def f(): ...")
        func = tree.body[0]
        assert isinstance(func, ast.AsyncFunctionDef)
        result = check_pydantic_routes._is_decorator_call(func.decorator_list[0])
        assert result is None

    def test_rejects_plain_name_decorator(self) -> None:
        """Plain-name decorators are skipped.

        Given: A decorator that is just ``@somefunc``,
        When: _is_decorator_call is called,
        Then: ``None`` is returned.
        """
        tree = ast.parse("@cached\nasync def f(): ...")
        func = tree.body[0]
        assert isinstance(func, ast.AsyncFunctionDef)
        result = check_pydantic_routes._is_decorator_call(func.decorator_list[0])
        assert result is None

    def test_rejects_attribute_decorator_without_call(self) -> None:
        """Bare-attribute decorators are not matched.

        Given: A decorator like ``@router.get`` (no call),
        When: _is_decorator_call is called,
        Then: ``None`` is returned.
        """
        tree = ast.parse("@router.get\nasync def f(): ...")
        func = tree.body[0]
        assert isinstance(func, ast.AsyncFunctionDef)
        result = check_pydantic_routes._is_decorator_call(func.decorator_list[0])
        assert result is None

    def test_rejects_plain_call_decorator(self) -> None:
        """Plain-name call decorators are not matched.

        Given: A decorator like ``@cached()`` (Call with Name func),
        When: _is_decorator_call is called,
        Then: ``None`` is returned because ``func`` is not an Attribute.
        """
        tree = ast.parse("@cached()\nasync def f(): ...")
        func = tree.body[0]
        assert isinstance(func, ast.AsyncFunctionDef)
        result = check_pydantic_routes._is_decorator_call(func.decorator_list[0])
        assert result is None

    def test_rejects_non_http_verb_attribute_call(self) -> None:
        """Attribute-call decorators with non-HTTP verbs are not matched.

        Given: A decorator like ``@router.middleware('http')``,
        When: _is_decorator_call is called,
        Then: ``None`` is returned because the attribute is not an HTTP verb.
        """
        tree = ast.parse("@router.middleware('http')\nasync def f(): ...")
        func = tree.body[0]
        assert isinstance(func, ast.AsyncFunctionDef)
        result = check_pydantic_routes._is_decorator_call(func.decorator_list[0])
        assert result is None


class TestHasResponsesWithModel:
    """Test suite for _has_responses_with_model."""

    def test_detects_responses_with_model(self) -> None:
        """``responses={200: {"model": X}}`` is detected.

        Given: A decorator call with a ``responses`` kwarg containing a model,
        When: _has_responses_with_model is called,
        Then: True is returned.
        """
        tree = ast.parse("@router.get('/x', responses={200: {'model': 'X'}})\nasync def f(): ...")
        func = tree.body[0]
        assert isinstance(func, ast.AsyncFunctionDef)
        call = func.decorator_list[0]
        assert isinstance(call, ast.Call)
        assert check_pydantic_routes._has_responses_with_model(call) is True

    def test_returns_false_when_responses_missing(self) -> None:
        """Missing ``responses`` kwarg falls through.

        Given: A decorator call without a ``responses`` kwarg,
        When: _has_responses_with_model is called,
        Then: False is returned.
        """
        tree = ast.parse("@router.get('/x')\nasync def f(): ...")
        func = tree.body[0]
        assert isinstance(func, ast.AsyncFunctionDef)
        call = func.decorator_list[0]
        assert isinstance(call, ast.Call)
        assert check_pydantic_routes._has_responses_with_model(call) is False

    def test_returns_false_when_responses_not_dict(self) -> None:
        """Non-dict ``responses`` value falls through.

        Given: A decorator with ``responses=value`` (not a dict literal),
        When: _has_responses_with_model is called,
        Then: False is returned.
        """
        tree = ast.parse("@router.get('/x', responses=value)\nasync def f(): ...")
        func = tree.body[0]
        assert isinstance(func, ast.AsyncFunctionDef)
        call = func.decorator_list[0]
        assert isinstance(call, ast.Call)
        assert check_pydantic_routes._has_responses_with_model(call) is False

    def test_returns_false_when_inner_value_not_dict(self) -> None:
        """Inner ``responses`` values that are not dicts are skipped.

        Given: A decorator with ``responses={200: ref}`` (non-dict inner value),
        When: _has_responses_with_model is called,
        Then: False is returned.
        """
        tree = ast.parse("@router.get('/x', responses={200: ref})\nasync def f(): ...")
        func = tree.body[0]
        assert isinstance(func, ast.AsyncFunctionDef)
        call = func.decorator_list[0]
        assert isinstance(call, ast.Call)
        assert check_pydantic_routes._has_responses_with_model(call) is False

    def test_returns_false_when_inner_dict_lacks_model(self) -> None:
        """Inner ``responses`` entries without a ``model`` key are rejected.

        Given: A decorator with ``responses={200: {"description": "..."}}``,
        When: _has_responses_with_model is called,
        Then: False is returned.
        """
        tree = ast.parse(
            "@router.get('/x', responses={200: {'description': 'y'}})\nasync def f(): ..."
        )
        func = tree.body[0]
        assert isinstance(func, ast.AsyncFunctionDef)
        call = func.decorator_list[0]
        assert isinstance(call, ast.Call)
        assert check_pydantic_routes._has_responses_with_model(call) is False


class TestResponseModelIsNone:
    """Test suite for _response_model_is_none."""

    def test_detects_explicit_none(self) -> None:
        """``response_model=None`` is detected.

        Given: A decorator call setting ``response_model=None``,
        When: _response_model_is_none is called,
        Then: True is returned.
        """
        tree = ast.parse("@router.get('/x', response_model=None)\nasync def f(): ...")
        func = tree.body[0]
        assert isinstance(func, ast.AsyncFunctionDef)
        call = func.decorator_list[0]
        assert isinstance(call, ast.Call)
        assert check_pydantic_routes._response_model_is_none(call) is True

    def test_returns_false_when_absent(self) -> None:
        """Missing ``response_model`` kwarg falls through.

        Given: A decorator call without a ``response_model`` kwarg,
        When: _response_model_is_none is called,
        Then: False is returned.
        """
        tree = ast.parse("@router.get('/x')\nasync def f(): ...")
        func = tree.body[0]
        assert isinstance(func, ast.AsyncFunctionDef)
        call = func.decorator_list[0]
        assert isinstance(call, ast.Call)
        assert check_pydantic_routes._response_model_is_none(call) is False

    def test_returns_false_when_response_model_is_a_class(self) -> None:
        """``response_model=MyModel`` is not ``None``.

        Given: A decorator call setting ``response_model=MyModel``,
        When: _response_model_is_none is called,
        Then: False is returned.
        """
        tree = ast.parse("@router.get('/x', response_model=MyModel)\nasync def f(): ...")
        func = tree.body[0]
        assert isinstance(func, ast.AsyncFunctionDef)
        call = func.decorator_list[0]
        assert isinstance(call, ast.Call)
        assert check_pydantic_routes._response_model_is_none(call) is False

    def test_skips_non_response_model_keywords(self) -> None:
        """Non-``response_model`` keywords are skipped during the scan.

        Given: A decorator call with other kwargs followed by ``response_model=None``,
        When: _response_model_is_none is called,
        Then: True is returned even though prior kwargs are different keys.
        """
        tree = ast.parse("@router.get('/x', tags=['a'], response_model=None)\nasync def f(): ...")
        func = tree.body[0]
        assert isinstance(func, ast.AsyncFunctionDef)
        call = func.decorator_list[0]
        assert isinstance(call, ast.Call)
        assert check_pydantic_routes._response_model_is_none(call) is True

    def test_returns_false_when_response_model_is_a_literal(self) -> None:
        """``response_model=0`` (non-None Constant) is not ``None``.

        Given: A decorator call setting ``response_model=0``,
        When: _response_model_is_none is called,
        Then: False is returned (Constant with non-None value).
        """
        tree = ast.parse("@router.get('/x', response_model=0)\nasync def f(): ...")
        func = tree.body[0]
        assert isinstance(func, ast.AsyncFunctionDef)
        call = func.decorator_list[0]
        assert isinstance(call, ast.Call)
        assert check_pydantic_routes._response_model_is_none(call) is False


class TestFlattenUnion:
    """Test suite for _flatten_union."""

    def test_flattens_pep604_union(self) -> None:
        """``A | B`` flattens into both branches.

        Given: A PEP 604 union annotation,
        When: _flatten_union is called,
        Then: Both branches appear.
        """
        tree = ast.parse("def f() -> int | str: ...")
        func = tree.body[0]
        assert isinstance(func, ast.FunctionDef)
        annotation = func.returns
        assert annotation is not None
        branches = check_pydantic_routes._flatten_union(annotation)
        names = {check_pydantic_routes._annotation_name(branch) for branch in branches}
        assert names == {"int", "str"}

    def test_flattens_union_subscript(self) -> None:
        """``Union[A, B]`` flattens into both branches.

        Given: A ``Union[...]`` subscript annotation,
        When: _flatten_union is called,
        Then: Both branches appear.
        """
        tree = ast.parse("def f() -> Union[int, str]: ...")
        func = tree.body[0]
        assert isinstance(func, ast.FunctionDef)
        annotation = func.returns
        assert annotation is not None
        branches = check_pydantic_routes._flatten_union(annotation)
        names = {check_pydantic_routes._annotation_name(branch) for branch in branches}
        assert names == {"int", "str"}

    def test_flattens_optional_subscript(self) -> None:
        """``Optional[A]`` falls through to the inner type.

        Given: An ``Optional[X]`` annotation,
        When: _flatten_union is called,
        Then: The inner type appears.
        """
        tree = ast.parse("def f() -> Optional[int]: ...")
        func = tree.body[0]
        assert isinstance(func, ast.FunctionDef)
        annotation = func.returns
        assert annotation is not None
        branches = check_pydantic_routes._flatten_union(annotation)
        assert {check_pydantic_routes._annotation_name(branch) for branch in branches} == {"int"}

    def test_returns_singleton_for_non_union(self) -> None:
        """Non-union annotations return a single-element list.

        Given: A bare annotation,
        When: _flatten_union is called,
        Then: A single-element list is returned.
        """
        tree = ast.parse("def f() -> int: ...")
        func = tree.body[0]
        assert isinstance(func, ast.FunctionDef)
        annotation = func.returns
        assert annotation is not None
        branches = check_pydantic_routes._flatten_union(annotation)
        assert len(branches) == 1


class TestReturnAnnotationViolation:
    """Test suite for _return_annotation_violation."""

    def test_flags_missing_annotation(self) -> None:
        """A missing return annotation is a violation.

        Given: ``None`` for the return annotation,
        When: _return_annotation_violation is called,
        Then: A descriptive message is returned.
        """
        message = check_pydantic_routes._return_annotation_violation(None)
        assert message == "missing return type annotation"

    def test_flags_dict_return(self) -> None:
        """Plain ``dict`` annotations are flagged.

        Given: ``-> dict[str, object]``,
        When: _return_annotation_violation is called,
        Then: A forbidden-type message is returned.
        """
        tree = ast.parse("def f() -> dict[str, object]: ...")
        func = tree.body[0]
        assert isinstance(func, ast.FunctionDef)
        annotation = func.returns
        assert annotation is not None
        message = check_pydantic_routes._return_annotation_violation(annotation)
        assert message is not None
        assert "dict" in message

    def test_accepts_pydantic_class_name(self) -> None:
        """An unknown identifier is treated as a Pydantic class.

        Given: ``-> MyModel`` for the return annotation,
        When: _return_annotation_violation is called,
        Then: No violation is returned.
        """
        tree = ast.parse("def f() -> MyModel: ...")
        func = tree.body[0]
        assert isinstance(func, ast.FunctionDef)
        annotation = func.returns
        assert annotation is not None
        assert check_pydantic_routes._return_annotation_violation(annotation) is None


class TestIsRouterDefinition:
    """Test suite for _is_router_definition."""

    def test_detects_apirouter_call(self) -> None:
        """Modules that construct ``APIRouter`` are router modules.

        Given: A module that calls ``APIRouter()``,
        When: _is_router_definition is called,
        Then: True is returned.
        """
        tree = ast.parse("router = APIRouter()")
        assert check_pydantic_routes._is_router_definition(tree) is True

    def test_detects_fastapi_call(self) -> None:
        """Modules that construct ``FastAPI`` are router modules.

        Given: A module that calls ``FastAPI()``,
        When: _is_router_definition is called,
        Then: True is returned.
        """
        tree = ast.parse("app = FastAPI()")
        assert check_pydantic_routes._is_router_definition(tree) is True

    def test_rejects_helper_module(self) -> None:
        """Modules without router construction are skipped.

        Given: A module with only helper code,
        When: _is_router_definition is called,
        Then: False is returned.
        """
        tree = ast.parse("def helper() -> int:\n    return 0\n")
        assert check_pydantic_routes._is_router_definition(tree) is False

    def test_ignores_unrelated_calls(self) -> None:
        """Calls to unrelated names do not trigger router detection.

        Given: A module that calls a helper function unrelated to FastAPI,
        When: _is_router_definition is called,
        Then: False is returned.
        """
        tree = ast.parse("helper()\nother.attr()")
        assert check_pydantic_routes._is_router_definition(tree) is False


class TestFunctionViolations:
    """Test suite for _function_violations."""

    def test_clean_route_has_no_violations(self) -> None:
        """A Pydantic-typed route returns no violations.

        Given: A route returning a model class,
        When: _function_violations is called,
        Then: No violations are returned.
        """
        tree = ast.parse("@router.get('/x')\nasync def f() -> MyModel: ...")
        func = tree.body[0]
        assert isinstance(func, ast.AsyncFunctionDef)
        assert check_pydantic_routes._function_violations(func) == []

    def test_dict_return_is_flagged(self) -> None:
        """A dict return type is flagged.

        Given: A route returning ``dict[str, object]``,
        When: _function_violations is called,
        Then: One violation appears in the result.
        """
        tree = ast.parse("@router.get('/x')\nasync def f() -> dict[str, object]: ...")
        func = tree.body[0]
        assert isinstance(func, ast.AsyncFunctionDef)
        violations = check_pydantic_routes._function_violations(func)
        assert len(violations) == 1
        assert "dict" in violations[0][1]

    def test_response_model_none_without_responses_is_flagged(self) -> None:
        """``response_model=None`` without a ``responses`` model is flagged.

        Given: A route with ``response_model=None`` and a Pydantic return,
        When: _function_violations is called,
        Then: One violation is returned about missing responses entry.
        """
        tree = ast.parse("@router.get('/x', response_model=None)\nasync def f() -> MyModel: ...")
        func = tree.body[0]
        assert isinstance(func, ast.AsyncFunctionDef)
        violations = check_pydantic_routes._function_violations(func)
        assert len(violations) == 1
        assert "response_model=None" in violations[0][1]

    def test_response_model_none_with_responses_passes(self) -> None:
        """``response_model=None`` with a ``responses`` model passes.

        Given: A route declaring schema via ``responses=`` only,
        When: _function_violations is called,
        Then: No violation is returned.
        """
        tree = ast.parse(
            "@router.get('/x', response_model=None, responses={200: {'model': 'M'}})"
            "\nasync def f() -> MyModel: ..."
        )
        func = tree.body[0]
        assert isinstance(func, ast.AsyncFunctionDef)
        assert check_pydantic_routes._function_violations(func) == []

    def test_non_route_function_is_ignored(self) -> None:
        """Functions without HTTP router decorators are ignored.

        Given: A function without a router decorator,
        When: _function_violations is called,
        Then: No violations are returned.
        """
        tree = ast.parse("async def f() -> dict[str, object]: ...")
        func = tree.body[0]
        assert isinstance(func, ast.AsyncFunctionDef)
        assert check_pydantic_routes._function_violations(func) == []

    def test_exempt_function_name_is_ignored(self) -> None:
        """Exempt entry-point functions are ignored.

        Given: A function whose name is in ``EXEMPT_FUNCTIONS``,
        When: _function_violations is called,
        Then: No violations are returned even with a forbidden annotation.
        """
        tree = ast.parse("@router.get('/x')\nasync def openapi_schema() -> dict: ...")
        func = tree.body[0]
        assert isinstance(func, ast.AsyncFunctionDef)
        assert check_pydantic_routes._function_violations(func) == []

    def test_skips_non_router_decorators_before_matching(self) -> None:
        """The first non-router decorator does not short-circuit the scan.

        Given: A function with a non-router decorator followed by a router one,
        When: _function_violations is called,
        Then: The router decorator is found and the function is audited.
        """
        tree = ast.parse("@cached\n@router.get('/x')\nasync def f() -> dict[str, object]: ...")
        func = tree.body[0]
        assert isinstance(func, ast.AsyncFunctionDef)
        violations = check_pydantic_routes._function_violations(func)
        assert len(violations) == 1
        assert "dict" in violations[0][1]

    def test_missing_return_annotation_is_flagged(self) -> None:
        """Missing return annotations are flagged.

        Given: A route without an explicit return annotation,
        When: _function_violations is called,
        Then: One violation about the missing annotation is returned.
        """
        tree = ast.parse("@router.get('/x')\nasync def f(): ...")
        func = tree.body[0]
        assert isinstance(func, ast.AsyncFunctionDef)
        violations = check_pydantic_routes._function_violations(func)
        assert len(violations) == 1
        assert "missing return type annotation" in violations[0][1]


class TestScanFile:
    """Test suite for scan_file."""

    def test_passes_clean_router(self, tmp_path: Path) -> None:
        """A clean router file yields no violations.

        Given: A router with a Pydantic-typed route,
        When: scan_file is called,
        Then: No violations are returned.
        """
        f = tmp_path / "clean.py"
        f.write_text(_GOOD_ROUTER_SOURCE)
        assert check_pydantic_routes.scan_file(f) == []

    def test_flags_dict_router(self, tmp_path: Path) -> None:
        """A router with a dict return is flagged.

        Given: A router that returns ``dict[str, object]``,
        When: scan_file is called,
        Then: One violation is returned.
        """
        f = tmp_path / "bad.py"
        f.write_text(_DICT_RETURN_SOURCE)
        violations = check_pydantic_routes.scan_file(f)
        assert len(violations) == 1

    def test_flags_response_none_without_responses(self, tmp_path: Path) -> None:
        """A router with ``response_model=None`` and no responses is flagged.

        Given: A router declaring ``response_model=None`` only,
        When: scan_file is called,
        Then: One violation is returned.
        """
        f = tmp_path / "rn.py"
        f.write_text(_RESPONSE_NONE_SOURCE)
        violations = check_pydantic_routes.scan_file(f)
        assert len(violations) == 1

    def test_accepts_response_none_with_responses(self, tmp_path: Path) -> None:
        """A router with ``response_model=None`` plus ``responses`` model passes.

        Given: A router declaring both ``response_model=None`` and ``responses=``,
        When: scan_file is called,
        Then: No violations are returned.
        """
        f = tmp_path / "rn_ok.py"
        f.write_text(_RESPONSE_NONE_WITH_RESPONSES_SOURCE)
        assert check_pydantic_routes.scan_file(f) == []

    def test_skips_non_router_module(self, tmp_path: Path) -> None:
        """Non-router helper modules are not scanned.

        Given: A helper module without ``APIRouter`` / ``FastAPI``,
        When: scan_file is called,
        Then: No violations are returned even with a dict return.
        """
        f = tmp_path / "helper.py"
        f.write_text(_NON_ROUTER_SOURCE)
        assert check_pydantic_routes.scan_file(f) == []

    def test_handles_unreadable_file(self, tmp_path: Path) -> None:
        """Unreadable files return an empty list.

        Given: A nonexistent file path,
        When: scan_file is called,
        Then: An empty list is returned.
        """
        f = tmp_path / "missing.py"
        assert check_pydantic_routes.scan_file(f) == []

    def test_handles_syntax_error(self, tmp_path: Path) -> None:
        """Files with syntax errors return an empty list.

        Given: A file with invalid Python syntax,
        When: scan_file is called,
        Then: An empty list is returned.
        """
        f = tmp_path / "broken.py"
        f.write_text("def :\n")
        assert check_pydantic_routes.scan_file(f) == []


class TestScanFiles:
    """Test suite for scan_files."""

    def test_aggregates_violations(self, tmp_path: Path) -> None:
        """Violations from multiple files are aggregated.

        Given: A tree with one clean file and one bad file,
        When: scan_files is called,
        Then: Only the bad file appears in the results.
        """
        src = tmp_path / "src" / "snapper" / "server"
        src.mkdir(parents=True)
        good = src / "good.py"
        bad = src / "bad.py"
        good.write_text(_GOOD_ROUTER_SOURCE)
        bad.write_text(_DICT_RETURN_SOURCE)

        results = check_pydantic_routes.scan_files(
            tmp_path,
            relative_roots=("src/snapper/server",),
        )
        assert bad in results
        assert good not in results


class TestRunScan:
    """Test suite for run_scan."""

    def test_returns_zero_when_clean(self, tmp_path: Path) -> None:
        """A clean tree returns exit code 0.

        Given: A clean router tree,
        When: run_scan is called in strict mode,
        Then: 0 is returned.
        """
        src = tmp_path / "src" / "snapper" / "server"
        src.mkdir(parents=True)
        (src / "ok.py").write_text(_GOOD_ROUTER_SOURCE)
        result = check_pydantic_routes.run_scan(
            tmp_path,
            strict_mode=True,
            relative_roots=("src/snapper/server",),
        )
        assert result == 0

    def test_returns_one_on_violation_strict(self, tmp_path: Path) -> None:
        """A violation triggers exit code 1 in strict mode.

        Given: A router with a dict return,
        When: run_scan is called with ``strict_mode=True``,
        Then: 1 is returned.
        """
        src = tmp_path / "src" / "snapper" / "server"
        src.mkdir(parents=True)
        (src / "bad.py").write_text(_DICT_RETURN_SOURCE)
        result = check_pydantic_routes.run_scan(
            tmp_path,
            strict_mode=True,
            relative_roots=("src/snapper/server",),
        )
        assert result == 1

    def test_returns_zero_on_violation_non_strict(self, tmp_path: Path) -> None:
        """A violation returns exit code 0 in non-strict mode.

        Given: A router with a dict return,
        When: run_scan is called without strict mode,
        Then: 0 is returned.
        """
        src = tmp_path / "src" / "snapper" / "server"
        src.mkdir(parents=True)
        (src / "bad.py").write_text(_DICT_RETURN_SOURCE)
        result = check_pydantic_routes.run_scan(
            tmp_path,
            strict_mode=False,
            relative_roots=("src/snapper/server",),
        )
        assert result == 0


class TestMain:
    """Test suite for main entry point."""

    def test_main_returns_run_scan_exit_code(self) -> None:
        """The main entry point forwards the run_scan exit code.

        Given: A patched run_scan returning 0,
        When: main is called,
        Then: 0 is returned.
        """
        with patch("scripts.check_pydantic_routes.run_scan", return_value=0) as mock_scan:
            result = check_pydantic_routes.main()
        assert result == 0
        mock_scan.assert_called_once()

    def test_main_propagates_strict_flag(self) -> None:
        """``--strict`` on argv is forwarded to run_scan.

        Given: ``sys.argv`` contains ``--strict``,
        When: main is called,
        Then: ``run_scan`` is invoked with ``strict_mode=True``.
        """
        with (
            patch("sys.argv", ["check_pydantic_routes.py", "--strict"]),
            patch("scripts.check_pydantic_routes.run_scan", return_value=0) as mock_scan,
        ):
            check_pydantic_routes.main()
        args = mock_scan.call_args[0]
        assert args[1] is True
