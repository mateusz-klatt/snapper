"""FastAPI dependency for JSON-mode Pydantic validation.

FastAPI parses JSON request bodies via ``request.json()`` (-> Python dict)
and validates with ``TypeAdapter.validate_python(dict)`` — Pydantic's
**Python mode**.  In strict mode this rejects natural JSON coercions like
``str -> datetime``.

This module provides:

- :func:`json_body` — async dependency that validates raw request bytes
  via ``model_validate_json`` (Pydantic **JSON mode**).
- :func:`openapi_schema` — builds ``openapi_extra`` for the route decorator
  and registers the schema for later injection.
- :func:`patch_openapi` — injects registered schemas into
  ``components/schemas`` so tooling (openapi-typescript, generators)
  can resolve them.

Usage::

    from snapper.server.json_body import json_body
    from snapper.server.json_body import openapi_schema
    from snapper.server.json_body import patch_openapi

    @router.post("/example", openapi_extra=openapi_schema(MyRequest))
    async def example(
        data: Annotated[MyRequest, Depends(json_body(MyRequest))],
    ) -> ...:
        ...

    app = FastAPI(...)
    app.include_router(router)
    patch_openapi(app)
"""

from collections.abc import Callable
from collections.abc import Coroutine
from typing import Any

from fastapi import FastAPI
from fastapi import Request
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel
from pydantic import ValidationError

_SCHEMA_REGISTRY: dict[str, dict[str, Any]] = {}


def optional_json_body[ModelT: BaseModel](
    model: type[ModelT],
) -> Callable[..., Coroutine[Any, Any, ModelT | None]]:
    """Create a FastAPI dependency that validates JSON when present.

    Returns a sentinel ``None`` when the request body is empty (``b""``)
    or omitted. Validates like :func:`json_body` for populated bodies;
    malformed JSON still raises ``RequestValidationError``.

    Used by endpoints that historically accepted no body but now want
    to accept an optional body without breaking existing zero-body
    callers (R15 blocker fix — the naive ``model()`` synthesis path
    is infeasible under ``PayloadRequest[...]`` envelopes whose
    required fields have no defaults).

    Args:
        model: The Pydantic model class to validate against.

    Returns:
        An async dependency callable returning ``ModelT | None``.
    """

    async def dependency(request: Request) -> ModelT | None:
        """Read raw request body; return ``None`` when empty, else validate."""
        raw = await request.body()
        if not raw:
            return None
        try:
            return model.model_validate_json(raw)
        except ValidationError as exc:
            errors = [{**err, "loc": ("body", *err["loc"])} for err in exc.errors()]
            raise RequestValidationError(errors, body=raw.decode("utf-8")) from exc

    return dependency


def json_body[ModelT: BaseModel](model: type[ModelT]) -> Callable[..., Coroutine[Any, Any, ModelT]]:
    """Create a FastAPI dependency that validates raw JSON bytes via Pydantic JSON mode.

    Args:
        model: The Pydantic model class to validate against.

    Returns:
        An async dependency callable for use with ``Depends()``.
    """

    async def dependency(request: Request) -> ModelT:
        """Read raw request body and validate in JSON mode.

        Args:
            request: The incoming FastAPI request.

        Returns:
            Validated model instance.

        Raises:
            RequestValidationError: If validation fails.
        """
        raw = await request.body()
        try:
            return model.model_validate_json(raw)
        except ValidationError as exc:
            errors = [{**err, "loc": ("body", *err["loc"])} for err in exc.errors()]
            raise RequestValidationError(errors, body=raw.decode("utf-8")) from exc

    return dependency


def _rewrite_refs(obj: Any) -> Any:
    """Rewrite ``$ref`` pointers from ``#/$defs/`` to ``#/components/schemas/``.

    Args:
        obj: JSON schema node (dict, list, or scalar).

    Returns:
        Node with rewritten ``$ref`` paths.
    """
    if isinstance(obj, dict):
        if "$ref" in obj:
            ref_name = obj["$ref"].rsplit("/", 1)[-1]
            return {"$ref": f"#/components/schemas/{ref_name}"}
        return {k: _rewrite_refs(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_rewrite_refs(v) for v in obj]
    return obj


def _clean_schema_node(node: dict[str, Any]) -> dict[str, Any]:
    """Strip ``default`` keys and trim ``required`` for one schema object.

    FastAPI does not emit ``default`` in property schemas and excludes
    defaulted fields from ``required``.  Pydantic does both.  This
    aligns one schema node so ``openapi-typescript`` generates
    optional (``?``) properties, matching native FastAPI output.

    Args:
        node: A single JSON schema object with ``properties``.

    Returns:
        Cleaned copy of the node.
    """
    props = node.get("properties", {})
    if not props:
        return node
    required = node.get("required")
    if isinstance(required, list):
        node["required"] = [f for f in required if "default" not in props.get(f, {})]
        if not node["required"]:
            del node["required"]
    for field_name, field_schema in props.items():
        if "default" in field_schema:
            node["properties"][field_name] = {
                k: v for k, v in field_schema.items() if k != "default"
            }
    return node


def _strip_defaults(obj: Any) -> Any:
    """Recursively align Pydantic schema output with FastAPI conventions.

    Walks the schema tree and applies :func:`_clean_schema_node` to
    every dict that contains ``properties``.

    Args:
        obj: JSON schema node (dict, list, or scalar).

    Returns:
        Node with ``required`` lists trimmed and ``default`` keys removed.
    """
    if isinstance(obj, dict):
        result = {k: _strip_defaults(v) for k, v in obj.items()}
        return _clean_schema_node(result)
    if isinstance(obj, list):
        return [_strip_defaults(v) for v in obj]
    return obj


def _lift_defs(schema: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Extract ``$defs`` and rewrite ``$ref`` paths to ``components/schemas``.

    Pydantic emits local ``$defs`` with ``#/$defs/Foo`` references.
    OpenAPI expects sub-schemas in ``components/schemas`` with
    ``#/components/schemas/Foo`` references.  This function extracts
    the ``$defs`` entries, rewrites ``$ref`` pointers, and strips
    fields with defaults from ``required`` lists.

    Args:
        schema: Raw JSON schema dict from Pydantic.

    Returns:
        Tuple of (rewritten root schema, dict of extracted sub-schemas).
    """
    defs = schema.pop("$defs", {})
    rewritten: dict[str, Any] = _strip_defaults(_rewrite_refs(schema))
    lifted: dict[str, Any] = {
        name: _strip_defaults(_rewrite_refs(defn)) for name, defn in defs.items()
    }
    return rewritten, lifted


def openapi_schema(model: type[BaseModel], *, required: bool = True) -> dict[str, Any]:
    """Build ``openapi_extra`` dict restoring the requestBody schema.

    FastAPI does not generate requestBody for ``Depends()`` parameters.
    Pass the return value as ``openapi_extra`` on the route decorator to
    restore the schema in the OpenAPI spec.

    The model and its sub-schemas are registered for injection by
    :func:`patch_openapi`.  The returned dict uses a ``$ref`` pointer
    to ``components/schemas/{ModelName}``.

    Args:
        model: The Pydantic model whose JSON schema to embed.
        required: Whether the requestBody is mandatory. Pass
            ``required=False`` for endpoints using
            :func:`optional_json_body` so the generated OpenAPI / typed
            clients don't wrongly mark the body required (R14 gpt-5.4
            fix #4).

    Returns:
        Dict suitable for the ``openapi_extra`` keyword argument.
    """
    name = model.__name__
    rewritten, sub_schemas = _lift_defs(model.model_json_schema())
    _SCHEMA_REGISTRY[name] = rewritten
    _SCHEMA_REGISTRY.update(sub_schemas)
    return {
        "requestBody": {
            "required": required,
            "content": {
                "application/json": {
                    "schema": {"$ref": f"#/components/schemas/{name}"},
                }
            },
        }
    }


def patch_openapi(app: FastAPI) -> None:
    """Inject registered request schemas into the app's OpenAPI spec.

    Wraps ``app.openapi()`` so that schemas collected by
    :func:`openapi_schema` appear in ``components/schemas``,
    allowing tooling (openapi-typescript, entity generators) to
    resolve the ``$ref`` pointers.

    Must be called once after all routers are included.

    Args:
        app: The FastAPI application instance.
    """
    original_openapi = app.openapi

    def custom_openapi() -> dict[str, Any]:
        """Extend the OpenAPI spec with registered json_body schemas."""
        spec = original_openapi()
        components: dict[str, Any] = spec.setdefault("components", {})
        schemas: dict[str, Any] = components.setdefault("schemas", {})
        for name, json_schema in _SCHEMA_REGISTRY.items():
            if name not in schemas:
                schemas[name] = json_schema
        return spec

    _attr = "openapi"
    setattr(app, _attr, custom_openapi)
