"""Tests for json_body FastAPI dependency and OpenAPI schema helpers."""

from datetime import UTC
from datetime import datetime
from typing import Any

import pytest
from fastapi import APIRouter
from fastapi import Depends
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.testclient import TestClient
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import ValidationError
from pydantic import field_validator
from pydantic_core import PydanticCustomError

from snapper.server.json_body import _SCHEMA_REGISTRY
from snapper.server.json_body import _lift_defs
from snapper.server.json_body import _strip_defaults
from snapper.server.json_body import _validation_text
from snapper.server.json_body import json_body
from snapper.server.json_body import openapi_schema
from snapper.server.json_body import optional_json_body
from snapper.server.json_body import patch_openapi


class _StubRequest:
    """Minimal Request stub that returns pre-set body bytes."""

    def __init__(self, raw: bytes) -> None:
        self._raw = raw

    async def body(self) -> bytes:
        """Return pre-set raw bytes."""
        return self._raw


class TestJsonBody:
    """Verify json_body dependency validates via Pydantic JSON mode."""

    @pytest.mark.asyncio
    async def test_valid_json_returns_model(self) -> None:
        """Validate correct JSON body is parsed into model.

        Given: Raw JSON bytes with an ISO timestamp,
        When: json_body dependency is called,
        Then: Returns a validated model with datetime coerced from string.
        """

        class StrictModel(BaseModel):
            model_config = ConfigDict(strict=True, extra="forbid")
            ts: datetime
            name: str

        dep = json_body(StrictModel)
        req = _StubRequest(b'{"ts": "2024-01-01T00:00:00Z", "name": "test"}')
        result = await dep(req)
        assert result.ts == datetime(2024, 1, 1, tzinfo=UTC)
        assert result.name == "test"

    @pytest.mark.asyncio
    async def test_invalid_json_raises_validation_error(self) -> None:
        """Validate malformed body raises RequestValidationError.

        Given: Raw JSON bytes that fail model validation,
        When: json_body dependency is called,
        Then: RequestValidationError is raised with body location.
        """

        class StrictModel(BaseModel):
            model_config = ConfigDict(strict=True, extra="forbid")
            ts: datetime

        dep = json_body(StrictModel)
        req = _StubRequest(b'{"ts": 12345}')
        with pytest.raises(RequestValidationError):
            await dep(req)

    @pytest.mark.asyncio
    async def test_json_mode_accepts_str_datetime_that_python_mode_rejects(self) -> None:
        """Demonstrate JSON mode allows str->datetime that Python mode rejects.

        Given: A strict model with a datetime field,
        When: An ISO string is in the JSON body,
        Then: json_body succeeds (JSON mode) while model_validate(dict) would fail.
        """

        class StrictModel(BaseModel):
            model_config = ConfigDict(strict=True, extra="forbid")
            ts: datetime

        dep = json_body(StrictModel)
        req = _StubRequest(b'{"ts": "2024-06-15T12:00:00+00:00"}')
        result = await dep(req)
        assert isinstance(result.ts, datetime)

        with pytest.raises(ValidationError):
            StrictModel.model_validate({"ts": "2024-06-15T12:00:00+00:00"})


class TestOptionalJsonBody:
    """Cover optional_json_body sentinel + validation paths."""

    @pytest.mark.asyncio
    async def test_empty_body_returns_none(self) -> None:
        """Empty body returns sentinel ``None`` (the optional-body contract).

        Given: An empty request body,
        When: optional_json_body dependency is called,
        Then: Returns ``None`` instead of synthesising a model.
        """

        class Body(BaseModel):
            model_config = ConfigDict(strict=True, extra="forbid")
            x: int

        dep = optional_json_body(Body)
        req = _StubRequest(b"")
        assert await dep(req) is None

    @pytest.mark.asyncio
    async def test_populated_body_validates_like_json_body(self) -> None:
        """Non-empty body validates exactly like ``json_body``.

        Given: A populated valid JSON body,
        When: optional_json_body dependency is called,
        Then: The validated model is returned.
        """

        class Body(BaseModel):
            model_config = ConfigDict(strict=True, extra="forbid")
            x: int

        dep = optional_json_body(Body)
        req = _StubRequest(b'{"x": 42}')
        result = await dep(req)
        assert result is not None
        assert result.x == 42

    @pytest.mark.asyncio
    async def test_malformed_json_raises_request_validation_error(self) -> None:
        """Malformed body still raises (NOT silently skipped).

        Given: A non-empty body that fails validation,
        When: optional_json_body dependency is called,
        Then: RequestValidationError is raised (matches json_body).
        """

        class Body(BaseModel):
            model_config = ConfigDict(strict=True, extra="forbid")
            x: int

        dep = optional_json_body(Body)
        req = _StubRequest(b'{"x": "not_an_int"}')
        with pytest.raises(RequestValidationError):
            await dep(req)


class TestStripDefaults:
    """Verify _strip_defaults aligns Pydantic output with FastAPI conventions."""

    def test_removes_default_keys_and_trims_required(self) -> None:
        """Validate default keys are removed and required list is trimmed.

        Given: A schema with properties that have default values,
        When: _lift_defs processes it,
        Then: default keys are removed and fields dropped from required.
        """
        schema: dict[str, Any] = {
            "type": "object",
            "required": ["name", "enabled"],
            "properties": {
                "name": {"type": "string"},
                "enabled": {"type": "boolean", "default": False},
            },
        }
        result: dict[str, Any] = _strip_defaults(schema)
        assert result["required"] == ["name"]
        assert "default" not in result["properties"]["enabled"]

    def test_deletes_required_when_all_defaulted(self) -> None:
        """Validate required key is deleted when all fields have defaults.

        Given: A schema where every field has a default,
        When: _strip_defaults processes it,
        Then: The required key is removed entirely.
        """
        schema: dict[str, Any] = {
            "type": "object",
            "required": ["x"],
            "properties": {
                "x": {"type": "integer", "default": 0},
            },
        }
        result: dict[str, Any] = _strip_defaults(schema)
        assert "required" not in result


class TestLiftDefs:
    """Verify _lift_defs extracts $defs and rewrites $ref paths."""

    def test_rewrites_ref_paths(self) -> None:
        """Validate $defs are extracted and $ref paths point to components/schemas.

        Given: A schema with $defs and local $ref pointers,
        When: _lift_defs is called,
        Then: Root schema refs point to components/schemas and sub-schemas are returned.
        """
        schema: dict[str, Any] = {
            "$defs": {
                "Inner": {"type": "object", "properties": {"x": {"type": "integer"}}},
            },
            "type": "object",
            "properties": {
                "payload": {"$ref": "#/$defs/Inner"},
            },
        }
        rewritten, subs = _lift_defs(schema)
        assert rewritten["properties"]["payload"] == {"$ref": "#/components/schemas/Inner"}
        assert "Inner" in subs
        assert "$defs" not in rewritten

    def test_no_defs_passthrough(self) -> None:
        """Validate schemas without $defs pass through unchanged.

        Given: A schema with no $defs,
        When: _lift_defs is called,
        Then: Schema is returned unchanged, no sub-schemas extracted.
        """
        schema: dict[str, Any] = {"type": "object", "properties": {"x": {"type": "string"}}}
        rewritten, subs = _lift_defs(schema)
        assert rewritten == {"type": "object", "properties": {"x": {"type": "string"}}}
        assert subs == {}


class TestOpenApiSchema:
    """Verify openapi_schema builds correct openapi_extra dict."""

    def test_returns_ref_to_components(self) -> None:
        """Validate openapi_schema returns $ref to components/schemas.

        Given: A Pydantic model,
        When: openapi_schema is called,
        Then: Returns requestBody with $ref to components/schemas/{ModelName}.
        """

        class MyModel(BaseModel):
            name: str

        result = openapi_schema(MyModel)
        ref = result["requestBody"]["content"]["application/json"]["schema"]
        assert ref == {"$ref": "#/components/schemas/MyModel"}


class TestPatchOpenapi:
    """Verify patch_openapi injects schemas into components/schemas."""

    def test_injects_registered_schemas(self) -> None:
        """Validate patch_openapi adds registered schemas to the spec.

        Given: An app with openapi_schema-registered models,
        When: patch_openapi is called and openapi() is invoked,
        Then: Registered schemas appear in components/schemas.
        """

        class PatchTestModel(BaseModel):
            value: int

        openapi_schema(PatchTestModel)
        assert "PatchTestModel" in _SCHEMA_REGISTRY

        app = FastAPI()
        patch_openapi(app)
        spec = app.openapi()
        schemas = spec.get("components", {}).get("schemas", {})
        assert "PatchTestModel" in schemas

    def test_does_not_overwrite_existing_schemas(self) -> None:
        """Validate patch_openapi preserves pre-existing native schemas.

        Given: A FastAPI app with a route whose response model produces a
               schema name that collides with a registered json_body schema,
        When: patch_openapi is called and openapi() is invoked,
        Then: The native schema is kept, not overwritten by the registry.
        """

        class SharedName(BaseModel):
            native_field: str

        openapi_schema(SharedName)

        router = APIRouter()

        @router.get("/test", response_model=SharedName)
        async def _get() -> SharedName:
            return SharedName(native_field="x")

        app = FastAPI()
        app.include_router(router)
        patch_openapi(app)
        spec = app.openapi()
        schema = spec["components"]["schemas"]["SharedName"]
        assert "native_field" in schema["properties"]


class _FiniteBody(BaseModel):
    """Exercise Pydantic numeric errors through actual HTTP serialization."""

    model_config = ConfigDict(extra="forbid")
    quantity: float = Field(gt=0, allow_inf_nan=False)


def _validation_client(model: type[BaseModel], optional: bool) -> TestClient:
    """Build a real HTTP endpoint using either JSON dependency."""
    app = FastAPI()
    dependency = optional_json_body(model) if optional else json_body(model)

    @app.post("/", dependencies=[Depends(dependency)])
    async def submit() -> dict[str, bool]:
        """Confirm valid requests reach the handler."""
        return {"ok": True}

    return TestClient(app)


@pytest.mark.parametrize("optional", [False, True])
@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity"])
def test_nonfinite_input_returns_structured_422(optional: bool, value: str) -> None:
    """Rejected nonfinite numbers never reach JSONResponse as raw error input.

    Given: A nonfinite quantity in a JSON body,
    When: Either JSON dependency rejects the request,
    Then: The response is a structured 422 without raw error input.
    """
    with _validation_client(_FiniteBody, optional) as client:
        response = client.post("/", content='{"quantity": ' + value + "}")
    assert response.status_code == 422
    assert response.json()["detail"] == [
        {
            "type": "finite_number",
            "loc": ["body", "quantity"],
            "msg": "Input should be a finite number",
        }
    ]


@pytest.mark.parametrize("optional", [False, True])
@pytest.mark.parametrize("raw", [b'{"quantity": "\xff"}', b'{"quantity": "\\ud800"}'])
def test_invalid_encoding_returns_structured_422(optional: bool, raw: bytes) -> None:
    """Invalid UTF-8 and lone surrogate escapes refuse without decoding failures.

    Given: Invalid UTF-8 or a lone surrogate escape in a request,
    When: Either JSON dependency parses the body,
    Then: A JSON-invalid 422 response is returned without a decoding failure.
    """
    with _validation_client(_FiniteBody, optional) as client:
        response = client.post("/", content=raw)
    assert response.status_code == 422
    error = response.json()["detail"][0]
    assert error["type"] == "json_invalid"
    assert error["loc"] == ["body"]
    assert "input" not in error


@pytest.mark.parametrize("optional", [False, True])
def test_custom_error_does_not_reflect_sensitive_input(optional: bool) -> None:
    """Custom messages survive without raw input or exception objects in context.

    Given: Sensitive input rejected by a custom validator,
    When: The validation error becomes an HTTP response,
    Then: Its descriptive message survives without the input or exception object.
    """

    class SecretBody(BaseModel):
        credential: str

        @field_validator("credential")
        @classmethod
        def reject(cls, value: str) -> str:
            """Reject sensitive input with a descriptive, server-controlled message."""
            raise ValueError("Credential format is invalid")

    with _validation_client(SecretBody, optional) as client:
        response = client.post("/", json={"credential": "private-credential"})
    assert response.status_code == 422
    assert response.json()["detail"] == [
        {
            "type": "value_error",
            "loc": ["body", "credential"],
            "msg": "Value error, Credential format is invalid",
        }
    ]
    assert "private-credential" not in response.text
    assert len(response.content) < 300


@pytest.mark.parametrize("optional", [False, True])
def test_constraint_context_retains_finite_limits(optional: bool) -> None:
    """Useful ordinary error messages and finite constraint context survive.

    Given: A zero quantity violating a finite positive bound,
    When: The request fails validation,
    Then: The response preserves the message and numeric constraint.
    """
    with _validation_client(_FiniteBody, optional) as client:
        response = client.post("/", json={"quantity": 0})
    assert response.json()["detail"] == [
        {
            "type": "greater_than",
            "loc": ["body", "quantity"],
            "msg": "Input should be greater than 0",
            "ctx": {"gt": 0.0},
        }
    ]


@pytest.mark.parametrize("optional", [False, True])
def test_nonfinite_constraint_context_serializes(optional: bool) -> None:
    """Nonfinite constraint metadata is rendered as text without changing the error.

    Given: A model with an infinite lower bound,
    When: A finite quantity fails validation,
    Then: The 422 context represents infinity as JSON-safe text.
    """

    class InfiniteConstraint(BaseModel):
        quantity: float = Field(gt=float("inf"))

    with _validation_client(InfiniteConstraint, optional) as client:
        response = client.post("/", json={"quantity": 1})
    assert response.status_code == 422
    assert response.json()["detail"][0]["ctx"] == {"gt": "inf"}


@pytest.mark.parametrize("optional", [False, True])
def test_validation_metadata_preserves_json_primitives(optional: bool) -> None:
    """Constraint primitives survive while object reprs are excluded from responses.

    Given: A custom error containing primitives and sensitive object metadata,
    When: The validation error becomes an HTTP response,
    Then: Primitives survive while exception and object metadata are omitted.
    """

    class UnsafeMetadata:
        """Represent metadata that must never be reflected into the response."""

        def __repr__(self) -> str:
            """Return deliberately large sensitive metadata."""
            return "private-credential" * 10000

    class MetadataBody(BaseModel):
        quantity: float

        @field_validator("quantity")
        @classmethod
        def reject(cls, value: float) -> float:
            """Provide a realistic custom error with mixed metadata types."""
            raise PydanticCustomError(
                "quantity_constraint",
                "Quantity violates the constraint",
                {
                    "unit": "units",
                    "enabled": True,
                    "optional": None,
                    "minimum": 1,
                    "maximum": 2.5,
                    "nan": float("nan"),
                    "negative_infinity": float("-inf"),
                    "unsafe": UnsafeMetadata(),
                    "error": "private-credential",
                },
            )

    with _validation_client(MetadataBody, optional) as client:
        response = client.post("/", json={"quantity": 1})
    assert response.status_code == 422
    assert response.json()["detail"] == [
        {
            "type": "quantity_constraint",
            "loc": ["body", "quantity"],
            "msg": "Quantity violates the constraint",
            "ctx": {
                "unit": "units",
                "enabled": True,
                "optional": None,
                "minimum": 1,
                "maximum": 2.5,
                "nan": "nan",
                "negative_infinity": "-inf",
            },
        }
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("optional", [False, True])
async def test_rejected_body_is_not_attached(optional: bool) -> None:
    """The exception retains no decoded raw body or rejected input field.

    Given: A raw request containing an invalid sensitive value,
    When: Either JSON dependency raises a validation error,
    Then: The exception retains neither the body nor rejected input metadata.
    """
    dependency = optional_json_body(_FiniteBody) if optional else json_body(_FiniteBody)
    request = _StubRequest(b'{"quantity": "private-credential"}')
    with pytest.raises(RequestValidationError) as caught:
        await dependency(request)
    assert caught.value.body is None
    assert "private-credential" not in str(caught.value)
    assert "input" not in caught.value.errors()[0]
    assert "url" not in caught.value.errors()[0]


@pytest.mark.parametrize("optional", [False, True])
def test_nested_error_location_preserves_array_index(optional: bool) -> None:
    """Nested field names and numeric array indices retain their exact meaning.

    Given: An invalid quantity nested inside an order list,
    When: The request fails validation,
    Then: The error location preserves every field name and array index.
    """

    class NestedBody(BaseModel):
        orders: list[_FiniteBody]

    with _validation_client(NestedBody, optional) as client:
        response = client.post("/", json={"orders": [{"quantity": 0}]})
    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["body", "orders", 0, "quantity"]


@pytest.mark.parametrize("value, expected", [("żółw 🐢", "żółw 🐢"), ("bad\ud800", "bad?")])
def test_validation_text_is_utf8_safe(value: str, expected: str) -> None:
    """Text metadata preserves Unicode characters and replaces lone surrogates.

    Given: Valid Unicode text or a lone surrogate in metadata,
    When: The validation text normalizer processes it,
    Then: Valid characters survive and lone surrogates become safe replacements.
    """
    assert _validation_text(value) == expected
