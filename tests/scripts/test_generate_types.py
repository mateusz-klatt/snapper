"""Tests for scripts/generate_types.py."""

import json
import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from scripts.generate_types import ENTITY_EXCLUDE_FIELDS
from scripts.generate_types import ENTITY_UNION_ID_FIELDS
from scripts.generate_types import SWIFT_KEYWORD_RENAMES
from scripts.generate_types import camel_to_lower
from scripts.generate_types import derive_entity_name
from scripts.generate_types import discover_ws_schemas
from scripts.generate_types import export_openapi_schemas
from scripts.generate_types import export_openapi_spec
from scripts.generate_types import export_ws_schemas
from scripts.generate_types import extract_repeated_unions
from scripts.generate_types import fix_refs_openapi
from scripts.generate_types import fix_refs_pydantic
from scripts.generate_types import generate_entities
from scripts.generate_types import generate_entity_interface
from scripts.generate_types import generate_ios_types
from scripts.generate_types import generate_swift_enum
from scripts.generate_types import generate_swift_struct
from scripts.generate_types import generate_swift_types
from scripts.generate_types import generate_zod_api
from scripts.generate_types import generate_zod_object_schema
from scripts.generate_types import generate_zod_schema_definition
from scripts.generate_types import generate_zod_ws
from scripts.generate_types import get_any_codable_helper
from scripts.generate_types import json_type_to_swift
from scripts.generate_types import json_type_to_ts_entity
from scripts.generate_types import json_type_to_zod
from scripts.generate_types import main
from scripts.generate_types import make_const_fields_required
from scripts.generate_types import snake_to_camel
from scripts.generate_types import strip_primitive_titles
from scripts.generate_types import to_camel_case
from scripts.generate_types import topological_sort_schemas


class TestFixRefsPydantic:
    """Tests for fix_refs_pydantic function."""

    def test_fixes_defs_to_definitions(self) -> None:
        """Converts $defs to definitions."""
        obj: dict[str, Any] = {"$ref": "#/$defs/SomeType"}
        result = fix_refs_pydantic(obj)
        assert result == {"$ref": "#/definitions/SomeType"}

    def test_preserves_non_defs_refs(self) -> None:
        """Keeps non-$defs refs unchanged."""
        obj: dict[str, Any] = {"$ref": "#/other/path"}
        result = fix_refs_pydantic(obj)
        assert result == {"$ref": "#/other/path"}

    def test_processes_nested_dicts(self) -> None:
        """Processes nested dictionaries."""
        obj: dict[str, Any] = {"properties": {"field": {"$ref": "#/$defs/FieldType"}}}
        result = fix_refs_pydantic(obj)
        expected: dict[str, Any] = {"properties": {"field": {"$ref": "#/definitions/FieldType"}}}
        assert result == expected

    def test_processes_lists(self) -> None:
        """Processes list items."""
        obj: list[Any] = [{"$ref": "#/$defs/Type1"}, {"$ref": "#/$defs/Type2"}]
        result = fix_refs_pydantic(obj)
        assert result == [
            {"$ref": "#/definitions/Type1"},
            {"$ref": "#/definitions/Type2"},
        ]

    def test_returns_primitives_unchanged(self) -> None:
        """Returns primitives unchanged."""
        assert fix_refs_pydantic("string") == "string"
        assert fix_refs_pydantic(123) == 123
        assert fix_refs_pydantic(True) is True
        assert fix_refs_pydantic(None) is None


class TestFixRefsOpenapi:
    """Tests for fix_refs_openapi function."""

    def test_fixes_components_schemas_to_definitions(self) -> None:
        """Converts components/schemas to definitions."""
        obj: dict[str, Any] = {"$ref": "#/components/schemas/SomeType"}
        result = fix_refs_openapi(obj)
        assert result == {"$ref": "#/definitions/SomeType"}

    def test_preserves_other_refs(self) -> None:
        """Keeps non-components refs unchanged."""
        obj: dict[str, Any] = {"$ref": "#/other/path"}
        result = fix_refs_openapi(obj)
        assert result == {"$ref": "#/other/path"}

    def test_processes_nested_structures(self) -> None:
        """Processes nested dictionaries."""
        obj: dict[str, Any] = {"items": {"$ref": "#/components/schemas/Item"}}
        result = fix_refs_openapi(obj)
        assert result == {"items": {"$ref": "#/definitions/Item"}}

    def test_processes_lists(self) -> None:
        """Processes list items."""
        obj: list[Any] = [
            {"$ref": "#/components/schemas/A"},
            {"$ref": "#/components/schemas/B"},
        ]
        result = fix_refs_openapi(obj)
        assert result == [
            {"$ref": "#/definitions/A"},
            {"$ref": "#/definitions/B"},
        ]

    def test_returns_primitives_unchanged(self) -> None:
        """Returns primitives unchanged."""
        assert fix_refs_openapi("string") == "string"
        assert fix_refs_openapi(42) == 42


class TestMakeConstFieldsRequired:
    """Tests for make_const_fields_required function."""

    def test_adds_const_fields_to_required(self) -> None:
        """Adds const fields to required list."""
        schema: dict[str, Any] = {
            "properties": {
                "type": {"const": "event"},
                "data": {"type": "object"},
            },
            "required": ["data"],
        }
        result = make_const_fields_required(schema)
        required = result.get("required", [])
        assert isinstance(required, list)
        assert "type" in required
        assert "data" in required

    def test_handles_missing_required_field(self) -> None:
        """Creates required list if missing."""
        schema: dict[str, Any] = {
            "properties": {
                "type": {"const": "event"},
            }
        }
        result = make_const_fields_required(schema)
        required = result.get("required", [])
        assert isinstance(required, list)
        assert "type" in required

    def test_removes_default_from_const_fields(self) -> None:
        """Removes default from const fields."""
        schema: dict[str, Any] = {
            "properties": {
                "type": {"const": "event", "default": "event"},
            }
        }
        result = make_const_fields_required(schema)
        assert isinstance(result, dict)
        props = result.get("properties", {})
        assert isinstance(props, dict)
        type_schema = props.get("type", {})
        assert isinstance(type_schema, dict)
        assert "default" not in type_schema

    def test_returns_non_dict_unchanged(self) -> None:
        """Returns non-dict unchanged."""
        not_dict: Any = "not a dict"
        result = make_const_fields_required(not_dict)
        assert result == "not a dict"

    def test_processes_nested_definitions(self) -> None:
        """Processes nested definitions."""
        schema: dict[str, Any] = {
            "definitions": {
                "SubType": {
                    "properties": {
                        "kind": {"const": "sub"},
                    }
                }
            }
        }
        result = make_const_fields_required(schema)
        assert "definitions" in result

    def test_processes_lists_in_schema(self) -> None:
        """Processes lists containing objects."""
        schema: dict[str, Any] = {
            "oneOf": [
                {"properties": {"type": {"const": "a"}}},
                {"properties": {"type": {"const": "b"}}},
            ]
        }
        result = make_const_fields_required(schema)
        assert "oneOf" in result

    def test_handles_non_dict_property_schema(self) -> None:
        """Handles non-dict property schema."""
        schema: dict[str, Any] = {
            "properties": {
                "field": "not_a_dict",
            }
        }
        result = make_const_fields_required(schema)
        assert isinstance(result, dict)
        props = result["properties"]
        assert isinstance(props, dict)
        assert props["field"] == "not_a_dict"

    def test_const_field_already_in_required(self) -> None:
        """Does not duplicate const field in required."""
        schema: dict[str, Any] = {
            "properties": {
                "type": {"const": "event"},
            },
            "required": ["type"],
        }
        result = make_const_fields_required(schema)
        required = result.get("required", [])
        assert isinstance(required, list)
        assert required.count("type") == 1

    def test_required_not_a_list(self) -> None:
        """Handles required not being a list."""
        schema: dict[str, Any] = {
            "properties": {
                "type": {"const": "event"},
            },
            "required": "invalid",
        }
        result = make_const_fields_required(schema)
        assert result["required"] == "invalid"


class TestStripPrimitiveTitles:
    """Tests for strip_primitive_titles function."""

    def test_strips_title_from_bare_string(self) -> None:
        """Strips title from a property that is just a string."""
        schema: dict[str, Any] = {"title": "Instrument", "type": "string"}
        result = strip_primitive_titles(schema)
        assert isinstance(result, dict)
        assert "title" not in result
        assert result == {"type": "string"}

    def test_strips_title_from_bare_number(self) -> None:
        """Strips title from a property that is just a number."""
        schema: dict[str, Any] = {"title": "Open", "type": "number"}
        result = strip_primitive_titles(schema)
        assert isinstance(result, dict)
        assert "title" not in result
        assert result == {"type": "number"}

    def test_preserves_title_with_const(self) -> None:
        """Preserves title when const is present."""
        schema: dict[str, Any] = {"title": "Type", "type": "string", "const": "bar"}
        result = strip_primitive_titles(schema)
        assert isinstance(result, dict)
        assert result["title"] == "Type"

    def test_preserves_title_with_enum(self) -> None:
        """Preserves title when enum is present."""
        schema: dict[str, Any] = {"title": "Side", "type": "string", "enum": ["buy", "sell"]}
        result = strip_primitive_titles(schema)
        assert isinstance(result, dict)
        assert result["title"] == "Side"

    def test_preserves_title_on_object_type(self) -> None:
        """Does not strip title from object type definitions."""
        schema: dict[str, Any] = {
            "title": "BarEnvelope",
            "type": "object",
            "properties": {"open": {"title": "Open", "type": "number"}},
        }
        result = strip_primitive_titles(schema)
        assert isinstance(result, dict)
        assert result["title"] == "BarEnvelope"
        props = result["properties"]
        assert isinstance(props, dict)
        assert "title" not in props["open"]

    def test_recurses_into_lists(self) -> None:
        """Recurses into list items."""
        schema: list[Any] = [{"title": "X", "type": "string"}]
        result = strip_primitive_titles(schema)
        assert isinstance(result, list)
        assert "title" not in result[0]

    def test_passes_through_scalars(self) -> None:
        """Returns scalar values unchanged."""
        assert strip_primitive_titles("hello") == "hello"
        assert strip_primitive_titles(42) == 42
        assert strip_primitive_titles(None) is None

    def test_handles_non_string_type_value(self) -> None:
        """Handles type values that are not strings."""
        schema: dict[str, Any] = {"title": "Mixed", "type": {"nested": True}}
        result = strip_primitive_titles(schema)
        assert isinstance(result, dict)
        assert result["title"] == "Mixed"

    def test_strips_title_with_format(self) -> None:
        """Strips title from datetime properties since json2ts ignores format."""
        schema: dict[str, Any] = {
            "title": "Timestamp",
            "type": "string",
            "format": "date-time",
        }
        result = strip_primitive_titles(schema)
        assert isinstance(result, dict)
        assert "title" not in result
        assert result == {"type": "string", "format": "date-time"}

    def test_strips_title_with_min_max(self) -> None:
        """Strips title from properties with min/max constraints."""
        schema: dict[str, Any] = {
            "title": "Strength",
            "type": "number",
            "minimum": 0.0,
            "maximum": 1.0,
        }
        result = strip_primitive_titles(schema)
        assert isinstance(result, dict)
        assert "title" not in result


class TestToCamelCase:
    """Tests for to_camel_case function."""

    def test_converts_snake_case(self) -> None:
        """Converts snake_case to camelCase."""
        assert to_camel_case("hello_world") == "helloWorld"

    def test_handles_single_word(self) -> None:
        """Handles single word."""
        assert to_camel_case("hello") == "hello"

    def test_handles_multiple_underscores(self) -> None:
        """Handles multiple underscores."""
        assert to_camel_case("hello_world_foo_bar") == "helloWorldFooBar"


class TestSnakeToCamel:
    """Tests for snake_to_camel function."""

    def test_converts_snake_case(self) -> None:
        """Converts snake_case to camelCase."""
        assert snake_to_camel("order_id") == "orderId"

    def test_handles_single_word(self) -> None:
        """Handles single word."""
        assert snake_to_camel("id") == "id"


class TestCamelToLower:
    """Tests for camel_to_lower function."""

    def test_converts_to_lower_camel(self) -> None:
        """Converts CamelCase to lowerCamelCase."""
        assert camel_to_lower("OrderCreate") == "orderCreate"

    def test_handles_empty_string(self) -> None:
        """Handles empty string."""
        assert camel_to_lower("") == ""


class TestJsonTypeToSwift:
    """Tests for json_type_to_swift function."""

    def test_handles_ref(self) -> None:
        """Handles $ref."""
        prop = {"$ref": "#/definitions/SomeType"}
        result = json_type_to_swift(prop, {})
        assert result == "SomeType?"

    def test_handles_anyof_single_non_null(self) -> None:
        """Handles anyOf with single non-null type."""
        prop = {"anyOf": [{"type": "string"}, {"type": "null"}]}
        result = json_type_to_swift(prop, {})
        assert result == "String?"

    def test_handles_anyof_multiple_types(self) -> None:
        """Handles anyOf with multiple types -> AnyCodable."""
        prop = {"anyOf": [{"type": "string"}, {"type": "integer"}]}
        result = json_type_to_swift(prop, {})
        assert result == "AnyCodable?"

    def test_handles_allof(self) -> None:
        """Handles allOf."""
        prop = {"allOf": [{"type": "string"}]}
        result = json_type_to_swift(prop, {})
        assert result == "String?"

    def test_handles_datetime(self) -> None:
        """Handles date-time format."""
        prop = {"type": "string", "format": "date-time"}
        result = json_type_to_swift(prop, {})
        assert result == "Date?"

    def test_handles_string(self) -> None:
        """Handles string type."""
        prop = {"type": "string"}
        result = json_type_to_swift(prop, {})
        assert result == "String?"

    def test_handles_integer(self) -> None:
        """Handles integer type."""
        prop = {"type": "integer"}
        result = json_type_to_swift(prop, {})
        assert result == "Int?"

    def test_handles_number(self) -> None:
        """Handles number type."""
        prop = {"type": "number"}
        result = json_type_to_swift(prop, {})
        assert result == "Double?"

    def test_handles_boolean(self) -> None:
        """Handles boolean type."""
        prop = {"type": "boolean"}
        result = json_type_to_swift(prop, {})
        assert result == "Bool?"

    def test_handles_array(self) -> None:
        """Handles array type."""
        prop = {"type": "array", "items": {"type": "string"}}
        result = json_type_to_swift(prop, {})
        assert result == "[String]?"

    def test_handles_object_with_additional_properties(self) -> None:
        """Handles object with additionalProperties."""
        prop = {"type": "object", "additionalProperties": {"type": "string"}}
        result = json_type_to_swift(prop, {})
        assert result == "[String: String]?"

    def test_handles_object_with_true_additional(self) -> None:
        """Handles object with additionalProperties=true."""
        prop = {"type": "object", "additionalProperties": True}
        result = json_type_to_swift(prop, {})
        assert result == "[String: AnyCodable]?"

    def test_handles_plain_object(self) -> None:
        """Handles plain object type."""
        prop = {"type": "object"}
        result = json_type_to_swift(prop, {})
        assert result == "[String: AnyCodable]?"

    def test_handles_unknown_type(self) -> None:
        """Handles unknown type."""
        prop = {"type": "unknown"}
        result = json_type_to_swift(prop, {})
        assert result == "AnyCodable?"

    def test_non_optional(self) -> None:
        """Handles required fields."""
        prop = {"type": "string"}
        result = json_type_to_swift(prop, {}, optional=False)
        assert result == "String"


class TestGenerateSwiftStruct:
    """Tests for generate_swift_struct function."""

    def test_generates_struct(self) -> None:
        """Generates Swift struct."""
        schema = {
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
            },
            "required": ["id"],
        }
        lines = generate_swift_struct("User", schema, {})
        result = "\n".join(lines)
        assert "struct User: Codable, Sendable" in result
        assert "let id: Int" in result
        assert "let name: String?" in result

    def test_generates_coding_keys(self) -> None:
        """Generates CodingKeys for snake_case fields."""
        schema = {
            "properties": {
                "user_id": {"type": "integer"},
            },
            "required": ["user_id"],
        }
        lines = generate_swift_struct("User", schema, {})
        result = "\n".join(lines)
        assert "CodingKeys" in result
        assert 'case userId = "user_id"' in result

    def test_includes_description(self) -> None:
        """Includes description as doc comment."""
        schema = {
            "properties": {
                "id": {"type": "integer", "description": "The user ID"},
            },
        }
        lines = generate_swift_struct("User", schema, {})
        result = "\n".join(lines)
        assert "/// The user ID" in result

    def test_no_coding_keys_when_camelcase_matches(self) -> None:
        """No CodingKeys when property names match camelCase."""
        schema = {
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
            },
            "required": ["id", "name"],
        }
        lines = generate_swift_struct("User", schema, {})
        result = "\n".join(lines)
        assert "CodingKeys" not in result

    def test_coding_keys_with_mixed_names(self) -> None:
        """CodingKeys with both snake_case and camelCase names."""
        schema = {
            "properties": {
                "id": {"type": "integer"},
                "user_name": {"type": "string"},
            },
            "required": ["id", "user_name"],
        }
        lines = generate_swift_struct("User", schema, {})
        result = "\n".join(lines)
        assert "CodingKeys" in result
        assert "case id" in result
        assert 'case userName = "user_name"' in result


class TestGenerateSwiftEnum:
    """Tests for generate_swift_enum function."""

    def test_generates_enum(self) -> None:
        """Generates Swift enum."""
        lines = generate_swift_enum("Status", ["active", "inactive"])
        result = "\n".join(lines)
        assert "enum Status: String, Codable, Sendable" in result
        assert "case active" in result
        assert "case inactive" in result

    def test_handles_snake_case_values(self) -> None:
        """Handles snake_case enum values."""
        lines = generate_swift_enum("Status", ["in_progress"])
        result = "\n".join(lines)
        assert 'case inProgress = "in_progress"' in result

    def test_renames_swift_keywords(self) -> None:
        """Renames Swift keywords to safe identifiers with raw values."""
        lines = generate_swift_enum("Type", ["class", "default"])
        result = "\n".join(lines)
        assert 'case classValue = "class"' in result
        assert 'case defaultValue = "default"' in result


class TestGetAnyCodableHelper:
    """Tests for get_any_codable_helper function."""

    def test_returns_helper_code(self) -> None:
        """Returns AnyCodable helper code."""
        lines = get_any_codable_helper()
        result = "\n".join(lines)
        assert "struct AnyCodable" in result
        assert "Codable, Sendable" in result


class TestGenerateSwiftTypes:
    """Tests for generate_swift_types function."""

    def test_generates_swift_file(self, tmp_path: Path) -> None:
        """Generates Swift types file."""
        schema_path = tmp_path / "schema.json"
        schema_path.write_text(
            json.dumps(
                {
                    "definitions": {
                        "Status": {"type": "string", "enum": ["active"]},
                        "User": {
                            "type": "object",
                            "properties": {"id": {"type": "integer"}},
                        },
                    }
                }
            )
        )
        output_path = tmp_path / "Types.swift"

        generate_swift_types(tmp_path, schema_path, output_path)

        content = output_path.read_text()
        assert "enum Status" in content
        assert "struct User" in content

    def test_generates_nested_enum(self, tmp_path: Path) -> None:
        """Generates enum from object property."""
        schema_path = tmp_path / "schema.json"
        schema_path.write_text(
            json.dumps(
                {
                    "definitions": {
                        "Order": {
                            "type": "object",
                            "properties": {
                                "status": {"type": "string", "enum": ["pending", "done"]}
                            },
                        },
                    }
                }
            )
        )
        output_path = tmp_path / "Types.swift"

        generate_swift_types(tmp_path, schema_path, output_path)

        content = output_path.read_text()
        assert "enum OrderStatus" in content

    def test_skips_duplicate_nested_enum(self, tmp_path: Path) -> None:
        """Skips duplicate nested enum generation."""
        schema_path = tmp_path / "schema.json"
        schema_path.write_text(
            json.dumps(
                {
                    "definitions": {
                        "OrderStatus": {"type": "string", "enum": ["pending", "done"]},
                        "Order": {
                            "type": "object",
                            "properties": {
                                "status": {"type": "string", "enum": ["pending", "done"]}
                            },
                        },
                    }
                }
            )
        )
        output_path = tmp_path / "Types.swift"

        generate_swift_types(tmp_path, schema_path, output_path)

        content = output_path.read_text()
        assert content.count("enum OrderStatus") == 1


class TestGenerateIosTypes:
    """Tests for generate_ios_types function."""

    def test_generates_ios_files(self, tmp_path: Path) -> None:
        """Generates iOS type files."""
        build_dir = tmp_path / "build"
        build_dir.mkdir()
        (build_dir / "ws-schemas.json").write_text(json.dumps({"definitions": {}}))
        (build_dir / "openapi-schemas.json").write_text(json.dumps({"definitions": {}}))

        ios_dir = tmp_path / "ios" / "Snapper" / "Models" / "Generated"
        ios_dir.mkdir(parents=True)

        generate_ios_types(tmp_path)

        assert (ios_dir / "WSMessages.swift").exists()
        assert (ios_dir / "APITypes.swift").exists()

    def test_skips_missing_schema_files(self, tmp_path: Path) -> None:
        """Skips if schema files do not exist."""
        ios_dir = tmp_path / "ios" / "Snapper" / "Models" / "Generated"
        ios_dir.mkdir(parents=True)

        generate_ios_types(tmp_path)

        assert not (ios_dir / "WSMessages.swift").exists()
        assert not (ios_dir / "APITypes.swift").exists()

    def test_generates_only_ws_when_api_missing(self, tmp_path: Path) -> None:
        """Generates only WS types when API schema missing."""
        build_dir = tmp_path / "build"
        build_dir.mkdir()
        (build_dir / "ws-schemas.json").write_text(json.dumps({"definitions": {}}))

        ios_dir = tmp_path / "ios" / "Snapper" / "Models" / "Generated"
        ios_dir.mkdir(parents=True)

        generate_ios_types(tmp_path)

        assert (ios_dir / "WSMessages.swift").exists()
        assert not (ios_dir / "APITypes.swift").exists()


class TestJsonTypeToZod:
    """Tests for json_type_to_zod function."""

    def test_handles_ref(self) -> None:
        """Handles $ref."""
        prop = {"$ref": "#/definitions/SomeType"}
        result = json_type_to_zod(prop, True, {})
        assert result == "SomeTypeSchema"

    def test_handles_anyof_nullable(self) -> None:
        """Handles anyOf with null."""
        prop = {"anyOf": [{"type": "string"}, {"type": "null"}]}
        result = json_type_to_zod(prop, True, {})
        assert ".nullable()" in result

    def test_handles_anyof_union(self) -> None:
        """Handles anyOf union."""
        prop = {"anyOf": [{"type": "string"}, {"type": "integer"}]}
        result = json_type_to_zod(prop, True, {})
        assert "z.union" in result

    def test_handles_anyof_nullable_union(self) -> None:
        """Handles anyOf union with null."""
        prop = {"anyOf": [{"type": "string"}, {"type": "integer"}, {"type": "null"}]}
        result = json_type_to_zod(prop, True, {})
        assert "z.union" in result
        assert ".nullable()" in result

    def test_handles_allof(self) -> None:
        """Handles allOf."""
        prop = {"allOf": [{"type": "string"}]}
        result = json_type_to_zod(prop, True, {})
        assert result == "z.string()"

    def test_handles_const(self) -> None:
        """Handles const."""
        prop = {"type": "string", "const": "event"}
        result = json_type_to_zod(prop, True, {})
        assert result == "z.literal('event')"

    def test_handles_datetime(self) -> None:
        """Handles date-time format."""
        prop = {"type": "string", "format": "date-time"}
        result = json_type_to_zod(prop, True, {})
        assert result == "z.iso.datetime()"

    def test_handles_uuid(self) -> None:
        """Handles uuid format."""
        prop = {"type": "string", "format": "uuid"}
        result = json_type_to_zod(prop, True, {})
        assert result == "z.string().uuid()"

    def test_handles_email(self) -> None:
        """Handles email format."""
        prop = {"type": "string", "format": "email"}
        result = json_type_to_zod(prop, True, {})
        assert result == "z.string().email()"

    def test_handles_enum(self) -> None:
        """Handles enum."""
        prop = {"type": "string", "enum": ["a", "b"]}
        result = json_type_to_zod(prop, True, {})
        assert result == "z.enum(['a', 'b'])"

    def test_handles_string_with_length(self) -> None:
        """Handles string with min/max length."""
        prop = {"type": "string", "minLength": 1, "maxLength": 10}
        result = json_type_to_zod(prop, True, {})
        assert ".min(1)" in result
        assert ".max(10)" in result

    def test_handles_string_with_only_min(self) -> None:
        """Handles string with only minLength."""
        prop = {"type": "string", "minLength": 1}
        result = json_type_to_zod(prop, True, {})
        assert ".min(1)" in result
        assert ".max(" not in result

    def test_handles_string_with_only_max(self) -> None:
        """Handles string with only maxLength."""
        prop = {"type": "string", "maxLength": 10}
        result = json_type_to_zod(prop, True, {})
        assert ".min(" not in result
        assert ".max(10)" in result

    def test_handles_integer(self) -> None:
        """Handles integer type."""
        prop = {"type": "integer"}
        result = json_type_to_zod(prop, True, {})
        assert result == "z.number().int()"

    def test_handles_number(self) -> None:
        """Handles number type."""
        prop = {"type": "number"}
        result = json_type_to_zod(prop, True, {})
        assert result == "z.number()"

    def test_handles_boolean(self) -> None:
        """Handles boolean type."""
        prop = {"type": "boolean"}
        result = json_type_to_zod(prop, True, {})
        assert result == "z.boolean()"

    def test_handles_null(self) -> None:
        """Handles null type."""
        prop = {"type": "null"}
        result = json_type_to_zod(prop, True, {})
        assert result == "z.null()"

    def test_handles_array(self) -> None:
        """Handles array type."""
        prop = {"type": "array", "items": {"type": "string"}}
        result = json_type_to_zod(prop, True, {})
        assert result == "z.array(z.string())"

    def test_handles_object_with_properties(self) -> None:
        """Handles object with properties."""
        prop = {"type": "object", "properties": {"id": {"type": "integer"}}}
        result = json_type_to_zod(prop, True, {})
        assert "z.object" in result

    def test_handles_record_with_unknown(self) -> None:
        """Handles object with additionalProperties=true."""
        prop = {"type": "object", "additionalProperties": True}
        result = json_type_to_zod(prop, True, {})
        assert result == "z.record(z.string(), z.unknown())"

    def test_handles_record_with_type(self) -> None:
        """Handles object with additionalProperties type."""
        prop = {"type": "object", "additionalProperties": {"type": "string"}}
        result = json_type_to_zod(prop, True, {})
        assert result == "z.record(z.string(), z.string())"

    def test_handles_plain_object(self) -> None:
        """Handles plain object type."""
        prop = {"type": "object"}
        result = json_type_to_zod(prop, True, {})
        assert result == "z.object({}).passthrough()"

    def test_handles_unknown_type(self) -> None:
        """Handles unknown type."""
        prop: dict[str, Any] = {}
        result = json_type_to_zod(prop, True, {})
        assert result == "z.unknown()"

    def test_handles_anyof_single_non_null_only(self) -> None:
        """Handles anyOf with single non-null without null present."""
        prop = {"anyOf": [{"type": "string"}]}
        result = json_type_to_zod(prop, True, {})
        assert result == "z.string()"


class TestGenerateZodObjectSchema:
    """Tests for generate_zod_object_schema function."""

    def test_generates_object_schema(self) -> None:
        """Generates Zod object schema."""
        schema = {
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
            },
            "required": ["id"],
        }
        result = generate_zod_object_schema(schema, {})
        assert "id: z.number().int()," in result
        assert "name: z.string().optional()," in result
        assert ".strict()" in result

    def test_handles_default_as_required(self) -> None:
        """Treats fields with default as required."""
        schema = {
            "properties": {
                "count": {"type": "integer", "default": 0},
            },
        }
        result = generate_zod_object_schema(schema, {})
        assert ".optional()" not in result or "count: z.number().int()," in result


class TestGenerateZodSchemaDefinition:
    """Tests for generate_zod_schema_definition function."""

    def test_generates_enum_schema(self) -> None:
        """Generates enum schema."""
        schema = {"type": "string", "enum": ["a", "b"]}
        result = generate_zod_schema_definition("Status", schema, {})
        assert "export const StatusSchema = z.enum(['a', 'b'])" in result

    def test_generates_object_schema(self) -> None:
        """Generates object schema."""
        schema = {"type": "object", "properties": {"id": {"type": "integer"}}}
        result = generate_zod_schema_definition("User", schema, {})
        assert "export const UserSchema" in result

    def test_generates_array_schema(self) -> None:
        """Generates array schema."""
        schema = {"type": "array", "items": {"type": "string"}}
        result = generate_zod_schema_definition("Names", schema, {})
        assert "export const NamesSchema = z.array(z.string())" in result

    def test_generates_fallback_for_other_types(self) -> None:
        """Generates fallback for non-object/array types."""
        schema = {"type": "string"}
        result = generate_zod_schema_definition("Name", schema, {})
        assert "export const NameSchema = z.string()" in result


class TestTopologicalSortSchemas:
    """Tests for topological_sort_schemas function."""

    def test_sorts_with_dependencies(self) -> None:
        """Sorts schemas with dependencies."""
        schemas = {
            "B": {"$ref": "#/definitions/A"},
            "A": {"type": "string"},
            "C": {"$ref": "#/definitions/B"},
        }
        result = topological_sort_schemas(schemas, "#/definitions/")
        assert result.index("A") < result.index("B")
        assert result.index("B") < result.index("C")

    def test_handles_circular_dependencies(self) -> None:
        """Handles circular dependencies."""
        schemas = {
            "A": {"$ref": "#/definitions/B"},
            "B": {"$ref": "#/definitions/A"},
        }
        result = topological_sort_schemas(schemas, "#/definitions/")
        assert set(result) == {"A", "B"}

    def test_handles_diamond_dependencies(self) -> None:
        """Handles diamond dependency pattern."""
        schemas = {
            "D": {"allOf": [{"$ref": "#/definitions/B"}, {"$ref": "#/definitions/C"}]},
            "B": {"$ref": "#/definitions/A"},
            "C": {"$ref": "#/definitions/A"},
            "A": {"type": "string"},
        }
        result = topological_sort_schemas(schemas, "#/definitions/")
        assert result.index("A") < result.index("B")
        assert result.index("A") < result.index("C")
        assert result.index("B") < result.index("D")
        assert result.index("C") < result.index("D")

    def test_handles_no_dependencies(self) -> None:
        """Handles schemas with no dependencies."""
        schemas = {
            "A": {"type": "string"},
            "B": {"type": "integer"},
            "C": {"type": "boolean"},
        }
        result = topological_sort_schemas(schemas, "#/definitions/")
        assert set(result) == {"A", "B", "C"}


class TestGenerateZodWs:
    """Tests for generate_zod_ws function."""

    def test_generates_ws_schemas(self, tmp_path: Path, capsys: Any) -> None:
        """Generates WebSocket Zod schemas."""
        build_dir = tmp_path / "build"
        build_dir.mkdir()
        (build_dir / "ws-schemas.json").write_text(
            json.dumps(
                {
                    "definitions": {
                        "Message": {"type": "object", "properties": {"id": {"type": "integer"}}},
                    }
                }
            )
        )
        frontend_dir = tmp_path / "frontend" / "src" / "lib" / "schemas"
        frontend_dir.mkdir(parents=True)

        generate_zod_ws(tmp_path)

        output = frontend_dir / "ws.generated.zod.ts"
        assert output.exists()
        content = output.read_text()
        assert "MessageSchema" in content

    def test_prints_error_if_schema_not_found(self, tmp_path: Path, capsys: Any) -> None:
        """Prints error if schema file not found."""
        generate_zod_ws(tmp_path)
        captured = capsys.readouterr()
        assert "not found" in captured.out

    def test_prints_error_if_no_definitions(self, tmp_path: Path, capsys: Any) -> None:
        """Prints error if no definitions."""
        build_dir = tmp_path / "build"
        build_dir.mkdir()
        (build_dir / "ws-schemas.json").write_text(json.dumps({}))

        generate_zod_ws(tmp_path)
        captured = capsys.readouterr()
        assert "No definitions" in captured.out


class TestGenerateZodApi:
    """Tests for generate_zod_api function."""

    def test_generates_api_schemas(self, tmp_path: Path, capsys: Any) -> None:
        """Generates API Zod schemas."""
        build_dir = tmp_path / "build"
        build_dir.mkdir()
        (build_dir / "openapi.json").write_text(
            json.dumps(
                {
                    "components": {
                        "schemas": {
                            "User": {"type": "object", "properties": {"id": {"type": "integer"}}},
                        }
                    }
                }
            )
        )
        frontend_dir = tmp_path / "frontend" / "src" / "lib" / "schemas"
        frontend_dir.mkdir(parents=True)

        generate_zod_api(tmp_path)

        output = frontend_dir / "api.generated.zod.ts"
        assert output.exists()
        content = output.read_text()
        assert "UserSchema" in content
        assert "export type User" in content

    def test_prints_error_if_openapi_not_found(self, tmp_path: Path, capsys: Any) -> None:
        """Prints error if openapi file not found."""
        generate_zod_api(tmp_path)
        captured = capsys.readouterr()
        assert "not found" in captured.out

    def test_prints_error_if_no_schemas(self, tmp_path: Path, capsys: Any) -> None:
        """Prints error if no schemas."""
        build_dir = tmp_path / "build"
        build_dir.mkdir()
        (build_dir / "openapi.json").write_text(json.dumps({"components": {}}))

        generate_zod_api(tmp_path)
        captured = capsys.readouterr()
        assert "No schemas" in captured.out


class TestJsonTypeToTsEntity:
    """Tests for json_type_to_ts_entity function."""

    def test_handles_ref(self) -> None:
        """Handles $ref by resolving schema."""
        all_schemas = {"User": {"type": "string"}}
        prop = {"$ref": "#/definitions/User"}
        result = json_type_to_ts_entity(prop, "user", True, all_schemas)
        assert result == "string"

    def test_handles_anyof_nullable(self) -> None:
        """Handles anyOf with null."""
        prop = {"anyOf": [{"type": "string"}, {"type": "null"}]}
        result = json_type_to_ts_entity(prop, "field", True)
        assert result == "string | null"

    def test_handles_anyof_union(self) -> None:
        """Handles anyOf union."""
        prop = {"anyOf": [{"type": "string"}, {"type": "integer"}]}
        result = json_type_to_ts_entity(prop, "field", True)
        assert "string" in result
        assert "number" in result

    def test_handles_anyof_union_with_null(self) -> None:
        """Handles anyOf union with null."""
        prop = {"anyOf": [{"type": "string"}, {"type": "integer"}, {"type": "null"}]}
        result = json_type_to_ts_entity(prop, "field", True)
        assert "string" in result
        assert "number" in result
        assert "null" in result

    def test_handles_anyof_single_non_null_only(self) -> None:
        """Handles anyOf with single non-null without null present."""
        prop = {"anyOf": [{"type": "string"}]}
        result = json_type_to_ts_entity(prop, "field", True)
        assert result == "string"

    def test_handles_allof(self) -> None:
        """Handles allOf."""
        prop = {"allOf": [{"type": "string"}]}
        result = json_type_to_ts_entity(prop, "field", True)
        assert result == "string"

    def test_handles_datetime(self) -> None:
        """Handles date-time as Date."""
        prop = {"type": "string", "format": "date-time"}
        result = json_type_to_ts_entity(prop, "field", True)
        assert result == "Date"

    def test_handles_id_fields(self) -> None:
        """Handles ID fields as union type."""
        prop = {"type": "string"}
        result = json_type_to_ts_entity(prop, "id", True)
        assert result == "string | number"

    def test_handles_order_id_fields(self) -> None:
        """Handles order_id fields as union type."""
        prop = {"type": "string"}
        result = json_type_to_ts_entity(prop, "order_id", True)
        assert result == "string | number"

    def test_handles_enum(self) -> None:
        """Handles enum as literal union."""
        prop = {"type": "string", "enum": ["a", "b"]}
        result = json_type_to_ts_entity(prop, "field", True)
        assert "'a'" in result
        assert "'b'" in result

    def test_handles_integer_and_number(self) -> None:
        """Handles integer and number as number."""
        assert json_type_to_ts_entity({"type": "integer"}, "f", True) == "number"
        assert json_type_to_ts_entity({"type": "number"}, "f", True) == "number"

    def test_handles_boolean(self) -> None:
        """Handles boolean."""
        assert json_type_to_ts_entity({"type": "boolean"}, "f", True) == "boolean"

    def test_handles_array(self) -> None:
        """Handles array."""
        prop = {"type": "array", "items": {"type": "string"}}
        result = json_type_to_ts_entity(prop, "field", True)
        assert result == "string[]"

    def test_handles_object_with_additional_properties(self) -> None:
        """Handles object with additionalProperties."""
        prop = {"type": "object", "additionalProperties": {"type": "string"}}
        result = json_type_to_ts_entity(prop, "field", True)
        assert result == "Record<string, string>"

    def test_handles_plain_object(self) -> None:
        """Handles plain object."""
        prop = {"type": "object"}
        result = json_type_to_ts_entity(prop, "field", True)
        assert result == "Record<string, unknown>"

    def test_handles_null(self) -> None:
        """Handles null type."""
        assert json_type_to_ts_entity({"type": "null"}, "f", True) == "null"

    def test_handles_unknown(self) -> None:
        """Handles unknown type."""
        assert json_type_to_ts_entity({}, "f", True) == "unknown"

    def test_handles_object_with_additional_properties_true(self) -> None:
        """Handles object with additionalProperties=true."""
        prop = {"type": "object", "additionalProperties": True}
        result = json_type_to_ts_entity(prop, "field", True)
        assert result == "Record<string, unknown>"


class TestGenerateEntityInterface:
    """Tests for generate_entity_interface function."""

    def test_generates_interface(self) -> None:
        """Generates TypeScript interface."""
        schema = {
            "properties": {
                "user_id": {"type": "integer"},
                "name": {"type": "string"},
            },
            "required": ["user_id"],
        }
        lines = generate_entity_interface("User", schema)
        result = "\n".join(lines)
        assert "export interface User" in result
        assert "userId: number" in result
        assert "name?: string" in result

    def test_includes_doc_comment(self) -> None:
        """Includes doc comment."""
        schema: dict[str, Any] = {"properties": {}}
        lines = generate_entity_interface("User", schema, "Test doc")
        result = "\n".join(lines)
        assert "/**" in result
        assert "Test doc" in result

    def test_excludes_entity_fields(self) -> None:
        """Excludes type and meta fields."""
        schema = {
            "properties": {
                "type": {"const": "event"},
                "meta": {"type": "object"},
                "data": {"type": "string"},
            },
            "required": ["type", "meta", "data"],
        }
        lines = generate_entity_interface("Event", schema)
        result = "\n".join(lines)
        assert "type:" not in result
        assert "meta:" not in result
        assert "data:" in result


class TestDeriveEntityName:
    """Tests for derive_entity_name function."""

    def test_removes_envelope_suffix(self) -> None:
        """Removes Envelope suffix."""
        assert derive_entity_name("OrderEnvelope", "Envelope") == "Order"

    def test_removes_snapshot_suffix(self) -> None:
        """Removes Snapshot suffix."""
        assert derive_entity_name("UserSnapshot", "Snapshot") == "User"


class TestGenerateEntities:
    """Tests for generate_entities function."""

    def test_generates_entities_file(self, tmp_path: Path, capsys: Any) -> None:
        """Generates entities file."""
        build_dir = tmp_path / "build"
        build_dir.mkdir()
        (build_dir / "ws-schemas.json").write_text(
            json.dumps(
                {
                    "definitions": {
                        "OrderEnvelope": {
                            "type": "object",
                            "properties": {"order_id": {"type": "string"}},
                            "required": ["order_id"],
                        },
                        "SomeOtherSchema": {
                            "type": "object",
                            "properties": {"field": {"type": "string"}},
                        },
                    }
                }
            )
        )
        (build_dir / "openapi.json").write_text(
            json.dumps(
                {
                    "components": {
                        "schemas": {
                            "UserSnapshot": {
                                "type": "object",
                                "properties": {"id": {"type": "integer"}},
                            },
                            "CreateRequest": {
                                "type": "object",
                                "properties": {"name": {"type": "string"}},
                            },
                            "SomethingElse": {
                                "type": "object",
                                "properties": {"data": {"type": "string"}},
                            },
                        }
                    }
                }
            )
        )
        frontend_dir = tmp_path / "frontend" / "src" / "types"
        frontend_dir.mkdir(parents=True)

        generate_entities(tmp_path)

        output = frontend_dir / "entities.generated.ts"
        assert output.exists()
        content = output.read_text()
        assert "interface Order" in content
        assert "interface User" in content
        assert "interface Create" in content

    def test_prints_error_if_ws_schemas_not_found(self, tmp_path: Path, capsys: Any) -> None:
        """Prints error if ws-schemas not found."""
        generate_entities(tmp_path)
        captured = capsys.readouterr()
        assert "not found" in captured.out

    def test_prints_error_if_openapi_not_found(self, tmp_path: Path, capsys: Any) -> None:
        """Prints error if openapi not found."""
        build_dir = tmp_path / "build"
        build_dir.mkdir()
        (build_dir / "ws-schemas.json").write_text(json.dumps({"definitions": {}}))

        generate_entities(tmp_path)
        captured = capsys.readouterr()
        assert "not found" in captured.out

    def test_skips_duplicate_entities(self, tmp_path: Path, capsys: Any) -> None:
        """Skips duplicate entity generation (already from WS)."""
        build_dir = tmp_path / "build"
        build_dir.mkdir()
        (build_dir / "ws-schemas.json").write_text(
            json.dumps(
                {
                    "definitions": {
                        "OrderEnvelope": {
                            "type": "object",
                            "properties": {"order_id": {"type": "string"}},
                        },
                    }
                }
            )
        )
        (build_dir / "openapi.json").write_text(
            json.dumps(
                {
                    "components": {
                        "schemas": {
                            "OrderSnapshot": {
                                "type": "object",
                                "properties": {"id": {"type": "integer"}},
                            },
                        }
                    }
                }
            )
        )
        frontend_dir = tmp_path / "frontend" / "src" / "types"
        frontend_dir.mkdir(parents=True)

        generate_entities(tmp_path)

        output = frontend_dir / "entities.generated.ts"
        content = output.read_text()
        assert content.count("export interface Order") == 1


class TestDiscoverWsSchemas:
    """Tests for discover_ws_schemas function."""

    def test_discovers_schemas(self) -> None:
        """Discovers WS schemas from modules."""
        schemas = discover_ws_schemas()
        names = [name for name, _ in schemas]
        assert "WsMessageBase" in names
        assert any("Envelope" in name for name in names)


class TestExportWsSchemas:
    """Tests for export_ws_schemas function."""

    def test_exports_schemas(self, tmp_path: Path, capsys: Any) -> None:
        """Exports WS schemas to JSON."""
        with (
            patch("scripts.generate_types.discover_ws_schemas") as mock_discover,
            patch("scripts.generate_types.WsMessageSchema"),
        ):
            mock_model = MagicMock()
            mock_model.model_json_schema.return_value = {
                "type": "object",
                "properties": {"type": {"const": "test"}},
            }
            mock_discover.return_value = [("TestMessage", mock_model)]

            result = export_ws_schemas(tmp_path)

            assert result.exists()
            with result.open() as f:
                data = json.load(f)
            assert "definitions" in data

    def test_exports_schemas_with_defs(self, tmp_path: Path, capsys: Any) -> None:
        """Exports WS schemas with $defs to JSON."""
        with (
            patch("scripts.generate_types.discover_ws_schemas") as mock_discover,
            patch("scripts.generate_types.WsMessageSchema"),
        ):
            mock_model = MagicMock()
            mock_model.model_json_schema.return_value = {
                "type": "object",
                "properties": {"data": {"$ref": "#/$defs/NestedType"}},
                "$defs": {
                    "NestedType": {"type": "object", "properties": {"id": {"type": "integer"}}}
                },
            }
            mock_discover.return_value = [("TestMessage", mock_model)]

            result = export_ws_schemas(tmp_path)

            assert result.exists()
            with result.open() as f:
                data = json.load(f)
            assert "definitions" in data
            assert "NestedType" in data["definitions"]

    def test_exports_schemas_with_non_dict_defs(self, tmp_path: Path, capsys: Any) -> None:
        """Exports WS schemas with non-dict $defs value."""
        with (
            patch("scripts.generate_types.discover_ws_schemas") as mock_discover,
            patch("scripts.generate_types.WsMessageSchema"),
        ):
            mock_model = MagicMock()
            mock_model.model_json_schema.return_value = {
                "type": "object",
                "properties": {"data": {"type": "string"}},
                "$defs": "not_a_dict",
            }
            mock_discover.return_value = [("TestMessage", mock_model)]

            result = export_ws_schemas(tmp_path)

            assert result.exists()
            with result.open() as f:
                data = json.load(f)
            assert "definitions" in data

    def test_handles_non_dict_schema_fixed(self, tmp_path: Path, capsys: Any) -> None:
        """Handles non-dict schema_fixed (unlikely but covered)."""
        with (
            patch("scripts.generate_types.discover_ws_schemas") as mock_discover,
            patch("scripts.generate_types.WsMessageSchema"),
            patch("scripts.generate_types.fix_refs_pydantic") as mock_fix,
        ):
            mock_model = MagicMock()
            mock_model.model_json_schema.return_value = {
                "type": "object",
                "properties": {},
            }
            mock_fix.return_value = "not_a_dict"
            mock_discover.return_value = [("TestMessage", mock_model)]

            result = export_ws_schemas(tmp_path)
            assert result.exists()


class TestExportOpenapiSpec:
    """Tests for export_openapi_spec function."""

    def test_exports_openapi(self, tmp_path: Path, capsys: Any) -> None:
        """Exports OpenAPI spec."""
        with patch("scripts.generate_types.create_app") as mock_create:
            mock_app = MagicMock()
            mock_app.openapi.return_value = {"openapi": "3.0.0", "paths": {}}
            mock_create.return_value = mock_app

            result = export_openapi_spec(tmp_path)

            assert result.exists()


class TestExportOpenapiSchemas:
    """Tests for export_openapi_schemas function."""

    def test_exports_schemas(self, tmp_path: Path, capsys: Any) -> None:
        """Exports OpenAPI schemas to JSON Schema format."""
        build_dir = tmp_path / "build"
        build_dir.mkdir()
        (build_dir / "openapi.json").write_text(
            json.dumps(
                {
                    "components": {
                        "schemas": {
                            "User": {"type": "object", "properties": {"id": {"type": "integer"}}},
                        }
                    }
                }
            )
        )

        result = export_openapi_schemas(tmp_path)

        assert result.exists()
        with result.open() as f:
            data = json.load(f)
        assert "definitions" in data
        assert "User" in data["definitions"]


class TestMain:
    """Tests for main function."""

    def test_runs_all_by_default(
        self, tmp_path: Path, capsys: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Runs all generators by default."""
        monkeypatch.setattr(sys, "argv", ["prog"])

        with (
            patch("scripts.generate_types.export_openapi_spec") as mock_openapi,
            patch("scripts.generate_types.export_ws_schemas") as mock_ws,
            patch("scripts.generate_types.export_openapi_schemas") as mock_api_schemas,
            patch("scripts.generate_types.generate_zod_ws") as mock_zod_ws,
            patch("scripts.generate_types.generate_zod_api") as mock_zod_api,
            patch("scripts.generate_types.generate_entities") as mock_entities,
            patch("scripts.generate_types.generate_ios_types") as mock_ios,
            patch.object(Path, "parent", tmp_path),
        ):
            result = main()

            assert result == 0
            mock_openapi.assert_called_once()
            mock_ws.assert_called_once()
            mock_api_schemas.assert_called_once()
            mock_zod_ws.assert_called_once()
            mock_zod_api.assert_called_once()
            mock_entities.assert_called_once()
            mock_ios.assert_called_once()

    def test_runs_only_openapi(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Runs only openapi export."""
        monkeypatch.setattr(sys, "argv", ["prog", "--openapi"])

        with (
            patch("scripts.generate_types.export_openapi_spec") as mock_openapi,
            patch("scripts.generate_types.export_ws_schemas") as mock_ws,
        ):
            result = main()

            assert result == 0
            mock_openapi.assert_called_once()
            mock_ws.assert_not_called()

    def test_runs_frontend_ws_only(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Runs only frontend-ws."""
        monkeypatch.setattr(sys, "argv", ["prog", "--frontend-ws"])

        with (
            patch("scripts.generate_types.generate_zod_ws") as mock_ws,
            patch("scripts.generate_types.generate_zod_api") as mock_api,
        ):
            result = main()

            assert result == 0
            mock_ws.assert_called_once()
            mock_api.assert_not_called()

    def test_runs_frontend_api_only(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Runs only frontend-api."""
        monkeypatch.setattr(sys, "argv", ["prog", "--frontend-api"])

        with (
            patch("scripts.generate_types.generate_zod_ws") as mock_ws,
            patch("scripts.generate_types.generate_zod_api") as mock_api,
        ):
            result = main()

            assert result == 0
            mock_ws.assert_not_called()
            mock_api.assert_called_once()

    def test_parses_args_from_argv(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Parses sys.argv arguments."""
        monkeypatch.setattr(sys, "argv", ["prog", "--all"])

        with (
            patch("scripts.generate_types.export_openapi_spec"),
            patch("scripts.generate_types.export_ws_schemas"),
            patch("scripts.generate_types.export_openapi_schemas"),
            patch("scripts.generate_types.generate_zod_ws"),
            patch("scripts.generate_types.generate_zod_api"),
            patch("scripts.generate_types.generate_entities"),
            patch("scripts.generate_types.generate_ios_types"),
        ):
            result = main()

            assert result == 0

    def test_strip_eslint_disable_removes_comment(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Strips eslint-disable comment from file when --strip-eslint-disable is used.

        Given: File containing eslint-disable comment,
        When: main() is called with --strip-eslint-disable FILE,
        Then: Comment is removed from file.
        """
        project_root = Path(__file__).resolve().parents[2]
        work_dir = project_root / "build" / "pytest" / "strip-eslint-disable" / tmp_path.name
        work_dir.mkdir(parents=True, exist_ok=True)
        test_file = work_dir / "test.ts"
        test_file.write_text("/* eslint-disable */\nexport const foo = 1;\n", encoding="utf-8")
        rel_path = test_file.relative_to(project_root)
        monkeypatch.setattr(sys, "argv", ["prog", "--strip-eslint-disable", str(rel_path)])

        result = main()

        assert result == 0
        assert test_file.read_text(encoding="utf-8") == "export const foo = 1;\n"

    def test_strip_eslint_disable_nonexistent_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Handles nonexistent file gracefully.

        Given: Path to nonexistent file,
        When: main() is called with --strip-eslint-disable,
        Then: Returns success without error.
        """
        project_root = Path(__file__).resolve().parents[2]
        work_dir = project_root / "build" / "pytest" / "strip-eslint-disable" / tmp_path.name
        work_dir.mkdir(parents=True, exist_ok=True)
        nonexistent = work_dir / "does_not_exist.ts"
        rel_path = nonexistent.relative_to(project_root)
        monkeypatch.setattr(sys, "argv", ["prog", "--strip-eslint-disable", str(rel_path)])

        result = main()

        assert result == 0

    def test_strip_eslint_disable_no_comment_present(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """File without comment is unchanged.

        Given: File without eslint-disable comment,
        When: main() is called with --strip-eslint-disable,
        Then: File content unchanged.
        """
        project_root = Path(__file__).resolve().parents[2]
        work_dir = project_root / "build" / "pytest" / "strip-eslint-disable" / tmp_path.name
        work_dir.mkdir(parents=True, exist_ok=True)
        test_file = work_dir / "clean.ts"
        original_content = "export const bar = 2;\n"
        test_file.write_text(original_content, encoding="utf-8")
        rel_path = test_file.relative_to(project_root)
        monkeypatch.setattr(sys, "argv", ["prog", "--strip-eslint-disable", str(rel_path)])

        result = main()

        assert result == 0
        assert test_file.read_text(encoding="utf-8") == original_content

    def test_strip_eslint_disable_rejects_absolute_path(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Rejects absolute paths for --strip-eslint-disable.

        Given: Absolute path is provided,
        When: main() is called,
        Then: Exits with a clear message.
        """
        test_file = tmp_path / "abs.ts"
        test_file.write_text("/* eslint-disable */\nexport const foo = 1;\n", encoding="utf-8")
        monkeypatch.setattr(sys, "argv", ["prog", "--strip-eslint-disable", str(test_file)])

        with pytest.raises(SystemExit, match="Expected a project-relative path"):
            main()

    def test_strip_eslint_disable_rejects_escape_project_root(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Rejects paths that escape the project root.

        Given: A path containing parent traversal,
        When: main() is called,
        Then: Exits with a clear message.
        """
        monkeypatch.setattr(sys, "argv", ["prog", "--strip-eslint-disable", "../pyproject.toml"])

        with pytest.raises(SystemExit, match="Path must be under the project root"):
            main()


class TestConstants:
    """Tests for module constants."""

    def test_swift_keyword_renames_is_dict(self) -> None:
        """SWIFT_KEYWORD_RENAMES is a dict mapping keywords to safe names."""
        assert isinstance(SWIFT_KEYWORD_RENAMES, dict)
        assert "class" in SWIFT_KEYWORD_RENAMES
        assert "func" in SWIFT_KEYWORD_RENAMES

    def test_entity_exclude_fields(self) -> None:
        """ENTITY_EXCLUDE_FIELDS contains expected fields."""
        assert "type" in ENTITY_EXCLUDE_FIELDS
        assert "meta" in ENTITY_EXCLUDE_FIELDS

    def test_entity_union_id_fields(self) -> None:
        """ENTITY_UNION_ID_FIELDS contains expected fields."""
        assert "id" in ENTITY_UNION_ID_FIELDS
        assert "order_id" in ENTITY_UNION_ID_FIELDS


class TestExtractRepeatedUnions:
    """Tests for extract_repeated_unions."""

    def test_returns_lines_unchanged_when_no_repeats(self) -> None:
        """Lines without repeated unions are returned as-is."""
        lines = [
            "/**",
            " * Header",
            " */",
            "export interface Foo {",
            "  side: 'buy' | 'sell'",
            "}",
        ]
        result = extract_repeated_unions(lines)
        assert result == lines

    def test_extracts_union_above_threshold(self) -> None:
        """Unions appearing >= 3 times are extracted as type aliases."""
        lines = [
            "/**",
            " * Header",
            " */",
            "export interface A {",
            "  side: 'buy' | 'sell'",
            "}",
            "export interface B {",
            "  side: 'buy' | 'sell'",
            "}",
            "export interface C {",
            "  side: 'buy' | 'sell'",
            "}",
        ]
        result = extract_repeated_unions(lines)
        assert "type Side = 'buy' | 'sell'" in result
        assert all(
            "  side: Side" in line for line in result if "side:" in line and "type" not in line
        )

    def test_alias_placed_before_first_interface(self) -> None:
        """Type alias is placed before the first interface JSDoc."""
        lines = [
            "/** header */",
            "",
            "/**",
            " * Doc A",
            " */",
            "export interface A {",
            "  side: 'buy' | 'sell'",
            "}",
            "export interface B {",
            "  side: 'buy' | 'sell'",
            "}",
            "export interface C {",
            "  side: 'buy' | 'sell'",
            "}",
        ]
        result = extract_repeated_unions(lines)
        alias_idx = next(i for i, line in enumerate(result) if line.startswith("type Side"))
        first_iface = next(
            i for i, line in enumerate(result) if line.startswith("export interface")
        )
        assert alias_idx < first_iface

    def test_multiple_unions_extracted(self) -> None:
        """Multiple repeated unions are each extracted."""
        lines = [
            "export interface A {",
            "  side: 'buy' | 'sell'",
            "  exchange: 'paper' | 'kraken' | 'zonda'",
            "}",
            "export interface B {",
            "  side: 'buy' | 'sell'",
            "  exchange: 'paper' | 'kraken' | 'zonda'",
            "}",
            "export interface C {",
            "  side: 'buy' | 'sell'",
            "  exchange: 'paper' | 'kraken' | 'zonda'",
            "}",
        ]
        result = extract_repeated_unions(lines)
        joined = "\n".join(result)
        assert "type Side = 'buy' | 'sell'" in joined
        assert "type Exchange = 'paper' | 'kraken' | 'zonda'" in joined

    def test_does_not_extract_below_threshold(self) -> None:
        """Unions appearing < 3 times are not extracted."""
        lines = [
            "export interface A {",
            "  side: 'buy' | 'sell'",
            "}",
            "export interface B {",
            "  side: 'buy' | 'sell'",
            "}",
        ]
        result = extract_repeated_unions(lines)
        assert result == lines

    def test_optional_fields_are_matched(self) -> None:
        """Optional fields (with ?) are also detected and replaced."""
        lines = [
            "export interface A {",
            "  side?: 'buy' | 'sell'",
            "}",
            "export interface B {",
            "  side?: 'buy' | 'sell'",
            "}",
            "export interface C {",
            "  side: 'buy' | 'sell'",
            "}",
        ]
        result = extract_repeated_unions(lines)
        assert "type Side = 'buy' | 'sell'" in result

    def test_snake_case_field_produces_pascal_alias(self) -> None:
        """Snake_case field names produce PascalCase aliases."""
        lines = [
            "export interface A {",
            "  orderType: 'market' | 'limit'",
            "}",
            "export interface B {",
            "  orderType: 'market' | 'limit'",
            "}",
            "export interface C {",
            "  orderType: 'market' | 'limit'",
            "}",
        ]
        result = extract_repeated_unions(lines)
        assert "type OrderType = 'market' | 'limit'" in result

    def test_no_export_interface_inserts_at_start(self) -> None:
        """Aliases are inserted at position 0 when no export interface exists."""
        lines = [
            "interface A {",
            "  side: 'buy' | 'sell'",
            "}",
            "interface B {",
            "  side: 'buy' | 'sell'",
            "}",
            "interface C {",
            "  side: 'buy' | 'sell'",
            "}",
        ]
        result = extract_repeated_unions(lines)
        assert result[0] == "type Side = 'buy' | 'sell'"
