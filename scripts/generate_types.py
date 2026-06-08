"""Unified type generator for frontend and iOS from backend schemas.

This script consolidates type generation for all platforms:
- Exports Pydantic WebSocket schemas to JSON Schema
- Exports OpenAPI REST schemas to JSON Schema
- Generates Zod schemas for frontend (TypeScript)
- Prepares schemas for iOS Swift generation (via quicktype)

Usage:
    python scripts/generate_types.py --export       # Export JSON schemas only
    python scripts/generate_types.py --frontend     # Generate frontend types (Zod)
    python scripts/generate_types.py --ios          # Prepare iOS schemas
    python scripts/generate_types.py --all          # All of the above (default)
"""

import argparse
import inspect
import json
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from snapper.api.schemas.base import StrictDataSchema
from snapper.auth.domain.permissions import RESOURCE_PERMISSIONS as BACKEND_RESOURCE_PERMISSIONS
from snapper.auth.domain.permissions import ROLE_PERMISSIONS as BACKEND_ROLE_PERMISSIONS
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.interface.websocket import schemas as ws_schemas
from snapper.interface.websocket.schemas import WSAuthCompleteResponse
from snapper.interface.websocket.schemas import WSAuthenticateRequest
from snapper.interface.websocket.schemas import WSAuthExpiredResponse
from snapper.interface.websocket.schemas import WSAuthFailedResponse
from snapper.interface.websocket.schemas import WSAuthOkResponse
from snapper.interface.websocket.schemas import WSAuthRequiredResponse
from snapper.interface.websocket.schemas import WSErrorResponse
from snapper.interface.websocket.schemas import WSPingRequest
from snapper.interface.websocket.schemas import WSPongResponse
from snapper.interface.websocket.schemas import WSReauthOkResponse
from snapper.interface.websocket.schemas import WSReauthRequest
from snapper.interface.websocket.schemas import WSReauthRequiredResponse
from snapper.interface.websocket.schemas import WSSubscribeRequest
from snapper.interface.websocket.schemas import WSSubscriptionSuccessResponse
from snapper.messaging.schemas import data as data_schemas
from snapper.messaging.schemas.data import AiReviewCapsViolationFrameData
from snapper.messaging.schemas.data import AiReviewDecisionAckFrameData
from snapper.messaging.schemas.data import AiReviewRequestFrameData
from snapper.messaging.schemas.data import OrderEventData
from snapper.messaging.schemas.data import SignalData
from snapper.server.app import create_app

JsonValue = dict[str, Any] | list[Any] | str | int | float | bool | None

_DEFS_REF_PREFIX = "#/definitions/"
_LEGACY_DEFS_REF_PREFIX = "#/$defs/"
_COMPONENTS_REF_PREFIX = "#/components/schemas/"
_WS_SCHEMAS_FILE = "ws-schemas.json"
_OPENAPI_FILE = "openapi.json"
_DEFS_KEY = "$defs"
_STRIP_ESLINT_DISABLE_TARGET = Path("frontend") / "src" / "types" / "ws.generated.ts"
_OPENAPI_TYPESCRIPT_TARGET = Path("frontend") / "src" / "types" / "api.generated.ts"
_SWIFT_HEADER_LINES = [
    "// This file was auto-generated from backend schemas.",
    "// DO NOT EDIT - regenerate with: make ios-gen-types",
    "",
    "import Foundation",
    "",
]
_TS_CONST_OBJECT_CLOSE = "} as const"

ENTITY_RENAMES: dict[str, str] = {}
DATA_SUFFIX = "Data"
SNAPSHOT_SUFFIX = "Snapshot"
REQUEST_SUFFIX = "Request"
ENTITY_EXCLUDE_FIELDS = {"type"}
ENTITY_UNION_ID_FIELDS = {"id", "order_id"}

SWIFT_KEYWORD_RENAMES: dict[str, str] = {
    "operator": "operatorRole",
    "open": "openStatus",
    "class": "classValue",
    "struct": "structValue",
    "enum": "enumValue",
    "protocol": "protocolValue",
    "extension": "extensionValue",
    "func": "funcValue",
    "var": "varValue",
    "let": "letValue",
    "import": "importValue",
    "return": "returnValue",
    "if": "ifValue",
    "else": "elseValue",
    "for": "forValue",
    "while": "whileValue",
    "switch": "switchValue",
    "case": "caseValue",
    "default": "defaultValue",
    "break": "breakValue",
    "continue": "continueValue",
    "in": "inValue",
    "true": "trueValue",
    "false": "falseValue",
    "nil": "nilValue",
    "self": "selfValue",
    "Self": "selfType",
    "super": "superValue",
    "init": "initValue",
    "deinit": "deinitValue",
    "get": "getValue",
    "set": "setValue",
    "willSet": "willSetValue",
    "didSet": "didSetValue",
    "throws": "throwsValue",
    "throw": "throwValue",
    "try": "tryValue",
    "catch": "catchValue",
    "as": "asValue",
    "is": "isValue",
    "Any": "anyValue",
    "Type": "typeValue",
    "static": "staticValue",
    "private": "privateValue",
    "public": "publicValue",
    "internal": "internalValue",
    "fileprivate": "fileprivateValue",
    "final": "finalValue",
    "override": "overrideValue",
    "mutating": "mutatingValue",
}


def fix_refs_pydantic(obj: JsonValue) -> JsonValue:
    """Convert Pydantic $defs refs to JSON Schema definitions format.

    Args:
        obj: JSON value to process for reference conversion.

    Returns:
        JSON value with converted references.
    """
    if isinstance(obj, dict):
        result: dict[str, JsonValue] = {}
        for key, value in obj.items():
            if (
                key == "$ref"
                and isinstance(value, str)
                and value.startswith(_LEGACY_DEFS_REF_PREFIX)
            ):
                result[key] = value.replace(_LEGACY_DEFS_REF_PREFIX, _DEFS_REF_PREFIX)
            else:
                result[key] = fix_refs_pydantic(value)
        return result
    if isinstance(obj, list):
        return [fix_refs_pydantic(item) for item in obj]
    return obj


def fix_refs_openapi(obj: JsonValue) -> JsonValue:
    """Convert OpenAPI $ref paths to JSON Schema definitions format.

    Args:
        obj: JSON value to process for reference conversion.

    Returns:
        JSON value with converted references.
    """
    if isinstance(obj, dict):
        result: dict[str, JsonValue] = {}
        for key, value in obj.items():
            if key == "$ref" and isinstance(value, str):
                if value.startswith(_COMPONENTS_REF_PREFIX):
                    result[key] = value.replace(_COMPONENTS_REF_PREFIX, _DEFS_REF_PREFIX)
                else:
                    result[key] = value
            else:
                result[key] = fix_refs_openapi(value)
        return result
    if isinstance(obj, list):
        return [fix_refs_openapi(item) for item in obj]
    return obj


def _process_property(
    prop_schema: JsonValue,
    const_fields: list[str],
    prop_name: str,
) -> JsonValue:
    """Process a single property schema, tracking const fields.

    Args:
        prop_schema: The property schema to process.
        const_fields: Accumulator for const field names (mutated in place).
        prop_name: Name of the property.

    Returns:
        Processed property schema.
    """
    if not isinstance(prop_schema, dict):
        return prop_schema
    processed_schema = prop_schema
    if "const" in prop_schema:
        const_fields.append(prop_name)
        if "default" in prop_schema:
            processed_schema = dict(prop_schema)
            del processed_schema["default"]
    return make_const_fields_required(processed_schema)


def _process_schema_value(
    key: str,
    value: JsonValue,
    const_fields: list[str],
) -> JsonValue:
    """Process a single key-value pair from a schema dict.

    Args:
        key: Schema key.
        value: Schema value.
        const_fields: Accumulator for const field names (mutated in place).

    Returns:
        Processed value.
    """
    if key == "properties" and isinstance(value, dict):
        return {
            prop_name: _process_property(prop_schema, const_fields, prop_name)
            for prop_name, prop_schema in value.items()
        }
    if isinstance(value, dict):
        return make_const_fields_required(value)
    if isinstance(value, list):
        return [
            make_const_fields_required(item) if isinstance(item, dict) else item for item in value
        ]
    return value


def _add_const_to_required(
    result: dict[str, JsonValue],
    const_fields: list[str],
) -> None:
    """Add const fields to the required list in place.

    Args:
        result: Schema result dict (mutated in place).
        const_fields: List of const field names to add.
    """
    required = result.get("required", [])
    if not isinstance(required, list):
        return
    for field in const_fields:
        if field not in required:
            required.append(field)
    result["required"] = required


def make_const_fields_required(schema: dict[str, JsonValue]) -> dict[str, JsonValue]:
    """Ensure const fields are in required list for proper discriminator handling.

    Args:
        schema: JSON Schema dictionary to process.

    Returns:
        Schema with const fields added to required list.
    """
    if not isinstance(schema, dict):
        return schema
    const_fields: list[str] = []
    result: dict[str, JsonValue] = {
        key: _process_schema_value(key, value, const_fields) for key, value in schema.items()
    }
    if const_fields:
        _add_const_to_required(result, const_fields)
    return result


def discover_ws_schemas() -> list[tuple[str, type[BaseModel]]]:
    """Auto-discover WebSocket schemas from snapper modules.

    Returns:
        List of tuples containing schema name and model class.
    """
    discovered: list[tuple[str, type[BaseModel]]] = []
    discovered.append(("WsMessageBase", StrictDataSchema))

    for name, obj in inspect.getmembers(data_schemas):
        if (
            inspect.isclass(obj)
            and issubclass(obj, StrictDataSchema)
            and obj is not StrictDataSchema
            and name.endswith("Data")
        ):
            discovered.append((name, obj))

    for name, obj in inspect.getmembers(ws_schemas):
        if (
            inspect.isclass(obj)
            and issubclass(obj, BaseModel)
            and obj is not BaseModel
            and name.startswith("WS")
        ):
            discovered.append((name, obj))

    return discovered


_PRIMITIVE_JSON_TYPES = frozenset({"string", "number", "integer", "boolean"})
_TS_AFFECTING_SCHEMA_KEYS = frozenset(
    {
        "const",
        "enum",
        "anyOf",
        "oneOf",
        "allOf",
        "$ref",
        "items",
        "properties",
    }
)


def strip_primitive_titles(schema: JsonValue) -> JsonValue:
    """Remove title from properties that are bare primitive types.

    When json-schema-to-typescript encounters ``{"title": "Open", "type": "number"}``,
    it emits ``export type Open = number`` which SonarCloud flags as a redundant
    type alias (S6564).  Stripping the title forces the tool to inline the primitive.

    Only strips title when the property has no TypeScript-affecting constraints
    (const, enum, anyOf, etc.) that would make a named alias meaningful.

    Args:
        schema: JSON Schema value to process (mutates nothing, returns a new tree).

    Returns:
        A copy of the schema with redundant titles removed.
    """
    if isinstance(schema, dict):
        result: dict[str, JsonValue] = {k: strip_primitive_titles(v) for k, v in schema.items()}
        type_value = result.get("type")
        has_primitive_type = isinstance(type_value, str) and type_value in _PRIMITIVE_JSON_TYPES
        has_title = "title" in result
        has_semantic_key = any(k in result for k in _TS_AFFECTING_SCHEMA_KEYS)
        if has_primitive_type and has_title and not has_semantic_key:
            del result["title"]
        return result
    if isinstance(schema, list):
        return [strip_primitive_titles(item) for item in schema]
    return schema


def export_ws_schemas(project_root: Path) -> Path:
    """Export WebSocket Pydantic schemas to JSON Schema.

    Args:
        project_root: Root directory of the project.

    Returns:
        Path to the generated JSON schema file.
    """
    output_path = project_root / "build" / _WS_SCHEMAS_FILE
    output_path.parent.mkdir(parents=True, exist_ok=True)
    schemas = discover_ws_schemas()

    all_definitions: dict[str, JsonValue] = {}

    for name, model in schemas:
        schema: dict[str, JsonValue] = model.model_json_schema(mode="serialization")
        if _DEFS_KEY in schema:
            defs_value = schema[_DEFS_KEY]
            if isinstance(defs_value, dict):
                for def_name, def_schema in defs_value.items():
                    all_definitions[def_name] = fix_refs_pydantic(def_schema)
            del schema[_DEFS_KEY]
        schema_fixed = fix_refs_pydantic(schema)
        if isinstance(schema_fixed, dict):
            all_definitions[name] = schema_fixed

    combined_schema: dict[str, JsonValue] = {
        "$schema": "https://json-schema.org/draft-07/schema#",
        "title": "WebSocket Messages",
        "description": "WebSocket message schemas for Snapper trading platform",
        "definitions": all_definitions,
        "oneOf": [{"$ref": f"{_DEFS_REF_PREFIX}{name}"} for name, _ in schemas],
    }
    combined_schema = make_const_fields_required(combined_schema)
    stripped = strip_primitive_titles(combined_schema)
    assert isinstance(stripped, dict)
    combined_schema = stripped

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(combined_schema, f, indent=2)

    print(f"Exported {len(schemas)} WebSocket schemas to {output_path}")
    return output_path


def export_openapi_spec(project_root: Path) -> Path:
    """Export OpenAPI spec from FastAPI app.

    Args:
        project_root: Root directory of the project.

    Returns:
        Path to the generated OpenAPI spec file.
    """
    output_path = project_root / "build" / _OPENAPI_FILE
    output_path.parent.mkdir(parents=True, exist_ok=True)
    app = create_app()
    spec = app.openapi()

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(spec, f, indent=2)

    print(f"Exported OpenAPI spec to {output_path}")
    return output_path


def _register_openapi_schema(
    name: str,
    fixed_schema: JsonValue,
    json_schema: dict[str, JsonValue],
) -> None:
    """Register a single OpenAPI schema into the combined JSON Schema document.

    Adds the fixed schema to the ``definitions`` mapping and appends a
    ``$ref`` entry to the ``oneOf`` list.  Both lookups are guarded by
    ``isinstance`` checks so the function is safe to call even when the
    parent document has an unexpected shape.

    Args:
        name: Schema name used as the definition key.
        fixed_schema: Schema dict with ``$ref`` paths already rewritten.
        json_schema: The combined JSON Schema document being assembled.
    """
    definitions = json_schema.get("definitions")
    one_of = json_schema.get("oneOf")
    if isinstance(definitions, dict):
        definitions[name] = fixed_schema
    if isinstance(one_of, list):
        one_of.append({"$ref": f"{_DEFS_REF_PREFIX}{name}"})


def export_openapi_schemas(project_root: Path) -> Path:
    """Export OpenAPI component schemas to JSON Schema format for quicktype.

    Args:
        project_root: Root directory of the project.

    Returns:
        Path to the generated JSON schema file.
    """
    openapi_path = project_root / "build" / _OPENAPI_FILE
    output_path = project_root / "build" / "openapi-schemas.json"

    with openapi_path.open(encoding="utf-8") as f:
        openapi_spec = json.load(f)

    schemas = openapi_spec.get("components", {}).get("schemas", {})

    json_schema: dict[str, JsonValue] = {
        "$schema": "https://json-schema.org/draft-07/schema#",
        "title": "APITypes",
        "description": "API schemas for Snapper trading platform",
        "definitions": {},
        "oneOf": [],
    }

    for name, schema in schemas.items():
        fixed_schema = fix_refs_openapi(schema)
        _register_openapi_schema(name, fixed_schema, json_schema)

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(json_schema, f, indent=2)

    print(f"Exported {len(schemas)} API schemas to {output_path}")
    return output_path


def to_camel_case(snake_str: str) -> str:
    """Convert snake_case to camelCase.

    Args:
        snake_str: String in snake_case format.

    Returns:
        String converted to camelCase format.
    """
    components = snake_str.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


_SWIFT_SIMPLE_TYPE_MAP: dict[str, str] = {
    "integer": "Int",
    "number": "Double",
    "boolean": "Bool",
}


def _swift_string_type(prop: dict[str, Any], suffix: str) -> str:
    """Resolve Swift type for a JSON Schema string property.

    Args:
        prop: Property schema.
        suffix: Optional suffix (e.g., '?').

    Returns:
        Swift type string.
    """
    if prop.get("format") == "date-time":
        return f"Date{suffix}"
    return f"String{suffix}"


def _swift_object_type(prop: dict[str, Any], definitions: dict[str, Any], suffix: str) -> str:
    """Resolve Swift type for a JSON Schema object property.

    Args:
        prop: Property schema.
        definitions: Schema definitions for reference resolution.
        suffix: Optional suffix.

    Returns:
        Swift type string.
    """
    additional = prop.get("additionalProperties")
    if additional:
        if additional is True:
            return f"[String: AnyCodable]{suffix}"
        value_type = json_type_to_swift(additional, definitions, optional=False)
        return f"[String: {value_type}]{suffix}"
    return f"[String: AnyCodable]{suffix}"


def _swift_ref_type(prop: dict[str, Any], suffix: str) -> str:
    """Resolve Swift type for a JSON Schema $ref property.

    Args:
        prop: Property schema with $ref.
        suffix: Optional suffix (e.g., '?').

    Returns:
        Swift type string.
    """
    ref_name = prop["$ref"].split("/")[-1]
    return f"{ref_name}{suffix}"


def _swift_anyof_type(prop: dict[str, Any], definitions: dict[str, Any], _suffix: str) -> str:
    """Resolve Swift type for a JSON Schema anyOf property.

    Args:
        prop: Property schema with anyOf.
        definitions: Schema definitions for reference resolution.
        _suffix: Optional suffix (unused; kept for call-site symmetry).

    Returns:
        Swift type string.
    """
    types = prop["anyOf"]
    non_null = [t for t in types if t.get("type") != "null"]
    if len(non_null) == 1:
        return json_type_to_swift(non_null[0], definitions, optional=True)
    return "AnyCodable?"


def _swift_array_type(prop: dict[str, Any], definitions: dict[str, Any], suffix: str) -> str:
    """Resolve Swift type for a JSON Schema array property.

    Args:
        prop: Property schema with array type.
        definitions: Schema definitions for reference resolution.
        suffix: Optional suffix.

    Returns:
        Swift type string.
    """
    items = prop.get("items", {})
    item_type = json_type_to_swift(items, definitions, optional=False)
    return f"[{item_type}]{suffix}"


def json_type_to_swift(
    prop: dict[str, Any], definitions: dict[str, Any], optional: bool = True
) -> str:
    """Convert JSON Schema type to Swift type.

    Args:
        prop: Property schema from JSON Schema.
        definitions: Schema definitions for reference resolution.
        optional: Whether the type should be optional.

    Returns:
        Swift type string representation.
    """
    suffix = "?" if optional else ""

    if "$ref" in prop:
        return _swift_ref_type(prop, suffix)

    if "anyOf" in prop:
        return _swift_anyof_type(prop, definitions, suffix)

    if "allOf" in prop:
        return json_type_to_swift(prop["allOf"][0], definitions, optional)

    prop_type = prop.get("type")

    simple_type = _SWIFT_SIMPLE_TYPE_MAP.get(prop_type or "")
    if simple_type:
        return f"{simple_type}{suffix}"

    type_handlers: dict[str, Callable[..., str]] = {
        "string": lambda: _swift_string_type(prop, suffix),
        "array": lambda: _swift_array_type(prop, definitions, suffix),
        "object": lambda: _swift_object_type(prop, definitions, suffix),
    }
    handler = type_handlers.get(prop_type or "")
    if handler:
        return handler()

    return f"AnyCodable{suffix}"


def generate_swift_struct(
    name: str, schema: dict[str, Any], definitions: dict[str, Any]
) -> list[str]:
    """Generate Swift struct from JSON Schema.

    Args:
        name: Name of the struct to generate.
        schema: JSON Schema definition for the struct.
        definitions: Schema definitions for reference resolution.

    Returns:
        List of Swift code lines for the struct.
    """
    lines: list[str] = []
    lines.append(f"struct {name}: Codable, Sendable {{")

    properties = schema.get("properties", {})
    required = set(schema.get("required", []))
    coding_keys: list[tuple[str, str]] = []

    for prop_name, prop_schema in properties.items():
        swift_name = to_camel_case(prop_name)
        is_required = prop_name in required
        swift_type = json_type_to_swift(prop_schema, definitions, optional=not is_required)
        lines.append(f"    let {swift_name}: {swift_type}")

        if swift_name != prop_name:
            coding_keys.append((swift_name, prop_name))

    prop_names = list(properties)
    lines.append("")
    lines.append("    init(")
    for index, prop_name in enumerate(prop_names):
        swift_name = to_camel_case(prop_name)
        is_required = prop_name in required
        swift_type = json_type_to_swift(
            properties[prop_name], definitions, optional=not is_required
        )
        default = "" if is_required else " = nil"
        comma = "," if index < len(prop_names) - 1 else ""
        lines.append(f"        {swift_name}: {swift_type}{default}{comma}")
    lines.append("    ) {")
    for prop_name in prop_names:
        swift_name = to_camel_case(prop_name)
        lines.append(f"        self.{swift_name} = {swift_name}")
    lines.append("    }")

    if coding_keys:
        lines.append("")
        lines.append("    enum CodingKeys: String, CodingKey {")
        for prop_name in properties:
            swift_name = to_camel_case(prop_name)
            if swift_name != prop_name:
                lines.append(f'        case {swift_name} = "{prop_name}"')
            else:
                lines.append(f"        case {swift_name}")
        lines.append("    }")

    lines.append("}")
    return lines


def generate_swift_enum(name: str, values: list[str]) -> list[str]:
    """Generate Swift enum from JSON Schema enum.

    Args:
        name: Name of the enum to generate.
        values: List of enum case values.

    Returns:
        List of Swift code lines for the enum.
    """
    lines: list[str] = []
    lines.append(f"enum {name}: String, Codable, Sendable {{")
    for value in values:
        swift_case = to_camel_case(value) if "_" in value else value
        renamed = SWIFT_KEYWORD_RENAMES.get(swift_case)
        if renamed:
            lines.append(f'    case {renamed} = "{value}"')
        elif swift_case != value:
            lines.append(f'    case {swift_case} = "{value}"')
        else:
            lines.append(f"    case {swift_case}")
    lines.append("}")
    return lines


def get_any_codable_helper() -> list[str]:
    """Get AnyCodable helper struct for Swift.

    Returns:
        List of Swift code lines for the AnyCodable helper struct.
    """
    return [
        "struct AnyCodable: Codable, @unchecked Sendable {",
        "    let value: Any",
        "",
        "    init(_ value: Any) {",
        "        self.value = value",
        "    }",
        "",
        "    init(from decoder: Decoder) throws {",
        "        let container = try decoder.singleValueContainer()",
        "        if container.decodeNil() {",
        "            value = NSNull()",
        "        } else if let bool = try? container.decode(Bool.self) {",
        "            value = bool",
        "        } else if let int = try? container.decode(Int.self) {",
        "            value = int",
        "        } else if let double = try? container.decode(Double.self) {",
        "            value = double",
        "        } else if let string = try? container.decode(String.self) {",
        "            value = string",
        "        } else if let array = try? container.decode([AnyCodable].self) {",
        "            value = array.map { $0.value }",
        "        } else if let dict = try? container.decode([String: AnyCodable].self) {",
        "            value = dict.mapValues { $0.value }",
        "        } else {",
        "            throw DecodingError.dataCorruptedError(in: container, "
        'debugDescription: "Cannot decode AnyCodable")',
        "        }",
        "    }",
        "",
        "    func encode(to encoder: Encoder) throws {",
        "        var container = encoder.singleValueContainer()",
        "        switch value {",
        "        case is NSNull:",
        "            try container.encodeNil()",
        "        case let bool as Bool:",
        "            try container.encode(bool)",
        "        case let int as Int:",
        "            try container.encode(int)",
        "        case let double as Double:",
        "            try container.encode(double)",
        "        case let string as String:",
        "            try container.encode(string)",
        "        case let array as [Any]:",
        "            try container.encode(array.map { AnyCodable($0) })",
        "        case let dict as [String: Any]:",
        "            try container.encode(dict.mapValues { AnyCodable($0) })",
        "        default:",
        "            try container.encodeNil()",
        "        }",
        "    }",
        "}",
        "",
    ]


def _collect_top_level_enums(
    definitions: dict[str, Any],
    lines: list[str],
    generated_enums: set[str],
) -> None:
    """Emit Swift enums for top-level string enum definitions.

    Args:
        definitions: JSON Schema definitions dict.
        lines: Mutable output lines list.
        generated_enums: Mutable set of already-generated enum names.
    """
    for name, type_schema in definitions.items():
        if type_schema.get("type") == "string" and "enum" in type_schema:
            if name not in generated_enums:
                lines.extend(generate_swift_enum(name, type_schema["enum"]))
                lines.append("")
                generated_enums.add(name)


def _collect_inline_enums(
    definitions: dict[str, Any],
    lines: list[str],
    generated_enums: set[str],
) -> None:
    """Emit Swift enums for inline string enum properties on object definitions.

    Args:
        definitions: JSON Schema definitions dict.
        lines: Mutable output lines list.
        generated_enums: Mutable set of already-generated enum names.
    """
    for name, type_schema in definitions.items():
        if type_schema.get("type") != "object":
            continue
        for prop_name, prop_schema in type_schema.get("properties", {}).items():
            if "enum" not in prop_schema or prop_schema.get("type") != "string":
                continue
            enum_name = f"{name}{prop_name.title().replace('_', '')}"
            if enum_name not in generated_enums:
                lines.extend(generate_swift_enum(enum_name, prop_schema["enum"]))
                lines.append("")
                generated_enums.add(enum_name)


def _collect_structs(
    definitions: dict[str, Any],
    lines: list[str],
    generated_structs: set[str] | None = None,
) -> set[str]:
    """Emit Swift structs for object definitions.

    Args:
        definitions: JSON Schema definitions dict.
        lines: Mutable output lines list.
        generated_structs: Struct names already emitted to skip.

    Returns:
        Set of struct names generated in this call.
    """
    skip = set(generated_structs or ())
    emitted: set[str] = set()
    for name, type_schema in definitions.items():
        if type_schema.get("type") == "object":
            if name in skip:
                continue
            lines.extend(generate_swift_struct(name, type_schema, definitions))
            lines.append("")
            emitted.add(name)
    return emitted


def generate_swift_types(
    _project_root: Path,
    schema_path: Path,
    output_path: Path,
    include_any_codable: bool = True,
    exclude_enums: set[str] | None = None,
    exclude_structs: set[str] | None = None,
) -> tuple[set[str], set[str]]:
    """Generate Swift types from JSON Schema.

    Args:
        _project_root: Root directory of the project (reserved for future use).
        schema_path: Path to the JSON Schema file.
        output_path: Path for the generated Swift file.
        include_any_codable: Whether to include the AnyCodable helper.
        exclude_enums: Enum names already emitted in another file to skip here.
        exclude_structs: Struct names already emitted in another file to skip.

    Returns:
        Tuple of (enum names, struct names) generated in this file.
    """
    with schema_path.open(encoding="utf-8") as f:
        schema = json.load(f)

    definitions = schema.get("definitions", {})

    lines: list[str] = [*_SWIFT_HEADER_LINES]
    if include_any_codable:
        lines.extend(get_any_codable_helper())

    generated_enums: set[str] = set(exclude_enums or ())
    _collect_top_level_enums(definitions, lines, generated_enums)
    _collect_inline_enums(definitions, lines, generated_enums)
    emitted_structs = _collect_structs(definitions, lines, exclude_structs)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Generated {output_path} ({len(definitions)} types)")
    return generated_enums - (exclude_enums or set()), emitted_structs


def generate_ios_types(project_root: Path) -> None:
    """Generate Swift types for iOS from JSON schemas.

    Args:
        project_root: Root directory of the project.
    """
    ios_gen_dir = project_root / "ios" / "Snapper" / "Models" / "Generated"
    ios_gen_dir.mkdir(parents=True, exist_ok=True)

    any_codable_path = ios_gen_dir / "AnyCodable.swift"
    any_codable_lines = [*_SWIFT_HEADER_LINES]
    any_codable_lines.extend(get_any_codable_helper())
    any_codable_path.write_text("\n".join(any_codable_lines), encoding="utf-8")
    print(f"Generated {any_codable_path}")

    ws_schema_path = project_root / "build" / _WS_SCHEMAS_FILE

    api_schema_path = project_root / "build" / "openapi-schemas.json"
    api_enums: set[str] = set()
    api_structs: set[str] = set()
    if api_schema_path.exists():
        api_enums, api_structs = generate_swift_types(
            project_root,
            api_schema_path,
            ios_gen_dir / "APITypes.swift",
            include_any_codable=False,
        )

    if ws_schema_path.exists():
        generate_swift_types(
            project_root,
            ws_schema_path,
            ios_gen_dir / "WSMessages.swift",
            include_any_codable=False,
            exclude_enums=api_enums,
            exclude_structs=api_structs,
        )

    generate_ios_permissions(project_root)


_ZOD_SIMPLE_TYPE_MAP: dict[str, str] = {
    "integer": "z.number().int()",
    "number": "z.number()",
    "boolean": "z.boolean()",
    "null": "z.null()",
}

_ZOD_STRING_FORMAT_MAP: dict[str, str] = {
    "date-time": "z.iso.datetime()",
    "uuid": "z.string().uuid()",
    "email": "z.string().email()",
}


def _zod_anyof_type(prop: dict[str, Any], definitions: dict[str, Any]) -> str:
    """Resolve Zod type for an anyOf JSON Schema property.

    Args:
        prop: Property schema containing 'anyOf'.
        definitions: Schema definitions for reference resolution.

    Returns:
        Zod type string.
    """
    types = prop["anyOf"]
    non_null = [t for t in types if t.get("type") != "null"]
    has_null = any(t.get("type") == "null" for t in types)
    if len(non_null) == 1:
        base = json_type_to_zod(non_null[0], True, definitions)
        return f"{base}.nullable()" if has_null else base
    zod_types = [json_type_to_zod(t, True, definitions) for t in non_null]
    union = f"z.union([{', '.join(zod_types)}])"
    return f"{union}.nullable()" if has_null else union


def _zod_string_type(prop: dict[str, Any]) -> str:
    """Resolve Zod type for a JSON Schema string property.

    Args:
        prop: Property schema with type 'string'.

    Returns:
        Zod type string.
    """
    if "const" in prop:
        return f"z.literal('{prop['const']}')"
    prop_format = prop.get("format")
    if prop_format in _ZOD_STRING_FORMAT_MAP:
        return _ZOD_STRING_FORMAT_MAP[prop_format]
    if "enum" in prop:
        enum_values = ", ".join(f"'{v}'" for v in prop["enum"])
        return f"z.enum([{enum_values}])"
    result = "z.string()"
    min_length = prop.get("minLength")
    max_length = prop.get("maxLength")
    if min_length is not None:
        result += f".min({min_length})"
    if max_length is not None:
        result += f".max({max_length})"
    return result


def _zod_array_or_tuple_type(prop: dict[str, Any], definitions: dict[str, Any]) -> str:
    """Resolve Zod type for a JSON Schema array property.

    A fixed-length tuple emitted by Pydantic for ``tuple[T1, T2, ...]``
    carries a ``prefixItems`` list describing each positional type. Map
    that to ``z.tuple([...])`` so runtime validation enforces the
    length + per-position type instead of degrading to
    ``z.array(z.unknown())``.

    A variable-length array (no ``prefixItems``) keeps the historical
    ``z.array(item_type)`` emission, with ``item_type`` derived from
    the ``items`` schema.

    Args:
        prop: Property schema with type ``array``.
        definitions: Schema definitions for reference resolution.

    Returns:
        Zod type string, either ``z.tuple([...])`` or
        ``z.array(...)``.
    """
    prefix_items = prop.get("prefixItems")
    if isinstance(prefix_items, list) and prefix_items:
        positional = [json_type_to_zod(item, True, definitions) for item in prefix_items]
        return f"z.tuple([{', '.join(positional)}])"
    items = prop.get("items", {})
    item_type = json_type_to_zod(items, True, definitions)
    return f"z.array({item_type})"


def _zod_object_type(prop: dict[str, Any], definitions: dict[str, Any]) -> str:
    """Resolve Zod type for a JSON Schema object property.

    Args:
        prop: Property schema with type 'object'.
        definitions: Schema definitions for reference resolution.

    Returns:
        Zod type string.
    """
    if "properties" in prop:
        return generate_zod_object_schema(prop, definitions)
    additional = prop.get("additionalProperties")
    _zod_record_unknown = "z.record(z.string(), z.unknown())"
    if additional:
        if additional is True:
            return _zod_record_unknown
        value_type = json_type_to_zod(additional, True, definitions)
        return f"z.record(z.string(), {value_type})"
    return _zod_record_unknown


def json_type_to_zod(prop: dict[str, Any], required: bool, definitions: dict[str, Any]) -> str:
    """Convert JSON Schema type to Zod type.

    Args:
        prop: Property schema from JSON Schema.
        required: Whether the field is required.
        definitions: Schema definitions for reference resolution.

    Returns:
        Zod type string representation.
    """
    if "$ref" in prop:
        ref_name = prop["$ref"].split("/")[-1]
        if ref_name in _RECURSIVE_JSON_TYPES:
            if ref_name == "JsonObject":
                return _ZOD_JSON_RECORD
            return "z.unknown()"
        return f"{ref_name}Schema"
    if "anyOf" in prop:
        return _zod_anyof_type(prop, definitions)
    if "allOf" in prop:
        return json_type_to_zod(prop["allOf"][0], required, definitions)
    prop_type = prop.get("type")
    if prop_type in _ZOD_SIMPLE_TYPE_MAP:
        return _ZOD_SIMPLE_TYPE_MAP[prop_type]
    if prop_type == "string":
        return _zod_string_type(prop)
    if prop_type == "array":
        return _zod_array_or_tuple_type(prop, definitions)
    if prop_type == "object":
        return _zod_object_type(prop, definitions)
    return "z.unknown()"


def generate_zod_object_schema(schema: dict[str, Any], definitions: dict[str, Any]) -> str:
    """Generate Zod object schema from JSON Schema.

    Args:
        schema: JSON Schema object definition.
        definitions: Schema definitions for reference resolution.

    Returns:
        Zod object schema string.
    """
    properties = schema.get("properties", {})
    required_fields = set(schema.get("required", []))
    fields = []
    for name, prop in properties.items():
        has_default = "default" in prop
        default_is_none = has_default and prop.get("default") is None
        is_required = name in required_fields or (has_default and not default_is_none)
        zod_type = json_type_to_zod(prop, is_required, definitions)
        if not is_required:
            zod_type = f"{zod_type}.optional()"
        fields.append(f"    {name}: {zod_type},")
    fields_str = "\n".join(fields)
    return f"z.object({{\n{fields_str}\n  }}).strict()"


def generate_zod_schema_definition(
    name: str,
    schema: dict[str, Any],
    definitions: dict[str, Any],
    exported: bool = True,
) -> str:
    """Generate Zod schema definition from JSON Schema.

    Args:
        name: Name of the schema to generate.
        schema: JSON Schema definition.
        definitions: Schema definitions for reference resolution.
        exported: When ``True``, emit ``export const {Name}Schema = ...``;
            when ``False``, emit ``const _{Name}RawSchema = ...`` so a
            downstream wrapper can re-cast the schema's inferred type
            before re-exporting under the original name. The REST API
            generator uses ``exported=False`` to align Zod's inferred
            output with openapi-typescript's exact-optional emission;
            the WebSocket generator keeps the default ``exported=True``.

    Returns:
        Zod schema definition statement.
    """
    decl_name = f"{name}Schema" if exported else f"_{name}RawSchema"
    decl_prefix = "export const " if exported else "const "
    if name in _RECURSIVE_JSON_TYPES:
        if name == "JsonObject":
            return f"{decl_prefix}{decl_name} = z.record(z.string(), z.any())"
        return f"{decl_prefix}{decl_name} = z.unknown()"
    if schema.get("type") == "string" and "enum" in schema:
        enum_values = ", ".join(f"'{v}'" for v in schema["enum"])
        return f"{decl_prefix}{decl_name} = z.enum([{enum_values}])"
    if schema.get("type") == "object":
        zod_schema = generate_zod_object_schema(schema, definitions)
        return f"{decl_prefix}{decl_name} = {zod_schema}"
    if schema.get("type") == "array":
        items = schema.get("items", {})
        item_type = json_type_to_zod(items, True, definitions)
        return f"{decl_prefix}{decl_name} = z.array({item_type})"
    zod_type = json_type_to_zod(schema, True, definitions)
    return f"{decl_prefix}{decl_name} = {zod_type}"


def _build_schema_deps(
    schemas: dict[str, Any],
    ref_prefix: str,
) -> dict[str, set[str]]:
    """Build a dependency graph from schema $ref references.

    Args:
        schemas: Dictionary of schema definitions.
        ref_prefix: Reference prefix to match in schema references.

    Returns:
        Mapping from schema name to its set of dependencies.
    """
    deps: dict[str, set[str]] = {}
    for name, schema in schemas.items():
        schema_str = json.dumps(schema)
        deps[name] = {
            other
            for other in schemas
            if other != name and f'"$ref": "{ref_prefix}{other}"' in schema_str
        }
    return deps


def topological_sort_schemas(schemas: dict[str, Any], ref_prefix: str) -> list[str]:
    """Topological sort for Zod schema generation order.

    Args:
        schemas: Dictionary of schema definitions.
        ref_prefix: Reference prefix to match in schema references.

    Returns:
        List of schema names in dependency order.
    """
    deps = _build_schema_deps(schemas, ref_prefix)
    result: list[str] = []
    no_deps = [n for n, d in deps.items() if not d]
    while no_deps:
        name = no_deps.pop(0)
        result.append(name)
        for other, other_deps in deps.items():
            if name in other_deps:
                other_deps.remove(name)
                if not other_deps and other not in result:
                    no_deps.append(other)
    remaining = [n for n in schemas if n not in result]
    result.extend(remaining)
    return result


def generate_zod_ws(project_root: Path) -> None:
    """Generate Zod schemas for WebSocket messages.

    Args:
        project_root: Root directory of the project.
    """
    schema_file = project_root / "build" / _WS_SCHEMAS_FILE
    output_file = project_root / "frontend" / "src" / "lib" / "schemas" / "ws.generated.zod.ts"
    if not schema_file.exists():
        print(f"Error: {schema_file} not found. Run --export first.")
        return
    with open(schema_file, encoding="utf-8") as f:
        schema = json.load(f)
    definitions = schema.get("definitions", {})
    if not definitions:
        print("Error: No definitions found in build/ws-schemas.json")
        return
    sorted_names = topological_sort_schemas(definitions, _DEFS_REF_PREFIX)
    lines = [
        "/**",
        " * Generated Zod schemas for WebSocket message validation.",
        " * DO NOT EDIT - regenerate with: make ui-gen-zod",
        " */",
        "",
        "import { z } from 'zod/v4'",
        "",
    ]
    for name in sorted_names:
        definition = definitions[name]
        schema_def = generate_zod_schema_definition(name, definition, definitions)
        lines.append(schema_def)
        lines.append("")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text("\n".join(lines))
    print(f"Generated {output_file}")
    print(f"  - {len(definitions)} schemas")


def generate_zod_api(project_root: Path) -> None:
    """Generate Zod schemas for REST API.

    Args:
        project_root: Root directory of the project.
    """
    openapi_path = project_root / "build" / _OPENAPI_FILE
    output_path = project_root / "frontend" / "src" / "lib" / "schemas" / "api.generated.zod.ts"
    if not openapi_path.exists():
        print(f"Error: {openapi_path} not found. Run --export first.")
        return
    with open(openapi_path, encoding="utf-8") as f:
        openapi = json.load(f)
    schemas = openapi.get("components", {}).get("schemas", {})
    if not schemas:
        print("Error: No schemas found in build/openapi.json")
        return
    sorted_names = topological_sort_schemas(schemas, _COMPONENTS_REF_PREFIX)
    lines = [
        "/**",
        " * Generated Zod schemas for REST API validation.",
        " * DO NOT EDIT - regenerate with: make ui-gen-api-zod",
        " *",
        " * Each `NameSchema` export is cast to",
        " * `z.ZodType<Components['schemas'][Name]>` so the inferred output type",
        " * matches openapi-typescript's exact-optional emission (key-level `?:`",
        " * for nullable / default-None fields). Without the cast Zod's `.optional()`",
        " * would infer `T | undefined` at the value level and clash with the",
        " * OpenAPI-derived contract used everywhere else in the SPA under",
        " * `tsconfig.json` `exactOptionalPropertyTypes: true`.",
        " *",
        " * Each `Name` type export is sourced from `api.generated.ts` so the",
        " * SPA-facing type is exactly the openapi-typescript emission.",
        " */",
        "",
        "import { z } from 'zod'",
        "",
        "import type { Components } from '../../types/api.generated'",
        "",
    ]
    for name in sorted_names:
        schema = schemas[name]
        raw_definition = generate_zod_schema_definition(name, schema, schemas, exported=False)
        lines.append(raw_definition)
        lines.append("")
        lines.append(
            f"export const {name}Schema = _{name}RawSchema as unknown as "
            f"z.ZodType<Components['schemas']['{name}']>"
        )
        lines.append("")
    lines.append("// Type exports")
    for name in sorted_names:
        lines.append(f"export type {name} = Components['schemas']['{name}']")
    content = "\n".join(lines) + "\n"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content)
    print(f"Generated {output_path}")
    print(f"  - {len(schemas)} schemas")


def snake_to_camel(name: str) -> str:
    """Convert snake_case to camelCase.

    Args:
        name: String in snake_case format.

    Returns:
        String converted to camelCase format.
    """
    parts = name.split("_")
    return parts[0] + "".join(p.capitalize() for p in parts[1:])


def camel_to_lower(name: str) -> str:
    """Convert CamelCase to lowerCamelCase.

    Args:
        name: String in CamelCase format.

    Returns:
        String converted to lowerCamelCase format.
    """
    if not name:
        return name
    return name[0].lower() + name[1:]


def _resolve_anyof_entity(
    types: list[dict[str, Any]],
    field_name: str,
    all_schemas: dict[str, Any] | None,
    _visited: set[str] | None = None,
) -> str:
    """Resolve anyOf union to a TypeScript type string.

    Args:
        types: List of type schemas from anyOf.
        field_name: Name of the field being converted.
        all_schemas: All schema definitions for reference resolution.
        _visited: Tracks visited $ref names to detect cycles.

    Returns:
        TypeScript union type string.
    """
    non_null = [t for t in types if t.get("type") != "null"]
    has_null = any(t.get("type") == "null" for t in types)
    if len(non_null) == 1:
        base = json_type_to_ts_entity(non_null[0], field_name, True, all_schemas, _visited)
    else:
        parts = [
            json_type_to_ts_entity(t, field_name, True, all_schemas, _visited) for t in non_null
        ]
        base = " | ".join(parts)
    return f"{base} | null" if has_null else base


_SIMPLE_TYPE_MAP: dict[str, str] = {
    "integer": "number",
    "number": "number",
    "boolean": "boolean",
    "null": "null",
}


def _resolve_primitive_type(
    prop: dict[str, Any],
    prop_type: str,
    field_name: str,
    all_schemas: dict[str, Any] | None,
    _visited: set[str] | None = None,
) -> str:
    """Resolve a primitive JSON Schema type to TypeScript.

    Args:
        prop: Property schema dict.
        prop_type: The JSON Schema type string.
        field_name: Name of the field.
        all_schemas: All schema definitions for reference resolution.
        _visited: Tracks visited $ref names to detect cycles.

    Returns:
        TypeScript type string.
    """
    if prop_type in _SIMPLE_TYPE_MAP:
        return _SIMPLE_TYPE_MAP[prop_type]
    if prop_type == "string":
        if "enum" in prop:
            return " | ".join(f"'{v}'" for v in prop["enum"])
        return "string"
    if prop_type == "array":
        items = prop.get("items", {})
        item_type = json_type_to_ts_entity(items, field_name, True, all_schemas, _visited)
        return f"{item_type}[]"
    if prop_type == "object":
        additional = prop.get("additionalProperties")
        if additional and isinstance(additional, dict):
            val_type = json_type_to_ts_entity(additional, field_name, True, all_schemas, _visited)
            return f"Record<string, {val_type}>"
        return _TS_RECORD_UNKNOWN
    return "unknown"


_RECURSIVE_JSON_TYPES = frozenset({"JsonValue", "JsonObject", "JsonArray", "JsonPrimitive"})
_TS_RECORD_UNKNOWN = "Record<string, unknown>"
_ZOD_JSON_RECORD = "z.record(z.string(), z.any())"


def json_type_to_ts_entity(
    prop: dict[str, Any],
    field_name: str,
    required: bool,
    all_schemas: dict[str, Any] | None = None,
    _visited: set[str] | None = None,
) -> str:
    """Convert JSON Schema type to TypeScript type for entities.

    Args:
        prop: Property schema from JSON Schema.
        field_name: Name of the field being converted.
        required: Whether the field is required.
        all_schemas: All schema definitions for reference resolution.
        _visited: Tracks visited $ref names to detect cycles.

    Returns:
        TypeScript type string representation.
    """
    if _visited is None:
        _visited = set()

    if "$ref" in prop and all_schemas:
        ref_name = prop["$ref"].split("/")[-1]
        if ref_name in _RECURSIVE_JSON_TYPES:
            return _TS_RECORD_UNKNOWN if ref_name == "JsonObject" else "unknown"
        if ref_name in _visited:
            return "unknown"
        _visited.add(ref_name)
        return json_type_to_ts_entity(
            all_schemas.get(ref_name, {}), field_name, required, all_schemas, _visited
        )

    if "anyOf" in prop:
        return _resolve_anyof_entity(prop["anyOf"], field_name, all_schemas, _visited)

    if "allOf" in prop:
        return json_type_to_ts_entity(prop["allOf"][0], field_name, required, all_schemas, _visited)

    prop_type: str = prop.get("type", "")
    if prop_type == "string" and prop.get("format") == "date-time":
        return "Date"

    if field_name in ENTITY_UNION_ID_FIELDS:
        return "string | number"

    return _resolve_primitive_type(prop, prop_type, field_name, all_schemas, _visited)


_ENTITY_UNION_THRESHOLD = 3
_UNION_LINE_RE = re.compile(r"^(\s+\w+\??:\s+)((?:'[\w]+' \| )*'[\w]+')\s*$")

_KNOWN_UNION_ALIASES: dict[str, str] = {
    "'kraken' | 'walutomat' | 'polygon'": "MarketDataExchange",
    "'paper' | 'kraken' | 'walutomat'": "OrderExchange",
    "'buy' | 'sell'": "TradeSide",
    "'market' | 'limit' | 'stop' | 'stop_limit'": "OrderType",
    "'filled' | 'partial'": "FillStatus",
}


def extract_repeated_unions(lines: list[str]) -> list[str]:
    """Extract repeated inline string-literal unions as type aliases.

    Scans generated TypeScript interface lines for inline union types
    that appear at least ``_ENTITY_UNION_THRESHOLD`` times and replaces
    them with type alias references.

    Args:
        lines: Generated TypeScript code lines.

    Returns:
        Modified lines with type aliases extracted.
    """
    repeated = _find_repeated_entity_unions(lines)
    if not repeated:
        return lines

    alias_map = _build_entity_union_alias_map(repeated)
    result = _replace_entity_union_occurrences(lines, repeated, alias_map)
    insert_idx = _find_entity_union_alias_insert_index(result)
    alias_lines = _build_entity_union_alias_lines(alias_map)
    return _insert_entity_union_alias_lines(result, insert_idx, alias_lines)


def _find_repeated_entity_unions(lines: list[str]) -> dict[str, list[tuple[int, str]]]:
    """Return union occurrences that meet the extraction threshold.

    Args:
        lines: Generated TypeScript code lines.

    Returns:
        Mapping of union-string to list of (line_index, field_name) occurrences.
    """
    union_occurrences: dict[str, list[tuple[int, str]]] = {}
    for idx, line in enumerate(lines):
        match = _UNION_LINE_RE.match(line)
        if not match:
            continue
        union_str = match.group(2)
        prefix = match.group(1).strip()
        field_name = prefix.split(":")[0].rstrip("?").strip()
        union_occurrences.setdefault(union_str, []).append((idx, field_name))

    return {
        union_str: occurrences
        for union_str, occurrences in union_occurrences.items()
        if len(occurrences) >= _ENTITY_UNION_THRESHOLD
    }


def _build_entity_union_alias_map(
    repeated: dict[str, list[tuple[int, str]]],
) -> dict[str, str]:
    """Build union-string to alias name mapping.

    Uses _KNOWN_UNION_ALIASES for semantic names that match the Python
    source type (e.g., MarketDataExchange, OrderExchange). Falls back
    to PascalCase field name with numeric collision suffix.

    Args:
        repeated: Mapping of repeated union-string occurrences.

    Returns:
        Mapping from union string to alias name.
    """
    alias_map: dict[str, str] = {}
    used_names: set[str] = set()
    for union_str, occurrences in repeated.items():
        known = _KNOWN_UNION_ALIASES.get(union_str)
        if known:
            name = known
        else:
            camel = snake_to_camel(occurrences[0][1])
            base_name = camel[0].upper() + camel[1:]
            name = base_name
            counter = 2
            while name in used_names:
                name = f"{base_name}{counter}"
                counter += 1
        alias_map[union_str] = name
        used_names.add(name)
    return alias_map


def _replace_entity_union_occurrences(
    lines: list[str],
    repeated: dict[str, list[tuple[int, str]]],
    alias_map: dict[str, str],
) -> list[str]:
    """Replace inline unions with alias names.

    Args:
        lines: Original TypeScript lines.
        repeated: Repeated union occurrences.
        alias_map: Union-string to alias mapping.

    Returns:
        New list of lines with replacements applied.
    """
    result = list(lines)
    for union_str, occurrences in repeated.items():
        alias = alias_map[union_str]
        for idx, _ in occurrences:
            result[idx] = result[idx].replace(union_str, alias)
    return result


def _find_entity_union_alias_insert_index(lines: list[str]) -> int:
    """Find insertion index for alias declarations.

    Args:
        lines: TypeScript code lines.

    Returns:
        Index where type aliases should be inserted.
    """
    for i, line in enumerate(lines):
        if not line.startswith("export interface"):
            continue
        insert_idx = i
        while insert_idx > 0 and lines[insert_idx - 1].startswith((" *", "/**")):
            insert_idx -= 1
        return insert_idx
    return 0


def _build_entity_union_alias_lines(alias_map: dict[str, str]) -> list[str]:
    """Build TypeScript alias declaration lines.

    Args:
        alias_map: Union-string to alias mapping.

    Returns:
        TypeScript lines for alias declarations.
    """
    alias_lines = [f"type {alias} = {union_str}" for union_str, alias in alias_map.items()]
    alias_lines.append("")
    return alias_lines


def _insert_entity_union_alias_lines(
    lines: list[str],
    insert_idx: int,
    alias_lines: list[str],
) -> list[str]:
    """Insert alias lines into the TypeScript output.

    Args:
        lines: TypeScript code lines.
        insert_idx: Index to insert at.
        alias_lines: Alias declaration lines.

    Returns:
        New list of lines with alias declarations inserted.
    """
    result = list(lines)
    for offset, decl_line in enumerate(alias_lines):
        result.insert(insert_idx + offset, decl_line)
    return result


def generate_entity_interface(
    entity_name: str,
    schema: dict[str, Any],
    doc_comment: str | None = None,
    all_schemas: dict[str, Any] | None = None,
) -> list[str]:
    """Generate TypeScript interface for an entity.

    Args:
        entity_name: Name of the interface to generate.
        schema: JSON Schema definition for the entity.
        doc_comment: Optional documentation comment for the interface.
        all_schemas: All schema definitions for reference resolution.

    Returns:
        List of TypeScript code lines for the interface.
    """
    lines: list[str] = []

    if doc_comment:
        lines.append("/**")
        for line in doc_comment.split("\n"):
            lines.append(f" * {line}" if line else " *")
        lines.append(" */")

    lines.append(f"export interface {entity_name} {{")

    properties = schema.get("properties", {})
    required_fields = set(schema.get("required", []))

    for field_name, prop in properties.items():
        if field_name in ENTITY_EXCLUDE_FIELDS:
            continue

        camel_name = snake_to_camel(field_name)
        is_required = field_name in required_fields
        ts_type = json_type_to_ts_entity(prop, field_name, is_required, all_schemas)

        optional_marker = "" if is_required else "?"
        lines.append(f"  {camel_name}{optional_marker}: {ts_type}")

    lines.append("}")
    return lines


def derive_entity_name(schema_name: str, suffix: str) -> str:
    """Derive entity name from schema name by removing suffix and applying renames.

    Args:
        schema_name: Original schema name with suffix.
        suffix: Suffix to remove from the schema name.

    Returns:
        Derived entity name after suffix removal and rename mapping.
    """
    base_name = schema_name.removesuffix(suffix)
    return ENTITY_RENAMES.get(base_name, base_name)


def generate_entities(project_root: Path) -> None:
    """Generate TypeScript entity interfaces with Date types.

    Args:
        project_root: Root directory of the project.
    """
    ws_schema_path = project_root / "build" / _WS_SCHEMAS_FILE
    api_schema_path = project_root / "build" / _OPENAPI_FILE
    output_path = project_root / "frontend" / "src" / "types" / "entities.generated.ts"

    if not ws_schema_path.exists():
        print(f"Error: {ws_schema_path} not found. Run --export first.")
        return
    if not api_schema_path.exists():
        print(f"Error: {api_schema_path} not found. Run --openapi first.")
        return

    with open(ws_schema_path, encoding="utf-8") as f:
        ws_schemas = json.load(f).get("definitions", {})

    with open(api_schema_path, encoding="utf-8") as f:
        api_schemas = json.load(f).get("components", {}).get("schemas", {})

    all_schemas = {**ws_schemas, **api_schemas}

    lines = [
        "/**",
        " * Generated entity types with Date objects instead of ISO strings.",
        " * DO NOT EDIT - regenerate with: make ui-gen-entities",
        " *",
        " * These are canonical entity types for use in the application.",
        " * They differ from raw API/WS types by using Date instead of string",
        " * for datetime fields and camelCase for field names.",
        " */",
        "",
        "// Re-export common types from generated schemas",
        "export type {",
        "  Side1 as TradeSide,",
        "  OrderType,",
        "  Status2 as HeartbeatStatus,",
        "} from './ws.generated'",
        "",
    ]

    generated_entities: set[str] = set()

    data_count = 0
    for schema_name, schema in ws_schemas.items():
        if not schema_name.endswith(DATA_SUFFIX):
            continue
        entity_name = derive_entity_name(schema_name, DATA_SUFFIX)
        generated_entities.add(entity_name)
        doc = f"Canonical {entity_name} entity.\nFrom WebSocket {schema_name}."
        interface_lines = generate_entity_interface(entity_name, schema, doc, all_schemas)
        lines.extend(interface_lines)
        lines.append("")
        data_count += 1

    snapshot_count = 0
    for schema_name, schema in api_schemas.items():
        if not schema_name.endswith(SNAPSHOT_SUFFIX):
            continue
        entity_name = derive_entity_name(schema_name, SNAPSHOT_SUFFIX)
        if entity_name in generated_entities:
            continue
        generated_entities.add(entity_name)
        doc = f"Canonical {entity_name} entity.\nFrom REST API {schema_name}."
        interface_lines = generate_entity_interface(entity_name, schema, doc, all_schemas)
        lines.extend(interface_lines)
        lines.append("")
        snapshot_count += 1

    lines.append("")

    request_count = 0
    for schema_name, schema in api_schemas.items():
        if not schema_name.endswith(REQUEST_SUFFIX):
            continue
        entity_name = derive_entity_name(schema_name, REQUEST_SUFFIX)
        doc = f"{entity_name} request entity.\nUse with {camel_to_lower(entity_name)}ToAPI() transform."
        interface_lines = generate_entity_interface(entity_name, schema, doc, all_schemas)
        lines.extend(interface_lines)
        lines.append("")
        request_count += 1

    lines = extract_repeated_unions(lines)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines))
    print(f"Generated {output_path}")
    print(f"  - {data_count} WS data entities")
    print(f"  - {snapshot_count} API snapshot entities")
    print(f"  - {request_count} request entities")


_PERMISSIONS_TARGET = Path("frontend") / "src" / "types" / "permissions.generated.ts"
_IOS_PERMISSIONS_TARGET = Path("ios") / "Snapper" / "Models" / "Generated" / "Permissions.swift"


def generate_permissions(project_root: Path) -> None:
    """Generate TypeScript permissions constants from backend source of truth.

    Reads ``Permission`` enum and ``ROLE_PERMISSIONS`` mapping from the backend
    auth domain and produces a generated TypeScript module that the frontend
    imports instead of duplicating the data.

    Args:
        project_root: Root directory of the project.
    """
    output_path = project_root / _PERMISSIONS_TARGET

    permission_entries: list[str] = []
    for perm in Permission:
        const_name = perm.name
        permission_entries.append(f"  {const_name}: '{perm.value}',")

    role_entries: list[str] = []
    for role in UserRole:
        perms = sorted(p.value for p in BACKEND_ROLE_PERMISSIONS[role])
        perm_list = ", ".join(f"'{p}'" for p in perms)
        role_entries.append(f"  {role.value}: [{perm_list}],")

    resource_entries: list[str] = []
    for resource, required_perm in BACKEND_RESOURCE_PERMISSIONS.items():
        allowed_roles: list[str] = []
        for role in UserRole:
            if required_perm is None or required_perm in BACKEND_ROLE_PERMISSIONS[role]:
                allowed_roles.append(f"'{role.value}'")
        resource_entries.append(f"  '{resource}': [{', '.join(allowed_roles)}],")

    lines = [
        "/**",
        " * Generated permission types from backend source of truth.",
        " * DO NOT EDIT - regenerate with: make ui-gen-permissions",
        " */",
        "",
        "export const Permission = {",
        *permission_entries,
        _TS_CONST_OBJECT_CLOSE,
        "",
        "export type Permission = (typeof Permission)[keyof typeof Permission]",
        "",
        f"type UserRole = {' | '.join(repr(r.value) for r in UserRole)}",
        "",
        "export const ROLE_PERMISSIONS: Record<UserRole, readonly Permission[]> = {",
        *role_entries,
        _TS_CONST_OBJECT_CLOSE,
        "",
        "export const RESOURCE_ACCESS: Record<string, readonly UserRole[]> = {",
        *resource_entries,
        _TS_CONST_OBJECT_CLOSE,
        "",
    ]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines))
    print(f"Generated {output_path}")
    print(f"  - {len(list(Permission))} permissions")
    print(f"  - {len(list(UserRole))} roles")
    print(f"  - {len(BACKEND_RESOURCE_PERMISSIONS)} resources")


def _perm_name_to_swift_case(name: str) -> str:
    """Convert UPPER_SNAKE_CASE permission name to Swift camelCase.

    Args:
        name: Permission name in UPPER_SNAKE_CASE, e.g. ``READ_MARKET_DATA``.

    Returns:
        Swift-safe camelCase identifier, e.g. ``readMarketData``.
    """
    parts = name.lower().split("_")
    return parts[0] + "".join(p.capitalize() for p in parts[1:])


def _role_value_to_swift_case(value: str) -> str:
    """Return the Swift enum case name for a role string value.

    Applies the same transformation pipeline used when ``UserRole`` is emitted
    in ``APITypes.swift``: snake_case values like ``"ai_delegate"`` become
    camelCase (``"aiDelegate"``), then ``SWIFT_KEYWORD_RENAMES`` maps reserved
    words (e.g. ``"operator"`` → ``"operatorRole"``). Without the snake→camel
    step, the generated ``rolePermissions`` / ``resourceAccess`` dictionaries
    reference enum cases that do not exist.

    Args:
        value: Raw role string value, e.g. ``"operator"`` or ``"ai_delegate"``.

    Returns:
        Swift-safe case name, e.g. ``"operatorRole"`` or ``"aiDelegate"``.
    """
    swift_case = snake_to_camel(value) if "_" in value else value
    return SWIFT_KEYWORD_RENAMES.get(swift_case, swift_case)


def generate_ios_permissions(project_root: Path) -> None:
    """Generate Swift permissions constants from backend source of truth.

    Produces ``Permissions.swift`` in ``ios/Snapper/Models/Generated/`` with a
    ``Permission`` enum, ``rolePermissions`` dictionary, and ``resourceAccess``
    dictionary that mirror ``permissions.generated.ts`` used by the frontend.
    ``UserRole`` is deliberately omitted because it is already emitted by the
    API schema generator in ``APITypes.swift``.

    Args:
        project_root: Root directory of the project.
    """
    output_path = project_root / _IOS_PERMISSIONS_TARGET

    perm_cases: list[str] = []
    for perm in Permission:
        case_name = _perm_name_to_swift_case(perm.name)
        perm_cases.append(f'    case {case_name} = "{perm.value}"')

    role_perm_entries: list[str] = []
    for role in UserRole:
        swift_role = _role_value_to_swift_case(role.value)
        perms = sorted(BACKEND_ROLE_PERMISSIONS[role], key=lambda p: p.value)
        perm_list = ", ".join(f".{_perm_name_to_swift_case(p.name)}" for p in perms)
        role_perm_entries.append(f"    .{swift_role}: [{perm_list}],")

    resource_entries: list[str] = []
    for resource, required_perm in BACKEND_RESOURCE_PERMISSIONS.items():
        allowed_roles: list[str] = []
        for role in UserRole:
            if required_perm is None or required_perm in BACKEND_ROLE_PERMISSIONS[role]:
                allowed_roles.append(f".{_role_value_to_swift_case(role.value)}")
        resource_entries.append(f'    "{resource}": [{", ".join(allowed_roles)}],')

    lines = [
        *_SWIFT_HEADER_LINES,
        "enum Permission: String, CaseIterable, Codable, Sendable {",
        *perm_cases,
        "}",
        "",
        "let rolePermissions: [UserRole: [Permission]] = [",
        *role_perm_entries,
        "]",
        "",
        "let resourceAccess: [String: [UserRole]] = [",
        *resource_entries,
        "]",
        "",
    ]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines))
    print(f"Generated {output_path}")
    print(f"  - {len(list(Permission))} permissions")
    print(f"  - {len(list(UserRole))} roles")
    print(f"  - {len(BACKEND_RESOURCE_PERMISSIONS)} resources")


_BRIDGE_OUTPUT_DEFAULT = (
    Path("integrations") / "snapper-mcp" / "src" / "generated" / "wire-contract.ts"
)

_BRIDGE_HEADER = (
    "// AUTOGENERATED — DO NOT EDIT MANUALLY.\n"
    "// Regenerated from Snapper backend message schemas.\n"
    "//\n"
    "// Re-run the type generator after editing the source schemas;\n"
    "// this file is overwritten on every regeneration.\n"
)


def _bridge_allowlist() -> list[tuple[str, type[BaseModel]]]:
    """Return the alphabetised allow-list of Pydantic classes the bridge consumes.

    Defaults closed: new schemas added to the backend do not leak into
    the bridge OSS bundle until they are explicitly added here.
    Alphabetical ordering produces deterministic generator output and
    minimises diff churn when the list grows.

    Returns:
        Pairs of ``(class_name, model_class)`` ready for the emitter.
    """
    classes: list[type[BaseModel]] = [
        AiReviewCapsViolationFrameData,
        AiReviewDecisionAckFrameData,
        AiReviewRequestFrameData,
        OrderEventData,
        SignalData,
        WSAuthCompleteResponse,
        WSAuthExpiredResponse,
        WSAuthFailedResponse,
        WSAuthOkResponse,
        WSAuthRequiredResponse,
        WSAuthenticateRequest,
        WSErrorResponse,
        WSPingRequest,
        WSPongResponse,
        WSReauthOkResponse,
        WSReauthRequest,
        WSReauthRequiredResponse,
        WSSubscribeRequest,
        WSSubscriptionSuccessResponse,
    ]
    return sorted(((cls.__name__, cls) for cls in classes), key=lambda pair: pair[0])


def _bridge_resolve_ref(ref: str, defs: dict[str, Any]) -> dict[str, Any]:
    """Resolve a ``#/$defs/<Name>`` JSON-Schema ref against the supplied defs.

    Args:
        ref: JSON-Schema reference string.
        defs: ``$defs`` map from the parent schema.

    Returns:
        The referenced definition.

    Raises:
        ValueError: When the ref cannot be resolved or has an unsupported shape.
    """
    if not ref.startswith(_DEFS_REF_PREFIX) and not ref.startswith(_LEGACY_DEFS_REF_PREFIX):
        raise ValueError(f"Bridge emitter cannot resolve non-local ref: {ref}")
    name = ref.rsplit("/", 1)[-1]
    if name not in defs:
        raise ValueError(f"Bridge emitter missing $defs entry for {name}")
    resolved: dict[str, Any] = defs[name]
    return resolved


def _bridge_render_any_of(members: object, defs: dict[str, Any], prop: dict[str, Any]) -> str:
    """Render a nullable anyOf union with one non-null member."""
    if not isinstance(members, list) or not all(isinstance(member, dict) for member in members):
        raise ValueError(f"Bridge emitter cannot map malformed anyOf: {prop}")
    non_null = [member for member in members if member.get("type") != "null"]
    has_null = any(member.get("type") == "null" for member in members)
    if len(non_null) != 1:
        raise ValueError(
            f"Bridge emitter cannot map anyOf with {len(non_null)} non-null members: {prop}"
        )
    rendered = _bridge_render_type(non_null[0], defs)
    return f"{rendered} | null" if has_null else rendered


def _bridge_render_const(const_value: object, prop: dict[str, Any]) -> str:
    """Render a string JSON-Schema const as a TypeScript string literal."""
    if isinstance(const_value, str):
        return f'"{const_value}"'
    raise ValueError(f"Bridge emitter only supports string const values: {prop}")


def _bridge_render_enum(members: object, prop: dict[str, Any]) -> str:
    """Render a string enum as a TypeScript string-literal union."""
    if not isinstance(members, list) or not all(isinstance(member, str) for member in members):
        raise ValueError(f"Bridge emitter only supports string enums: {prop}")
    return " | ".join(f'"{member}"' for member in members)


def _bridge_render_array(prop: dict[str, Any], defs: dict[str, Any]) -> str:
    """Render an array schema whose items are another supported property schema."""
    items = prop.get("items")
    if not isinstance(items, dict):
        raise ValueError(f"Bridge emitter requires items dict on array: {prop}")
    return f"readonly {_bridge_render_type(items, defs)}[]"


def _bridge_render_type(prop: dict[str, Any], defs: dict[str, Any]) -> str:
    """Render a Pydantic-derived JSON-Schema property as a TypeScript type expression.

    The bridge emitter only needs to handle the property shapes that
    the allow-listed Pydantic classes actually produce. Anything more
    exotic raises so the schema author has to make a deliberate
    decision about how to expose the field on the bridge.

    Args:
        prop: JSON Schema fragment for a single property (already de-titled).
        defs: ``$defs`` map for ref resolution.

    Returns:
        TypeScript type expression (without ``readonly`` or trailing semicolon).
    """
    if "$ref" in prop:
        resolved = _bridge_resolve_ref(prop["$ref"], defs)
        return _bridge_render_type(resolved, defs)

    if "anyOf" in prop:
        return _bridge_render_any_of(prop["anyOf"], defs, prop)

    if "const" in prop:
        return _bridge_render_const(prop["const"], prop)

    if "enum" in prop:
        return _bridge_render_enum(prop["enum"], prop)

    schema_type = prop.get("type")
    if schema_type == "string":
        return "string"
    if schema_type in ("integer", "number"):
        return "number"
    if schema_type == "boolean":
        return "boolean"
    if schema_type == "array":
        return _bridge_render_array(prop, defs)
    if schema_type == "object":
        return "Readonly<Record<string, unknown>>"

    raise ValueError(f"Bridge emitter cannot render property: {prop}")


def _bridge_render_field(name: str, prop: dict[str, Any], defs: dict[str, Any]) -> str:
    """Render a single ``readonly <name>: <type>;`` line for an interface body.

    The ``type`` discriminator emits as a string-literal type rather
    than a generic string so consumers can switch on ``frame.type``
    with full narrowing.

    Args:
        name: Field name.
        prop: JSON Schema property fragment.
        defs: ``$defs`` map for ref resolution.

    Returns:
        Single TypeScript field line including the trailing newline.
    """
    if name == "type" and "const" in prop:
        return f'  readonly type: "{prop["const"]}";\n'
    rendered = _bridge_render_type(prop, defs)
    return f"  readonly {name}: {rendered};\n"


def _bridge_envelope_spec() -> tuple[frozenset[str], str]:
    """Derive the bridge ``FrameEnvelope`` shape from ``StrictDataSchema``.

    The bridge envelope mirrors whichever provenance fields the
    ``StrictDataSchema`` base currently declares (excluding the
    per-subclass ``type`` discriminator). Today that's the four
    fields ``session_id`` / ``sequence_id`` / ``public_id`` /
    ``timestamp``; once the upcoming topic-on-data work lands, the
    base picks up ``topic: str | None`` and this helper reflects the
    fifth field automatically — every regenerated bridge interface
    inherits it without further generator changes.

    Returns:
        A pair of:

        - ``frozenset`` of field names to skip when emitting per-class
          interface bodies (these live on ``FrameEnvelope`` instead).
        - The full ``export interface FrameEnvelope { ... }`` block
          ready to drop into the generated TS file. Field order
          mirrors the Pydantic dataclass declaration order.
    """
    schema = StrictDataSchema.model_json_schema(mode="serialization")
    properties: dict[str, Any] = schema.get("properties", {})
    defs: dict[str, Any] = schema.get(_DEFS_KEY, {})
    field_names: list[str] = []
    field_lines: list[str] = []
    for name, prop in properties.items():
        if name == "type":
            continue
        field_names.append(name)
        rendered = _bridge_render_type(prop, defs)
        field_lines.append(f"  readonly {name}: {rendered};\n")
    declaration = "export interface FrameEnvelope {\n" + "".join(field_lines) + "}\n"
    return (frozenset(field_names), declaration)


def _bridge_render_class(
    name: str,
    model: type[BaseModel],
    envelope_fields: frozenset[str],
) -> str:
    """Render a single ``export interface <Name> extends FrameEnvelope { ... }`` block.

    Envelope fields are excluded from the per-class body because they
    live on the shared ``FrameEnvelope`` parent. The set is supplied
    by the caller so a single derivation from ``StrictDataSchema``
    covers both the FrameEnvelope declaration and the per-class
    body's skip-list — the two cannot drift.

    Args:
        name: Backend Pydantic class name (preserved verbatim on the bridge side).
        model: The Pydantic class itself.
        envelope_fields: Field names that live on the shared
            ``FrameEnvelope`` parent and must NOT be re-emitted per
            class.

    Returns:
        Multi-line TypeScript interface declaration with trailing blank line separator
        prepared for joining.
    """
    schema: dict[str, Any] = model.model_json_schema(mode="serialization")
    defs: dict[str, Any] = schema.get(_DEFS_KEY, {})
    properties: dict[str, Any] = schema.get("properties", {})

    lines: list[str] = [f"export interface {name} extends FrameEnvelope {{\n"]
    for field_name, prop in properties.items():
        if field_name in envelope_fields:
            continue
        lines.append(_bridge_render_field(field_name, prop, defs))
    lines.append("}\n")
    return "".join(lines)


def generate_bridge_wire_contract(output_path: Path) -> str:
    """Generate the bridge wire-contract TypeScript file.

    Emits a header-only TS module containing a shared ``FrameEnvelope``
    interface plus one ``export interface`` per allow-listed Pydantic
    class. The output is deterministic: classes are emitted in
    alphabetical order, fields preserve their Pydantic dataclass-field
    declaration order, and no per-interface JSDoc is emitted (the
    output stays free of internal Snapper jargon by construction).

    Args:
        output_path: Absolute path to the generated TS file. The
            parent directory is created if missing. The file is
            overwritten unconditionally.

    Returns:
        The generated content (also written to disk).
    """
    envelope_fields, envelope_declaration = _bridge_envelope_spec()
    blocks: list[str] = [_BRIDGE_HEADER, "", envelope_declaration]
    for name, model in _bridge_allowlist():
        blocks.append("")
        blocks.append(_bridge_render_class(name, model, envelope_fields))
    content = "\n".join(blocks)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content, encoding="utf-8")
    print(f"Generated {output_path}")
    print(f"  - {len(_bridge_allowlist())} bridge interfaces")
    return content


class GenerateTypesArgs(argparse.Namespace):
    """Typed CLI arguments for the type generator."""

    openapi: bool
    export: bool
    frontend: bool
    frontend_ws: bool
    frontend_api: bool
    entities: bool
    permissions: bool
    ios: bool
    bridge: bool
    bridge_output: str | None
    strip_eslint_disable: bool
    postprocess_openapi_types: bool
    all: bool


def _strip_jsdoc_blocks(content: str, keep_first: bool = True) -> str:
    """Strip JSDoc ``/** ... */`` comment blocks from generated TypeScript.

    The openapi-typescript + json-schema-to-typescript generators emit a JSDoc
    block for every operation / schema / field carrying the backend's Python
    docstring. Those descriptions duplicate the Python source of truth and
    balloon the generated file by ~50%. Stripping them shrinks Sonar's
    analyzed-code surface dramatically without changing any type.

    When ``keep_first`` is True (default), the first JSDoc block is preserved
    because generators emit it as the "do not hand-edit" file header.
    """
    lines = content.splitlines(keepends=True)
    result: list[str] = []
    in_skip = False
    seen_first = False

    for line in lines:
        stripped = line.lstrip()
        if in_skip:
            if "*/" in line:
                in_skip = False
            continue
        if stripped.startswith("/**"):
            if keep_first and not seen_first:
                seen_first = True
                result.append(line)
                continue
            if "*/" in line[line.index("/**") + 3 :]:
                continue
            in_skip = True
            continue
        result.append(line)
    return "".join(result)


def postprocess_openapi_typescript_file(
    file_path: Path, *, openapi_spec_path: Path | None = None
) -> None:
    """Normalize openapi-typescript root export naming + strip JSDoc noise.

    This repository enforces naming conventions that prefer PascalCase for
    exported types. The `openapi-typescript` generator emits root exports named
    `paths`, `components`, and `operations`. This function renames those exports
    to `Paths`, `Components`, and `Operations` and updates intra-file references.

    Also strips JSDoc blocks (duplicate Python docstrings) to shrink the file
    footprint for static analysis tools.

    Args:
        file_path: Path to the generated `api.generated.ts` file.
        openapi_spec_path: Path to the source OpenAPI JSON. When provided,
            the response-side widening pass uses the spec's ``requestBody``
            references (transitively closed) to identify request-side schemas
            that must NOT be widened, instead of falling back to a name-suffix
            heuristic. Pass None to skip the widening entirely.
    """
    if not file_path.is_file():
        return

    content = file_path.read_text(encoding="utf-8")
    updated = content

    updated = re.sub(r"\bexport\s+interface\s+paths\b", "export interface Paths", updated)
    updated = re.sub(r"\bexport\s+interface\s+components\b", "export interface Components", updated)
    updated = re.sub(r"\bexport\s+interface\s+operations\b", "export interface Operations", updated)

    updated = re.sub(r"\bexport\s+type\s+paths\s*=", "export type Paths =", updated)
    updated = re.sub(r"\bexport\s+type\s+components\s*=", "export type Components =", updated)
    updated = re.sub(r"\bexport\s+type\s+operations\s*=", "export type Operations =", updated)

    updated = updated.replace("paths[", "Paths[")
    updated = updated.replace("components[", "Components[")
    updated = updated.replace("operations[", "Operations[")

    _json_type_replacements = {
        'Components["schemas"]["JsonValue"]': _TS_RECORD_UNKNOWN,
        'Components["schemas"]["JsonObject"]': _TS_RECORD_UNKNOWN,
        'Components["schemas"]["JsonPrimitive"]': "string | number | boolean | null",
        'Components["schemas"]["JsonArray"]': "unknown[]",
    }
    for old, new in _json_type_replacements.items():
        updated = updated.replace(old, new)

    if openapi_spec_path is not None and openapi_spec_path.is_file():
        request_schema_names = _request_schema_names_from_openapi(openapi_spec_path)
    else:
        request_schema_names = frozenset()
    updated = _widen_optional_nullable_to_undefined(updated, request_schema_names)
    updated = _strip_jsdoc_blocks(updated)

    if updated != content:
        file_path.write_text(updated, encoding="utf-8")


_OPTIONAL_NULLABLE_PATTERN = re.compile(r"(?P<prefix>\?:\s*[^;\n]*?\|\s*null)(?P<suffix>;)")
_SCHEMA_BLOCK_OPEN = re.compile(r"^\s+(?P<name>[A-Za-z][A-Za-z0-9_]*):\s*\{\s*$")
_OPENAPI_REF_PATTERN = re.compile(r"#/components/schemas/(\w+)")


def _widen_optional_nullable_to_undefined(
    content: str, request_schema_names: frozenset[str]
) -> str:
    """Widen ``key?: T | null;`` to ``key?: T | null | undefined;``.

    Reconciles ``openapi-typescript`` output (which omits ``| undefined`` on
    optional properties) with Zod-inferred response shapes (Zod's
    ``.nullable().optional()`` returns ``T | null | undefined``). Without this
    widening, ``exactOptionalPropertyTypes: true`` rejects the assignment of
    Zod's parse output to the OpenAPI type because the optional property's
    value type does not include ``undefined``.

    The patch is idempotent: a property already widened to
    ``key?: T | null | undefined;`` will not match the regex (the trailing
    ``| null`` capture requires a semicolon immediately after).

    Scope rules:

    1. Only ``key?: T | null;`` (trailing ``| null`` + semicolon) is widened.
       Plain ``key?: T;`` patterns are mostly request-body framework or
       discrimination (``?: never``); leaving them alone is safe.
    2. **Request-side schemas are excluded.** ``request_schema_names`` is the
       transitively-closed set of schema names reachable from any
       ``requestBody.$ref`` in the OpenAPI spec. Those types are JS→server
       inputs whose contract is "absent OR null, never explicit
       ``undefined``"; widening them would let callers pass literal
       ``undefined``, weakening the input contract.

    Args:
        content: Generated TypeScript source.
        request_schema_names: Schemas reachable from request bodies (including
            payload sub-schemas). Pass an empty frozenset to widen everything.

    Returns:
        Source with response-side optional nullable properties widened.
    """
    out_lines: list[str] = []
    block_stack: list[str | None] = []

    for line in content.splitlines(keepends=True):
        stripped_open = _SCHEMA_BLOCK_OPEN.match(line)
        if stripped_open:
            block_stack.append(stripped_open.group("name"))
        elif line.lstrip().startswith("};") or line.lstrip().startswith("}"):
            if block_stack:
                block_stack.pop()

        innermost_named_block = next(
            (name for name in reversed(block_stack) if name is not None),
            None,
        )
        is_request_block = (
            innermost_named_block is not None and innermost_named_block in request_schema_names
        )

        if is_request_block:
            out_lines.append(line)
            continue

        out_lines.append(_OPTIONAL_NULLABLE_PATTERN.sub(_widen_optional_nullable_replace, line))

    return "".join(out_lines)


def _widen_optional_nullable_replace(match: re.Match[str]) -> str:
    return f"{match.group('prefix')} | undefined{match.group('suffix')}"


def _request_schema_names_from_openapi(openapi_spec_path: Path) -> frozenset[str]:
    """Transitively close schemas reachable from any ``requestBody`` in the spec.

    Returns the canonical "do-not-widen" set: every schema that's reachable
    by walking ``$ref`` links starting from each operation's
    ``requestBody.content.*.schema``. Includes inner payload schemas wrapped
    in request envelopes (e.g. ``RefreshTokenPayload`` referenced via
    ``RefreshTokenRequest.payload``).

    Args:
        openapi_spec_path: Path to the OpenAPI JSON spec.

    Returns:
        Schema names that participate in any request body.
    """
    spec = json.loads(openapi_spec_path.read_text(encoding="utf-8"))
    schemas = spec.get("components", {}).get("schemas", {})

    request_roots: set[str] = set()
    for path_item in spec.get("paths", {}).values():
        if not isinstance(path_item, dict):
            continue
        for operation in path_item.values():
            if not isinstance(operation, dict):
                continue
            request_body = operation.get("requestBody")
            if isinstance(request_body, dict):
                _collect_refs(request_body, request_roots)

    seen: set[str] = set()
    pending = set(request_roots)
    while pending:
        name = pending.pop()
        if name in seen:
            continue
        seen.add(name)
        schema = schemas.get(name)
        if schema is None:
            continue
        sub_refs: set[str] = set()
        _collect_refs(schema, sub_refs)
        pending |= sub_refs - seen

    return frozenset(seen)


def _collect_refs(node: object, refs: set[str]) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "$ref" and isinstance(value, str):
                match = _OPENAPI_REF_PATTERN.match(value)
                if match:
                    refs.add(match.group(1))
            else:
                _collect_refs(value, refs)
    elif isinstance(node, list):
        for item in node:
            _collect_refs(item, refs)


def main() -> int:
    """Main entry point.

    Returns:
        Exit code (0 for success)
    """
    parser = argparse.ArgumentParser(
        description="Unified type generator for frontend and iOS",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python scripts/generate_types.py --openapi       # Export OpenAPI spec from FastAPI
    python scripts/generate_types.py --export        # Export JSON schemas only
    python scripts/generate_types.py --frontend      # Generate all Zod schemas
    python scripts/generate_types.py --frontend-ws   # Generate WS Zod schemas only
    python scripts/generate_types.py --frontend-api  # Generate API Zod schemas only
    python scripts/generate_types.py --entities      # Generate entity interfaces
    python scripts/generate_types.py --permissions   # Generate permissions types
    python scripts/generate_types.py --ios           # Generate Swift types
    python scripts/generate_types.py --all           # Everything (default)
        """,
    )
    parser.add_argument("--openapi", action="store_true", help="Export OpenAPI spec from FastAPI")
    parser.add_argument("--export", action="store_true", help="Export JSON schemas")
    parser.add_argument("--frontend", action="store_true", help="Generate all frontend Zod schemas")
    parser.add_argument("--frontend-ws", action="store_true", help="Generate WS Zod schemas only")
    parser.add_argument("--frontend-api", action="store_true", help="Generate API Zod schemas only")
    parser.add_argument("--entities", action="store_true", help="Generate entity interfaces")
    parser.add_argument(
        "--permissions", action="store_true", help="Generate frontend permissions types"
    )
    parser.add_argument("--ios", action="store_true", help="Generate iOS Swift types")
    parser.add_argument(
        "--bridge",
        action="store_true",
        help="Generate the snapper-mcp bridge wire-contract TypeScript file",
    )
    parser.add_argument(
        "--bridge-output",
        default=None,
        help=(
            "Override the bridge wire-contract output path "
            "(default: integrations/snapper-mcp/src/generated/wire-contract.ts)"
        ),
    )
    parser.add_argument(
        "--strip-eslint-disable",
        action="store_true",
        help="Strip eslint-disable comment from frontend ws.generated.ts",
    )
    parser.add_argument(
        "--postprocess-openapi-types",
        action="store_true",
        help="Post-process frontend api.generated.ts to use PascalCase root exports",
    )
    parser.add_argument("--all", action="store_true", help="All of the above (default)")
    args: GenerateTypesArgs = parser.parse_args(namespace=GenerateTypesArgs())

    project_root = Path(__file__).parent.parent

    if not _args_has_specific_targets(args):
        args.all = True

    _run_openapi_export(args, project_root)
    _run_schema_exports(args, project_root)
    _run_frontend_generators(args, project_root)
    _run_entity_generator(args, project_root)
    _run_permissions_generator(args, project_root)
    _run_ios_generator(args, project_root)
    _run_bridge_generator(args, project_root)
    _run_strip_eslint_disable(args, project_root)
    _run_openapi_typescript_postprocess(args, project_root)

    return 0


def _args_has_specific_targets(args: GenerateTypesArgs) -> bool:
    """Return True when at least one explicit generator target is requested.

    Args:
        args: Parsed CLI arguments.

    Returns:
        True when any target flag is set, False otherwise.
    """
    return any(
        [
            args.openapi,
            args.export,
            args.frontend,
            args.frontend_ws,
            args.frontend_api,
            args.entities,
            args.permissions,
            args.ios,
            args.bridge,
            bool(args.strip_eslint_disable),
            bool(args.postprocess_openapi_types),
        ]
    )


def _run_openapi_export(args: GenerateTypesArgs, project_root: Path) -> None:
    """Run OpenAPI export when requested."""
    if args.openapi or args.all:
        print("=== Exporting OpenAPI Spec ===")
        export_openapi_spec(project_root)


def _run_schema_exports(args: GenerateTypesArgs, project_root: Path) -> None:
    """Run schema exports when requested."""
    if args.export or args.all:
        print("=== Exporting JSON Schemas ===")
        export_ws_schemas(project_root)
        export_openapi_schemas(project_root)


def _run_frontend_generators(args: GenerateTypesArgs, project_root: Path) -> None:
    """Run frontend type generation depending on flags."""
    if args.frontend or args.all:
        print("\n=== Generating Frontend Types (Zod) ===")
        generate_zod_ws(project_root)
        generate_zod_api(project_root)
        return

    if args.frontend_ws:
        generate_zod_ws(project_root)
        return

    if args.frontend_api:
        generate_zod_api(project_root)


def _run_entity_generator(args: GenerateTypesArgs, project_root: Path) -> None:
    """Run entity interface generation when requested."""
    if args.entities or args.all:
        print("\n=== Generating Entity Interfaces ===")
        generate_entities(project_root)


def _run_permissions_generator(args: GenerateTypesArgs, project_root: Path) -> None:
    """Run permissions type generation when requested."""
    if args.permissions or args.all:
        print("\n=== Generating Permissions Types ===")
        generate_permissions(project_root)


def _run_ios_generator(args: GenerateTypesArgs, project_root: Path) -> None:
    """Run iOS type generation when requested."""
    if args.ios or args.all:
        print("\n=== Generating iOS Types (Swift) ===")
        generate_ios_types(project_root)


def _run_bridge_generator(args: GenerateTypesArgs, project_root: Path) -> None:
    """Run bridge wire-contract generation when requested.

    The bridge target is intentionally excluded from ``--all``: regen
    crosses a submodule boundary, and the bridge stack typecheck is
    not part of the main repo's default quality gate. Developers run
    ``make ts-bridge`` (or ``make bridge-regen``) to opt in.
    """
    if not args.bridge:
        return
    print("\n=== Generating Bridge Wire-Contract ===")
    if args.bridge_output is not None:
        output_path = Path(args.bridge_output)
        if not output_path.is_absolute():
            output_path = (project_root / output_path).resolve()
    else:
        output_path = (project_root / _BRIDGE_OUTPUT_DEFAULT).resolve()
    generate_bridge_wire_contract(output_path)


def strip_eslint_disable_file(file_path: Path) -> None:
    """Remove the eslint-disable header + JSDoc noise from a generated TS file.

    Args:
        file_path: Path to the generated file.
    """
    if not file_path.is_file():
        return

    content = file_path.read_text(encoding="utf-8")
    updated = content.replace("/* eslint-disable */\n", "")
    updated = _strip_jsdoc_blocks(updated)

    if updated != content:
        file_path.write_text(updated, encoding="utf-8")


def _run_strip_eslint_disable(args: GenerateTypesArgs, project_root: Path) -> None:
    """Strip eslint-disable header from the generated frontend file when requested."""
    if not args.strip_eslint_disable:
        return

    file_path = project_root.resolve() / _STRIP_ESLINT_DISABLE_TARGET
    strip_eslint_disable_file(file_path)


def _run_openapi_typescript_postprocess(args: GenerateTypesArgs, project_root: Path) -> None:
    """Post-process openapi-typescript output when requested."""
    if not args.postprocess_openapi_types:
        return

    file_path = project_root.resolve() / _OPENAPI_TYPESCRIPT_TARGET
    openapi_spec_path = project_root.resolve() / "build" / "openapi.json"
    postprocess_openapi_typescript_file(file_path, openapi_spec_path=openapi_spec_path)


if __name__ == "__main__":
    raise SystemExit(main())
