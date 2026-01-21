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
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from snapper.api.schemas.base import WsMessageSchema
from snapper.interface.websocket import schemas as ws_schemas
from snapper.messaging.schemas import messages as msg_schemas
from snapper.server.app import create_app

JsonValue = dict[str, Any] | list[Any] | str | int | float | bool | None

ENTITY_RENAMES: dict[str, str] = {}
ENVELOPE_SUFFIX = "Envelope"
SNAPSHOT_SUFFIX = "Snapshot"
REQUEST_SUFFIX = "Request"
ENTITY_EXCLUDE_FIELDS = {"type", "meta"}
ENTITY_UNION_ID_FIELDS = {"id", "order_id"}

SWIFT_KEYWORDS = {
    "operator",
    "class",
    "struct",
    "enum",
    "protocol",
    "extension",
    "func",
    "var",
    "let",
    "import",
    "return",
    "if",
    "else",
    "for",
    "while",
    "switch",
    "case",
    "default",
    "break",
    "continue",
    "in",
    "true",
    "false",
    "nil",
    "self",
    "Self",
    "super",
    "init",
    "deinit",
    "get",
    "set",
    "willSet",
    "didSet",
    "throws",
    "throw",
    "try",
    "catch",
    "as",
    "is",
    "Any",
    "Type",
    "static",
    "private",
    "public",
    "internal",
    "fileprivate",
    "open",
    "final",
    "override",
    "mutating",
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
            if key == "$ref" and isinstance(value, str) and value.startswith("#/$defs/"):
                result[key] = value.replace("#/$defs/", "#/definitions/")
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
                if value.startswith("#/components/schemas/"):
                    result[key] = value.replace("#/components/schemas/", "#/definitions/")
                else:
                    result[key] = value
            else:
                result[key] = fix_refs_openapi(value)
        return result
    if isinstance(obj, list):
        return [fix_refs_openapi(item) for item in obj]
    return obj


def make_const_fields_required(schema: dict[str, JsonValue]) -> dict[str, JsonValue]:
    """Ensure const fields are in required list for proper discriminator handling.

    Args:
        schema: JSON Schema dictionary to process.

    Returns:
        Schema with const fields added to required list.
    """
    if not isinstance(schema, dict):
        return schema
    result: dict[str, JsonValue] = {}
    const_fields: list[str] = []
    for key, value in schema.items():
        if key == "properties" and isinstance(value, dict):
            fixed_props: dict[str, JsonValue] = {}
            for prop_name, prop_schema in value.items():
                if isinstance(prop_schema, dict):
                    processed_schema = prop_schema
                    if "const" in prop_schema:
                        const_fields.append(prop_name)
                        if "default" in prop_schema:
                            processed_schema = dict(prop_schema)
                            del processed_schema["default"]
                    fixed_props[prop_name] = make_const_fields_required(processed_schema)
                else:
                    fixed_props[prop_name] = prop_schema
            result[key] = fixed_props
        elif isinstance(value, dict):
            result[key] = make_const_fields_required(value)
        elif isinstance(value, list):
            result[key] = [
                make_const_fields_required(item) if isinstance(item, dict) else item
                for item in value
            ]
        else:
            result[key] = value
    if const_fields:
        required = result.get("required", [])
        if isinstance(required, list):
            for field in const_fields:
                if field not in required:
                    required.append(field)
            result["required"] = required
    return result


def discover_ws_schemas() -> list[tuple[str, type[BaseModel]]]:
    """Auto-discover WebSocket schemas from snapper modules.

    Returns:
        List of tuples containing schema name and model class.
    """
    discovered: list[tuple[str, type[BaseModel]]] = []
    discovered.append(("WsMessageBase", WsMessageSchema))

    for name, obj in inspect.getmembers(msg_schemas):
        if (
            inspect.isclass(obj)
            and issubclass(obj, BaseModel)
            and obj is not BaseModel
            and name.endswith("Envelope")
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


def export_ws_schemas(project_root: Path) -> Path:
    """Export WebSocket Pydantic schemas to JSON Schema.

    Args:
        project_root: Root directory of the project.

    Returns:
        Path to the generated JSON schema file.
    """
    output_path = project_root / "build" / "ws-schemas.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    schemas = discover_ws_schemas()

    all_definitions: dict[str, JsonValue] = {}

    for name, model in schemas:
        schema: dict[str, JsonValue] = model.model_json_schema(mode="serialization")
        if "$defs" in schema:
            defs_value = schema["$defs"]
            if isinstance(defs_value, dict):
                for def_name, def_schema in defs_value.items():
                    all_definitions[def_name] = fix_refs_pydantic(def_schema)
            del schema["$defs"]
        schema_fixed = fix_refs_pydantic(schema)
        if isinstance(schema_fixed, dict):
            all_definitions[name] = schema_fixed

    combined_schema: dict[str, JsonValue] = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "WebSocket Messages",
        "description": "WebSocket message schemas for Snapper trading platform",
        "definitions": all_definitions,
        "oneOf": [{"$ref": f"#/definitions/{name}"} for name, _ in schemas],
    }
    combined_schema = make_const_fields_required(combined_schema)

    with output_path.open("w") as f:
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
    output_path = project_root / "build" / "openapi.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    app = create_app()
    spec = app.openapi()

    with output_path.open("w") as f:
        json.dump(spec, f, indent=2)

    print(f"Exported OpenAPI spec to {output_path}")
    return output_path


def export_openapi_schemas(project_root: Path) -> Path:
    """Export OpenAPI component schemas to JSON Schema format for quicktype.

    Args:
        project_root: Root directory of the project.

    Returns:
        Path to the generated JSON schema file.
    """
    openapi_path = project_root / "build" / "openapi.json"
    output_path = project_root / "build" / "openapi-schemas.json"

    with openapi_path.open() as f:
        openapi_spec = json.load(f)

    schemas = openapi_spec.get("components", {}).get("schemas", {})

    json_schema: dict[str, JsonValue] = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "APITypes",
        "description": "API schemas for Snapper trading platform",
        "definitions": {},
        "oneOf": [],
    }

    for name, schema in schemas.items():
        fixed_schema = fix_refs_openapi(schema)
        definitions = json_schema.get("definitions")
        one_of = json_schema.get("oneOf")
        if isinstance(definitions, dict):
            definitions[name] = fixed_schema
        if isinstance(one_of, list):
            one_of.append({"$ref": f"#/definitions/{name}"})

    with output_path.open("w") as f:
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
        ref_name = prop["$ref"].split("/")[-1]
        return f"{ref_name}{suffix}"

    if "anyOf" in prop:
        types = prop["anyOf"]
        non_null = [t for t in types if t.get("type") != "null"]
        if len(non_null) == 1:
            return json_type_to_swift(non_null[0], definitions, optional=True)
        return "AnyCodable?"

    if "allOf" in prop:
        return json_type_to_swift(prop["allOf"][0], definitions, optional)

    prop_type = prop.get("type")
    prop_format = prop.get("format")

    if prop_type == "string":
        if prop_format == "date-time":
            return f"Date{suffix}"
        return f"String{suffix}"

    if prop_type == "integer":
        return f"Int{suffix}"

    if prop_type == "number":
        return f"Double{suffix}"

    if prop_type == "boolean":
        return f"Bool{suffix}"

    if prop_type == "array":
        items = prop.get("items", {})
        item_type = json_type_to_swift(items, definitions, optional=False)
        return f"[{item_type}]{suffix}"

    if prop_type == "object":
        additional = prop.get("additionalProperties")
        if additional:
            if additional is True:
                return f"[String: AnyCodable]{suffix}"
            value_type = json_type_to_swift(additional, definitions, optional=False)
            return f"[String: {value_type}]{suffix}"
        return f"[String: AnyCodable]{suffix}"

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

        if "description" in prop_schema:
            lines.append(f"    /// {prop_schema['description']}")

        lines.append(f"    let {swift_name}: {swift_type}")

        if swift_name != prop_name:
            coding_keys.append((swift_name, prop_name))

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
        escaped_case = f"`{swift_case}`" if swift_case in SWIFT_KEYWORDS else swift_case

        if swift_case != value:
            lines.append(f'    case {escaped_case} = "{value}"')
        else:
            lines.append(f"    case {escaped_case}")
    lines.append("}")
    return lines


def get_any_codable_helper() -> list[str]:
    """Get AnyCodable helper struct for Swift.

    Returns:
        List of Swift code lines for the AnyCodable helper struct.
    """
    return [
        "/// Type-erased Codable value for dynamic JSON fields.",
        "struct AnyCodable: Codable, Sendable {",
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


def generate_swift_types(
    project_root: Path, schema_path: Path, output_path: Path, include_any_codable: bool = True
) -> None:
    """Generate Swift types from JSON Schema.

    Args:
        project_root: Root directory of the project.
        schema_path: Path to the JSON Schema file.
        output_path: Path for the generated Swift file.
        include_any_codable: Whether to include the AnyCodable helper.
    """
    with schema_path.open() as f:
        schema = json.load(f)

    definitions = schema.get("definitions", {})

    lines: list[str] = [
        "// This file was auto-generated from backend schemas.",
        "// DO NOT EDIT - regenerate with: make ios-gen-types",
        "",
        "import Foundation",
        "",
    ]
    if include_any_codable:
        lines.extend(get_any_codable_helper())

    generated_enums: set[str] = set()

    for name, type_schema in definitions.items():
        if type_schema.get("type") == "string" and "enum" in type_schema:
            lines.extend(generate_swift_enum(name, type_schema["enum"]))
            lines.append("")
            generated_enums.add(name)

    for name, type_schema in definitions.items():
        if type_schema.get("type") == "object":
            for prop_name, prop_schema in type_schema.get("properties", {}).items():
                if "enum" in prop_schema and prop_schema.get("type") == "string":
                    enum_name = f"{name}{prop_name.title().replace('_', '')}"
                    if enum_name not in generated_enums:
                        lines.extend(generate_swift_enum(enum_name, prop_schema["enum"]))
                        lines.append("")
                        generated_enums.add(enum_name)

    for name, type_schema in definitions.items():
        if type_schema.get("type") == "object":
            lines.extend(generate_swift_struct(name, type_schema, definitions))
            lines.append("")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines))
    print(f"Generated {output_path} ({len(definitions)} types)")


def generate_ios_types(project_root: Path) -> None:
    """Generate Swift types for iOS from JSON schemas.

    Args:
        project_root: Root directory of the project.
    """
    ios_gen_dir = project_root / "ios" / "Snapper" / "Models" / "Generated"
    ios_gen_dir.mkdir(parents=True, exist_ok=True)

    any_codable_path = ios_gen_dir / "AnyCodable.swift"
    any_codable_lines = [
        "// This file was auto-generated from backend schemas.",
        "// DO NOT EDIT - regenerate with: make ios-gen-types",
        "",
        "import Foundation",
        "",
    ]
    any_codable_lines.extend(get_any_codable_helper())
    any_codable_path.write_text("\n".join(any_codable_lines))
    print(f"Generated {any_codable_path}")

    ws_schema_path = project_root / "build" / "ws-schemas.json"
    if ws_schema_path.exists():
        generate_swift_types(
            project_root,
            ws_schema_path,
            ios_gen_dir / "WSMessages.swift",
            include_any_codable=False,
        )

    api_schema_path = project_root / "build" / "openapi-schemas.json"
    if api_schema_path.exists():
        generate_swift_types(
            project_root,
            api_schema_path,
            ios_gen_dir / "APITypes.swift",
            include_any_codable=False,
        )


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
        ref_path = prop["$ref"]
        ref_name = ref_path.split("/")[-1]
        return f"{ref_name}Schema"
    if "anyOf" in prop:
        types = prop["anyOf"]
        non_null = [t for t in types if t.get("type") != "null"]
        has_null = any(t.get("type") == "null" for t in types)
        if len(non_null) == 1:
            base = json_type_to_zod(non_null[0], True, definitions)
            if has_null:
                return f"{base}.nullable()"
            return base
        zod_types = [json_type_to_zod(t, True, definitions) for t in non_null]
        union = f"z.union([{', '.join(zod_types)}])"
        if has_null:
            return f"{union}.nullable()"
        return union
    if "allOf" in prop:
        return json_type_to_zod(prop["allOf"][0], required, definitions)
    prop_type = prop.get("type")
    prop_format = prop.get("format")
    min_length = prop.get("minLength")
    max_length = prop.get("maxLength")
    if prop_type == "string":
        if "const" in prop:
            return f"z.literal('{prop['const']}')"
        if prop_format == "date-time":
            return "z.string().datetime()"
        if prop_format == "uuid":
            return "z.string().uuid()"
        if prop_format == "email":
            return "z.string().email()"
        if "enum" in prop:
            enum_values = ", ".join(f"'{v}'" for v in prop["enum"])
            return f"z.enum([{enum_values}])"
        result = "z.string()"
        if min_length is not None:
            result += f".min({min_length})"
        if max_length is not None:
            result += f".max({max_length})"
        return result
    if prop_type == "integer":
        return "z.number().int()"
    if prop_type == "number":
        return "z.number()"
    if prop_type == "boolean":
        return "z.boolean()"
    if prop_type == "null":
        return "z.null()"
    if prop_type == "array":
        items = prop.get("items", {})
        item_type = json_type_to_zod(items, True, definitions)
        return f"z.array({item_type})"
    if prop_type == "object":
        if "properties" in prop:
            return generate_zod_object_schema(prop, definitions)
        additional = prop.get("additionalProperties")
        if additional:
            if additional is True:
                return "z.record(z.string(), z.unknown())"
            value_type = json_type_to_zod(additional, True, definitions)
            return f"z.record(z.string(), {value_type})"
        return "z.object({}).passthrough()"
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
        is_required = name in required_fields or has_default
        zod_type = json_type_to_zod(prop, is_required, definitions)
        if not is_required:
            zod_type = f"{zod_type}.optional()"
        fields.append(f"    {name}: {zod_type},")
    fields_str = "\n".join(fields)
    return f"z.object({{\n{fields_str}\n  }}).strict()"


def generate_zod_schema_definition(
    name: str, schema: dict[str, Any], definitions: dict[str, Any]
) -> str:
    """Generate Zod schema definition from JSON Schema.

    Args:
        name: Name of the schema to generate.
        schema: JSON Schema definition.
        definitions: Schema definitions for reference resolution.

    Returns:
        Zod schema definition as an export statement.
    """
    if schema.get("type") == "string" and "enum" in schema:
        enum_values = ", ".join(f"'{v}'" for v in schema["enum"])
        return f"export const {name}Schema = z.enum([{enum_values}])"
    if schema.get("type") == "object":
        zod_schema = generate_zod_object_schema(schema, definitions)
        return f"export const {name}Schema = {zod_schema}"
    if schema.get("type") == "array":
        items = schema.get("items", {})
        item_type = json_type_to_zod(items, True, definitions)
        return f"export const {name}Schema = z.array({item_type})"
    zod_type = json_type_to_zod(schema, True, definitions)
    return f"export const {name}Schema = {zod_type}"


def topological_sort_schemas(schemas: dict[str, Any], ref_prefix: str) -> list[str]:
    """Topological sort for Zod schema generation order.

    Args:
        schemas: Dictionary of schema definitions.
        ref_prefix: Reference prefix to match in schema references.

    Returns:
        List of schema names in dependency order.
    """
    deps: dict[str, set[str]] = {}
    for name, schema in schemas.items():
        deps[name] = set()
        schema_str = json.dumps(schema)
        for other_name in schemas:
            if other_name != name and f'"$ref": "{ref_prefix}{other_name}"' in schema_str:
                deps[name].add(other_name)
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
    for name in schemas:
        if name not in result:
            result.append(name)
    return result


def generate_zod_ws(project_root: Path) -> None:
    """Generate Zod schemas for WebSocket messages.

    Args:
        project_root: Root directory of the project.
    """
    schema_file = project_root / "build" / "ws-schemas.json"
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
    sorted_names = topological_sort_schemas(definitions, "#/definitions/")
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
    openapi_path = project_root / "build" / "openapi.json"
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
    sorted_names = topological_sort_schemas(schemas, "#/components/schemas/")
    lines = [
        "/**",
        " * Generated Zod schemas for REST API validation.",
        " * DO NOT EDIT - regenerate with: make ui-gen-api-zod",
        " */",
        "",
        "import { z } from 'zod'",
        "",
    ]
    for name in sorted_names:
        schema = schemas[name]
        definition = generate_zod_schema_definition(name, schema, schemas)
        lines.append(definition)
        lines.append("")
    lines.append("// Type exports")
    for name in sorted_names:
        lines.append(f"export type {name} = z.infer<typeof {name}Schema>")
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


def json_type_to_ts_entity(
    prop: dict[str, Any],
    field_name: str,
    required: bool,
    all_schemas: dict[str, Any] | None = None,
) -> str:
    """Convert JSON Schema type to TypeScript type for entities.

    Args:
        prop: Property schema from JSON Schema.
        field_name: Name of the field being converted.
        required: Whether the field is required.
        all_schemas: All schema definitions for reference resolution.

    Returns:
        TypeScript type string representation.
    """
    if "$ref" in prop and all_schemas:
        ref_path = prop["$ref"]
        ref_name = ref_path.split("/")[-1]
        ref_schema = all_schemas.get(ref_name, {})
        return json_type_to_ts_entity(ref_schema, field_name, required, all_schemas)

    if "anyOf" in prop:
        types = prop["anyOf"]
        non_null = [t for t in types if t.get("type") != "null"]
        has_null = any(t.get("type") == "null" for t in types)
        if len(non_null) == 1:
            base = json_type_to_ts_entity(non_null[0], field_name, True, all_schemas)
            if has_null:
                return f"{base} | null"
            return base
        ts_types = [json_type_to_ts_entity(t, field_name, True, all_schemas) for t in non_null]
        union = " | ".join(ts_types)
        if has_null:
            return f"{union} | null"
        return union

    if "allOf" in prop:
        return json_type_to_ts_entity(prop["allOf"][0], field_name, required, all_schemas)

    prop_type = prop.get("type")
    prop_format = prop.get("format")

    if prop_type == "string" and prop_format == "date-time":
        return "Date"

    if field_name in ENTITY_UNION_ID_FIELDS:
        return "string | number"

    if prop_type == "string":
        if "enum" in prop:
            return " | ".join(f"'{v}'" for v in prop["enum"])
        return "string"
    if prop_type in ("integer", "number"):
        return "number"
    if prop_type == "boolean":
        return "boolean"
    if prop_type == "array":
        items = prop.get("items", {})
        item_type = json_type_to_ts_entity(items, field_name, True, all_schemas)
        return f"{item_type}[]"
    if prop_type == "object":
        additional = prop.get("additionalProperties")
        if additional and isinstance(additional, dict):
            val_type = json_type_to_ts_entity(additional, field_name, True, all_schemas)
            return f"Record<string, {val_type}>"
        return "Record<string, unknown>"
    if prop_type == "null":
        return "null"

    return "unknown"


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
    ws_schema_path = project_root / "build" / "ws-schemas.json"
    api_schema_path = project_root / "build" / "openapi.json"
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
        "import type {",
        "  Side1 as TradeSide,",
        "  OrderType,",
        "  Status2 as HeartbeatStatus,",
        "} from './ws.generated'",
        "",
        "export type { TradeSide, OrderType, HeartbeatStatus }",
        "",
    ]

    generated_entities: set[str] = set()

    envelope_count = 0
    for schema_name, schema in ws_schemas.items():
        if not schema_name.endswith(ENVELOPE_SUFFIX):
            continue
        entity_name = derive_entity_name(schema_name, ENVELOPE_SUFFIX)
        generated_entities.add(entity_name)
        doc = f"Canonical {entity_name} entity.\nFrom WebSocket {schema_name}."
        interface_lines = generate_entity_interface(entity_name, schema, doc, all_schemas)
        lines.extend(interface_lines)
        lines.append("")
        envelope_count += 1

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

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines))
    print(f"Generated {output_path}")
    print(f"  - {envelope_count} WS envelope entities")
    print(f"  - {snapshot_count} API snapshot entities")
    print(f"  - {request_count} request entities")


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
    parser.add_argument("--ios", action="store_true", help="Generate iOS Swift types")
    parser.add_argument("--all", action="store_true", help="All of the above (default)")
    args = parser.parse_args()

    project_root = Path(__file__).parent.parent

    has_specific = any(
        [
            args.openapi,
            args.export,
            args.frontend,
            args.frontend_ws,
            args.frontend_api,
            args.entities,
            args.ios,
        ]
    )
    if not has_specific:
        args.all = True

    if args.openapi or args.all:
        print("=== Exporting OpenAPI Spec ===")
        export_openapi_spec(project_root)

    if args.export or args.all:
        print("=== Exporting JSON Schemas ===")
        export_ws_schemas(project_root)
        export_openapi_schemas(project_root)

    if args.frontend or args.all:
        print("\n=== Generating Frontend Types (Zod) ===")
        generate_zod_ws(project_root)
        generate_zod_api(project_root)
    elif args.frontend_ws:
        generate_zod_ws(project_root)
    elif args.frontend_api:
        generate_zod_api(project_root)

    if args.entities or args.all:
        print("\n=== Generating Entity Interfaces ===")
        generate_entities(project_root)

    if args.ios or args.all:
        print("\n=== Generating iOS Types (Swift) ===")
        generate_ios_types(project_root)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
