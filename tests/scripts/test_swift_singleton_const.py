"""Regression coverage for named singleton string constants in generated iOS types."""

import json
from pathlib import Path

import pytest

from scripts.generate_types import generate_ios_types
from scripts.generate_types import generate_swift_types
from snapper.core.json_types import JsonObject


def test_ios_generation_declares_referenced_singleton_const_once(tmp_path: Path) -> None:
    """Declare a shared singleton string type without changing field optionality.

    Given:
        API and WebSocket schemas reference the same named string constant.
    When:
        The real iOS generation pipeline renders both schema files.
    Then:
        One enum serves required, nullable and defaulted fields in both files.
    """
    schema: JsonObject = {
        "definitions": {
            "FxExclusionReason": {
                "type": "string",
                "const": "fx_venue_not_selected",
            },
            "FreshnessStatus": {"type": "string", "enum": ["fresh", "stale"]},
            "Exclusion": {
                "type": "object",
                "properties": {
                    "reason": {"$ref": "#/definitions/FxExclusionReason"},
                    "nullable_reason": {
                        "anyOf": [
                            {"$ref": "#/definitions/FxExclusionReason"},
                            {"type": "null"},
                        ]
                    },
                    "default_reason": {
                        "$ref": "#/definitions/FxExclusionReason",
                        "default": "fx_venue_not_selected",
                    },
                    "status": {"$ref": "#/definitions/FreshnessStatus"},
                    "type": {
                        "type": "string",
                        "const": "exclusion",
                        "default": "exclusion",
                    },
                },
                "required": ["reason", "nullable_reason", "status"],
            },
        }
    }
    build_dir = tmp_path / "build"
    build_dir.mkdir()
    for filename in ("openapi-schemas.json", "ws-schemas.json"):
        (build_dir / filename).write_text(json.dumps(schema), encoding="utf-8")

    generate_ios_types(tmp_path)

    output_dir = tmp_path / "ios" / "Snapper" / "Models" / "Generated"
    api_content = (output_dir / "APITypes.swift").read_text(encoding="utf-8")
    ws_content = (output_dir / "WSMessages.swift").read_text(encoding="utf-8")
    all_content = api_content + ws_content
    assert all_content.count("enum FxExclusionReason: String, Codable, Sendable {") == 1
    assert 'case fxVenueNotSelected = "fx_venue_not_selected"' in api_content
    assert "    let reason: FxExclusionReason\n" in api_content
    assert "    let nullableReason: FxExclusionReason?\n" in api_content
    assert "    let defaultReason: FxExclusionReason?\n" in api_content
    assert 'case nullableReason = "nullable_reason"' in api_content
    assert 'case defaultReason = "default_reason"' in api_content
    assert all_content.count("enum FreshnessStatus: String, Codable, Sendable {") == 1
    assert "    case fresh\n    case stale\n" in api_content
    assert "    let status: FreshnessStatus\n" in api_content
    assert "    let type: String?\n" in api_content
    assert "enum ExclusionType" not in all_content
    assert "struct Exclusion" not in ws_content


@pytest.mark.parametrize(
    "definition",
    [
        {"type": "string"},
        {"type": "integer", "const": 1},
        {"type": "string", "const": 1},
    ],
)
def test_singleton_enum_generation_leaves_other_definitions_unchanged(
    tmp_path: Path, definition: JsonObject
) -> None:
    """Keep the singleton fix limited to named string constants.

    Given:
        A definition has no constant or has a non-string constant value.
    When:
        The Swift generator collects named enum declarations.
    Then:
        It preserves the existing behavior without creating a spurious enum.
    """
    schema_path = tmp_path / "schema.json"
    schema_path.write_text(json.dumps({"definitions": {"Unchanged": definition}}), encoding="utf-8")
    output_path = tmp_path / "Types.swift"

    emitted = generate_swift_types(tmp_path, schema_path, output_path)

    assert emitted == (set(), set())
    assert "enum Unchanged" not in output_path.read_text(encoding="utf-8")
