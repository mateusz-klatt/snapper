"""Self-contained JSON type aliases for the delegate agent plane.

Duplicated locally rather than imported from ``snapper.core.json_types`` so the
agent plane carries ZERO ``snapper.*`` imports. That keeps the runner a pure
client a minimal blackbox image can run without any backend source, migrations,
or seeds on disk. The boundary guard's default allowlist is empty as a result;
see ``scripts/check_delegate_boundary.py``.

Types:
    JsonPrimitive: Leaf JSON values (str, int, float, bool, None).
    JsonValue: Any valid JSON value (recursive).
    JsonArray: JSON array (list of JsonValue).
    JsonObject: JSON object (dict with string keys and JsonValue values).
"""

type JsonPrimitive = str | int | float | bool | None
type JsonValue = JsonPrimitive | list[JsonValue] | dict[str, JsonValue]
type JsonArray = list[JsonValue]
type JsonObject = dict[str, JsonValue]

__all__ = [
    "JsonArray",
    "JsonObject",
    "JsonPrimitive",
    "JsonValue",
]
