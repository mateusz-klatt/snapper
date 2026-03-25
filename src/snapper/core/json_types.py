"""Type aliases for JSON-shaped data.

Replaces ``dict[str, Any]`` where the value is genuinely arbitrary JSON,
documenting intent ("this is JSON") vs ("I gave up typing").

No runtime impact — pure type annotation improvement.

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
