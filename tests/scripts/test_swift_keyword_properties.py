"""Regression coverage for Swift identifiers derived from backend wire fields."""

from scripts.generate_types import generate_swift_enum
from scripts.generate_types import generate_swift_struct
from snapper.api.schemas.android_devices import RegisterAndroidDeviceBody


def test_actual_android_registration_preserves_required_protocol_wire_field() -> None:
    """Preserve the real registration contract in generated Swift.

    Given:
        The actual backend registration schema requires protocol.
    When:
        The schema is rendered as a Codable Swift structure.
    Then:
        Protocol is escaped and existing wire aliases are retained.
    """
    schema = RegisterAndroidDeviceBody.model_json_schema()
    result = "\n".join(generate_swift_struct("RegisterAndroidDeviceBody", schema, {}))
    assert "let `protocol`: String" in result
    assert "case `protocol`" in result
    assert 'case expectedRevision = "expected_revision"' in result
    assert "protocolValue" not in result


def test_keyword_only_property_uses_synthesized_codable_key() -> None:
    """Keep synthesized Codable keys stable for keyword-only payloads.

    Given:
        A payload has a required protocol property.
    When:
        The generator emits its stored property.
    Then:
        Escaping changes syntax without inventing a CodingKeys alias.
    """
    schema = {"properties": {"protocol": {"type": "string"}}, "required": ["protocol"]}
    result = "\n".join(generate_swift_struct("ProtocolPayload", schema, {}))
    assert "let `protocol`: String" in result
    assert "CodingKeys" not in result


def test_camel_cased_keyword_keeps_original_wire_name() -> None:
    """Preserve a wire alias that camel-cases into a Swift keyword.

    Given:
        The backend field has a trailing underscore: protocol_.
    When:
        The generator camel-cases and escapes the property.
    Then:
        The CodingKeys raw value preserves the original spelling.
    """
    schema = {"properties": {"protocol_": {"type": "string"}}, "required": ["protocol_"]}
    result = "\n".join(generate_swift_struct("ProtocolAlias", schema, {}))
    assert "let `protocol`: String" in result
    assert 'case `protocol` = "protocol_"' in result


def test_existing_keyword_enum_case_keeps_its_established_name() -> None:
    """Preserve established keyword enum cases for existing consumers.

    Given:
        An enum contains protocol and an ordinary snake-case value.
    When:
        The generator renders the enum.
    Then:
        Both established case names retain their original raw values.
    """
    result = "\n".join(generate_swift_enum("TransportKind", ["protocol", "fcm_fid"]))
    assert 'case protocolValue = "protocol"' in result
    assert 'case fcmFid = "fcm_fid"' in result
