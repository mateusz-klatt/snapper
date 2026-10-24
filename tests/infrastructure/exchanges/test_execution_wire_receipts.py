"""Verify original private payload capture without altering public SDK decoding."""

import json
from decimal import Decimal
from types import ModuleType

import kraken.futures.websocket as futures_websocket
import kraken.spot.websocket.connectors as spot_websocket
import pytest

from snapper.infrastructure.exchanges.execution_wire_receipts import PrivateExecutionJsonCodec
from snapper.infrastructure.exchanges.execution_wire_receipts import PrivateWireMessage
from snapper.infrastructure.exchanges.execution_wire_receipts import _install_connector_codec
from snapper.infrastructure.exchanges.execution_wire_receipts import (
    decode_private_execution_receipt,
)
from snapper.infrastructure.exchanges.execution_wire_receipts import (
    install_private_execution_wire_capture,
)
from snapper.infrastructure.exchanges.execution_wire_receipts import private_message_wire_bytes


@pytest.mark.parametrize(
    "selector",
    [
        '"feed":"fills"',
        '"feed":"fills_snapshot"',
        '"feed":"account_log"',
        '"feed":"account_log_snapshot"',
        '"feed":"open_orders"',
        '"feed":"open_orders_snapshot"',
        '"feed":"open_positions"',
        '"feed":"open_positions_snapshot"',
        '"feed":"balances"',
        '"feed":"balances_snapshot"',
        '"channel":"executions"',
        '"channel":"balances"',
    ],
)
def test_private_payload_retains_original_bytes_and_decimal_value(selector: str) -> None:
    """Preserve original payload spelling and exact financial operands.

    Given: A private message with a decimal beyond binary-float precision,
    When: The SDK boundary codec decodes the payload,
    Then: Its original UTF-8 JSON and exact Decimal value remain available together.
    """
    source = "{ " + selector + ', "qty": 0.123456789012345678901, "account":"żółć" }'

    decoded = PrivateExecutionJsonCodec.loads(source)

    assert isinstance(decoded, PrivateWireMessage)
    assert decoded["qty"] == Decimal("0.123456789012345678901")
    assert decoded["account"] == "żółć"
    assert private_message_wire_bytes(decoded) == source.encode("utf-8")
    assert set(decoded) == {selector.split('"')[1], "qty", "account"}


@pytest.mark.parametrize(
    "source", [b'{"feed":"fills","qty":1.20}', bytearray(b'{"feed":"fills","qty":1.20}')]
)
def test_binary_payload_retains_original_bytes(source: bytes | bytearray) -> None:
    """Capture byte-oriented SDK inputs without a JSON roundtrip.

    Given: A private payload supplied as bytes or bytearray,
    When: The boundary decoder parses its numeric value,
    Then: Receipt bytes match the original and decimal trailing zeros are retained.
    """
    decoded = PrivateExecutionJsonCodec.loads(source)

    assert isinstance(decoded, PrivateWireMessage)
    assert decoded["qty"] == Decimal("1.20")
    assert str(decoded["qty"]) == "1.20"
    assert decoded.wire_bytes == bytes(source)


@pytest.mark.parametrize(
    "source",
    [
        '{"feed":"ticker","price":0.1}',
        '{"channel":"ticker","price":0.1}',
        '{"feed":[],"channel":{},"price":0.1}',
        '[0.1, {"feed":"fills"}]',
        "null",
        '{"feed":"fills","event":"subscribed","api_key":"test-public-key"}',
    ],
)
def test_public_and_subscription_messages_keep_standard_decoding(source: str) -> None:
    """Keep public traffic and authentication acknowledgements outside private capture.

    Given: A public message or subscription acknowledgement,
    When: It passes through the SDK codec,
    Then: Its ordinary JSON result is unchanged and no private receipt is attached.
    """
    decoded = PrivateExecutionJsonCodec.loads(source)

    assert decoded == json.loads(source)
    assert not isinstance(decoded, PrivateWireMessage)


def test_explicit_decoder_options_are_preserved() -> None:
    """Respect explicit SDK decoder options without claiming default boundary capture.

    Given: A private payload and a caller-supplied numeric parser,
    When: The codec receives those explicit options,
    Then: It uses the requested parser and leaves the result unqualified as raw evidence.
    """
    decoded = PrivateExecutionJsonCodec.loads('{"feed":"fills","qty":1.20}', parse_float=str)

    assert decoded == {"feed": "fills", "qty": "1.20"}
    assert not isinstance(decoded, PrivateWireMessage)


def test_receipt_does_not_change_when_decoded_mapping_changes() -> None:
    """Keep original evidence independent of later callback normalization.

    Given: A captured private payload,
    When: A downstream callback changes a decoded field,
    Then: The original receipt still contains the unmodified provider payload.
    """
    source = '{"feed":"fills","qty":1.20}'
    decoded = PrivateExecutionJsonCodec.loads(source)
    assert isinstance(decoded, PrivateWireMessage)

    decoded["qty"] = "changed"

    assert decoded.wire_bytes == source.encode()


def test_provider_key_cannot_spoof_boundary_metadata() -> None:
    """Require receipt metadata from the boundary object rather than a provider key.

    Given: An ordinary mapping claiming to contain original wire bytes,
    When: A caller asks for boundary receipt metadata,
    Then: The unqualified mapping provides no original receipt.
    """
    assert private_message_wire_bytes({"wire_bytes": b"claimed", "feed": "fills"}) is None


@pytest.mark.parametrize(
    "control",
    (
        {"event": "subscribed_failed"},
        {"event": "unsubscribed_failed"},
        {"event": " CHALLENGE "},
        {"method": "subscribe"},
        {"token": "synthetic-control-only"},
        {"signed_challenge": "synthetic-control-only"},
        {"python-kraken-sdk": {"error": "synthetic-control-only"}},
    ),
)
def test_control_and_authentication_envelopes_never_receive_wire_qualification(
    control: dict[str, object],
) -> None:
    """A financial feed name cannot turn an authentication/control envelope into captured evidence.

    Given: A private feed label combined with a known control or authentication discriminator,
    When: The actual SDK codec decodes that envelope,
    Then: No genuine private receipt metadata is attached.
    """
    message = {"feed": "fills", "qty": 0.1, **control}
    decoded = PrivateExecutionJsonCodec.loads(json.dumps(message))
    assert not isinstance(decoded, PrivateWireMessage)


def test_financial_message_can_have_an_unrelated_event_field() -> None:
    """A genuine financial field named event does not automatically mean authentication.

    Given: A private financial data payload carrying its own trade event field,
    When: The SDK codec recognizes its actual feed and excludes known controls,
    Then: Original wire and exact operands remain available.
    """
    source = '{"feed":"fills","event":"trade","qty":0.10000000}'
    decoded = PrivateExecutionJsonCodec.loads(source)
    assert isinstance(decoded, PrivateWireMessage)
    assert decoded["qty"] == Decimal("0.10000000")
    assert decoded.wire_bytes == source.encode()


def test_malformed_json_keeps_standard_decoder_error() -> None:
    """Do not turn malformed input into an invented decoded execution.

    Given: Invalid JSON at the SDK boundary,
    When: The codec tries to decode it,
    Then: The ordinary JSON decoder error propagates.
    """
    with pytest.raises(json.JSONDecodeError):
        PrivateExecutionJsonCodec.loads('{"feed":"fills",')


def test_outbound_encoding_keeps_standard_options() -> None:
    """Preserve SDK signing/subscription encoding byte-for-byte.

    Given: An outbound subscription and explicit standard encoder options,
    When: The SDK encodes it through the codec,
    Then: The result equals standard JSON and the standard module stays unchanged.
    """
    value = {"feed": "fills", "text": "żółć"}

    actual = PrivateExecutionJsonCodec.dumps(value, ensure_ascii=False, separators=(",", ":"))

    assert actual == json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    assert isinstance(json.loads("0.1"), float)


def test_installation_is_module_local_and_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Install once in each pinned connector while retaining standard-library behavior.

    Given: Both pinned SDK connectors using their normal JSON dependency,
    When: Receipt capture is installed twice,
    Then: The same connector-local codecs remain and global JSON is untouched.
    """
    monkeypatch.setattr(futures_websocket, "json", json)
    monkeypatch.setattr(spot_websocket, "json", json)

    install_private_execution_wire_capture()
    futures_codec = futures_websocket.json
    spot_codec = spot_websocket.json
    install_private_execution_wire_capture()

    assert isinstance(futures_codec, PrivateExecutionJsonCodec)
    assert isinstance(spot_codec, PrivateExecutionJsonCodec)
    assert futures_websocket.json is futures_codec
    assert spot_websocket.json is spot_codec
    assert isinstance(json.loads("0.1"), float)


def test_unrecognized_sdk_boundary_is_refused() -> None:
    """Expose unsupported SDK dependency drift rather than silently replacing it.

    Given: A connector whose JSON dependency differs from the supported package,
    When: The boundary installer checks it,
    Then: Installation refuses without changing the unknown dependency.
    """
    connector = ModuleType("unsupported_connector")

    with pytest.raises(RuntimeError, match="differs from the supported SDK"):
        _install_connector_codec(connector)

    assert not hasattr(connector, "json")


def test_financial_decode_preserves_exact_nested_numbers() -> None:
    """Interpret a retained payload without passing monetary operands through float.

    Given: Original receipt bytes with a nested high-precision decimal and integer,
    When: Financial interpretation reparses the preserved JSON,
    Then: Decimal precision and the original integer remain exact.
    """
    decoded = decode_private_execution_receipt(
        b'{"fills":[{"qty":0.123456789012345678901,"time":1743676200000}]}'
    )

    assert decoded == {
        "fills": [{"qty": Decimal("0.123456789012345678901"), "time": 1743676200000}]
    }


@pytest.mark.parametrize(
    "source",
    [
        b'{"qty":1,"qty":2}',
        b'{"fill":{"qty":1,"qty":2}}',
        b'{"qty":1,"\\u0071ty":2}',
        b'{"qty":NaN}',
        b'{"qty":Infinity}',
        b'{"qty":-Infinity}',
        b"[]",
    ],
)
def test_ambiguous_or_nonfinite_financial_receipt_is_refused(source: bytes) -> None:
    """Refuse interpretation of ambiguous or non-JSON numeric evidence.

    Given: A retained receipt with duplicate keys, non-finite values or a non-object root,
    When: Financial interpretation validates the original bytes,
    Then: It raises before any operand can be admitted to accounting.
    """
    with pytest.raises(ValueError):
        decode_private_execution_receipt(source)
