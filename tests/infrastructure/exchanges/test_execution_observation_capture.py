"""Qualify genuine SDK payloads independently of reconstructed callback fields."""

import base64
from datetime import UTC
from datetime import datetime
from decimal import Decimal

import pytest

from snapper.infrastructure.exchanges.contracts import ExecutionObservationContext
from snapper.infrastructure.exchanges.execution_observation_capture import (
    build_private_execution_observation,
)
from snapper.infrastructure.exchanges.execution_wire_receipts import PrivateWireMessage


def _context() -> ExecutionObservationContext:
    """Keep one actual record selector with its separate callback observation clock."""
    return ExecutionObservationContext(
        kind="execution",
        channel="kraken.ws.executions",
        observed_at=datetime(2026, 10, 23, tzinfo=UTC),
        target_json_pointer="/data/0",
    )


def test_genuine_wire_receipt_keeps_full_original_payload_and_selector() -> None:
    """A selected record keeps the original whole message instead of a reserialized leaf.

    Given: A genuine SDK boundary object carrying an original precise JSON payload,
    When: Its first execution record is captured,
    Then: Exact original bytes and the independent selector survive together.
    """
    source = b'{ "channel":"executions", "data":[{"last_qty":0.100000000000000001}] }'
    message = PrivateWireMessage({"data": [{"last_qty": Decimal("0.100000000000000001")}]}, source)
    result = build_private_execution_observation(message, _context())
    assert result.representation == "wire_bytes_base64"
    assert base64.b64decode(result.material) == source
    assert result.context.target_json_pointer == "/data/0"


def test_reconstructed_callback_fields_remain_explicitly_weaker_than_original_wire() -> None:
    """A plain decoded mapping has no original payload-byte or decimal-precision proof.

    Given: An ordinary callback mapping with binary-float financial fields,
    When: It is retained without a genuine boundary object,
    Then: Its reconstruction is tagged parsed fields rather than original decoded JSON text.
    """
    result = build_private_execution_observation({"qty": 0.1}, _context())
    assert result.representation == "parsed_fields_json"
    assert result.material == '{"qty":0.1}'


@pytest.mark.parametrize("value", (Decimal("0.100000000000000001"), float("nan")))
def test_unserializable_or_non_json_fallback_is_retained_without_false_json_claim(
    value: object,
) -> None:
    """Unqualified malformed callback values remain retainable without certifying exact venue operands.

    Given: An ordinary mapping with a Decimal or a non-JSON nonfinite numeric extension,
    When: Reconstructed strict JSON cannot represent it,
    Then: The fallback remains opaque and keeps the same observation selector.
    """
    result = build_private_execution_observation({"qty": value}, _context())
    assert result.representation == "opaque_text"
    assert "qty" in result.material
    assert result.context.target_json_pointer == "/data/0"
