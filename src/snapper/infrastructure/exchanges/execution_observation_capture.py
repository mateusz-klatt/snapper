"""Qualify provider payload evidence before any individual fill is interpreted."""

import base64
import json
from collections.abc import Mapping

from snapper.infrastructure.exchanges.contracts import ExecutionObservationContext
from snapper.infrastructure.exchanges.contracts import PrivateExecutionObservation
from snapper.infrastructure.exchanges.execution_wire_receipts import private_message_wire_bytes


def build_private_execution_observation(
    message: Mapping[str, object], context: ExecutionObservationContext
) -> PrivateExecutionObservation:
    """Capture one selected observation without interpreting or rounding its amounts.

    Args:
        message: The actual provider callback object before downstream normalization.
        context: Observed channel, capture time and exact record selector.

    Returns:
        Original payload bytes when available; otherwise an honestly tagged decoded
        representation or opaque fallback that cannot certify exact venue numbers.
    """
    wire_bytes = private_message_wire_bytes(message)
    if wire_bytes is not None:
        return PrivateExecutionObservation(
            context=context,
            representation="wire_bytes_base64",
            material=base64.b64encode(wire_bytes).decode("ascii"),
        )
    try:
        material = json.dumps(dict(message), sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError):
        return PrivateExecutionObservation(
            context=context, representation="opaque_text", material=repr(dict(message))
        )
    return PrivateExecutionObservation(
        context=context, representation="parsed_fields_json", material=material
    )
