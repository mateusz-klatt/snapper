"""Accept actual issued inventory HTTP before retaining any new successful outcome source.

The caller independently owns the committed request and actual proof/root
binding. Public successful DTOs cannot create new observations. Stored-source
retry parsing is available only after the store loaded its original immutable
outcome and will compare exact original insertion identity and result.
"""

import base64

from pydantic import TypeAdapter

from snapper.application.trade.futures_open_inventory import parse_futures_open_orders
from snapper.application.trade.futures_open_inventory import parse_futures_open_positions
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.futures_inventory_read import FuturesInventoryReadAttempt
from snapper.core.futures_inventory_read import FuturesInventoryReadFailure
from snapper.core.futures_inventory_read import FuturesInventoryReadResult
from snapper.core.futures_inventory_read import FuturesInventoryReadSource
from snapper.core.futures_inventory_read import InventoryParserPolicy
from snapper.core.futures_inventory_read import InventoryReadKeyReference
from snapper.core.futures_inventory_read import InventoryReadPurpose
from snapper.data.futures_inventory_key_binding import FuturesInventoryBindingError
from snapper.infrastructure.exchanges.futures_inventory_wire import GuardedFuturesInventoryReceipt
from snapper.infrastructure.exchanges.futures_inventory_wire import (
    require_guarded_inventory_receipt,
)

_POLICIES: dict[InventoryReadPurpose, InventoryParserPolicy] = {
    "kraken_futures.open_orders": "kraken_futures_open_orders_v1",
    "kraken_futures.open_positions": "kraken_futures_open_positions_v1",
}
_ATTEMPT_UUID: TypeAdapter[str] = TypeAdapter(EvidenceUUID)
type FuturesInventoryWriteResult = FuturesInventoryReadResult | GuardedFuturesInventoryReceipt


def _parse_inventory_source(entity: bytes, purpose: InventoryReadPurpose) -> InventoryParserPolicy:
    """Parse the full purpose-specific array before retained encoding or hashing."""
    if purpose == "kraken_futures.open_orders":
        parse_futures_open_orders(entity)
    else:
        parse_futures_open_positions(entity)
    return _POLICIES[purpose]


def _actual_guarded_result(
    received: GuardedFuturesInventoryReceipt,
    attempt_public_id: str,
    attempt: FuturesInventoryReadAttempt,
) -> FuturesInventoryReadResult:
    """Match actual issuance to its original request and every proof/root coordinate."""
    original = require_guarded_inventory_receipt(received)
    reference = InventoryReadKeyReference.model_validate(
        original.key_proof.model_dump(mode="python")
    )
    if (
        original.inventory_attempt_public_id != attempt_public_id
        or original.purpose != attempt.purpose
        or original.client_selection != attempt.client_selection
        or reference != attempt.key_proof
        or original.request_started_at < attempt.started_at
    ):
        raise FuturesInventoryBindingError("futures_inventory_actual_transport_attempt_mismatch")
    try:
        policy = _parse_inventory_source(original.entity_bytes, attempt.purpose)
    except (ValueError, UnicodeError, RecursionError, ArithmeticError):
        return FuturesInventoryReadFailure(reason="source_invalid")
    return FuturesInventoryReadSource(
        parser_policy=policy,
        body=base64.b64encode(original.entity_bytes).decode("ascii"),
        request_started_at=original.request_started_at,
        observed_at=original.observed_at,
    )


def _stored_source_result(
    candidate: FuturesInventoryReadSource, attempt: FuturesInventoryReadAttempt
) -> FuturesInventoryReadSource:
    """Reparse an original stored retry without granting new issuance from public source shape."""
    try:
        original = FuturesInventoryReadSource.model_validate(candidate)
        if (
            original.parser_policy != _POLICIES[attempt.purpose]
            or original.request_started_at < attempt.started_at
        ):
            raise FuturesInventoryBindingError("futures_inventory_stored_source_attempt_mismatch")
        _parse_inventory_source(base64.b64decode(original.body, validate=True), attempt.purpose)
    except (ValueError, UnicodeError, RecursionError, ArithmeticError):
        raise FuturesInventoryBindingError("futures_inventory_stored_source_invalid") from None
    return original


def inventory_result_for_storage(
    candidate: FuturesInventoryWriteResult,
    attempt_public_id: str,
    attempt: FuturesInventoryReadAttempt,
    *,
    allow_stored_source: bool = False,
) -> FuturesInventoryReadResult:
    """Classify a genuine source or an independently loaded exact original-outcome retry.

    Args:
        candidate: Actual issued inventory HTTP, strict safe failure or existing immutable result.
        attempt_public_id: Independently loaded committed request identity for this write.
        attempt: Rebound original request with its actual public key proof and immutable owner root.
        allow_stored_source: True only after the store loaded the original bound immutable outcome.

    Returns:
        A validated result; invalid new semantics retain only fixed source_invalid metadata.

    Raises:
        ValueError: If issuance, original request/proof/clock binding or stored retry parsing fails.
    """
    proven = FuturesInventoryReadAttempt.model_validate(attempt)
    actual_id = _ATTEMPT_UUID.validate_python(attempt_public_id, strict=True)
    if isinstance(candidate, GuardedFuturesInventoryReceipt):
        return _actual_guarded_result(candidate, actual_id, proven)
    if isinstance(candidate, FuturesInventoryReadFailure):
        return FuturesInventoryReadFailure.model_validate(candidate)
    if allow_stored_source is True and isinstance(candidate, FuturesInventoryReadSource):
        return _stored_source_result(candidate, proven)
    raise FuturesInventoryBindingError("futures_inventory_new_success_requires_actual_transport")
