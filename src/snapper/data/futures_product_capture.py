"""Require original issued product HTTP for new successes while preserving exact stored retries.

A public parsed success is not a new transport observation. The trusted store
may accept that shape only after it has independently loaded the original
immutable outcome and will compare the exact original identity and result.
Fixed failure metadata remains conservative and carries no provider body.
"""

import base64

from snapper.application.trade.futures_product_source import FuturesProductUnsafeSourceError
from snapper.application.trade.futures_product_source import parse_safe_futures_product_source
from snapper.core.futures_product_read import FuturesProductReadAttempt
from snapper.core.futures_product_read import FuturesProductReadFailure
from snapper.core.futures_product_read import FuturesProductReadResult
from snapper.core.futures_product_read import FuturesProductReadSource
from snapper.core.futures_product_read import ProductReadKeyReference
from snapper.data.futures_product_key_binding import FuturesProductBindingError
from snapper.infrastructure.exchanges.futures_trading_instruments_wire import (
    GuardedFuturesProductReceipt,
)
from snapper.infrastructure.exchanges.futures_trading_instruments_wire import (
    require_guarded_product_receipt,
)

FuturesProductWriteResult = FuturesProductReadResult | GuardedFuturesProductReceipt


def _actual_guarded_result(
    received: GuardedFuturesProductReceipt,
    attempt_public_id: str,
    attempt: FuturesProductReadAttempt,
) -> FuturesProductReadResult:
    """Bind actual dispatch to its own committed request before any body retention or hashing."""
    original = require_guarded_product_receipt(received)
    reference = ProductReadKeyReference.model_validate(original.key_proof.model_dump(mode="python"))
    if (
        original.product_attempt_public_id != attempt_public_id
        or original.client_selection != attempt.client_selection
        or reference != attempt.key_proof
        or original.request_started_at < attempt.started_at
    ):
        raise FuturesProductBindingError("futures_product_actual_transport_attempt_mismatch")
    if original.status_code != 200:
        return FuturesProductReadFailure(reason="http_error")
    try:
        parse_safe_futures_product_source(original.entity_bytes)
    except FuturesProductUnsafeSourceError:
        return FuturesProductReadFailure(reason="source_unsafe")
    except ValueError:
        return FuturesProductReadFailure(reason="source_invalid")
    return FuturesProductReadSource(
        status_code=original.status_code,
        body=base64.b64encode(original.entity_bytes).decode("ascii"),
        request_started_at=original.request_started_at,
        observed_at=original.observed_at,
    )


def product_result_for_storage(
    candidate: FuturesProductWriteResult,
    attempt_public_id: str,
    attempt: FuturesProductReadAttempt,
    *,
    allow_stored_source: bool = False,
) -> FuturesProductReadResult:
    """Classify genuine source evidence or an exact existing-outcome retry before persistence.

    Args:
        candidate: Actual issued HTTP response, conservative failure, or existing immutable result.
        attempt_public_id: Independently loaded committed request identity for this write.
        attempt: Independently rebound request, selected credential and original key proof/root.
        allow_stored_source: True only after the caller loaded the actual original bound outcome.

    Returns:
        Strict result; a public success cannot create a new captured outcome without actual transport.

    Raises:
        ValueError: When source issuance, actual request/proof/clock copies or retry provenance fail.
    """
    if isinstance(candidate, GuardedFuturesProductReceipt):
        return _actual_guarded_result(candidate, attempt_public_id, attempt)
    if isinstance(candidate, FuturesProductReadFailure):
        return FuturesProductReadFailure.model_validate(candidate)
    if allow_stored_source and isinstance(candidate, FuturesProductReadSource):
        source = FuturesProductReadSource.model_validate(candidate)
        parse_safe_futures_product_source(base64.b64decode(source.body, validate=True))
        return source
    raise FuturesProductBindingError("futures_product_new_success_requires_actual_transport")
