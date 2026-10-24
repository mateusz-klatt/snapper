"""Fixed authenticated product HTTP source bound to the actual previously checked signing key.

This source is not a financial receipt or an order permission. Its producer
must retain an independently ordered product attempt before this call, then
persist a safe outcome and rebind the same committed public proof and root.
The original response stays transient here; only a privacy-checked persistence
boundary may retain it. No authentication header or body hash is exposed.
"""

from datetime import UTC
from datetime import datetime
from time import monotonic
from typing import Never
from typing import Self
from typing import SupportsIndex
from weakref import WeakKeyDictionary

from kraken.futures import User
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_validator
from requests import PreparedRequest

from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import _entity_bytes
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import (
    _request_guarded_original_response,
)
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import _routed_session
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import _selected_origin
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyProofReference
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyToken

_PRODUCT_PATH = "/derivatives/api/v3/trading/instruments"


class FuturesProductTransportError(ValueError):
    """The actual product transport cannot establish its fixed authenticated public source."""


class FuturesTradingInstrumentsWireReceipt(BaseModel):
    """Original transient HTTP entity plus actual selected-key proof and local clocks.

    Constructing this public shape does not establish an actual HTTP dispatch.
    A closed producer must own the call and the committed attempt. Sensitive
    control/error bodies must be refused before any persistence or body digest.
    """

    model_config = ConfigDict(
        frozen=True,
        strict=True,
        extra="forbid",
        revalidate_instances="always",
        hide_input_in_errors=True,
    )

    product_attempt_public_id: EvidenceUUID
    client_selection: ExecutionClientSelection
    key_proof: PreparedKeyProofReference
    request_started_at: EvidenceInstant
    observed_at: EvidenceInstant
    status_code: int = Field(ge=100, le=599)
    entity_bytes: bytes = Field(repr=False)

    @model_validator(mode="after")
    def validate_original_source(self) -> Self:
        """Keep same-key public selection and original request knowledge internally consistent.

        Returns:
            The same transient source, without asserting durable provenance or current permission.
        """
        _selected_origin(self.client_selection)
        if self.key_proof.client_selection != self.client_selection:
            raise FuturesProductTransportError("futures_product_selected_key_mismatch")
        if not self.key_proof.recorded_at <= self.request_started_at <= self.observed_at:
            raise FuturesProductTransportError("futures_product_actual_source_clock_mismatch")
        return self


class GuardedFuturesProductReceipt:
    """Opaque transient actual transport result without keys, signatures or persistence authority."""

    __slots__ = ("_receipt", "__weakref__")
    _receipt: FuturesTradingInstrumentsWireReceipt

    def __init__(self, receipt: FuturesTradingInstrumentsWireReceipt) -> None:
        """Construct shape only; the closed transport must separately record its actual issuance."""
        object.__setattr__(self, "_receipt", receipt)

    def __setattr__(self, name: str, value: object) -> Never:
        """Refuse replacement of the original response and its same-operation key proof."""
        raise AttributeError("Guarded Futures product receipt is immutable")

    def __repr__(self) -> str:
        """Hide unknown source bodies and sensitive provider material from ordinary logging."""
        return "<GuardedFuturesProductReceipt:opaque>"

    def __reduce_ex__(self, protocol: SupportsIndex) -> Never:
        """Refuse persistence before the trusted producer checks the original source body."""
        raise TypeError("Guarded Futures product receipt cannot be serialized")

    def __deepcopy__(self, memo: dict[int, object]) -> Never:
        """Refuse generic copy traversal into unclassified original response bytes."""
        raise TypeError("Guarded Futures product receipt cannot be copied")


_ISSUED_PRODUCT_RECEIPTS: WeakKeyDictionary[
    GuardedFuturesProductReceipt, FuturesTradingInstrumentsWireReceipt
] = WeakKeyDictionary()


def require_guarded_product_receipt(candidate: object) -> FuturesTradingInstrumentsWireReceipt:
    """Require an unchanged result actually issued by the final-request guarded transport.

    Args:
        candidate: The closed transport result, never a plain caller-authored source observation.

    Returns:
        Isolated original source view; privacy and durable binding remain separate requirements.

    Raises:
        FuturesProductTransportError: If no unchanged actually issued source belongs to this object.
    """
    if not isinstance(candidate, GuardedFuturesProductReceipt):
        raise FuturesProductTransportError("futures_product_actual_transport_required")
    original = _ISSUED_PRODUCT_RECEIPTS.get(candidate)
    if original is None or original != candidate._receipt:
        raise FuturesProductTransportError("futures_product_actual_transport_required")
    return original.model_copy(deep=True)


def read_futures_trading_instruments_wire(
    client: User,
    selection: ExecutionClientSelection,
    token: PreparedKeyToken,
    product_attempt_public_id: str,
) -> GuardedFuturesProductReceipt:
    """Read once through the actual routed session with the same checked prepared API key.

    Args:
        client: Actual original SDK User retained by the closed same-operation key token.
        selection: Frozen public selection belonging to that same committed proof.
        token: Actually issued and committed safe-key token, never reconstructed from current User state.
        product_attempt_public_id: Actual committed independent product request owned by this invocation.

    Returns:
        Actually issued transient source; separate privacy and durable attempt/outcome checks remain.

    Raises:
        ValueError: When the actual route, prepared key, original proof or response source changes.
    """
    selected = ExecutionClientSelection.model_validate(selection)
    session = _routed_session(client, selected)
    expected_url = _selected_origin(selected) + _PRODUCT_PATH
    method, url, headers, payload, query = client._prepare_request(
        method="GET", uri=_PRODUCT_PATH, post_params={}, query_params={}, auth=True
    )
    if method != "GET" or url != expected_url or payload or query:
        raise FuturesProductTransportError("futures_product_prepared_request_mismatch")

    def guard(prepared: PreparedRequest, actual_send_at: datetime) -> PreparedKeyProofReference:
        """Require the final key after Requests authentication and session merging have completed."""
        return token.assert_prepared_key(
            client, selected, prepared.headers.get("APIKey"), actual_send_at
        )

    deadline = monotonic() + 30
    response, proof, started_at = _request_guarded_original_response(
        session, expected_url, headers, guard
    )
    try:
        if response.url != expected_url or response.history:
            raise FuturesProductTransportError("futures_product_response_route_mismatch")
        entity = _entity_bytes(response, deadline)
        observed_at = datetime.now(UTC)
        if _routed_session(client, selected) is not session:
            raise FuturesProductTransportError("futures_product_actual_session_changed")
        original = FuturesTradingInstrumentsWireReceipt(
            product_attempt_public_id=product_attempt_public_id,
            client_selection=selected,
            key_proof=proof,
            request_started_at=started_at,
            observed_at=observed_at,
            status_code=response.status_code,
            entity_bytes=entity,
        )
        result = GuardedFuturesProductReceipt(original)
        _ISSUED_PRODUCT_RECEIPTS[result] = original.model_copy(deep=True)
        return result
    finally:
        response.close()
