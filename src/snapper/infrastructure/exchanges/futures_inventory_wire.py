"""Issue fixed inventory GET receipts only after actual same-key transport and privacy checks.

The caller owns the original User, routed Session and prepared owner token.
This synchronous leaf creates no durable attempt, account authority, financial
receipt or order permission. Its future producer must commit an independent
purpose-specific attempt first and rebind this actual issued response before
recording a terminal outcome. Original decoded entity bytes remain unchanged.
"""

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from functools import partial
from time import monotonic
from typing import Literal
from typing import Never
from typing import Self
from typing import SupportsIndex
from weakref import WeakKeyDictionary

from kraken.futures import User
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import TypeAdapter
from pydantic import ValidationError
from pydantic import model_validator
from requests import PreparedRequest

from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.futures_inventory_read import InventoryReadPurpose
from snapper.infrastructure.exchanges.execution_wire_receipts import _AUTHENTICATION_FIELDS
from snapper.infrastructure.exchanges.execution_wire_receipts import _CONTROL_EVENTS
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import (
    NativeAccountsTransportError,
)
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import _entity_bytes
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import (
    _request_guarded_original_response,
)
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import _routed_session
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import _selected_origin
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyProofReference
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyToken
from snapper.infrastructure.exchanges.private_control_decoder import _AUTH_NAMES

_PATHS: dict[InventoryReadPurpose, str] = {
    "kraken_futures.open_orders": "/derivatives/api/v3/openorders",
    "kraken_futures.open_positions": "/derivatives/api/v3/openpositions",
}
_ARRAYS: dict[InventoryReadPurpose, str] = {
    "kraken_futures.open_orders": "openOrders",
    "kraken_futures.open_positions": "openPositions",
}
_MAX_ENTITY_BYTES = 8 * 1024 * 1024
_MAX_FIELDS = 65536
_MAX_NODES = 65536
_MAX_DEPTH = 64
_PRIVATE_NAMES = (
    frozenset(value.casefold() for value in _AUTHENTICATION_FIELDS)
    | _AUTH_NAMES
    | frozenset(
        {
            "authent",
            "nonce",
            "privatekey",
            "apisecret",
            "private_key",
            "access_token",
            "accesstoken",
            "python-kraken-sdk",
        }
    )
)
_ATTEMPT_UUID: TypeAdapter[str] = TypeAdapter(EvidenceUUID)
type InventoryWireFailureReason = Literal[
    "transport_error", "http_error", "source_invalid", "source_unsafe"
]


class FuturesInventoryWireError(ValueError):
    """Expose only a fixed source disposition without a body, header or credential digest."""

    def __init__(self, reason: InventoryWireFailureReason) -> None:
        """Retain the safe enum used by the durable failed-outcome producer."""
        super().__init__(f"futures_inventory_wire_{reason}")
        self._failure_reason = reason

    @property
    def reason(self) -> InventoryWireFailureReason:
        """Return the original safe disposition without exposing a public mutable field.

        Returns:
            Fixed transport, HTTP, invalid-source or unsafe-source failure metadata.
        """
        return self._failure_reason


class FuturesInventoryWireReceipt(BaseModel):
    """Public privacy-checked original source view, without authority from plain construction.

    The endpoint envelope is classified here; the existing purpose-specific
    inventory parser and actual immutable proof/attempt loaders remain required
    before any captured durable outcome can be accepted.
    """

    model_config = ConfigDict(
        frozen=True,
        strict=True,
        extra="forbid",
        revalidate_instances="always",
        hide_input_in_errors=True,
    )

    inventory_attempt_public_id: EvidenceUUID
    purpose: InventoryReadPurpose
    client_selection: ExecutionClientSelection
    key_proof: PreparedKeyProofReference
    request_started_at: EvidenceInstant
    observed_at: EvidenceInstant
    status_code: Literal[200] = 200
    entity_bytes: bytes = Field(repr=False, min_length=1, max_length=_MAX_ENTITY_BYTES)

    @model_validator(mode="after")
    def require_original_selection_and_clocks(self) -> Self:
        """Keep actual proof, supported endpoint selection and original local knowledge consistent.

        Returns:
            The unchanged source view without asserting actual issuance or a durable attempt commit.
        """
        _selected_origin(self.client_selection)
        if self.key_proof.client_selection != self.client_selection:
            raise FuturesInventoryWireError("source_invalid")
        if not self.key_proof.recorded_at <= self.request_started_at <= self.observed_at:
            raise FuturesInventoryWireError("source_invalid")
        return self


class GuardedFuturesInventoryReceipt:
    """An opaque issued inventory response retaining no SDK objects or signing material."""

    __slots__ = ("_receipt", "__weakref__")
    _receipt: FuturesInventoryWireReceipt

    def __init__(self, receipt: FuturesInventoryWireReceipt) -> None:
        """Construct shape only; the closed transport separately records actual issuance."""
        object.__setattr__(self, "_receipt", receipt)

    def __setattr__(self, name: str, value: object) -> Never:
        """Refuse ordinary replacement of the original issued source and its public provenance."""
        raise AttributeError("Guarded Futures inventory receipt is immutable")

    def __repr__(self) -> str:
        """Hide original inventory entity bytes from ordinary source logging."""
        return "<GuardedFuturesInventoryReceipt:opaque>"

    def __reduce_ex__(self, protocol: SupportsIndex) -> Never:
        """Require actual producer binding before any durable representation can be created."""
        raise TypeError("Guarded Futures inventory receipt cannot be serialized")

    def __deepcopy__(self, memo: dict[int, object]) -> Never:
        """Refuse generic copy traversal through the opaque source ownership boundary."""
        raise TypeError("Guarded Futures inventory receipt cannot be copied")


_ISSUED_INVENTORY_RECEIPTS: WeakKeyDictionary[
    GuardedFuturesInventoryReceipt, FuturesInventoryWireReceipt
] = WeakKeyDictionary()


@dataclass(slots=True)
class _PrivacyInspection:
    """Bound original object pairs before duplicate overwrite or source retention is possible."""

    fields: int = 0


def _private_member(name: str, value: object) -> bool:
    """Detect auth/control labels without rewriting original financial identifiers."""
    label = name.strip().casefold()
    if label in _PRIVATE_NAMES or label == "method" and isinstance(value, str):
        return True
    return (
        label == "event" and isinstance(value, str) and value.strip().casefold() in _CONTROL_EVENTS
    )


def _inventory_pairs(
    pairs: list[tuple[str, object]], inspection: _PrivacyInspection
) -> dict[str, object]:
    """Inspect every original JSON pair, refusing all duplicate keys and private/control members."""
    inspection.fields += len(pairs)
    if inspection.fields > _MAX_FIELDS:
        raise FuturesInventoryWireError("source_invalid")
    document: dict[str, object] = {}
    for name, value in pairs:
        if name in document:
            raise FuturesInventoryWireError("source_invalid")
        if _private_member(name, value):
            raise FuturesInventoryWireError("source_unsafe")
        document[name] = value
    return document


def _number_text(value: str) -> str:
    """Keep numeric lexemes available for known-auth matching without interpreting an amount."""
    return value


def _nonfinite_number(value: str) -> Never:
    """Refuse non-JSON numbers without including original source text in an error."""
    raise FuturesInventoryWireError("source_invalid")


def _require_safe_text(value: str, known_auth: tuple[str, ...]) -> None:
    """Inspect decoded keys and values so JSON escaping cannot hide same-operation credentials."""
    try:
        value.encode("utf-8")
    except UnicodeError:
        raise FuturesInventoryWireError("source_invalid") from None
    if "\x00" in value:
        raise FuturesInventoryWireError("source_invalid")
    if any(secret in value for secret in known_auth):
        raise FuturesInventoryWireError("source_unsafe")


def _inspect_inventory_tree(document: dict[str, object], known_auth: tuple[str, ...]) -> None:
    """Bound the complete decoded tree and check every scalar/key before source issuance."""
    pending: list[tuple[object, int]] = [(document, 0)]
    visited = 0
    while pending:
        value, depth = pending.pop()
        visited += 1
        if visited > _MAX_NODES or depth > _MAX_DEPTH:
            raise FuturesInventoryWireError("source_invalid")
        if isinstance(value, dict):
            for name, child in value.items():
                _require_safe_text(name, known_auth)
                pending.append((child, depth + 1))
        elif isinstance(value, list):
            pending.extend((child, depth + 1) for child in value)
        elif isinstance(value, str):
            _require_safe_text(value, known_auth)


def _require_inventory_privacy(
    entity: bytes, purpose: InventoryReadPurpose, known_auth: tuple[str, ...]
) -> None:
    """Classify only this fixed endpoint's success envelope before receipt creation or hashing."""
    if not entity or len(entity) > _MAX_ENTITY_BYTES:
        raise FuturesInventoryWireError("source_invalid")
    try:
        document: object = json.loads(
            entity.decode("utf-8"),
            object_pairs_hook=partial(_inventory_pairs, inspection=_PrivacyInspection()),
            parse_int=_number_text,
            parse_float=_number_text,
            parse_constant=_nonfinite_number,
        )
    except FuturesInventoryWireError:
        raise
    except (ValueError, UnicodeError, RecursionError):
        raise FuturesInventoryWireError("source_invalid") from None
    if not isinstance(document, dict):
        raise FuturesInventoryWireError("source_invalid")
    _inspect_inventory_tree(document, known_auth)
    if (
        document.get("result") != "success"
        or "error" in document
        or "errors" in document
        or not isinstance(document.get("serverTime"), str)
        or not isinstance(document.get(_ARRAYS[purpose]), list)
    ):
        raise FuturesInventoryWireError("source_invalid")


def _known_auth_values(headers: Mapping[str, object], secret: object) -> tuple[str, ...]:
    """Keep signing material transient and refuse incomplete same-operation privacy inputs."""
    values = headers.get("APIKey"), headers.get("Authent"), headers.get("Nonce"), secret
    if not all(isinstance(value, str) and value for value in values):
        raise FuturesInventoryWireError("transport_error")
    return tuple(value for value in values if isinstance(value, str))


def require_guarded_inventory_receipt(candidate: object) -> FuturesInventoryWireReceipt:
    """Require the unchanged exact object actually issued by the closed inventory transport.

    Args:
        candidate: Actual returned opaque source, never a shaped DTO or equality-alias subclass.

    Returns:
        An isolated original public source view requiring independent durable attempt/proof binding.
    """
    if type(candidate) is not GuardedFuturesInventoryReceipt:
        raise FuturesInventoryWireError("source_invalid")
    original = _ISSUED_INVENTORY_RECEIPTS.get(candidate)
    if original is None or original != candidate._receipt:
        raise FuturesInventoryWireError("source_invalid")
    return original.model_copy(deep=True)


def _read_futures_inventory_wire(
    client: User,
    selection: ExecutionClientSelection,
    token: PreparedKeyToken,
    inventory_attempt_public_id: str,
    purpose: InventoryReadPurpose,
) -> GuardedFuturesInventoryReceipt:
    """Own one same-key request, bounded response and inventory-specific privacy classification."""
    selected = ExecutionClientSelection.model_validate(selection)
    attempt_id = _ATTEMPT_UUID.validate_python(inventory_attempt_public_id, strict=True)
    path = _PATHS[purpose]
    session = _routed_session(client, selected)
    expected_url = _selected_origin(selected) + path
    original_secret = getattr(client, "_secret", None)
    method, url, headers, payload, query = client._prepare_request(
        method="GET", uri=path, post_params={}, query_params={}, auth=True
    )
    if method != "GET" or url != expected_url or payload or query:
        raise FuturesInventoryWireError("transport_error")
    known_auth = _known_auth_values(headers, original_secret)

    def guard(prepared: PreparedRequest, actual_send_at: datetime) -> PreparedKeyProofReference:
        """Pin the original routed Session and key after Requests authentication."""
        nonlocal known_auth
        if _routed_session(client, selected) is not session:
            raise FuturesInventoryWireError("transport_error")
        proof = token.assert_prepared_key(
            client, selected, prepared.headers.get("APIKey"), actual_send_at
        )
        known_auth += _known_auth_values(prepared.headers, getattr(client, "_secret", None))
        return proof

    deadline = monotonic() + 30
    response, proof, started_at = _request_guarded_original_response(
        session, expected_url, headers, guard
    )
    try:
        if response.url != expected_url or response.history:
            raise FuturesInventoryWireError("transport_error")
        if response.status_code != 200:
            raise FuturesInventoryWireError("http_error")
        try:
            entity = _entity_bytes(response, deadline)
        except NativeAccountsTransportError as failure:
            if str(failure) == "native_accounts_transport_entity_too_large":
                raise FuturesInventoryWireError("source_invalid") from None
            raise
        observed_at = datetime.now(UTC)
        known_auth += _known_auth_values(headers, getattr(client, "_secret", None))
        _require_inventory_privacy(entity, purpose, known_auth)
        if _routed_session(client, selected) is not session:
            raise FuturesInventoryWireError("transport_error")
        token.assert_prepared_key(client, selected, headers.get("APIKey"), observed_at)
        original = FuturesInventoryWireReceipt(
            inventory_attempt_public_id=attempt_id,
            purpose=purpose,
            client_selection=selected,
            key_proof=proof,
            request_started_at=started_at,
            observed_at=observed_at,
            entity_bytes=entity,
        )
        result = GuardedFuturesInventoryReceipt(original)
        _ISSUED_INVENTORY_RECEIPTS[result] = original.model_copy(deep=True)
        return result
    finally:
        response.close()


def read_futures_inventory_wire(
    client: User,
    selection: ExecutionClientSelection,
    token: PreparedKeyToken,
    inventory_attempt_public_id: str,
    purpose: InventoryReadPurpose,
) -> GuardedFuturesInventoryReceipt:
    """Read one fixed inventory endpoint without durable side effects or order-access prerequisites.

    Args:
        client: Original SDK User whose routed Session is owned by the caller's dispatcher.
        selection: Exact public credential/environment already bound by the actual owner token.
        token: Original owner token, proof-bound after its producer awaited the actual commit.
        inventory_attempt_public_id: Independent request UUID without proof of its commit.
        purpose: Exact supported orders or positions purpose, without query or margin selectors.

    Returns:
        Actually issued privacy-checked original bytes, original clocks and same-key public proof.

    Raises:
        FuturesInventoryWireError: Safe metadata only; original bodies and auth stay local.
    """
    reason: InventoryWireFailureReason
    try:
        return _read_futures_inventory_wire(
            client, selection, token, inventory_attempt_public_id, purpose
        )
    except FuturesInventoryWireError as failure:
        reason = failure.reason
    except ValidationError:
        reason = "source_invalid"
    except Exception:
        reason = "transport_error"
    raise FuturesInventoryWireError(reason) from None
