"""Pure authority for the per-scope execution tamper-evidence hash chain.

The module folds an ordered run of committed ``executions`` rows for one scope
``(wallet_public_id, exchange, mode)`` into a single SHA-256 tip. Producer
(anchor bootstrap, which commits the tip) and consumer (replay verification,
which re-derives it) share this one canonical serialization so the sealed and
the checked bytes cannot drift.

The chain is DERIVED: nothing is materialized on ``executions`` (the shipped
0030 immutability triggers reject UPDATE, so a per-row hash column could not be
backfilled), and only the tip is committed later, in the immutable anchor.

Theorem, stated honestly. Given a TRUSTED base tip ``H_i`` committed over rows
``r_1..r_i`` and the surviving rows ``r_{i+1}..r_j``, re-deriving
``extend_execution_chain(H_i, r_{i+1}..r_j)`` reproduces the honest extension if
and only if every folded row is byte-identical, in its canonical fields, to the
row that was blessed. Any in-place field mutation, reorder, insertion, or
deletion in ``(i, j]`` changes the tip. The seal covers execution-ROW values
only; instrument lineage (which lives on the order, not the execution) is the
venue-history map's responsibility, and a DB superuser who also rewrites the
committed tips in their non-triggered tables is out of scope for the in-DB
chain and needs an external, authenticated tip commitment.

Canonical serialization is dual-dialect stable: the same logical row read via
``sqlite+aiosqlite`` and via ``postgresql+asyncpg`` produces identical bytes.
UUIDs canonicalize to their 16 raw bytes (spelling and case cannot drift),
instants encode as exact integer microseconds since the UTC epoch, exact
decimals are the stored ``*_decimal`` text verbatim, and every field is
length-prefixed with a presence byte for nullables so distinct rows cannot
collide. The lossy ``price``/``size``/``fee`` float mirrors are excluded: the
fold consumes the exact ``*_decimal`` text, and floats carry a signed-zero /
non-finite dual-dialect hazard. Malformed input fails closed with
``ExecutionChainError`` rather than producing a tip.
"""

import hashlib
import struct
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from uuid import UUID

EXECUTION_CHAIN_DOMAIN = b"snapper:spot-execution-chain:v2"
_GENESIS_TAG = b"\x00"
_LINK_TAG = b"\x01"
_ABSENT = b"\x00"
_PRESENT = b"\x01"
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MIN_YEAR = 1970
_MAX_YEAR = 9000
_HEX_DIGITS = frozenset("0123456789abcdef")
_TIP_HEX_LENGTH = 64


class ExecutionChainError(Exception):
    """A record or base tip cannot be canonically serialized; fail closed.

    Raised for a malformed base tip, a non-canonical UUID, a naive or
    out-of-domain instant, a boolean where a counter is required, or a blank
    scope component. Verification callers must catch this and persist a stable
    ``incomplete`` result before any matched-verdict writer is reachable, never
    let it escape as a crash.
    """


@dataclass(frozen=True)
class ExecutionChainRecord:
    """One execution's immutable fold-input identity, in canonical fold order.

    Carries the execution-row values a spot replay fold consumes plus the row's
    provenance identity. Excludes the surrogate ``id`` (a database-assigned
    surrogate with no balance meaning), the ``known_to`` SCD2 sentinel (whose
    SQLite active-predicate is raw-string sensitive), the bus-transport
    ``session_id``/``sequence_id``, and the lossy float mirrors.
    """

    scope_sequence: int
    public_id: str
    order_public_id: str
    wallet_public_id: str
    operator_public_id: str | None
    exchange: str
    mode: str
    exec_id: str | None
    trade_id: str | None
    side: str
    status: str
    fee_asset: str
    price_decimal: str | None
    size_decimal: str | None
    fee_decimal: str | None
    counter_amount_decimal: str | None
    numeric_provenance: str | None
    liquidity_role: str
    timestamp: datetime
    executed_at: datetime | None


def _length_prefixed(value: bytes) -> bytes:
    """Frame one field as ``uint32_be(len) || bytes`` so boundaries cannot shift."""
    return struct.pack(">I", len(value)) + value


def _string_field(value: str) -> bytes:
    """Encode a required string as its length-prefixed UTF-8 bytes."""
    return _length_prefixed(value.encode("utf-8"))


def _optional_string_field(value: str | None) -> bytes:
    """Encode a nullable string with a leading presence byte so NULL and '' differ."""
    if value is None:
        return _ABSENT
    return _PRESENT + _string_field(value)


def _uuid_field(value: str) -> bytes:
    """Canonicalize a UUID to its 16 raw bytes so spelling and case cannot drift."""
    try:
        canonical = UUID(value).bytes
    except ValueError as error:
        raise ExecutionChainError(f"non-canonical uuid: {value!r}") from error
    return _length_prefixed(canonical)


def _optional_uuid_field(value: str | None) -> bytes:
    """Encode a nullable UUID with a leading presence byte."""
    if value is None:
        return _ABSENT
    return _PRESENT + _uuid_field(value)


def _integer_field(value: int) -> bytes:
    """Encode an integer as its length-prefixed decimal ASCII; reject bool.

    ``bool`` is an ``int`` subclass whose ``str`` is ``'True'``/``'False'``, so
    a boolean masquerading as a counter would fold to the wrong bytes; refuse it.
    """
    if isinstance(value, bool):
        raise ExecutionChainError("boolean is not a valid counter field")
    return _length_prefixed(str(value).encode("ascii"))


def _datetime_field(value: datetime) -> bytes:
    """Encode a tz-aware instant as exact integer microseconds since the UTC epoch.

    Uses ``timedelta`` integer components, never a lossy float ``timestamp()``.
    Rejects naive instants and instants outside ``[1970, 9000)`` so the
    ``datetime.max`` / asyncpg ``infinity`` sentinels cannot collide with a
    real value.
    """
    if value.tzinfo is None:
        raise ExecutionChainError("naive datetime is not permitted in the execution chain")
    instant = value.astimezone(UTC)
    if instant.year < _MIN_YEAR or instant.year >= _MAX_YEAR:
        raise ExecutionChainError(f"datetime out of chain domain: {instant.isoformat()}")
    delta = instant - _EPOCH
    micros = (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
    return _length_prefixed(str(micros).encode("ascii"))


def _optional_datetime_field(value: datetime | None) -> bytes:
    """Encode a nullable instant with a leading presence byte."""
    if value is None:
        return _ABSENT
    return _PRESENT + _datetime_field(value)


def canonical_execution_record(record: ExecutionChainRecord) -> bytes:
    """Serialize one record to injective, dual-dialect-stable canonical bytes.

    Args:
        record: The execution fields to seal, in canonical fold order.

    Returns:
        The record's length-prefixed canonical byte encoding.
    """
    return b"".join(
        (
            _integer_field(record.scope_sequence),
            _uuid_field(record.public_id),
            _uuid_field(record.order_public_id),
            _uuid_field(record.wallet_public_id),
            _optional_uuid_field(record.operator_public_id),
            _string_field(record.exchange),
            _string_field(record.mode),
            _optional_string_field(record.exec_id),
            _optional_string_field(record.trade_id),
            _string_field(record.side),
            _string_field(record.status),
            _string_field(record.fee_asset),
            _optional_string_field(record.price_decimal),
            _optional_string_field(record.size_decimal),
            _optional_string_field(record.fee_decimal),
            _optional_string_field(record.counter_amount_decimal),
            _optional_string_field(record.numeric_provenance),
            _string_field(record.liquidity_role),
            _datetime_field(record.timestamp),
            _optional_datetime_field(record.executed_at),
        )
    )


def execution_chain_genesis(wallet_public_id: str, exchange: str, mode: str) -> str:
    """Return the scope-binding genesis tip so a chain for one scope is invalid elsewhere.

    Args:
        wallet_public_id: The scope's wallet identity.
        exchange: The scope's venue; must be non-empty.
        mode: The scope's trading mode; must be non-empty.

    Returns:
        The 64-character lowercase hex genesis tip for the scope.
    """
    if not exchange:
        raise ExecutionChainError("genesis requires a non-empty exchange")
    if not mode:
        raise ExecutionChainError("genesis requires a non-empty mode")
    material = (
        EXECUTION_CHAIN_DOMAIN
        + _GENESIS_TAG
        + _uuid_field(wallet_public_id)
        + _string_field(exchange)
        + _string_field(mode)
    )
    return hashlib.sha256(material).hexdigest()


def _validate_base_tip(value: str) -> None:
    """Reject a base tip that is not exactly 64 lowercase hex characters."""
    if len(value) != _TIP_HEX_LENGTH:
        raise ExecutionChainError("base tip must be 64 lowercase hex characters")
    if any(character not in _HEX_DIGITS for character in value):
        raise ExecutionChainError("base tip must be 64 lowercase hex characters")


def extend_execution_chain(base_tip: str, records: Iterable[ExecutionChainRecord]) -> str:
    """Fold ``records`` in order onto ``base_tip``, returning the new tip.

    ``base_tip`` is either ``execution_chain_genesis(...)`` (bootstrap over the
    whole prefix) or a checkpoint tip committed by a trusted anchor/verdict
    (extension over the surviving range). Each link hashes the domain, the link
    tag, the previous tip's 32 raw bytes, and the record's canonical bytes.

    Args:
        base_tip: A trusted 64-character lowercase hex tip to extend from.
        records: The execution records to fold, in scope-sequence order.

    Returns:
        The 64-character lowercase hex tip after folding every record.
    """
    _validate_base_tip(base_tip)
    current = base_tip
    for record in records:
        material = (
            EXECUTION_CHAIN_DOMAIN
            + _LINK_TAG
            + bytes.fromhex(current)
            + canonical_execution_record(record)
        )
        current = hashlib.sha256(material).hexdigest()
    return current
