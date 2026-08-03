"""Unit tests for the pure per-scope execution tamper-evidence hash chain."""

import dataclasses
import hashlib
import struct
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from uuid import UUID

import pytest

from snapper.application.portfolio.execution_chain import EXECUTION_CHAIN_DOMAIN
from snapper.application.portfolio.execution_chain import EXECUTION_ROW_DIGEST_DOMAIN
from snapper.application.portfolio.execution_chain import ExecutionChainError
from snapper.application.portfolio.execution_chain import ExecutionChainRecord
from snapper.application.portfolio.execution_chain import canonical_execution_record
from snapper.application.portfolio.execution_chain import execution_chain_genesis
from snapper.application.portfolio.execution_chain import execution_row_digest
from snapper.application.portfolio.execution_chain import extend_execution_chain

_WALLET = "00000000-0000-7000-8000-000000000101"
_ORDER = "00000000-0000-7000-8000-000000000201"
_PUBLIC = "00000000-0000-7000-8000-000000000301"
_OPERATOR = "00000000-0000-7000-8000-000000000401"
_EXCHANGE = "walutomat"
_MODE = "live"
_TS = datetime(2026, 7, 17, 8, 0, tzinfo=UTC)


def _record(
    *,
    scope_sequence: int = 1,
    public_id: str = _PUBLIC,
    order_public_id: str = _ORDER,
    wallet_public_id: str = _WALLET,
    operator_public_id: str | None = _OPERATOR,
    exchange: str = _EXCHANGE,
    mode: str = _MODE,
    exec_id: str | None = "E-1",
    trade_id: str | None = "T-1",
    side: str = "buy",
    status: str = "filled",
    fee_asset: str = "PLN",
    price_decimal: str | None = "1.25",
    size_decimal: str | None = "2.0",
    fee_decimal: str | None = "0.1",
    counter_amount_decimal: str | None = "2.5",
    numeric_provenance: str | None = "venue_raw",
    liquidity_role: str = "maker",
    timestamp: datetime = _TS,
    executed_at: datetime | None = _TS,
) -> ExecutionChainRecord:
    """Build a fully populated record; each test overrides exactly one field."""
    return ExecutionChainRecord(
        scope_sequence=scope_sequence,
        public_id=public_id,
        order_public_id=order_public_id,
        wallet_public_id=wallet_public_id,
        operator_public_id=operator_public_id,
        exchange=exchange,
        mode=mode,
        exec_id=exec_id,
        trade_id=trade_id,
        side=side,
        status=status,
        fee_asset=fee_asset,
        price_decimal=price_decimal,
        size_decimal=size_decimal,
        fee_decimal=fee_decimal,
        counter_amount_decimal=counter_amount_decimal,
        numeric_provenance=numeric_provenance,
        liquidity_role=liquidity_role,
        timestamp=timestamp,
        executed_at=executed_at,
    )


def _genesis() -> str:
    """Return the genesis tip for the shared test scope."""
    return execution_chain_genesis(_WALLET, _EXCHANGE, _MODE)


def test_chain_tip_is_deterministic() -> None:
    """Folding the same records onto the same base always yields the same tip.

    Given the genesis tip and a fixed pair of records,
    When the chain is folded twice,
    Then both tips are equal and 64 hex characters long.
    """
    genesis = _genesis()
    first = extend_execution_chain(genesis, [_record(scope_sequence=1), _record(scope_sequence=2)])
    second = extend_execution_chain(genesis, [_record(scope_sequence=1), _record(scope_sequence=2)])
    assert first == second
    assert len(first) == 64


_FIELD_MUTATIONS = [
    pytest.param(_record(scope_sequence=2), id="scope_sequence"),
    pytest.param(_record(public_id="00000000-0000-7000-8000-0000000003ff"), id="public_id"),
    pytest.param(_record(order_public_id="00000000-0000-7000-8000-0000000002ff"), id="order"),
    pytest.param(_record(wallet_public_id="00000000-0000-7000-8000-0000000001ff"), id="wallet"),
    pytest.param(_record(operator_public_id=None), id="operator_absent"),
    pytest.param(_record(operator_public_id="00000000-0000-7000-8000-0000000004ff"), id="operator"),
    pytest.param(_record(exchange="kraken"), id="exchange"),
    pytest.param(_record(mode="paper"), id="mode"),
    pytest.param(_record(exec_id=None), id="exec_id_absent"),
    pytest.param(_record(exec_id="E-2"), id="exec_id"),
    pytest.param(_record(trade_id=None), id="trade_id_absent"),
    pytest.param(_record(trade_id="T-2"), id="trade_id"),
    pytest.param(_record(side="sell"), id="side"),
    pytest.param(_record(status="partial"), id="status"),
    pytest.param(_record(fee_asset="USD"), id="fee_asset"),
    pytest.param(_record(price_decimal=None), id="price_decimal_absent"),
    pytest.param(_record(price_decimal="1.26"), id="price_decimal"),
    pytest.param(_record(size_decimal=None), id="size_decimal_absent"),
    pytest.param(_record(size_decimal="2.1"), id="size_decimal"),
    pytest.param(_record(fee_decimal=None), id="fee_decimal_absent"),
    pytest.param(_record(fee_decimal="0.2"), id="fee_decimal"),
    pytest.param(_record(counter_amount_decimal=None), id="counter_amount_decimal_absent"),
    pytest.param(_record(counter_amount_decimal="2.6"), id="counter_amount_decimal"),
    pytest.param(_record(numeric_provenance=None), id="numeric_provenance_absent"),
    pytest.param(_record(numeric_provenance="legacy_float"), id="numeric_provenance"),
    pytest.param(_record(liquidity_role="taker"), id="liquidity_role"),
    pytest.param(_record(timestamp=_TS + timedelta(seconds=1)), id="timestamp"),
    pytest.param(_record(executed_at=None), id="executed_at_absent"),
    pytest.param(_record(executed_at=_TS + timedelta(seconds=1)), id="executed_at"),
]


@pytest.mark.parametrize("mutated", _FIELD_MUTATIONS)
def test_any_folded_field_mutation_changes_the_tip(mutated: ExecutionChainRecord) -> None:
    """Mutating any single folded field yields a different chain tip.

    Given a baseline record and the same record with one field changed,
    When each is folded onto the genesis tip,
    Then the two tips differ.
    """
    genesis = _genesis()
    baseline = extend_execution_chain(genesis, [_record()])
    assert extend_execution_chain(genesis, [mutated]) != baseline


def test_reordering_two_records_changes_the_tip() -> None:
    """The fold is order-sensitive: swapping two records changes the tip.

    Given two distinct records,
    When they are folded in each order,
    Then the two tips differ.
    """
    genesis = _genesis()
    first = _record(scope_sequence=1)
    second = _record(scope_sequence=2)
    assert extend_execution_chain(genesis, [first, second]) != extend_execution_chain(
        genesis, [second, first]
    )


def test_inserting_or_dropping_a_record_changes_the_tip() -> None:
    """Presence matters: adding a record changes the tip, and an empty fold is the base.

    Given one-record and two-record folds and an empty fold,
    When each is computed from the genesis tip,
    Then adding a record changes the tip and an empty fold returns the base.
    """
    genesis = _genesis()
    one = extend_execution_chain(genesis, [_record(scope_sequence=1)])
    two = extend_execution_chain(genesis, [_record(scope_sequence=1), _record(scope_sequence=2)])
    assert one != two
    assert extend_execution_chain(genesis, []) == genesis


def test_null_and_empty_string_are_distinct() -> None:
    """A NULL optional field and an empty-string field serialize differently.

    Given records whose exec_id is None and empty string,
    When each is canonically serialized,
    Then the two encodings differ.
    """
    absent = canonical_execution_record(_record(exec_id=None))
    empty = canonical_execution_record(_record(exec_id=""))
    assert absent != empty


def test_decimal_text_is_hashed_verbatim() -> None:
    """The exact decimal text is sealed, so a value-preserving reformat is detected.

    Given records with equivalent but differently spelled decimal text,
    When each is canonically serialized,
    Then the encodings differ and a null decimal differs from an empty string.
    """
    assert canonical_execution_record(_record(size_decimal="2.0")) != canonical_execution_record(
        _record(size_decimal="2.00")
    )
    assert canonical_execution_record(_record(price_decimal=None)) != canonical_execution_record(
        _record(price_decimal="")
    )


def test_canonical_datetime_is_utc_microsecond_stable() -> None:
    """The same instant in different zones encodes identically; naive is refused.

    Given one instant in UTC and in a shifted zone, plus a naive instant,
    When each timestamp is canonically serialized,
    Then the two aware encodings match and the naive one fails closed.
    """
    in_utc = _record(timestamp=datetime(2026, 7, 17, 8, 0, tzinfo=UTC))
    shifted = _record(timestamp=datetime(2026, 7, 17, 10, 0, tzinfo=timezone(timedelta(hours=2))))
    assert canonical_execution_record(in_utc) == canonical_execution_record(shifted)
    naive_timestamp_record = _record(timestamp=datetime(2026, 7, 17, 8, 0))
    with pytest.raises(ExecutionChainError):
        canonical_execution_record(naive_timestamp_record)


def test_datetime_outside_the_chain_domain_is_refused() -> None:
    """Pre-epoch and the year-9999 / infinity sentinel band both fail closed.

    Given a timestamp before the epoch and one at the year-9999 sentinel,
    When each is canonically serialized,
    Then serialization fails closed.
    """
    pre_epoch_record = _record(timestamp=datetime(1969, 1, 1, tzinfo=UTC))
    with pytest.raises(ExecutionChainError):
        canonical_execution_record(pre_epoch_record)
    sentinel_band_record = _record(timestamp=datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC))
    with pytest.raises(ExecutionChainError):
        canonical_execution_record(sentinel_band_record)


def test_boolean_scope_sequence_is_refused() -> None:
    """A boolean masquerading as a counter would fold to 'True'; refuse it.

    Given a record whose scope_sequence is a boolean,
    When it is canonically serialized,
    Then serialization fails closed.
    """
    boolean_sequence_record = _record(scope_sequence=True)
    with pytest.raises(ExecutionChainError):
        canonical_execution_record(boolean_sequence_record)


def test_genesis_binds_scope() -> None:
    """A different wallet, exchange, or mode yields a different genesis and tip.

    Given genesis tips for scopes differing by one component,
    When each is computed and one record is folded on it,
    Then the genesis tips and the folded tips all differ.
    """
    genesis = _genesis()
    assert genesis != execution_chain_genesis(
        "00000000-0000-7000-8000-0000000001ff", _EXCHANGE, _MODE
    )
    assert genesis != execution_chain_genesis(_WALLET, "kraken", _MODE)
    assert genesis != execution_chain_genesis(_WALLET, _EXCHANGE, "paper")
    assert extend_execution_chain(genesis, [_record()]) != extend_execution_chain(
        execution_chain_genesis(_WALLET, "kraken", _MODE), [_record()]
    )


def test_genesis_matches_the_specified_byte_layout() -> None:
    """Pin the genesis byte layout so the domain tag and framing cannot silently change.

    Given the specified domain, tag, and length-prefixed scope fields,
    When the expected SHA-256 is computed independently,
    Then it equals the module's genesis tip.
    """
    expected = hashlib.sha256(
        EXECUTION_CHAIN_DOMAIN
        + b"\x00"
        + struct.pack(">I", 16)
        + UUID(_WALLET).bytes
        + struct.pack(">I", len(_EXCHANGE.encode("utf-8")))
        + _EXCHANGE.encode("utf-8")
        + struct.pack(">I", len(_MODE.encode("utf-8")))
        + _MODE.encode("utf-8")
    ).hexdigest()
    assert execution_chain_genesis(_WALLET, _EXCHANGE, _MODE) == expected


def test_record_matches_the_specified_byte_layout() -> None:
    """Pin the record byte layout so field order and encoders cannot silently change.

    Given a record whose every field holds a distinct value,
    When its canonical serialization is hashed,
    Then it equals the frozen v2 layout digest (any reorder or encoder change breaks it).
    """
    golden = _record(executed_at=datetime(2026, 7, 17, 8, 0, 1, tzinfo=UTC))
    digest = hashlib.sha256(canonical_execution_record(golden)).hexdigest()
    assert digest == "d288353adef065dd131cd6f8bb7caf758c1db6210f602c2d8e08a1eded908cc3"


def test_uuid_spelling_and_case_do_not_change_the_tip() -> None:
    """UUIDs canonicalize to raw bytes, so case or brace spelling cannot drift the tip.

    Given records whose public_id differs only by letter case,
    When each is canonically serialized,
    Then the two encodings are identical.
    """
    lower = _record(public_id="0000abcd-0000-7000-8000-00000000ffff")
    upper = _record(public_id="0000ABCD-0000-7000-8000-00000000FFFF")
    assert canonical_execution_record(lower) == canonical_execution_record(upper)


def test_invalid_uuid_fails_closed() -> None:
    """A non-canonical UUID string is refused rather than silently reframed.

    Given a record whose public_id is not a valid UUID,
    When it is canonically serialized,
    Then serialization fails closed.
    """
    invalid_uuid_record = _record(public_id="not-a-uuid")
    with pytest.raises(ExecutionChainError):
        canonical_execution_record(invalid_uuid_record)


def test_extend_from_a_committed_base_matches_full_derivation() -> None:
    """Checkpoint algebra: extending from a mid-chain tip equals the full derivation.

    Given three records and a checkpoint tip after the first,
    When the tail is folded onto the checkpoint,
    Then the result equals folding all three from genesis.
    """
    genesis = _genesis()
    rows = [_record(scope_sequence=index) for index in (1, 2, 3)]
    full = extend_execution_chain(genesis, rows)
    checkpoint = extend_execution_chain(genesis, rows[:1])
    staged = extend_execution_chain(checkpoint, rows[1:])
    assert full == staged


def test_blank_scope_component_fails_closed() -> None:
    """Genesis refuses a blank exchange or mode.

    Given a blank exchange and, separately, a blank mode,
    When the genesis tip is requested,
    Then it fails closed in each case.
    """
    with pytest.raises(ExecutionChainError):
        execution_chain_genesis(_WALLET, "", _MODE)
    with pytest.raises(ExecutionChainError):
        execution_chain_genesis(_WALLET, _EXCHANGE, "")


def test_malformed_base_tip_fails_closed() -> None:
    """Extension refuses a base tip that is not exactly 64 lowercase hex characters.

    Given a short tip, a non-hex tip, and an uppercased tip,
    When an extension is attempted from each,
    Then it fails closed in every case.
    """
    short_tip_rows = [_record()]
    with pytest.raises(ExecutionChainError):
        extend_execution_chain("deadbeef", short_tip_rows)
    non_hex_tip_rows = [_record()]
    with pytest.raises(ExecutionChainError):
        extend_execution_chain("g" * 64, non_hex_tip_rows)
    uppercased_tip = _genesis().upper()
    uppercased_tip_rows = [_record()]
    with pytest.raises(ExecutionChainError):
        extend_execution_chain(uppercased_tip, uppercased_tip_rows)


def _digest_field_variants() -> dict[str, object]:
    """Return one changed value for every field the canonical record carries."""
    return {
        "scope_sequence": 2,
        "public_id": "00000000-0000-7000-8000-0000000003ff",
        "order_public_id": "00000000-0000-7000-8000-0000000002ff",
        "wallet_public_id": "00000000-0000-7000-8000-0000000001ff",
        "operator_public_id": None,
        "exchange": "kraken",
        "mode": "paper",
        "exec_id": "E-2",
        "trade_id": None,
        "side": "sell",
        "status": "partially_filled",
        "fee_asset": "USD",
        "price_decimal": "1.26",
        "size_decimal": "2.1",
        "fee_decimal": "0.2",
        "counter_amount_decimal": "2.6",
        "numeric_provenance": "legacy_float",
        "liquidity_role": "taker",
        "timestamp": _TS + timedelta(microseconds=1),
        "executed_at": None,
    }


def test_execution_row_digest_is_deterministic_and_domain_separated() -> None:
    """The row digest is stable, well-formed, and never collides with a chain tip.

    Given one execution record,
    When its row digest is computed twice and compared with the chain values
        derived from the same canonical bytes,
    Then both computations agree on one 64-character lowercase hex string that
        equals the explicit domain-tagged SHA-256, and differs from both the
        scope genesis tip and the single-record chain extension — so a digest
        can never be replayed as a tip, nor a tip accepted as a digest.
    """
    record = _record()
    digest = execution_row_digest(record)
    assert digest == execution_row_digest(record)
    assert len(digest) == 64
    assert digest == digest.lower()
    assert (
        digest
        == hashlib.sha256(
            EXECUTION_ROW_DIGEST_DOMAIN + canonical_execution_record(record)
        ).hexdigest()
    )
    assert digest != _genesis()
    assert digest != extend_execution_chain(_genesis(), [record])


def test_execution_row_digest_ignores_a_hypothetical_bitemporal_close() -> None:
    """A close of the target row cannot move its binding digest.

    Given the canonical field mapping of one execution record, extended with the
        ``known_to``, ``id``, ``session_id``, and ``sequence_id`` keys a stored
        row also carries — a hypothetical close constructed by dict manipulation
        rather than by mutating an append-only row,
    When the record is rebuilt from only the fields the canonical form declares
        and its digest is compared with the original,
    Then the digests are equal and none of the added keys were ever declared
        fields, so the annulment's binding proof cannot be invalidated — nor
        silently satisfied — by a bitemporal sentinel or a surrogate key.
    """
    record = _record()
    declared = {field.name for field in dataclasses.fields(record)}
    closed = dataclasses.asdict(record)
    closed["known_to"] = datetime(2026, 7, 25, 12, 0, tzinfo=UTC)
    closed["id"] = 4242
    closed["session_id"] = "00000000-0000-7000-8000-0000000009ff"
    closed["sequence_id"] = 77
    assert declared.isdisjoint({"known_to", "id", "session_id", "sequence_id"})
    rebuilt = ExecutionChainRecord(**{key: closed[key] for key in declared})
    assert execution_row_digest(rebuilt) == execution_row_digest(record)


@pytest.mark.parametrize("field_name", sorted(_digest_field_variants()))
def test_execution_row_digest_is_sensitive_to_every_canonical_field(field_name: str) -> None:
    """Changing any single canonical field changes the binding digest.

    Given one baseline execution record and one altered value for the named
        canonical field,
    When the digest of the altered record is compared with the baseline,
    Then they differ, so no economic value, identity, or instant of a target
        row can be changed while still satisfying a committed annulment's
        binding proof.
    """
    baseline = _record()
    altered = dataclasses.replace(baseline, **{field_name: _digest_field_variants()[field_name]})
    assert execution_row_digest(altered) != execution_row_digest(baseline)
