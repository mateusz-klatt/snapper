"""Durable target, scope, temporal catalog and physical immutability witnesses.

Draft only: run after applying the isolated 0056 patch with the coordinated
SQLite test environment. These tests do not contact a venue or production DB.
"""

import json
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from typing import Literal
from typing import TypedDict

import pytest
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from snapper.core.execution_unit_snapshot import ExecutionUnitSnapshotPayload
from snapper.core.execution_unit_snapshot import FillSnapshotTarget
from snapper.core.execution_unit_snapshot import SnapshotCommandMaterial
from snapper.core.execution_unit_snapshot import SnapshotReceipt
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.core.execution_unit_snapshot import SubmissionSnapshotTarget
from snapper.core.execution_unit_snapshot import command_request_digest
from snapper.data.execution_unit_snapshot_store import SnapshotBindingError
from snapper.data.execution_unit_snapshot_store import SnapshotContentConflictError
from snapper.data.execution_unit_snapshot_store import _submission_knowledge_time
from snapper.data.execution_unit_snapshot_store import bound_execution_digest
from snapper.data.execution_unit_snapshot_store import bound_venue_event_digest
from snapper.data.execution_unit_snapshot_store import durable_command_material
from snapper.data.execution_unit_snapshot_store import insert_execution_unit_snapshot
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Execution
from snapper.data.models import ExecutionUnitSnapshot
from snapper.data.models import Instrument
from snapper.data.models import InstrumentSpec
from snapper.data.models import Order
from snapper.data.models import Symbol
from snapper.data.models import TradeCommand
from snapper.data.models import VenueEvent
from snapper.data.repository import ExecutionPhysicalMutationError
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import _guard_immutable_ledger

_NOW = datetime(2026, 10, 22, 23, tzinfo=UTC)
_SPEC_TIME = _NOW - timedelta(hours=1)


def _id(number: int) -> str:
    """Provide canonical UUIDs for isolated durable lineage."""
    return f"00000000-0000-7000-8000-{number:012d}"


class _Stamp(TypedDict):
    """Typed temporal envelope for real ORM fixture rows."""

    public_id: str
    session_id: str
    sequence_id: int
    timestamp: datetime


class _ExecutionNumbers(TypedDict):
    """Exact execution evidence and its separately persisted float mirrors."""

    price: float
    size: float
    fee: float
    price_decimal: str
    size_decimal: str
    fee_decimal: str


def _stamp(number: int, at: datetime) -> _Stamp:
    """Create explicit identities without relying on uncommitted ORM defaults."""
    return {"public_id": _id(number), "session_id": _id(99), "sequence_id": number, "timestamp": at}


def _provenance(number: int = 20) -> SnapshotWriteProvenance:
    """Keep insertion metadata independent from immutable observation payloads."""
    return SnapshotWriteProvenance(
        public_id=_id(number),
        session_id=_id(99),
        sequence_id=number,
        timestamp=_NOW + timedelta(seconds=2),
    )


async def _seed(
    repo: SQLAlchemyRepository,
    *,
    executed_at: datetime = _NOW,
    initial_base: str = "BTC",
    numbers: _ExecutionNumbers | None = None,
    order_side: Literal["buy", "sell"] = "buy",
) -> None:
    """Persist real command/order/execution/receipt lineage for both target phases."""
    numeric = numbers or _ExecutionNumbers(
        price=100.0,
        size=2.0,
        fee=0.1,
        price_decimal="100.00",
        size_decimal="2.0000",
        fee_decimal="0.10",
    )
    receipt = SnapshotReceipt(
        representation="decoded_json_text",
        material=json.dumps(
            {
                "qty": numeric["size_decimal"],
                "price": numeric["price_decimal"],
                "fee": numeric["fee_decimal"],
            }
        ),
        endpoint_or_channel="fills",
        observed_at=_NOW,
        reported_quantity_unit="base_asset",
        reported_quantity_convention="not_applicable",
    )
    async with repo.session() as session:
        session.add_all(
            [
                Symbol(
                    **_stamp(1, _SPEC_TIME),
                    native_symbol="opaque-PERP",
                    base=initial_base,
                    quote="USD",
                    asset_type="crypto",
                    created_at=_SPEC_TIME,
                ),
                Instrument(
                    **_stamp(2, _SPEC_TIME), symbol_public_id=_id(1), exchange="kraken_futures"
                ),
                InstrumentSpec(
                    **_stamp(4, _SPEC_TIME),
                    instrument_public_id=_id(2),
                    tick_size=0.5,
                    lot_size=0.0001,
                    min_order_size=0.0001,
                    qty_decimals=4,
                    status="active",
                    instrument_kind="perpetual",
                    contract_size=Decimal(1),
                    quantity_unit="contract_count",
                    native_order_quantity_unit="base_asset",
                    contract_payoff_model="linear",
                    contract_value_currency="BTC",
                    settlement_currency="USD",
                    spec_source="kraken_futures:rest.get_instruments",
                    spec_version="s2a-v3:fixture",
                    spec_observed_at=_SPEC_TIME,
                    unit_certified=True,
                ),
                TradeCommand(
                    **_stamp(5, _NOW),
                    command_type="create",
                    shard_key="test-shard",
                    wallet_public_id=_id(3),
                    exchange="kraken_futures",
                    instrument="opaque-PERP",
                    mode="live",
                    strategy_id="manual",
                    client_order_id="client-1",
                    venue_client_id="client-1",
                    side="buy",
                    order_type="limit",
                    quantity=2.0,
                    price=100.0,
                    status="created",
                    created_at=_NOW,
                    correlation_id=_id(15),
                    reduce_only=False,
                    post_only=False,
                ),
                Order(
                    **_stamp(6, _NOW),
                    instrument_public_id=_id(2),
                    mode="live",
                    wallet_public_id=_id(3),
                    client_order_id="client-1",
                    exchange_order_id="venue-order-1",
                    created_at=_NOW,
                    side=order_side,
                    order_type="limit",
                    price=100.0,
                    size=2.0,
                    status="open",
                ),
                Execution(
                    **_stamp(7, _NOW),
                    order_public_id=_id(6),
                    wallet_public_id=_id(3),
                    exchange="kraken_futures",
                    mode="live",
                    scope_sequence=1,
                    exec_id="exec-1",
                    trade_id="trade-1",
                    side="buy",
                    status="filled",
                    **numeric,
                    fee_asset="USD",
                    numeric_provenance="venue_raw",
                    executed_at=executed_at,
                ),
                VenueEvent(
                    **_stamp(8, _NOW),
                    event_type="fill_observed",
                    shard_key="test-shard",
                    wallet_public_id=_id(3),
                    command_public_id=_id(5),
                    exchange="kraken_futures",
                    instrument="opaque-PERP",
                    mode="live",
                    exchange_order_id="venue-order-1",
                    client_order_id="client-1",
                    side="buy",
                    status="filled",
                    fill_price=numeric["price"],
                    fill_size=numeric["size"],
                    fee=numeric["fee"],
                    fee_asset="USD",
                    exec_id="exec-1",
                    trade_id="trade-1",
                    venue_timestamp=_NOW,
                    received_at=_NOW,
                    payload_json=receipt.model_dump_json(),
                ),
            ]
        )
        await session.commit()


@pytest.fixture
async def snapshot_repo() -> AsyncIterator[SQLAlchemyRepository]:
    """Create an isolated real schema with both migration-equivalent trigger installers."""
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await repo.create_all()
    await _seed(repo)
    try:
        yield repo
    finally:
        await repo.engine.dispose()


async def _payload(
    repo: SQLAlchemyRepository, phase: Literal["submission", "fill"]
) -> ExecutionUnitSnapshotPayload:
    """Build typed evidence using digests of actual durable target records."""
    async with repo.session() as session:
        command = (await session.execute(select(TradeCommand))).scalar_one()
        execution = (await session.execute(select(Execution))).scalar_one()
        event = (await session.execute(select(VenueEvent))).scalar_one()
        material = durable_command_material(command)
        target: SubmissionSnapshotTarget | FillSnapshotTarget
        if phase == "submission":
            target = SubmissionSnapshotTarget(
                command_public_id=command.public_id,
                client_order_id=command.client_order_id,
                request_digest=command_request_digest(material),
            )
            receipt = SnapshotReceipt(
                representation="decoded_json_text",
                material=material.model_dump_json(),
                endpoint_or_channel="sendorder-intent",
                observed_at=_NOW,
            )
        else:
            target = FillSnapshotTarget(
                execution_public_id=execution.public_id,
                execution_digest=bound_execution_digest(execution),
                venue_event_public_id=event.public_id,
                venue_event_digest=bound_venue_event_digest(event),
            )
            receipt = SnapshotReceipt.model_validate_json(event.payload_json or "null")
    return ExecutionUnitSnapshotPayload.model_validate(
        {
            "target": target,
            "scope": {
                "wallet_public_id": _id(3),
                "exchange": "kraken_futures",
                "mode": "live",
                "instrument_public_id": _id(2),
            },
            "catalog": {
                "spec_public_id": _id(4),
                "spec_timestamp": _SPEC_TIME,
                "source": "kraken_futures:rest.get_instruments",
                "version": "s2a-v3:fixture",
                "observed_at": _SPEC_TIME,
            },
            "dimensions": {
                "base_currency": "BTC",
                "quote_currency": "USD",
                "native_order_quantity_unit": "base_asset",
                "internal_quantity_unit": "contract_count",
                "signed_quantity_unit": "base_asset",
                "side": "buy",
                "signed_quantity": Decimal(2),
                "quantity_quantum": Decimal("0.0001"),
                "price": Decimal(100),
                "price_quantum": Decimal("0.5"),
                "price_currency": "USD",
                "price_quantity_convention": "quote_per_base",
                "fee": None if phase == "submission" else Decimal("0.1"),
                "fee_currency": None if phase == "submission" else "USD",
                "catalog_contract_multiplier": Decimal(1),
                "payoff_model": "linear",
                "contract_multiplier": Decimal(1),
                "contract_value_currency": "BTC",
                "settlement_currency": "USD",
            },
            "venue_identity": {
                "native_symbol": "opaque-PERP",
                "source": "catalog",
                "source_version": "s2a-v3:fixture",
                "observed_at": _SPEC_TIME,
            },
            "rebase": {"quantity_convention": "not_applicable"},
            "receipt": receipt,
            "interpreted_at": _NOW + timedelta(seconds=1),
        }
    )


@pytest.mark.parametrize("phase", ("submission", "fill"))
async def test_real_target_roundtrip_and_retry_preserve_one_immutable_identity(
    snapshot_repo: SQLAlchemyRepository, phase: Literal["submission", "fill"]
) -> None:
    """Retry provenance cannot create a second semantic target.

    Given: A proven durable command or fill and its exact typed unit interpretation,
    When: The same evidence is retried with a different insertion UUID,
    Then: The original snapshot identity and content remain unchanged.
    """
    payload = await _payload(snapshot_repo, phase)
    first = await insert_execution_unit_snapshot(snapshot_repo, payload, _provenance())
    retried = await insert_execution_unit_snapshot(snapshot_repo, payload, _provenance(21))
    assert first.created is True
    assert retried.created is False
    assert (first.id, first.public_id, first.content_digest) == (
        retried.id,
        retried.public_id,
        retried.content_digest,
    )
    async with snapshot_repo.session() as session:
        record = (await session.execute(select(ExecutionUnitSnapshot))).scalar_one()
        assert record.known_to == KNOWN_TO_MAX
        assert ExecutionUnitSnapshotPayload.model_validate_json(record.canonical_payload) == payload


@pytest.mark.parametrize("phase", ("submission", "fill"))
@pytest.mark.parametrize(
    ("section", "field", "value"),
    (
        ("scope", "wallet_public_id", _id(70)),
        ("scope", "mode", "paper"),
        ("scope", "instrument_public_id", _id(71)),
        ("scope", "exchange", "kraken"),
        ("catalog", "spec_public_id", _id(72)),
        ("catalog", "version", "different-version"),
        ("catalog", "source", "unverified-source"),
        ("dimensions", "price", Decimal(101)),
        ("dimensions", "signed_quantity", Decimal(3)),
        ("dimensions", "quantity_quantum", Decimal("0.01")),
        ("dimensions", "price_quantum", Decimal(1)),
        ("dimensions", "base_currency", "ETH"),
        ("dimensions", "quote_currency", "EUR"),
        ("venue_identity", "native_symbol", "other-symbol"),
    ),
)
async def test_payload_self_consistency_cannot_override_durable_target_or_catalog(
    snapshot_repo: SQLAlchemyRepository,
    phase: Literal["submission", "fill"],
    section: str,
    field: str,
    value: object,
) -> None:
    """Durable lineage defeats caller-forged scope and operands.

    Given: A typed snapshot changed to claim another account, instrument or economic value,
    When: The fenced writer checks the actual target and exact historical spec,
    Then: No immutable snapshot is inserted for the forged interpretation.
    """
    payload = await _payload(snapshot_repo, phase)
    changed = payload.model_dump(mode="python")
    changed[section][field] = value
    with pytest.raises((SnapshotBindingError, ValidationError)):
        await insert_execution_unit_snapshot(
            snapshot_repo, ExecutionUnitSnapshotPayload.model_validate(changed), _provenance()
        )
    async with snapshot_repo.session() as session:
        assert (await session.execute(select(ExecutionUnitSnapshot))).scalars().all() == []


async def test_different_content_for_one_execution_is_a_conflict_not_another_fill(
    snapshot_repo: SQLAlchemyRepository,
) -> None:
    """Independent target uniqueness does not include the digest.

    Given: One execution already bound to a complete immutable snapshot,
    When: A caller changes interpretation provenance for that same execution,
    Then: The writer reports a conflict and retains exactly one snapshot.
    """
    payload = await _payload(snapshot_repo, "fill")
    await insert_execution_unit_snapshot(snapshot_repo, payload, _provenance())
    changed = payload.model_copy(update={"interpreted_at": _NOW + timedelta(seconds=2)})
    with pytest.raises(SnapshotContentConflictError):
        await insert_execution_unit_snapshot(snapshot_repo, changed, _provenance(21))


@pytest.mark.parametrize(
    "field",
    ("execution_digest", "venue_event_digest", "execution_public_id", "venue_event_public_id"),
)
async def test_fill_target_must_bind_real_execution_and_receipt(
    snapshot_repo: SQLAlchemyRepository, field: str
) -> None:
    """Caller-supplied UUIDs or digests do not manufacture a proven fill.

    Given: A legitimate fill snapshot with one incorrect target identity or digest,
    When: The fenced writer reads actual execution, event and order lineage,
    Then: The incorrect binding is refused before insertion.
    """
    payload = await _payload(snapshot_repo, "fill")
    value = "0" * 64 if field.endswith("digest") else _id(75)
    changed = payload.model_copy(
        update={"target": payload.target.model_copy(update={field: value})}
    )
    with pytest.raises(SnapshotBindingError):
        await insert_execution_unit_snapshot(snapshot_repo, changed, _provenance())


@pytest.mark.parametrize(
    ("field", "value"), (("fee", Decimal("0.2")), ("fee_currency", "EUR"), ("side", "sell"))
)
async def test_fill_fee_and_direction_match_canonical_raw_execution_operands(
    snapshot_repo: SQLAlchemyRepository, field: str, value: object
) -> None:
    """A correct execution UUID and digest cannot bless wrong fees or direction.

    Given: An otherwise proven fill whose supplied economics disagree with the ledger,
    When: The writer compares exact execution decimal strings and direction,
    Then: The false economic interpretation is refused.
    """
    payload = await _payload(snapshot_repo, "fill")
    changed = payload.model_dump(mode="python")
    changed["dimensions"][field] = value
    if field == "side":
        changed["dimensions"]["signed_quantity"] = Decimal(-2)
    with pytest.raises(SnapshotBindingError):
        await insert_execution_unit_snapshot(
            snapshot_repo, ExecutionUnitSnapshotPayload.model_validate(changed), _provenance()
        )


async def test_current_catalog_refresh_does_not_reinterpret_an_old_target(
    snapshot_repo: SQLAlchemyRepository,
) -> None:
    """Historical spec identity survives a later active metadata revision.

    Given: A target executed under one exact catalog observation,
    When: A later spec supersedes it with another multiplier and unit interpretation,
    Then: The old target binds its original version rather than today's active row.
    """
    payload = await _payload(snapshot_repo, "fill")
    async with snapshot_repo.session() as session:
        spec = (await session.execute(select(InstrumentSpec))).scalar_one()
        spec.known_to = _NOW + timedelta(minutes=1)
        session.add(
            InstrumentSpec(
                **_stamp(4, _NOW + timedelta(minutes=1)),
                instrument_public_id=_id(2),
                contract_size=Decimal(99),
                quantity_unit="contract_count",
                native_order_quantity_unit="contract_count",
                spec_source="new-source",
                spec_version="new-version",
                spec_observed_at=_NOW + timedelta(minutes=1),
                unit_certified=False,
            )
        )
        await session.commit()
    stored = await insert_execution_unit_snapshot(snapshot_repo, payload, _provenance())
    assert stored.created is True


@pytest.mark.parametrize(
    "statement",
    (
        "UPDATE execution_unit_snapshots SET canonical_payload = '{}'",
        "DELETE FROM execution_unit_snapshots",
    ),
)
async def test_create_all_database_physically_rejects_snapshot_mutation(
    snapshot_repo: SQLAlchemyRepository, statement: str
) -> None:
    """Fresh ORM-built databases enforce the same immutable theorem as migrations.

    Given: A committed snapshot created through the fenced writer,
    When: A connection attempts raw SQL UPDATE or DELETE,
    Then: A physical trigger rejects the mutation.
    """
    await insert_execution_unit_snapshot(
        snapshot_repo, await _payload(snapshot_repo, "fill"), _provenance()
    )
    async with snapshot_repo.session() as session:
        with pytest.raises(IntegrityError):
            await session.execute(text(statement))


def test_generic_temporal_mutation_refuses_the_snapshot_table_by_physical_name() -> None:
    """Generic revision helpers cannot silently close or supersede immutable evidence.

    Given: The new snapshot model and its physical table name,
    When: A generic mutation primitive resolves either spelling,
    Then: The central immutable ledger registry refuses the target.
    """
    for target in (
        ExecutionUnitSnapshot,
        ExecutionUnitSnapshot.__table__,
        "execution_unit_snapshots",
    ):
        with pytest.raises(ExecutionPhysicalMutationError):
            _guard_immutable_ledger(target, "snapshot mutation forbidden")


async def test_exact_retry_uses_immutable_snapshot_after_source_event_changes(
    snapshot_repo: SQLAlchemyRepository,
) -> None:
    """A later mutable source edit cannot invalidate an already-proven exact retry.

    Given: One immutable fill snapshot and its original canonical payload,
    When: The source event changes and the same snapshot is retried,
    Then: The writer returns the original identity without reinterpreting mutable lineage.
    """
    payload = await _payload(snapshot_repo, "fill")
    original = await insert_execution_unit_snapshot(snapshot_repo, payload, _provenance())
    async with snapshot_repo.session() as session:
        event = (await session.execute(select(VenueEvent))).scalar_one()
        event.payload_json = '{"changed":true}'
        event.exchange_order_id = "later-source-change"
        await session.commit()
    repeated = await insert_execution_unit_snapshot(snapshot_repo, payload, _provenance(21))
    assert repeated.id == original.id
    assert repeated.public_id == original.public_id
    assert repeated.content_digest == original.content_digest
    assert repeated.created is False


@pytest.mark.parametrize(
    ("phase", "reason"),
    (("submission", "submission_target_missing"), ("fill", "precedes_durable")),
)
async def test_new_interpretation_cannot_precede_real_durable_target_evidence(
    snapshot_repo: SQLAlchemyRepository, phase: Literal["submission", "fill"], reason: str
) -> None:
    """Consistent caller timestamps cannot invent knowledge before an actual target exists.

    Given: A command or fill whose durable bus timestamp follows claimed interpretation,
    When: The caller supplies older self-consistent observation metadata,
    Then: The writer checks actual durable evidence and refuses the premature interpretation.
    """
    early = _NOW - timedelta(microseconds=1)
    if phase == "fill":
        async with snapshot_repo.session() as session:
            event = (await session.execute(select(VenueEvent))).scalar_one()
            receipt = SnapshotReceipt.model_validate_json(event.payload_json or "null")
            event.payload_json = receipt.model_copy(update={"observed_at": early}).model_dump_json()
            await session.commit()
    payload = await _payload(snapshot_repo, phase)
    changed = payload.model_dump(mode="python")
    changed["interpreted_at"] = early
    changed["venue_identity"]["observed_at"] = early
    changed["receipt"]["observed_at"] = early
    with pytest.raises(SnapshotBindingError, match=reason):
        await insert_execution_unit_snapshot(
            snapshot_repo, ExecutionUnitSnapshotPayload.model_validate(changed), _provenance()
        )


async def test_late_venue_execution_uses_ingestion_knowledge_clock_for_catalog_binding() -> None:
    """An older economic venue timestamp does not select a pre-ingestion catalog.

    Given: An ingested fill whose venue execution precedes the available catalog observation,
    When: The snapshot binds its actual durable execution knowledge timestamp,
    Then: The exact catalog effective at ingestion is retained without rewriting economic time.
    """
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    try:
        await repo.create_all()
        await _seed(repo, executed_at=_SPEC_TIME - timedelta(hours=1))
        payload = await _payload(repo, "fill")
        stored = await insert_execution_unit_snapshot(repo, payload, _provenance())
        assert stored.created is True
        async with repo.session() as session:
            execution = (await session.execute(select(Execution))).scalar_one()
            assert execution.executed_at == _SPEC_TIME - timedelta(hours=1)
    finally:
        await repo.engine.dispose()


async def test_instrument_identity_uses_the_same_target_knowledge_clock_as_catalog() -> None:
    """A symbol revision between specification and execution uses target-time identity.

    Given: One exact spec and a later canonical symbol revision before the fill is ingested,
    When: The snapshot verifies identity and dimensions at the durable target timestamp,
    Then: It binds the target-time symbol rather than an unrelated older catalog-time label.
    """
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    try:
        await repo.create_all()
        await _seed(repo, initial_base="OLD-BASE-LABEL")
        async with repo.session() as session:
            symbol = (await session.execute(select(Symbol))).scalar_one()
            revision_at = _NOW - timedelta(minutes=1)
            symbol.known_to = revision_at
            session.add(
                Symbol(
                    **_stamp(1, revision_at),
                    native_symbol="opaque-PERP",
                    base="BTC",
                    quote="USD",
                    asset_type="crypto",
                    created_at=_SPEC_TIME,
                )
            )
            await session.commit()
        stored = await insert_execution_unit_snapshot(
            repo, await _payload(repo, "fill"), _provenance()
        )
        assert stored.created is True
    finally:
        await repo.engine.dispose()


async def test_status_only_command_revision_keeps_initial_intent_knowledge_clock(
    snapshot_repo: SQLAlchemyRepository,
) -> None:
    """A later command lifecycle row does not change the intent's original catalog binding.

    Given: A create command later advanced to dispatched after its original spec closes,
    When: A new submission snapshot is bound to the unchanged logical request,
    Then: The first durable command timestamp selects the original specification.
    """
    payload = await _payload(snapshot_repo, "submission")
    await snapshot_repo.update_trade_command_status(
        public_id=_id(5),
        new_status="dispatched",
        bus_time=_NOW + timedelta(minutes=5),
        session_id=_id(99),
        sequence_id=100,
    )
    async with snapshot_repo.session() as session:
        spec = (await session.execute(select(InstrumentSpec))).scalar_one()
        spec.known_to = _NOW + timedelta(minutes=1)
        await session.commit()
    later = payload.model_copy(update={"interpreted_at": _NOW + timedelta(minutes=6)})
    stamp = _provenance().model_copy(update={"timestamp": _NOW + timedelta(minutes=7)})
    stored = await insert_execution_unit_snapshot(snapshot_repo, later, stamp)
    assert stored.created is True


@pytest.mark.parametrize(
    "changes",
    (
        {"exchange_order_id": None, "client_order_id": None},
        {"exchange_order_id": "", "client_order_id": None},
        {"exchange_order_id": "venue-order-1", "client_order_id": "other-client"},
        {"exchange_order_id": "", "client_order_id": "client-1"},
        {"exec_id": None, "trade_id": None},
        {"exec_id": None, "trade_id": " "},
        {"exec_id": "exec-1", "trade_id": "other-trade"},
        {"exec_id": "other-execution", "trade_id": "trade-1"},
    ),
)
async def test_fill_correlation_requires_affirmative_ids_and_consistent_secondary_ids(
    snapshot_repo: SQLAlchemyRepository, changes: dict[str, str | None]
) -> None:
    """Null equality, blank IDs and conflicting secondary IDs never prove correlation.

    Given: A real execution with missing or inconsistent order/fill source identities,
    When: The snapshot writer validates every reported durable correlation field,
    Then: It refuses the new binding even with a correctly recomputed event digest.
    """
    async with snapshot_repo.session() as session:
        event = (await session.execute(select(VenueEvent))).scalar_one()
        for field, value in changes.items():
            setattr(event, field, value)
        await session.commit()
    with pytest.raises(SnapshotBindingError, match="identity_(missing|mismatch)"):
        await insert_execution_unit_snapshot(
            snapshot_repo, await _payload(snapshot_repo, "fill"), _provenance()
        )


@pytest.mark.parametrize(
    "changes",
    (
        {"exchange_order_id": None},
        {"client_order_id": None},
        {"exec_id": None},
        {"trade_id": None},
    ),
)
async def test_one_affirmative_order_and_fill_id_allows_absent_secondary_ids(
    snapshot_repo: SQLAlchemyRepository, changes: dict[str, str | None]
) -> None:
    """An absent optional secondary ID does not discard an affirmatively correlated fill.

    Given: A matching order identity and fill identity with one optional source ID absent,
    When: Every actually reported ID matches the real durable target,
    Then: The complete known evidence is stored without inventing the missing identifier.
    """
    async with snapshot_repo.session() as session:
        event = (await session.execute(select(VenueEvent))).scalar_one()
        for field, value in changes.items():
            setattr(event, field, value)
        await session.commit()
    result = await insert_execution_unit_snapshot(
        snapshot_repo, await _payload(snapshot_repo, "fill"), _provenance()
    )
    assert result.created is True


@pytest.mark.parametrize(
    "changes",
    (
        {"reported_quantity_unit": "base_asset"},
        {"reported_quantity_convention": "spv_token"},
        {"reporting_option_name": "rebased", "reporting_option_value": "true"},
    ),
)
async def test_command_material_receipts_cannot_claim_actual_fill_reporting_context(
    snapshot_repo: SQLAlchemyRepository, changes: dict[str, str]
) -> None:
    """A submission command receipt never doubles as observed fill-unit metadata.

    Given: A real command material receipt with claimed fill units or reporting options,
    When: A new submission snapshot tries to bind that conflicting receipt role,
    Then: The writer refuses the reporting claim while preserving intent semantics separately.
    """
    payload = await _payload(snapshot_repo, "submission")
    changed = payload.model_copy(update={"receipt": payload.receipt.model_copy(update=changes)})
    with pytest.raises(SnapshotBindingError, match="cannot_claim_fill_reporting"):
        await insert_execution_unit_snapshot(snapshot_repo, changed, _provenance())


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("post_only", True),
        ("reduce_only", True),
        ("quantity", 3.0),
        ("price", 101.0),
        ("stop_price", 90.0),
        ("leverage", 2),
    ),
)
async def test_changed_economic_command_history_cannot_reuse_initial_intent_clock(
    snapshot_repo: SQLAlchemyRepository, field: str, value: object
) -> None:
    """A changed request cannot borrow the historical timestamp of another economic intent.

    Given: One logical create command with an inconsistent later SCD2 request revision,
    When: A snapshot otherwise matches the active revision's request and operands,
    Then: Complete durable history comparison rejects the retroactive intent substitution.
    """
    payload = await _payload(snapshot_repo, "submission")
    await snapshot_repo.update_trade_command_status(
        public_id=_id(5),
        new_status="dispatched",
        bus_time=_NOW + timedelta(seconds=1),
        session_id=_id(99),
        sequence_id=100,
    )
    async with snapshot_repo.session() as session:
        active = (
            await session.execute(select(TradeCommand).where(TradeCommand.known_to == KNOWN_TO_MAX))
        ).scalar_one()
        setattr(active, field, value)
        await session.commit()
        request = durable_command_material(active)
    changed = payload.model_dump(mode="python")
    changed["target"]["request_digest"] = command_request_digest(request)
    changed["receipt"]["material"] = request.model_dump_json()
    changed["dimensions"]["signed_quantity"] = request.quantity
    changed["dimensions"]["price"] = request.price
    changed["dimensions"]["stop_price"] = request.stop_price
    changed["interpreted_at"] = _NOW + timedelta(seconds=2)
    stamp = _provenance().model_copy(update={"timestamp": _NOW + timedelta(seconds=3)})
    with pytest.raises(SnapshotBindingError, match="request_changed_between_versions"):
        await insert_execution_unit_snapshot(
            snapshot_repo, ExecutionUnitSnapshotPayload.model_validate(changed), stamp
        )


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("fill_size", 100.0),
        ("fill_price", 999.0),
        ("fee", None),
        ("fee", 9.0),
        ("fee_asset", "EUR"),
    ),
)
async def test_source_event_numeric_mirror_contradictions_cannot_bind_an_execution(
    snapshot_repo: SQLAlchemyRepository, field: str, value: object
) -> None:
    """Matching IDs and a recomputed receipt digest cannot hide incompatible observed economics.

    Given: A source event whose persisted numeric mirror or fee currency conflicts with execution,
    When: The caller correctly recomputes the event digest for those conflicting facts,
    Then: The new binding refuses the contradiction instead of certifying matching IDs alone.
    """
    async with snapshot_repo.session() as session:
        event = (await session.execute(select(VenueEvent))).scalar_one()
        setattr(event, field, value)
        await session.commit()
    with pytest.raises(SnapshotBindingError, match="numeric_mirror_contradiction"):
        await insert_execution_unit_snapshot(
            snapshot_repo, await _payload(snapshot_repo, "fill"), _provenance()
        )


async def test_raw_execution_precision_is_preserved_beyond_matching_numeric_mirrors() -> None:
    """A float mirror match checks contradiction without rounding exact execution operands.

    Given: Exact canonical quantity text with significant digits beyond the shared float mirror,
    When: The source and execution mirrors agree and the snapshot preserves the exact raw Decimal,
    Then: The writer accepts the binding without comparing a float approximation to raw text.
    """
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    precise = "2.00000000000000000001"
    try:
        await repo.create_all()
        await _seed(repo, numbers=_execution_numbers("size", precise, 2.0))
        payload = await _payload(repo, "fill")
        exact = payload.model_copy(
            update={
                "dimensions": payload.dimensions.model_copy(
                    update={"signed_quantity": Decimal(precise)}
                )
            }
        )
        result = await insert_execution_unit_snapshot(repo, exact, _provenance())
        assert result.created is True
    finally:
        await repo.engine.dispose()


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("wallet_public_id", _id(77)),
        ("exchange", "kraken"),
        ("mode", "paper"),
        ("instrument", "other-native"),
        ("side", "sell"),
        ("client_order_id", "other-client"),
        ("exchange_order_id", "other-order"),
    ),
)
async def test_reported_command_must_match_real_execution_order_lineage(
    snapshot_repo: SQLAlchemyRepository, field: str, value: str
) -> None:
    """A reported command UUID cannot point to another account, instrument, direction or order.

    Given: Matching event/execution IDs but a contradictory actual reported command,
    When: The new fill snapshot reads command lineage at the execution knowledge timestamp,
    Then: It refuses that separate command rather than trusting the caller's UUID alone.
    """
    async with snapshot_repo.session() as session:
        command = (await session.execute(select(TradeCommand))).scalar_one()
        setattr(command, field, value)
        await session.commit()
    with pytest.raises(SnapshotBindingError, match="(reported_command|command_order)"):
        await insert_execution_unit_snapshot(
            snapshot_repo, await _payload(snapshot_repo, "fill"), _provenance()
        )


@pytest.mark.parametrize("command_id", (None, _id(78)))
async def test_optional_reported_command_is_absent_evidence_or_an_actual_durable_target(
    snapshot_repo: SQLAlchemyRepository, command_id: str | None
) -> None:
    """An absent command is honest evidence while an invented reported command is refused.

    Given: A real fill with no command claim or an unknown command UUID claim,
    When: Its exact event receipt and order/execution IDs are bound,
    Then: Absence remains unknown and an affirmative nonexistent target fails closed.
    """
    async with snapshot_repo.session() as session:
        event = (await session.execute(select(VenueEvent))).scalar_one()
        event.command_public_id = command_id
        await session.commit()
    payload = await _payload(snapshot_repo, "fill")
    if command_id is None:
        assert (await insert_execution_unit_snapshot(snapshot_repo, payload, _provenance())).created
    else:
        with pytest.raises(SnapshotBindingError, match="reported_command_missing"):
            await insert_execution_unit_snapshot(snapshot_repo, payload, _provenance())


@pytest.mark.parametrize("venue_client_id", ("client-1", "wrong-client", ""))
async def test_reported_venue_client_id_matches_the_bound_actual_command(
    snapshot_repo: SQLAlchemyRepository, venue_client_id: str
) -> None:
    """Optional venue-client identity is checked whenever the source reports an actual command.

    Given: A command-bound fill with a matching, conflicting or blank reported venue client ID,
    When: The writer verifies every reported source identity,
    Then: Only the exact nonblank durable command identity is accepted.
    """
    async with snapshot_repo.session() as session:
        event = (await session.execute(select(VenueEvent))).scalar_one()
        event.venue_client_id = venue_client_id
        await session.commit()
    payload = await _payload(snapshot_repo, "fill")
    if venue_client_id == "client-1":
        assert (await insert_execution_unit_snapshot(snapshot_repo, payload, _provenance())).created
    else:
        with pytest.raises(SnapshotBindingError, match="command_venue_client_identity"):
            await insert_execution_unit_snapshot(snapshot_repo, payload, _provenance())


@pytest.mark.parametrize(
    "field", ("timestamp", "venue_client_id", "fill_size", "fill_price", "fee", "fee_asset")
)
async def test_venue_event_digest_binds_every_new_source_guard_operand(
    snapshot_repo: SQLAlchemyRepository, field: str
) -> None:
    """Durable clock and mirror checks rely on fields included in the source binding digest.

    Given: One durable source event and its complete binding digest,
    When: Any newly guarded identity, timestamp or numeric mirror field changes,
    Then: The source digest changes independently of its otherwise unchanged receipt material.
    """
    async with snapshot_repo.session() as session:
        event = (await session.execute(select(VenueEvent))).scalar_one()
        before = bound_venue_event_digest(event)
        changes: dict[str, object] = {
            "timestamp": _NOW + timedelta(microseconds=1),
            "venue_client_id": "other-client",
            "fill_size": 3.0,
            "fill_price": 101.0,
            "fee": 0.2,
            "fee_asset": "EUR",
        }
        setattr(event, field, changes[field])
        assert bound_venue_event_digest(event) != before


def _execution_numbers(
    field: Literal["price", "size", "fee"], raw: str, mirror: float
) -> _ExecutionNumbers:
    """Vary one exact operand and its independent durable float mirror.

    Args:
        field: Economic operand varied in the seeded execution.
        raw: Exact canonical venue decimal string.
        mirror: Independently supplied legacy float mirror.

    Returns:
        Complete typed financial fields for a fresh immutable execution.
    """
    numbers = _ExecutionNumbers(
        price=100.0,
        size=2.0,
        fee=0.1,
        price_decimal="100.00",
        size_decimal="2.0000",
        fee_decimal="0.10",
    )
    if field == "price":
        numbers["price_decimal"], numbers["price"] = raw, mirror
    elif field == "size":
        numbers["size_decimal"], numbers["size"] = raw, mirror
    else:
        numbers["fee_decimal"], numbers["fee"] = raw, mirror
    return numbers


async def _numeric_payload(
    repo: SQLAlchemyRepository, numbers: _ExecutionNumbers
) -> ExecutionUnitSnapshotPayload:
    """Keep copied operands exactly equal to a fresh execution's raw strings.

    Args:
        repo: Isolated repository containing the real execution and source receipt.
        numbers: Exact strings already inserted into that immutable execution.

    Returns:
        A coherent payload whose only disputed evidence is numeric mirror compatibility.
    """
    payload = await _payload(repo, "fill")
    return payload.model_copy(
        update={
            "dimensions": payload.dimensions.model_copy(
                update={
                    "signed_quantity": Decimal(numbers["size_decimal"]),
                    "price": Decimal(numbers["price_decimal"]),
                    "fee": Decimal(numbers["fee_decimal"]),
                }
            )
        }
    )


@pytest.mark.parametrize(
    ("field", "raw", "mirror", "reason"),
    (
        ("price", "999", 100.0, "raw_mirror_contradiction"),
        ("size", "999", 2.0, "raw_mirror_contradiction"),
        ("fee", "999", 0.1, "raw_mirror_contradiction"),
        ("price", "1e999", 100.0, "raw_projection_nonfinite"),
        ("size", "1e999", 2.0, "raw_projection_nonfinite"),
        ("fee", "1e999", 0.1, "raw_projection_nonfinite"),
        ("price", "1e-999", 0.0, "raw_projection_underflow"),
        ("size", "1e-999", 0.0, "raw_projection_underflow"),
        ("fee", "1e-999", 0.0, "raw_projection_underflow"),
        ("fee", "-1e-999", -0.0, "raw_projection_underflow"),
    ),
)
async def test_raw_execution_operands_must_have_finite_visible_compatible_mirrors(
    field: Literal["price", "size", "fee"], raw: str, mirror: float, reason: str
) -> None:
    """Matching source mirrors cannot certify contradictory or invisible exact economics.

    Given: A fresh execution whose source event shares its mirrors but whose raw operand disagrees,
    When: A snapshot preserves that exact raw operand and the real execution and source digests,
    Then: The public writer refuses before inserting any false unit evidence.
    """
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    numbers = _execution_numbers(field, raw, mirror)
    try:
        await repo.create_all()
        await _seed(repo, numbers=numbers)
        payload = await _numeric_payload(repo, numbers)
        with pytest.raises(SnapshotBindingError, match=reason):
            await insert_execution_unit_snapshot(repo, payload, _provenance())
        async with repo.session() as session:
            assert (await session.scalars(select(ExecutionUnitSnapshot))).all() == []
    finally:
        await repo.engine.dispose()


@pytest.mark.parametrize(
    ("field", "raw", "mirror"),
    (
        ("price", "100.00000000000000000001", 100.0),
        ("size", "2.00000000000000000001", 2.0),
        ("fee", "0.10000000000000000001", 0.1),
    ),
)
async def test_compatible_float_projection_preserves_all_exact_raw_operands(
    field: Literal["price", "size", "fee"], raw: str, mirror: float
) -> None:
    """Accept legitimate float rounding without replacing exact snapshot economics.

    Given: Canonical venue evidence with significant digits beyond its finite float projection,
    When: A snapshot binds those exact strings and compatible durable source mirrors,
    Then: The stored typed operands retain their exact Decimal values rather than the mirrors.
    """
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    numbers = _execution_numbers(field, raw, mirror)
    try:
        await repo.create_all()
        await _seed(repo, numbers=numbers)
        payload = await _numeric_payload(repo, numbers)
        result = await insert_execution_unit_snapshot(repo, payload, _provenance())
        assert result.created is True
        async with repo.session() as session:
            stored = (await session.scalars(select(ExecutionUnitSnapshot))).one()
            restored = ExecutionUnitSnapshotPayload.model_validate_json(stored.canonical_payload)
            assert restored == payload
    finally:
        await repo.engine.dispose()


@pytest.mark.parametrize("raw", ("0", "0.0000", "-0.0000"))
async def test_exact_zero_fees_are_known_zero_without_nonzero_underflow(raw: str) -> None:
    """Distinguish legitimate explicit zero fees from an invisible nonzero fee.

    Given: A real execution whose exact fee string is zero, including a signed-zero spelling,
    When: Its source mirror is zero and a snapshot preserves that known fee,
    Then: The public writer accepts the binding while the immutable raw string remains unchanged.
    """
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    numbers = _execution_numbers("fee", raw, 0.0)
    try:
        await repo.create_all()
        await _seed(repo, numbers=numbers)
        result = await insert_execution_unit_snapshot(
            repo, await _numeric_payload(repo, numbers), _provenance()
        )
        assert result.created is True
        async with repo.session() as session:
            execution = (await session.scalars(select(Execution))).one()
            assert execution.fee_decimal == raw
    finally:
        await repo.engine.dispose()


async def test_correlated_fill_direction_cannot_contradict_its_durable_order() -> None:
    """Matching venue identities do not excuse a contradictory durable order direction.

    Given: A freshly persisted sell order with correlated buy execution and source observation,
    When: A caller supplies their actual IDs, exact raw operands and valid digests,
    Then: The public writer refuses the lineage contradiction without changing durable evidence.
    """
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    try:
        await repo.create_all()
        await _seed(repo, order_side="sell")
        payload = await _payload(repo, "fill")
        with pytest.raises(SnapshotBindingError, match="direction_mismatch"):
            await insert_execution_unit_snapshot(repo, payload, _provenance())
        async with repo.session() as session:
            assert (await session.scalars(select(ExecutionUnitSnapshot))).all() == []
            assert (await session.scalars(select(Order))).one().side == "sell"
    finally:
        await repo.engine.dispose()


async def test_submission_uuid_must_resolve_to_an_actual_durable_create(
    snapshot_repo: SQLAlchemyRepository,
) -> None:
    """A valid-looking intent UUID cannot manufacture a missing durable submission.

    Given: Independently stored command evidence and a different nonexistent target UUID,
    When: The public writer tries to bind the supplied submission target,
    Then: Missing durable intent is refused before any snapshot insertion.
    """
    payload = await _payload(snapshot_repo, "submission")
    changed = payload.model_copy(
        update={"target": payload.target.model_copy(update={"command_public_id": _id(75)})}
    )
    with pytest.raises(SnapshotBindingError, match="submission_target_missing"):
        await insert_execution_unit_snapshot(snapshot_repo, changed, _provenance())
    async with snapshot_repo.session() as session:
        assert (await session.scalars(select(ExecutionUnitSnapshot))).all() == []


async def test_submission_interpretation_cannot_read_a_command_first_known_later(
    snapshot_repo: SQLAlchemyRepository,
) -> None:
    """A later durable command cannot justify an earlier otherwise valid snapshot.

    Given: A valid snapshot payload and a real command first known after its interpretation,
    When: The public writer resolves command material at that interpretation timestamp,
    Then: It refuses the unavailable as-of target and stores no snapshot or rewritten evidence.
    """
    payload = await _payload(snapshot_repo, "submission")
    later = _NOW + timedelta(seconds=3)
    async with snapshot_repo.session() as session:
        command = (await session.scalars(select(TradeCommand))).one()
        command.timestamp = later
        await session.commit()
    with pytest.raises(SnapshotBindingError, match="submission_target_missing"):
        await insert_execution_unit_snapshot(snapshot_repo, payload, _provenance())
    async with snapshot_repo.session() as session:
        assert (await session.scalars(select(ExecutionUnitSnapshot))).all() == []
        command = (await session.scalars(select(TradeCommand))).one()
        assert command.timestamp == later > payload.interpreted_at


async def test_initial_knowledge_lookup_refuses_material_without_any_durable_history(
    snapshot_repo: SQLAlchemyRepository,
) -> None:
    """The defensive history lookup never assigns a clock to an unpersisted request.

    Given: An independently authored valid command-material token with no stored versions,
    When: The history helper queries the real repository for its first knowledge timestamp,
    Then: It refuses rather than inventing an initial observation time.
    """
    request = SnapshotCommandMaterial(
        command_public_id=_id(75),
        wallet_public_id=_id(3),
        exchange="kraken_futures",
        mode="live",
        native_symbol="opaque-PERP",
        client_order_id="unpersisted-client",
        side="buy",
        order_type="limit",
        quantity=Decimal(2),
        price=Decimal(100),
        stop_price=None,
        leverage=None,
        post_only=False,
        reduce_only=False,
    )
    async with snapshot_repo.session() as session:
        with pytest.raises(SnapshotBindingError, match="initial_knowledge_time_missing"):
            await _submission_knowledge_time(session, request)
        assert (await session.scalars(select(ExecutionUnitSnapshot))).all() == []


async def test_non_decimal_execution_text_is_not_a_certified_numeric_operand() -> None:
    """Nonblank immutable raw text still needs a valid decimal interpretation.

    Given: A real execution retaining malformed nonblank price text and compatible legacy mirrors,
    When: A snapshot binds the actual execution and source receipt digests,
    Then: Decimal parsing refuses the evidence without editing the execution or storing a snapshot.
    """
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    try:
        await repo.create_all()
        await _seed(repo, numbers=_execution_numbers("price", "not-a-decimal", 100.0))
        payload = await _payload(repo, "fill")
        with pytest.raises(SnapshotBindingError, match="fill_raw_evidence_invalid"):
            await insert_execution_unit_snapshot(repo, payload, _provenance())
        async with repo.session() as session:
            assert (await session.scalars(select(ExecutionUnitSnapshot))).all() == []
            assert (await session.scalars(select(Execution))).one().price_decimal == "not-a-decimal"
    finally:
        await repo.engine.dispose()


async def test_fill_requires_an_order_version_effective_when_execution_was_known(
    snapshot_repo: SQLAlchemyRepository,
) -> None:
    """An order UUID observed only after the fill does not prove its temporal lineage.

    Given: A real execution whose order version begins after its durable knowledge timestamp,
    When: The public writer resolves order identity at that timestamp,
    Then: It refuses the missing historical order without accepting present-day identity alone.
    """
    async with snapshot_repo.session() as session:
        order = (await session.scalars(select(Order))).one()
        order.timestamp = _NOW + timedelta(seconds=1)
        await session.commit()
    with pytest.raises(SnapshotBindingError, match="fill_order_lineage_missing"):
        await insert_execution_unit_snapshot(
            snapshot_repo, await _payload(snapshot_repo, "fill"), _provenance()
        )
    async with snapshot_repo.session() as session:
        assert (await session.scalars(select(ExecutionUnitSnapshot))).all() == []


async def test_catalog_spec_cannot_replace_missing_target_time_instrument_lineage(
    snapshot_repo: SQLAlchemyRepository,
) -> None:
    """A retained specification cannot certify an instrument outside its effective interval.

    Given: An exact retained spec but an instrument version closed before the target was known,
    When: The writer resolves the instrument and symbol at the actual target timestamp,
    Then: Missing target-time instrument lineage refuses the new snapshot.
    """
    async with snapshot_repo.session() as session:
        instrument = (await session.scalars(select(Instrument))).one()
        instrument.known_to = _NOW - timedelta(microseconds=1)
        await session.commit()
    with pytest.raises(SnapshotBindingError, match="catalog_instrument_lineage_missing"):
        await insert_execution_unit_snapshot(
            snapshot_repo, await _payload(snapshot_repo, "fill"), _provenance()
        )
    async with snapshot_repo.session() as session:
        assert (await session.scalars(select(ExecutionUnitSnapshot))).all() == []


async def test_malformed_durable_receipt_contract_is_a_binding_refusal(
    snapshot_repo: SQLAlchemyRepository,
) -> None:
    """A matching digest does not make an incomplete stored receipt a typed observation.

    Given: A valid caller payload and actual source event whose receipt becomes an empty object,
    When: The caller binds the event's new actual digest and the writer parses durable evidence,
    Then: The public boundary reports unavailable binding and inserts no snapshot.
    """
    payload = await _payload(snapshot_repo, "fill")
    async with snapshot_repo.session() as session:
        event = (await session.scalars(select(VenueEvent))).one()
        event.payload_json = "{}"
        await session.commit()
        digest = bound_venue_event_digest(event)
    changed = payload.model_copy(
        update={"target": payload.target.model_copy(update={"venue_event_digest": digest})}
    )
    with pytest.raises(SnapshotBindingError, match="Durable evidence does not satisfy"):
        await insert_execution_unit_snapshot(snapshot_repo, changed, _provenance())
    async with snapshot_repo.session() as session:
        assert (await session.scalars(select(ExecutionUnitSnapshot))).all() == []
        assert (await session.scalars(select(VenueEvent))).one().payload_json == "{}"
