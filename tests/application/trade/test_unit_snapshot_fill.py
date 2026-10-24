"""Actual private source, late ACK correlation and atomic fill-unit evidence."""

import base64
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from datetime import tzinfo
from pathlib import Path
from typing import TypedDict

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import snapper.application.trade.unit_snapshot_submission as admission_producer
from snapper.application.trade.unit_snapshot_fill import FillUnitCaptureRequest
from snapper.application.trade.unit_snapshot_fill import FuturesFillSourceRequest
from snapper.application.trade.unit_snapshot_fill import bind_futures_fill_source
from snapper.application.trade.unit_snapshot_fill import capture_bound_futures_fill_units
from snapper.application.trade.unit_snapshot_fill import futures_fill_receipt
from snapper.core.contract_accounting_observation import AccountingObservationScope
from snapper.core.contract_accounting_observation import AccountingWriteProvenance
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.execution_route_authority import client_selection_digest
from snapper.core.execution_route_authority import client_selection_from_authority
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.data.contract_accounting_store import capture_accounting_observation
from snapper.data.execution_route_authority_store import insert_execution_route_authority
from snapper.data.execution_unit_snapshot_store import SnapshotBindingError
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import AccountingRawReceipt
from snapper.data.models import AccountingScopeHold
from snapper.data.models import Execution
from snapper.data.models import ExecutionRouteAuthority
from snapper.data.models import ExecutionUnitSnapshot
from snapper.data.models import Order
from snapper.data.models import SymbolAlias
from snapper.data.models import TradeCommand
from snapper.data.models import VenueEvent
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import ExecutionInsertRow
from snapper.data.repository_types import VenueEventInsertRow
from tests.helpers.unit_admission import AdmissionScenario
from tests.helpers.unit_admission import admission_id
from tests.helpers.unit_admission import admission_identity
from tests.helpers.unit_admission import admission_stamp
from tests.helpers.unit_admission import seed_admission_scenario

_NOW = datetime(2026, 10, 23, 9, tzinfo=UTC)
_VENUE = _NOW + timedelta(seconds=1)
_OBSERVED = _NOW + timedelta(milliseconds=1500)
_ACK = _NOW + timedelta(seconds=2)
_KNOWN = _NOW + timedelta(seconds=3)


class _Clock:
    """Keep only the isolated admission producer clock deterministic."""

    @staticmethod
    def now(timezone: tzinfo | None = None) -> datetime:
        """Return the explicit pre-fill submission instant."""
        return _NOW if timezone is None else _NOW.astimezone(timezone)


@dataclass(frozen=True)
class _Scenario:
    """An admitted command and retained source, before any financial target rows exist."""

    admission: AdmissionScenario
    source_request: FuturesFillSourceRequest


class _ExecutionOverrides(TypedDict, total=False):
    """Explicit unsupported target variants inserted through the real repository writer."""

    exec_id: str | None
    executed_at: datetime | None
    price_decimal: str | None
    size_decimal: str | None
    fee_decimal: str | None


def _apply_execution_overrides(row: ExecutionInsertRow, changes: _ExecutionOverrides) -> None:
    """Apply only explicitly typed fixture operands without widening the financial-row contract."""
    if "exec_id" in changes:
        row["exec_id"] = changes["exec_id"]
    if "executed_at" in changes:
        row["executed_at"] = changes["executed_at"]
    if "price_decimal" in changes:
        row["price_decimal"] = changes["price_decimal"]
    if "size_decimal" in changes:
        row["size_decimal"] = changes["size_decimal"]
    if "fee_decimal" in changes:
        row["fee_decimal"] = changes["fee_decimal"]


def _observation(admission: AdmissionScenario) -> RawAccountingObservation:
    """Retain a genuine isolated wire-format fill observed before its REST ACK."""
    raw = json.dumps(
        {
            "feed": "fills",
            "account": "provider-account-A",
            "fills": [
                {
                    "instrument": "PF_XBTUSD",
                    "time": int((_VENUE - datetime(1970, 1, 1, tzinfo=UTC)).total_seconds()) * 1000,
                    "seq": 12,
                    "buy": True,
                    "qty": 1,
                    "price": 100,
                    "fee_paid": "0.10",
                    "fee_currency": "USD",
                    "remaining_order_qty": 1,
                    "order_id": "exchange-order-1",
                    "fill_id": "native-fill-1",
                    "cli_ord_id": "client-1",
                    "fill_type": "maker",
                }
            ],
        }
    ).encode()
    selection = client_selection_from_authority(admission.authority)
    return RawAccountingObservation(
        scope=AccountingObservationScope(
            wallet_public_id=selection.wallet_public_id,
            exchange="kraken_futures",
            mode="live",
            environment=selection.environment,
        ),
        route_revision=client_selection_digest(selection),
        route_authority_public_id=admission.route.authority_public_id,
        credential_public_id=selection.credential_public_id,
        credential_timestamp=selection.credential_timestamp,
        kind="execution",
        channel="fills",
        representation="wire_bytes_base64",
        material=base64.b64encode(raw).decode("ascii"),
        target_json_pointer="/fills/0",
        observed_at=_OBSERVED,
    )


async def _seed_order_history(repository: SQLAlchemyRepository) -> None:
    """Preserve the actual pre-ACK row and the later bounded correlation version."""
    old_at = _NOW - timedelta(minutes=1)
    async with repository.session() as session:
        session.add(
            SymbolAlias(
                **admission_stamp(200, old_at).model_dump(mode="python"),
                symbol_public_id=admission_id(1),
                exchange="kraken_futures",
                channel="ws",
                exchange_symbol="PF_XBTUSD",
                created_at=old_at,
            )
        )
        for at, known_to, native_id in (
            (old_at, _ACK, None),
            (_ACK, KNOWN_TO_MAX, "exchange-order-1"),
        ):
            session.add(
                Order(
                    **admission_stamp(201, at).model_dump(mode="python"),
                    known_to=known_to,
                    wallet_public_id=admission_id(3),
                    instrument_public_id=admission_id(2),
                    mode="live",
                    client_order_id="client-1",
                    exchange_order_id=native_id,
                    created_at=old_at,
                    side="buy",
                    order_type="limit",
                    price=100.0,
                    size=2.0,
                    status="open",
                )
            )
        await session.commit()


@pytest.fixture
async def scenario(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[_Scenario]:
    """Build real temporal catalog, admission, account, alias, order and retained source rows."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'fill.db'}")
    monkeypatch.setattr(admission_producer, "datetime", _Clock)
    await repository.create_all()
    try:
        admission = await seed_admission_scenario(repository, _NOW, source="kraken_futures")
        await admission_producer.capture_submission_unit_admission(
            repository, admission.order, admission.route, admission_identity()
        )
        await _seed_order_history(repository)
        raw = await capture_accounting_observation(
            repository,
            _observation(admission),
            AccountingWriteProvenance(
                public_id=admission_id(202),
                session_id=admission_id(99),
                sequence_id=202,
                timestamp=_OBSERVED,
            ),
        )
        yield _Scenario(
            admission,
            FuturesFillSourceRequest(
                wallet_public_id=admission_id(3),
                raw_receipt_public_id=raw.public_id,
                route_authority_public_id=admission.route.authority_public_id,
            ),
        )
    finally:
        await repository.engine.dispose()


async def _stage_targets(
    session: AsyncSession,
    scenario: _Scenario,
    wrong_quantity: bool = False,
    *,
    execution_overrides: _ExecutionOverrides | None = None,
    event_overrides: VenueEventInsertRow | None = None,
) -> FillUnitCaptureRequest:
    """Use actual no-commit repository writers before binding the source-selected unit witness."""
    repository = scenario.admission.repository
    source = await bind_futures_fill_source(session, scenario.source_request, _KNOWN)
    quantity = 2.0 if wrong_quantity else 1.0
    execution_row: ExecutionInsertRow = {
        "wallet_public_id": admission_id(3),
        "order_public_id": admission_id(201),
        "timestamp": _KNOWN,
        "executed_at": source.fill.occurred_at,
        "side": "buy",
        "status": "partial",
        "price": 100.0,
        "size": quantity,
        "fee": 0.1,
        "fee_asset": "USD",
        "exec_id": source.fill.execution_id,
        "price_decimal": "100.00",
        "size_decimal": str(quantity),
        "fee_decimal": "0.10",
        "numeric_provenance": "venue_raw",
        "session_id": admission_id(99),
        "sequence_id": 203,
    }
    if execution_overrides is not None:
        _apply_execution_overrides(execution_row, execution_overrides)
    execution = await repository.insert_execution_in_transaction(session, execution_row)
    event_row: VenueEventInsertRow = {
        "wallet_public_id": admission_id(3),
        "command_public_id": admission_id(5),
        "event_type": "fill_observed",
        "shard_key": "isolated-futures-fill",
        "exchange": "kraken_futures",
        "instrument": scenario.admission.order.instrument,
        "mode": "live",
        "timestamp": _KNOWN,
        "received_at": _OBSERVED,
        "venue_timestamp": source.fill.occurred_at,
        "side": "buy",
        "fill_size": quantity,
        "fill_price": 100.0,
        "fee": 0.1,
        "fee_asset": "USD",
        "exec_id": source.fill.execution_id,
        "exchange_order_id": source.fill.order_id,
        "client_order_id": "client-1",
        "venue_client_id": source.fill.client_order_id,
        "payload_json": futures_fill_receipt(source).model_dump_json(),
        "session_id": admission_id(99),
        "sequence_id": 204,
    }
    if event_overrides is not None:
        event_row.update(event_overrides)
    event = await repository.insert_venue_event_in_transaction(session, event_row)
    return FillUnitCaptureRequest(
        **scenario.source_request.model_dump(),
        execution_public_id=execution.public_id,
        venue_event_public_id=event.public_id,
    )


def _stamp() -> SnapshotWriteProvenance:
    """Provide an independent immutable unit proof identity at the actual knowledge horizon."""
    return admission_stamp(205, _KNOWN)


async def test_fill_before_ack_is_bound_only_after_actual_correlation_is_known(
    scenario: _Scenario,
) -> None:
    """Later durable correlation preserves the original venue execution time.

    Given: A fill before ACK.
    When: Later correlation is durable.
    Then: Original source time survives.
    """
    repository = scenario.admission.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        request = await _stage_targets(session, scenario)
        result = await capture_bound_futures_fill_units(session, request, _stamp())
        assert result.result.created
        assert result.source.fill.occurred_at == _VENUE
        assert result.source.alias.public_id == admission_id(200)
        assert result.payload.receipt.material == _observation(scenario.admission).material
        assert result.source.raw.target_json_pointer == "/fills/0"
        assert result.payload.dimensions.signed_quantity == 1
        await session.commit()
    async with repository.session() as session:
        execution = (await session.execute(select(Execution))).scalar_one()
        assert execution.timestamp == _KNOWN
        assert execution.executed_at == _VENUE
        assert (await session.execute(select(AccountingScopeHold))).scalar_one()


async def test_failed_unit_binding_rolls_back_financial_rows_but_not_raw_evidence(
    scenario: _Scenario,
) -> None:
    """Failed source binding rolls back financial facts while raw evidence stays retained.

    Given: An aggregate mistaken for one source fill.
    When: Binding fails.
    Then: No financial row commits.
    """
    repository = scenario.admission.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        request = await _stage_targets(session, scenario, wrong_quantity=True)
        with pytest.raises(SnapshotBindingError, match="fill_quantity_price_direction_mismatch"):
            await capture_bound_futures_fill_units(session, request, _stamp())
        await session.rollback()
    async with repository.session() as session:
        assert (await session.execute(select(Execution))).first() is None
        assert (await session.execute(select(VenueEvent))).first() is None
        assert (
            await session.execute(
                select(ExecutionUnitSnapshot).where(ExecutionUnitSnapshot.phase == "fill")
            )
        ).first() is None
        assert (await session.execute(select(AccountingRawReceipt))).scalar_one()
        assert (await session.execute(select(AccountingScopeHold))).scalar_one()


async def test_missing_targets_cannot_be_replaced_with_caller_uuids(scenario: _Scenario) -> None:
    """Caller UUIDs cannot substitute for missing execution and event rows.

    Given: Nonexistent financial targets.
    When: A unit witness is requested.
    Then: It is refused.
    """
    request = FillUnitCaptureRequest(
        **scenario.source_request.model_dump(),
        execution_public_id=admission_id(777),
        venue_event_public_id=admission_id(778),
    )
    async with scenario.admission.repository.session() as session:
        with pytest.raises(SnapshotBindingError, match="fill_actual_target_missing"):
            await capture_bound_futures_fill_units(session, request, _stamp())


async def test_source_cannot_be_interpreted_before_it_was_observed(scenario: _Scenario) -> None:
    """Source proof cannot predate the actual observation.

    Given: A pre-observation knowledge clock.
    When: Source is bound.
    Then: No backdated proof appears.
    """
    async with scenario.admission.repository.session() as session:
        with pytest.raises(SnapshotBindingError, match="knowledge_precedes_raw_observation"):
            await bind_futures_fill_source(session, scenario.source_request, _NOW)


async def _retain_variant(
    scenario: _Scenario, changes: dict[str, object]
) -> FuturesFillSourceRequest:
    """Append distinct original source evidence rather than mutate the retained immutable row."""
    material = {**_observation(scenario.admission).model_dump(mode="python"), **changes}
    observation = RawAccountingObservation.model_validate(material)
    result = await capture_accounting_observation(
        scenario.admission.repository,
        observation,
        AccountingWriteProvenance(
            public_id=admission_id(302),
            session_id=admission_id(99),
            sequence_id=302,
            timestamp=observation.observed_at,
        ),
    )
    return FuturesFillSourceRequest(
        **{
            **scenario.source_request.model_dump(mode="python"),
            "raw_receipt_public_id": result.public_id,
        }
    )


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"credential_public_id": admission_id(999)}, "fill_actual_client_scope_mismatch"),
        ({"route_revision": "f" * 64}, "fill_actual_client_scope_mismatch"),
        (
            {"route_authority_public_id": admission_id(998)},
            "fill_actual_account_scope_mismatch",
        ),
        (
            {"observed_at": _NOW - timedelta(hours=3)},
            "fill_selected_credential_from_future",
        ),
    ],
)
async def test_source_cannot_borrow_another_selected_client_or_knowledge_clock(
    scenario: _Scenario, changes: dict[str, object], reason: str
) -> None:
    """Historical source evidence stays bound to its actual client and clocks.

    Given: Unrelated retained source metadata.
    When: Bound.
    Then: No caller-supplied identity wins.
    """
    request = await _retain_variant(scenario, changes)
    async with scenario.admission.repository.session() as session:
        with pytest.raises(SnapshotBindingError, match=reason):
            await bind_futures_fill_source(session, request, _KNOWN)


async def test_source_without_capture_time_authority_can_bind_its_actual_historical_client(
    scenario: _Scenario,
) -> None:
    """Source retained without authority can bind later historical evidence.

    Given: Source retained before a balance cycle.
    When: Exact later proof exists.
    Then: Bind it.
    """
    request = await _retain_variant(scenario, {"route_authority_public_id": None})
    async with scenario.admission.repository.session() as session:
        result = await bind_futures_fill_source(session, request, _KNOWN)
        assert result.raw.route_authority_public_id is None
        assert result.catalog.instrument.public_id == admission_id(2)


async def test_missing_provider_alias_cannot_be_guessed_from_a_similar_native_symbol(
    scenario: _Scenario,
) -> None:
    """Missing provider spellings cannot be inferred from similar symbols.

    Given: A source spelling absent from the historical map.
    When: Binding.
    Then: Retain the hold.
    """
    source = _observation(scenario.admission)
    wire = base64.b64decode(source.material).replace(b"PF_XBTUSD", b"pf_xbtusd")
    request = await _retain_variant(scenario, {"material": base64.b64encode(wire).decode("ascii")})
    async with scenario.admission.repository.session() as session:
        with pytest.raises(SnapshotBindingError, match="fill_historical_provider_alias_missing"):
            await bind_futures_fill_source(session, request, _KNOWN)


async def _source_with_later_route(
    scenario: _Scenario, observed_at: datetime, recorded_at: datetime
) -> FuturesFillSourceRequest:
    """Commit a real independent authority without rewriting the source's earlier unknown route."""
    source_request = await _retain_variant(scenario, {"route_authority_public_id": None})
    authority = scenario.admission.authority.model_copy(update={"observed_at": observed_at})
    result = await insert_execution_route_authority(
        scenario.admission.repository, authority, admission_stamp(400, recorded_at)
    )
    return FuturesFillSourceRequest(
        wallet_public_id=source_request.wallet_public_id,
        raw_receipt_public_id=source_request.raw_receipt_public_id,
        route_authority_public_id=result.public_id,
    )


@pytest.mark.parametrize("observed_at", (_ACK, _KNOWN + timedelta(seconds=1)))
async def test_route_first_recorded_after_execution_knowledge_cannot_be_borrowed(
    scenario: _Scenario, observed_at: datetime
) -> None:
    """Later authority cannot become proof that was already known at a backdated execution.

    Given: An actual route observed before or after execution but first retained later,
    When: An older execution knowledge horizon requests its original fill units,
    Then: Binding refuses the future durable authority and retains the raw source.
    """
    request = await _source_with_later_route(scenario, observed_at, _KNOWN + timedelta(seconds=1))
    async with scenario.admission.repository.session() as session:
        recorded_at = (
            await session.execute(
                select(ExecutionRouteAuthority.timestamp).where(
                    ExecutionRouteAuthority.public_id == request.route_authority_public_id
                )
            )
        ).scalar_one()
        assert recorded_at > _KNOWN
        with pytest.raises(
            SnapshotBindingError, match="fill_route_authority_not_known_at_execution"
        ):
            await bind_futures_fill_source(session, request, _KNOWN)
        assert (await session.execute(select(AccountingScopeHold))).first() is not None


async def test_raw_receipt_first_recorded_after_execution_knowledge_cannot_be_borrowed(
    scenario: _Scenario,
) -> None:
    """An earlier arrival clock does not make later-retained source available in the past.

    Given: Actual wire material with an earlier observed clock but a later durable timestamp,
    When: An older financial interpretation requests that retained receipt,
    Then: Binding refuses the future record rather than trust observed_at alone.
    """
    raw = await capture_accounting_observation(
        scenario.admission.repository,
        _observation(scenario.admission),
        AccountingWriteProvenance(
            public_id=admission_id(401),
            session_id=admission_id(99),
            sequence_id=401,
            timestamp=_KNOWN + timedelta(seconds=1),
        ),
    )
    request = FuturesFillSourceRequest(
        **{
            **scenario.source_request.model_dump(mode="python"),
            "raw_receipt_public_id": raw.public_id,
        }
    )
    async with scenario.admission.repository.session() as session:
        recorded_at = (
            await session.execute(
                select(AccountingRawReceipt.timestamp).where(
                    AccountingRawReceipt.public_id == raw.public_id
                )
            )
        ).scalar_one()
        assert _observation(scenario.admission).observed_at < _KNOWN < recorded_at
        with pytest.raises(SnapshotBindingError, match="fill_raw_receipt_not_known_at_execution"):
            await bind_futures_fill_source(session, request, _KNOWN)


async def test_later_route_known_by_execution_can_bind_earlier_unscoped_source(
    scenario: _Scenario,
) -> None:
    """Later correlation is valid when all evidence is known by the local execution clock.

    Given: Source observed before the actual account-route witness was retained,
    When: That witness is retained exactly at the execution knowledge horizon,
    Then: Historical binding succeeds without pretending authority existed at venue time.
    """
    request = await _source_with_later_route(scenario, _ACK, _KNOWN)
    async with scenario.admission.repository.session() as session:
        result = await bind_futures_fill_source(session, request, _KNOWN)
        assert result.raw.observed_at < result.authority.observed_at <= _KNOWN
        assert result.raw.route_authority_public_id is None
        assert result.alias.public_id == admission_id(200)


async def test_expired_historical_authority_is_not_current_admission_permission(
    scenario: _Scenario,
) -> None:
    """Historical fills retain valid earlier authority after its CREATE freshness window ends.

    Given: Real source, catalog and route evidence retained before local execution knowledge,
    When: A later historical interpretation occurs beyond the route's valid_until,
    Then: Binding preserves that history without requiring current submission freshness.
    """
    knowledge_at = _KNOWN + timedelta(minutes=5)
    async with scenario.admission.repository.session() as session:
        result = await bind_futures_fill_source(session, scenario.source_request, knowledge_at)
        assert result.authority.valid_until < knowledge_at
        assert result.fill.occurred_at == _VENUE


async def test_command_history_gap_cannot_authorize_fill_units(scenario: _Scenario) -> None:
    """An actual historical command is insufficient when its execution-time successor is absent.

    Given: A real admitted command whose known interval ends before local execution knowledge,
    When: Real execution and source-event rows still reference its retained public identity,
    Then: The producer refuses the missing current command without erasing historical intent.
    """
    repository = scenario.admission.repository
    async with repository.session() as session:
        command = (
            await session.execute(
                select(TradeCommand).where(TradeCommand.public_id == admission_id(5))
            )
        ).scalar_one()
        command.known_to = _ACK
        await session.commit()
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        request = await _stage_targets(session, scenario)
        with pytest.raises(SnapshotBindingError, match="fill_admitted_command_missing"):
            await capture_bound_futures_fill_units(session, request, _stamp())
        await session.rollback()
    async with repository.session() as session:
        command = (
            await session.execute(
                select(TradeCommand).where(TradeCommand.public_id == admission_id(5))
            )
        ).scalar_one()
        assert command.known_to == _ACK
        assert (
            await session.execute(
                select(ExecutionUnitSnapshot).where(
                    ExecutionUnitSnapshot.command_public_id == command.public_id
                )
            )
        ).scalar_one()


async def test_another_command_with_same_cid_cannot_borrow_original_admission(
    scenario: _Scenario,
) -> None:
    """Matching client and economic terms cannot replace one command's missing original unit proof.

    Given: Another real command reuses an admitted command's CID but has no unit snapshot of its own,
    When: Actual financial target rows reference that second durable command,
    Then: The producer refuses rather than borrow the first command's original units by CID.
    """
    repository = scenario.admission.repository
    unadmitted_id = admission_id(500)
    async with repository.session() as session:
        original = (
            await session.execute(
                select(TradeCommand).where(TradeCommand.public_id == admission_id(5))
            )
        ).scalar_one()
        material: dict[str, object] = {
            column.name: getattr(original, column.name)
            for column in TradeCommand.__table__.columns
            if column.name != "id"
        }
        material.update(
            public_id=unadmitted_id,
            timestamp=_ACK,
            created_at=_ACK,
            sequence_id=500,
            correlation_id=admission_id(501),
        )
        session.add(TradeCommand(**material))
        await session.commit()
    async with repository.session() as session:
        assert (
            await session.execute(
                select(TradeCommand).where(TradeCommand.public_id == unadmitted_id)
            )
        ).scalar_one().client_order_id == scenario.admission.order.client_order_id
        assert (
            await session.execute(
                select(ExecutionUnitSnapshot).where(
                    ExecutionUnitSnapshot.command_public_id == unadmitted_id
                )
            )
        ).first() is None
        await session.rollback()
        await repository._begin_execution_insert_transaction(session)
        request = await _stage_targets(
            session, scenario, event_overrides={"command_public_id": unadmitted_id}
        )
        with pytest.raises(SnapshotBindingError, match="fill_admitted_unit_snapshot_missing"):
            await capture_bound_futures_fill_units(session, request, _stamp())
        await session.rollback()
