"""Real issued private fills, later native account logs and actual Execution/unit fixture for v2 positions."""

import base64
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import select

from snapper.application.trade.account_bound_submission_admission import (
    capture_account_submission_admission,
)
from snapper.application.trade.execution_observation_recovery import recoverable_frame_children
from snapper.core.contract_accounting_observation import AccountingObservationScope
from snapper.core.contract_accounting_observation import AccountingWriteProvenance
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.execution_route_authority import client_selection_digest
from snapper.core.execution_unit_admission import ExecutionRouteReference
from snapper.core.execution_unit_snapshot import ExecutionUnitSnapshotPayload
from snapper.core.execution_unit_snapshot import FillSnapshotTarget
from snapper.core.execution_unit_snapshot import SnapshotDimensions
from snapper.core.execution_unit_snapshot import SnapshotReceipt
from snapper.core.execution_unit_snapshot import SnapshotScope
from snapper.core.execution_unit_snapshot import SnapshotVenueIdentity
from snapper.core.financial_account_owner import FinancialAccountOwnerAssignment
from snapper.core.financial_account_owner import FinancialAccountOwnerPolicy
from snapper.core.financial_account_owner import financial_account_owner_key_from_authority
from snapper.core.native_account_authority import NativeAccountAuthorityPayload
from snapper.core.private_frame_ticket import require_authenticated_private_frame
from snapper.data.contract_accounting_capture_session import capture_bound_accounting_observation
from snapper.data.execution_unit_catalog import catalog_economic_context
from snapper.data.execution_unit_catalog import catalog_rebase_context
from snapper.data.execution_unit_catalog import snapshot_catalog
from snapper.data.execution_unit_snapshot_store import SnapshotWriteResult
from snapper.data.execution_unit_snapshot_store import _insert_bound_snapshot
from snapper.data.execution_unit_snapshot_store import bound_execution_digest
from snapper.data.execution_unit_snapshot_store import bound_venue_event_digest
from snapper.data.financial_account_owner_policy_writer import FinancialOwnerPolicyChange
from snapper.data.financial_account_owner_policy_writer import write_financial_owner_policy
from snapper.data.models import InstrumentSpec
from snapper.data.models import Order
from snapper.data.models import SymbolAlias
from snapper.data.native_account_authority_store import NativeAuthorityFreshnessPolicy
from snapper.data.native_position_fill_binding_v2 import BoundOriginalPositionFillV2
from snapper.data.native_position_fill_binding_v2 import OriginalPositionFillRequestV2
from snapper.data.native_position_fill_binding_v2 import bind_original_position_fill_v2
from snapper.data.native_position_source_v2 import NativePositionSourceRequestV2
from snapper.data.private_source_ownership_store import capture_private_source_ownership
from snapper.data.repository import InstrumentSpecInput
from snapper.data.repository_types import ExecutionInsertRow
from snapper.data.repository_types import VenueEventInsertRow
from tests.helpers.account_bound_admission import AccountAdmissionScenario
from tests.helpers.account_bound_admission import account_admission_scenario
from tests.helpers.native_account_authority import NativeAuthorityScenario
from tests.helpers.native_account_authority import native_authority_uuid
from tests.helpers.native_cash_accounting_v2 import CapturedNativeCashV2
from tests.helpers.native_cash_accounting_v2 import fresh_native_cash_authority_v2
from tests.helpers.native_cash_accounting_v2 import native_cash_identity
from tests.helpers.native_cash_accounting_v2 import receive_native_cash_v2
from tests.helpers.unit_admission import admission_id
from tests.helpers.unit_admission import admission_identity
from tests.helpers.unit_admission import admission_stamp
from tests.infrastructure.exchanges.test_authenticated_private_socket import (
    _scenario as _socket_scenario,
)


@dataclass(frozen=True, slots=True)
class NativePositionScenarioV2:
    """Actual original targets and two independently authenticated source channels without a position fact."""

    admission: AccountAdmissionScenario
    fill: CapturedNativeCashV2
    position: CapturedNativeCashV2
    request: NativePositionSourceRequestV2
    authority: NativeAccountAuthorityPayload

    @property
    def source(self) -> NativeAuthorityScenario:
        """Return the actual selected SDK, repository and signed local HTTP source owner.

        Returns:
            The same real isolated source context used by both original private subscriptions.
        """
        return self.admission.native


def _fill_wire(fill_number: int = 1) -> bytes:
    """Provide an actual-format original fill whose provider account label is distinct from safe UID."""
    occurred = datetime.now(UTC) - timedelta(milliseconds=20)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    elapsed = occurred - epoch
    milliseconds = elapsed.days * 86400000 + elapsed.seconds * 1000 + elapsed.microseconds // 1000
    return json.dumps(
        {
            "feed": "fills",
            "account": "DemoUser",
            "fills": [
                {
                    "instrument": "PF_XBTUSD",
                    "time": milliseconds,
                    "seq": fill_number,
                    "buy": True,
                    "qty": 1,
                    "price": 100,
                    "fee_paid": "0.10",
                    "fee_currency": "USD",
                    "remaining_order_qty": 2 - fill_number,
                    "order_id": "native-position-order-1",
                    "fill_id": f"native-position-fill-{fill_number}",
                    "cli_ord_id": "native-admission-client-1",
                    "fill_type": "maker",
                }
            ],
        },
        separators=(",", ":"),
    ).encode()


async def _receive_fill(
    source: NativeAuthorityScenario, monkeypatch: pytest.MonkeyPatch, fill_number: int = 1
) -> CapturedNativeCashV2:
    """Commit original fill raw/hold/sidecar from the actual encoded fills authentication path."""
    assert fill_number in {1, 2}
    identity_offset = (fill_number - 1) * 2000
    wire = _fill_wire(fill_number)
    source.clock.current = datetime.now(UTC)
    async with _socket_scenario(
        source, monkeypatch, "valid", financial_wire=wire.decode()
    ) as socket:
        task = await socket.start()
        await socket.ws.subscribe(feed="fills")
        await task
        ticket = socket.tickets[0]
        assert ticket is not None
        context = require_authenticated_private_frame(ticket, wire)
        assert context.subscription_feed == "fills"
        selected = context.client_selection
        envelope = RawAccountingObservation(
            scope=AccountingObservationScope(
                wallet_public_id=selected.wallet_public_id,
                exchange=selected.exchange,
                mode=selected.mode,
                environment=selected.environment,
            ),
            route_revision=client_selection_digest(selected),
            credential_public_id=selected.credential_public_id,
            credential_timestamp=selected.credential_timestamp,
            kind="unknown",
            channel="kraken_futures.ws.fills",
            representation="wire_bytes_base64",
            material=base64.b64encode(wire).decode(),
            observed_at=datetime.now(UTC),
        )
        child = next(
            value
            for value in recoverable_frame_children(envelope)
            if value.target_json_pointer == "/fills/0"
        )
        async with source.repository.session() as session:
            await source.repository._begin_execution_insert_transaction(session)
            await source.repository._acquire_execution_fence_lock(
                session, selected.wallet_public_id
            )
            stamp = AccountingWriteProvenance(
                **native_cash_identity(6100 + identity_offset).model_dump(mode="python"),
                timestamp=datetime.now(UTC),
            )
            raw = await capture_bound_accounting_observation(session, child, stamp)
            assert raw.created and not raw.conflicted
            ownership = await capture_private_source_ownership(
                session, raw.public_id, ticket, native_cash_identity(6200 + identity_offset)
            )
            await session.commit()
    return CapturedNativeCashV2(child, raw, ownership)


async def _observe_current_position_catalog(
    admission: AccountAdmissionScenario, observed_at: datetime
) -> None:
    """Record a current provider-fixture revision without relabeling its retained original catalog."""
    repository = admission.native.repository
    async with repository.session() as session:
        old = (
            await session.scalars(
                select(InstrumentSpec).where(
                    InstrumentSpec.instrument_public_id == admission_id(2),
                    InstrumentSpec.timestamp <= observed_at,
                    InstrumentSpec.known_to > observed_at,
                )
            )
        ).one()
        old_id, old_at, old_observed = old.id, old.timestamp, old.spec_observed_at
        assert old_observed is not None and old_at < observed_at
        spec = InstrumentSpecInput(
            tick_size=old.tick_size,
            lot_size=old.lot_size,
            min_order_size=old.min_order_size,
            max_order_size=old.max_order_size,
            cost_decimals=old.cost_decimals,
            qty_decimals=old.qty_decimals,
            margin_initial=old.margin_initial,
            position_limit_long=old.position_limit_long,
            position_limit_short=old.position_limit_short,
            status=old.status,
            contract_size=old.contract_size,
            quantity_unit=old.quantity_unit,
            native_order_quantity_unit=old.native_order_quantity_unit,
            contract_payoff_model=old.contract_payoff_model,
            contract_value_currency=old.contract_value_currency,
            settlement_currency=old.settlement_currency,
            spec_source=old.spec_source,
            spec_version=old.spec_version,
            spec_observed_at=observed_at,
            unit_certified=old.unit_certified,
            expiry_at=old.expiry_at,
            instrument_kind=old.instrument_kind,
            funding_type=old.funding_type,
            funding_frequency_hours=old.funding_frequency_hours,
            rollover_rate_long=old.rollover_rate_long,
            rollover_rate_short=old.rollover_rate_short,
            max_funding_rate=old.max_funding_rate,
        )
    new_id = await repository.revise_instrument_spec(
        admission_id(2), native_authority_uuid(9000), 6050, observed_at, spec
    )
    assert new_id != old_id
    async with repository.session() as session:
        original = await session.get(InstrumentSpec, old_id)
        current = await session.get(InstrumentSpec, new_id)
        assert original is not None and current is not None
        assert (original.timestamp, original.spec_observed_at) == (old_at, old_observed)
        assert original.known_to == current.timestamp == current.spec_observed_at == observed_at
        assert current.contract_size == original.contract_size
        assert current.native_order_quantity_unit == original.native_order_quantity_unit
        assert current.contract_payoff_model == original.contract_payoff_model
        assert current.settlement_currency == original.settlement_currency


async def _seed_native_order(admission: AccountAdmissionScenario) -> None:
    """Store genuine command-correlated order and historical provider alias for the original fill."""
    at = datetime.now(UTC) - timedelta(seconds=1)
    async with admission.native.repository.session() as session:
        session.add_all(
            [
                SymbolAlias(
                    **admission_stamp(6201, at).model_dump(mode="python"),
                    symbol_public_id=admission_id(1),
                    exchange="kraken_futures",
                    channel="ws",
                    exchange_symbol="PF_XBTUSD",
                    created_at=at,
                ),
                Order(
                    **admission_stamp(6202, at).model_dump(mode="python"),
                    wallet_public_id=admission.native.selection.wallet_public_id,
                    instrument_public_id=admission_id(2),
                    mode="live",
                    client_order_id="native-admission-client-1",
                    exchange_order_id="native-position-order-1",
                    created_at=at,
                    side="buy",
                    order_type="limit",
                    price=100.0,
                    size=2.0,
                    status="open",
                ),
            ]
        )
        await session.commit()


def _receipt(source: BoundOriginalPositionFillV2) -> SnapshotReceipt:
    """Bind the unchanged whole original wire to the actual historical provider quantity dimension."""
    context = catalog_economic_context(source.catalog)
    return SnapshotReceipt(
        representation="wire_bytes_base64",
        material=source.raw.material,
        endpoint_or_channel=source.raw.channel,
        observed_at=source.raw.observed_at,
        reported_quantity_unit=context.native_order_quantity_unit,
        reported_quantity_convention="not_applicable",
    )


async def _create_actual_unit(
    admission: AccountAdmissionScenario,
    request: OriginalPositionFillRequestV2,
    trace_base: int = 6300,
) -> tuple[SnapshotWriteResult, BoundOriginalPositionFillV2]:
    """Use real repository target writers and the unchanged bound unit store, without an order send."""
    repository = admission.native.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(session, request.wallet_public_id)
        known_at = datetime.now(UTC)
        source = await bind_original_position_fill_v2(session, request, known_at)
        fill = source.fill
        receipt = _receipt(source)
        execution_row: ExecutionInsertRow = {
            "wallet_public_id": request.wallet_public_id,
            "order_public_id": admission_id(6202),
            "timestamp": known_at,
            "executed_at": fill.occurred_at,
            "side": fill.side,
            "status": "partial" if fill.remaining_quantity else "filled",
            "price": float(fill.price),
            "size": float(fill.quantity),
            "fee": float(fill.fee),
            "fee_asset": fill.fee_currency,
            "exec_id": fill.execution_id,
            "price_decimal": str(fill.price),
            "size_decimal": str(fill.quantity),
            "fee_decimal": str(fill.fee),
            "numeric_provenance": "venue_raw",
            "session_id": native_authority_uuid(9000),
            "sequence_id": trace_base,
        }
        execution = await repository.insert_execution_in_transaction(session, execution_row)
        event_row: VenueEventInsertRow = {
            "wallet_public_id": request.wallet_public_id,
            "command_public_id": admission_id(5000),
            "event_type": "fill_observed",
            "shard_key": "kraken_futures.BTC-USD.live",
            "exchange": "kraken_futures",
            "instrument": admission.native_order.instrument,
            "mode": "live",
            "timestamp": known_at,
            "received_at": source.raw.observed_at,
            "venue_timestamp": fill.occurred_at,
            "side": fill.side,
            "fill_size": float(fill.quantity),
            "fill_price": float(fill.price),
            "fee": float(fill.fee),
            "fee_asset": fill.fee_currency,
            "exec_id": fill.execution_id,
            "exchange_order_id": fill.order_id,
            "client_order_id": fill.client_order_id,
            "venue_client_id": fill.client_order_id,
            "payload_json": receipt.model_dump_json(),
            "session_id": native_authority_uuid(9000),
            "sequence_id": trace_base + 1,
        }
        event = await repository.insert_venue_event_in_transaction(session, event_row)
        context = catalog_economic_context(source.catalog)
        dimensions = SnapshotDimensions.model_validate(
            {
                **context.model_dump(
                    exclude={"quantity_convention", "instrument_kind", "expiry_at"}
                ),
                "signed_quantity_unit": context.native_order_quantity_unit,
                "side": fill.side,
                "signed_quantity": fill.quantity,
                "price": fill.price,
                "fee": fill.fee,
                "fee_currency": fill.fee_currency,
            }
        )
        payload = ExecutionUnitSnapshotPayload(
            target=FillSnapshotTarget(
                execution_public_id=execution.public_id,
                execution_digest=bound_execution_digest(execution),
                venue_event_public_id=event.public_id,
                venue_event_digest=bound_venue_event_digest(event),
            ),
            scope=SnapshotScope(
                wallet_public_id=request.wallet_public_id,
                exchange="kraken_futures",
                mode="live",
                instrument_public_id=source.catalog.instrument.public_id,
            ),
            catalog=snapshot_catalog(source.catalog),
            dimensions=dimensions,
            venue_identity=SnapshotVenueIdentity(
                native_symbol=source.catalog.symbol.native_symbol,
                source="kraken.futures.websocket.fills",
                source_version="1",
                observed_at=source.raw.observed_at,
                wsname=fill.provider_symbol,
            ),
            rebase=catalog_rebase_context(source.catalog),
            receipt=receipt,
            interpreted_at=known_at,
        )
        result = await _insert_bound_snapshot(
            session, payload, admission_stamp(trace_base + 2, known_at)
        )
        await session.commit()
    return result, source


def _position_wire(
    fill: BoundOriginalPositionFillV2, fill_number: int = 1, source_log_id: int | None = None
) -> bytes:
    """Name the exact provider execution in a distinct genuine position-log entry, without cash synthesis."""
    return json.dumps(
        {
            "feed": "account_log",
            "new_entry": {
                "id": 30 + fill_number if source_log_id is None else source_log_id,
                "booking_uid": f"native-position-booking-{fill_number}",
                "date": (fill.fill.occurred_at + timedelta(milliseconds=10)).isoformat(),
                "asset": fill.fill.provider_symbol,
                "contract": fill.fill.provider_symbol,
                "collateral": "USD",
                "margin_account": "flex",
                "execution": fill.fill.execution_id,
                "info": "futures trade",
                "old_balance": fill_number - 1,
                "new_balance": fill_number,
                "old_average_entry_price": 0 if fill_number == 1 else 100,
                "new_average_entry_price": 100,
                "fee": "0.10",
            },
        },
        separators=(",", ":"),
    ).encode()


@asynccontextmanager
async def native_position_scenario_v2(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[NativePositionScenarioV2]:
    """Own real admitted units, separately issued fills/logs, retained targets and explicit owner policy.

    Args:
        tmp_path: Caller-owned isolated actual SQLite database namespace.
        monkeypatch: Lifetime owner of closed HTTP, source clocks and actual SDK provider bindings.

    Yields:
        Genuine original target/source references before any position, native cash or outbox row exists.
    """
    async with account_admission_scenario(tmp_path, monkeypatch) as admission:
        source = admission.native
        source.policy = NativeAuthorityFreshnessPolicy(timedelta(seconds=60), timedelta(seconds=60))
        fill_reference, fill_authority = await fresh_native_cash_authority_v2(source, 6000)
        source.clock.current = datetime.now(UTC)
        await _observe_current_position_catalog(admission, source.clock.current)
        await capture_account_submission_admission(
            source.repository,
            admission.native_order,
            ExecutionRouteReference(
                authority_public_id=fill_reference.public_id,
                authority_content_digest=fill_reference.content_digest,
                scope=fill_authority.scope,
                route_revision=fill_authority.route_revision,
            ),
            admission_identity(6001),
            source.policy,
        )
        await _seed_native_order(admission)
        captured_fill = await _receive_fill(source, monkeypatch)
        original = OriginalPositionFillRequestV2(
            wallet_public_id=source.selection.wallet_public_id,
            raw_receipt_public_id=captured_fill.raw.public_id,
            authority_reference=fill_reference,
            ownership_reference=captured_fill.ownership.reference,
        )
        unit, bound_fill = await _create_actual_unit(admission, original)
        captured_position = await receive_native_cash_v2(
            source, monkeypatch, _position_wire(bound_fill), 6400
        )
        reference, authority = await fresh_native_cash_authority_v2(source, 6500)
        policy = FinancialAccountOwnerPolicy(
            assignments=(
                FinancialAccountOwnerAssignment(
                    owner_key=financial_account_owner_key_from_authority(authority),
                    owner_wallet_public_id=authority.scope.wallet_public_id,
                ),
            )
        )
        await write_financial_owner_policy(
            source.repository,
            FinancialOwnerPolicyChange(policy=policy, updated_by="isolated-native-position-owner"),
            source.tracker,
        )
        request = NativePositionSourceRequestV2(
            fill=original,
            unit_snapshot_public_id=unit.public_id,
            position_receipt_public_id=captured_position.raw.public_id,
            position_authority_reference=reference,
        )
        yield NativePositionScenarioV2(
            admission, captured_fill, captured_position, request, authority
        )


async def next_native_position_request_v2(
    scenario: NativePositionScenarioV2,
    monkeypatch: pytest.MonkeyPatch,
    source_log_id: int = 32,
) -> NativePositionSourceRequestV2:
    """Complete the same genuine two-unit admitted order with a second original fill and native log.

    Args:
        scenario: Actual existing source, admitted two-unit order and first retained source pair.
        monkeypatch: Lifetime owner of the same closed provider SDK and HTTP boundaries.
        source_log_id: Actual selected provider entry ID, allowing independent currency rows between fills.

    Returns:
        Actual second Execution/unit/fill/log references with separate original and later authorities.
    """
    source = scenario.source
    original_reference, _original_authority = await fresh_native_cash_authority_v2(source, 8000)
    captured = await _receive_fill(source, monkeypatch, 2)
    original = OriginalPositionFillRequestV2(
        wallet_public_id=source.selection.wallet_public_id,
        raw_receipt_public_id=captured.raw.public_id,
        authority_reference=original_reference,
        ownership_reference=captured.ownership.reference,
    )
    unit, bound_fill = await _create_actual_unit(scenario.admission, original, 8300)
    position = await receive_native_cash_v2(
        source, monkeypatch, _position_wire(bound_fill, 2, source_log_id), 8400
    )
    reference, _authority = await fresh_native_cash_authority_v2(source, 8600)
    return NativePositionSourceRequestV2(
        fill=original,
        unit_snapshot_public_id=unit.public_id,
        position_receipt_public_id=position.raw.public_id,
        position_authority_reference=reference,
    )
