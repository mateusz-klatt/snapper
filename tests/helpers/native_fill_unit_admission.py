"""Actual signed native sources, original side-specific intent and real targets for unit admission."""

import base64
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Literal

import pytest
from sqlalchemy import select

from snapper.application.trade.account_bound_submission_admission import (
    capture_account_submission_admission,
)
from snapper.application.trade.execution_observation_recovery import recoverable_frame_children
from snapper.application.trade.native_fill_units_v2 import CapturedNativeFillUnitsV2
from snapper.application.trade.native_fill_units_v2 import NativeFillUnitCaptureRequestV2
from snapper.application.trade.native_fill_units_v2 import capture_bound_native_fill_units_v2
from snapper.application.trade.native_fill_units_v2 import native_fill_receipt_v2
from snapper.core.contract_accounting_observation import AccountingObservationScope
from snapper.core.contract_accounting_observation import AccountingWriteProvenance
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.execution_route_authority import client_selection_digest
from snapper.core.execution_unit_admission import ExecutionRouteReference
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.core.native_account_authority import NativeAccountAuthorityPayload
from snapper.core.native_fill_unit_admission import NativeFillUnitAdmissionEvidence
from snapper.core.native_fill_unit_admission import NativeUnitRecordReference
from snapper.core.private_frame_ticket import require_authenticated_private_frame
from snapper.data.account_bound_submission_admission_store import BoundAccountSubmissionAdmission
from snapper.data.account_bound_submission_admission_store import (
    get_bound_account_submission_admission,
)
from snapper.data.contract_accounting_capture_session import capture_bound_accounting_observation
from snapper.data.models import InstrumentSpec
from snapper.data.models import Order
from snapper.data.models import SymbolAlias
from snapper.data.models import TradeCommand
from snapper.data.native_account_authority_store import NativeAuthorityFreshnessPolicy
from snapper.data.native_fill_unit_admission_store import BoundNativeFillUnitAdmission
from snapper.data.native_fill_unit_admission_store import bind_native_fill_unit_admission
from snapper.data.native_position_fill_binding_v2 import OriginalPositionFillRequestV2
from snapper.data.native_position_fill_binding_v2 import bind_original_position_fill_v2
from snapper.data.private_source_ownership_store import capture_private_source_ownership
from snapper.data.repository import InstrumentSpecInput
from snapper.data.repository_types import ExecutionInsertRow
from snapper.data.repository_types import VenueEventInsertRow
from snapper.messaging.schemas.data import OrderRequestData
from tests.helpers.account_bound_admission import AccountAdmissionScenario
from tests.helpers.account_bound_admission import account_admission_scenario
from tests.helpers.native_account_authority import NativeAuthorityScenario
from tests.helpers.native_account_authority import native_authority_uuid
from tests.helpers.native_cash_accounting_v2 import CapturedNativeCashV2
from tests.helpers.native_cash_accounting_v2 import fresh_native_cash_authority_v2
from tests.helpers.native_cash_accounting_v2 import native_cash_identity
from tests.helpers.unit_admission import admission_id
from tests.helpers.unit_admission import admission_identity
from tests.helpers.unit_admission import admission_stamp
from tests.infrastructure.exchanges.test_authenticated_private_socket import (
    _scenario as socket_scenario,
)

type NativeFillSide = Literal["buy", "sell"]
type NativeProfileChange = Literal["unchanged", "account_type", "settlement", "balance"]


@dataclass(frozen=True, slots=True)
class NativeFillUnitScenario:
    """Genuine original source and admitted command before the new unit producer runs."""

    account: AccountAdmissionScenario
    order: OrderRequestData
    admission: BoundAccountSubmissionAdmission
    original_fill: OriginalPositionFillRequestV2
    request: NativeFillUnitCaptureRequestV2
    provenance: SnapshotWriteProvenance
    fill_authority: NativeAccountAuthorityPayload
    fill_route: ExecutionRouteReference

    @property
    def source(self) -> NativeAuthorityScenario:
        """Expose the original real repository, selected client and local HTTP source.

        Returns:
            The unchanged source context owned by this fixture.
        """
        return self.account.native


def profile_bytes(change: NativeProfileChange, later: bool) -> bytes:
    """Supply real response bytes with independently selectable structural and balance changes.

    Args:
        change: Actual REST profile difference under the same literal wallet key and account UID.
        later: Select the original admission profile or the later fill-authority source.

    Returns:
        Original JSON account bytes for the existing signed HTTP adapter.
    """
    if change == "settlement":
        currency = "XBT" if later else "USD"
        return json.dumps(
            {
                "flex": {
                    "type": "marginAccount",
                    "currency": currency,
                    "balances": {currency: "12.3400"},
                }
            }
        ).encode()
    if change == "account_type" and later:
        return b'{"flex":{"type":"marginAccount","currency":"USD","balances":{"USD":"12.3400"}}}'
    amount = b"99.5000000000000001" if change == "balance" and later else b"12.3400000000000001"
    return (
        b'{"flex":{"type":"multiCollateralMarginAccount","currencies":{"USD":{"quantity":'
        + amount
        + b"}}}}"
    )


async def _observe_current_unit_catalog(
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


async def _seed_original_intent(
    account: AccountAdmissionScenario, side: NativeFillSide
) -> OrderRequestData:
    """Persist the selected side before any admission, fill, Execution or unit is created."""
    at = datetime.now(UTC)
    wallet = account.native.selection.wallet_public_id
    client_id = "native-fill-unit-client-1"
    async with account.native.repository.session() as session:
        session.add(
            TradeCommand(
                **admission_stamp(7101, at).model_dump(mode="python"),
                command_type="create",
                shard_key="kraken_futures.BTC-USD.live",
                wallet_public_id=wallet,
                exchange="kraken_futures",
                instrument=account.native_order.instrument,
                mode="live",
                strategy_id="manual",
                client_order_id=client_id,
                venue_client_id=client_id,
                side=side,
                order_type="limit",
                quantity=2.0,
                price=100.0,
                post_only=True,
                reduce_only=False,
                status="created",
                created_at=at,
                correlation_id=admission_id(7102),
            )
        )
        session.add(
            SymbolAlias(
                **admission_stamp(7103, at).model_dump(mode="python"),
                symbol_public_id=admission_id(1),
                exchange="kraken_futures",
                channel="ws",
                exchange_symbol="PF_XBTUSD",
                created_at=at,
            )
        )
        session.add(
            Order(
                **admission_stamp(7104, at).model_dump(mode="python"),
                wallet_public_id=wallet,
                instrument_public_id=admission_id(2),
                mode="live",
                client_order_id=client_id,
                exchange_order_id="native-fill-unit-order-1",
                created_at=at,
                side=side,
                order_type="limit",
                price=100.0,
                size=2.0,
                status="open",
            )
        )
        await session.commit()
    material = account.native_order.model_dump(mode="python")
    material.update(
        public_id=client_id,
        client_order_id=client_id,
        wallet_public_id=wallet,
        timestamp=at,
        sequence_id=7101,
        side=side,
    )
    return OrderRequestData.model_validate(material)


def _wire(side: NativeFillSide) -> bytes:
    """Encode the actual selected buy/sell bit in the original authenticated fill frame."""
    occurred = datetime.now(UTC)
    elapsed = occurred - datetime(1970, 1, 1, tzinfo=UTC)
    milliseconds = elapsed.days * 86400000 + elapsed.seconds * 1000 + elapsed.microseconds // 1000
    return json.dumps(
        {
            "feed": "fills",
            "account": "DemoUser",
            "fills": [
                {
                    "instrument": "PF_XBTUSD",
                    "time": milliseconds,
                    "seq": 1,
                    "buy": side == "buy",
                    "qty": 1,
                    "price": 100,
                    "fee_paid": "0.10000000000000001",
                    "fee_currency": "USD",
                    "remaining_order_qty": 1,
                    "order_id": "native-fill-unit-order-1",
                    "fill_id": "native-fill-unit-execution-1",
                    "cli_ord_id": "native-fill-unit-client-1",
                    "fill_type": "maker",
                }
            ],
        },
        separators=(",", ":"),
    ).encode()


async def _receive_original_fill(
    source: NativeAuthorityScenario, monkeypatch: pytest.MonkeyPatch, side: NativeFillSide
) -> CapturedNativeCashV2:
    """Use actual encoded SDK subscription and issuer, then retain the original raw child and sidecar."""
    wire = _wire(side)
    source.clock.current = datetime.now(UTC)
    async with socket_scenario(
        source, monkeypatch, "valid", financial_wire=wire.decode()
    ) as socket:
        task = await socket.start()
        await socket.ws.subscribe(feed="fills")
        await task
        ticket = socket.tickets[0]
        assert ticket is not None
        context = require_authenticated_private_frame(ticket, wire)
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
                **native_cash_identity(7401).model_dump(mode="python"), timestamp=datetime.now(UTC)
            )
            raw = await capture_bound_accounting_observation(session, child, stamp)
            ownership = await capture_private_source_ownership(
                session, raw.public_id, ticket, native_cash_identity(7402)
            )
            await session.commit()
    return CapturedNativeCashV2(child, raw, ownership)


async def _create_original_targets(
    account: AccountAdmissionScenario, original: OriginalPositionFillRequestV2
) -> tuple[str, str]:
    """Write genuine Execution and VenueEvent rows from the actual bound source, without a unit."""
    repository = account.native.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(session, original.wallet_public_id)
        known_at = datetime.now(UTC)
        source = await bind_original_position_fill_v2(session, original, known_at)
        fill = source.fill
        receipt = native_fill_receipt_v2(source)
        execution_row: ExecutionInsertRow = {
            "wallet_public_id": original.wallet_public_id,
            "order_public_id": admission_id(7104),
            "timestamp": known_at,
            "executed_at": fill.occurred_at,
            "side": fill.side,
            "status": "partial",
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
            "sequence_id": 7501,
        }
        execution = await repository.insert_execution_in_transaction(session, execution_row)
        event_row: VenueEventInsertRow = {
            "wallet_public_id": original.wallet_public_id,
            "command_public_id": admission_id(7101),
            "event_type": "fill_observed",
            "shard_key": "kraken_futures.BTC-USD.live",
            "exchange": "kraken_futures",
            "instrument": account.native_order.instrument,
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
            "sequence_id": 7502,
        }
        event = await repository.insert_venue_event_in_transaction(session, event_row)
        await session.commit()
    return execution.public_id, event.public_id


@asynccontextmanager
async def native_fill_unit_scenario(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    side: NativeFillSide = "buy",
    profile_change: NativeProfileChange = "unchanged",
    wrong_command: bool = False,
) -> AsyncIterator[NativeFillUnitScenario]:
    """Own actual original wire, admission and target sources while leaving unit capture to the test.

    Args:
        tmp_path: Caller-owned isolated SQLite namespace.
        monkeypatch: Original actual SDK/HTTP fixture lifetime.
        side: Direction encoded in the original command, order and authenticated fill from the outset.
        profile_change: Actual later signed REST structural or balance change under the same UID/key.
        wrong_command: Admit the fixture's other genuine durable command before this fill occurs.

    Yields:
        Genuine retained source references before the new unit producer is invoked.
    """
    async with account_admission_scenario(tmp_path, monkeypatch) as account:
        source = account.native
        source.policy = NativeAuthorityFreshnessPolicy(timedelta(seconds=60), timedelta(seconds=60))
        await _observe_current_unit_catalog(account, datetime.now(UTC))
        order = await _seed_original_intent(account, side)
        source.adapter.profiles = profile_bytes(profile_change, False)
        original_reference, original_authority = await fresh_native_cash_authority_v2(source, 7201)
        route = ExecutionRouteReference(
            authority_public_id=original_reference.public_id,
            authority_content_digest=original_reference.content_digest,
            scope=original_authority.scope,
            route_revision=original_authority.route_revision,
        )
        result = await capture_account_submission_admission(
            source.repository,
            account.native_order if wrong_command else order,
            route,
            admission_identity(7202),
            source.policy,
        )
        async with source.repository.session() as session:
            admission = await get_bound_account_submission_admission(session, result.public_id)
        source.adapter.profiles = profile_bytes(profile_change, True)
        fill_reference, fill_authority = await fresh_native_cash_authority_v2(source, 7301)
        captured = await _receive_original_fill(source, monkeypatch, side)
        original = OriginalPositionFillRequestV2(
            wallet_public_id=source.selection.wallet_public_id,
            raw_receipt_public_id=captured.raw.public_id,
            authority_reference=fill_reference,
            ownership_reference=captured.ownership.reference,
        )
        execution_id, event_id = await _create_original_targets(account, original)
        request = NativeFillUnitCaptureRequestV2(
            **original.model_dump(mode="python"),
            execution_public_id=execution_id,
            venue_event_public_id=event_id,
            submission_admission=NativeUnitRecordReference(
                public_id=admission.public_id,
                content_digest=admission.content_digest,
                recorded_at=admission.recorded_at,
            ),
        )
        fill_route = ExecutionRouteReference(
            authority_public_id=fill_reference.public_id,
            authority_content_digest=fill_reference.content_digest,
            scope=fill_authority.scope,
            route_revision=fill_authority.route_revision,
        )
        yield NativeFillUnitScenario(
            account,
            order,
            admission,
            original,
            request,
            admission_stamp(7601, datetime.now(UTC)),
            fill_authority,
            fill_route,
        )


async def capture_native_fill_unit(
    scenario: NativeFillUnitScenario, request: NativeFillUnitCaptureRequestV2 | None = None
) -> CapturedNativeFillUnitsV2:
    """Call the actual new producer under its actual wallet fence and commit only on success.

    Args:
        scenario: Real original wire, retained command/admission and durable target identities.
        request: Optional changed reference for a genuine public-boundary refusal case.

    Returns:
        The actual new producer result, without constructing or replacing a bound source.
    """
    repository = scenario.source.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(
            session, scenario.original_fill.wallet_public_id
        )
        captured = await capture_bound_native_fill_units_v2(
            session, scenario.request if request is None else request, scenario.provenance
        )
        await session.commit()
        return captured


async def read_native_fill_unit(
    scenario: NativeFillUnitScenario,
    evidence: NativeFillUnitAdmissionEvidence,
    known_at: datetime | None = None,
) -> BoundNativeFillUnitAdmission:
    """Call the actual historical public binder using only the original retained source references.

    Args:
        scenario: Genuine original source and actual unit/admission target context.
        evidence: Original durable-envelope references or a deliberately corrupted public reference.
        known_at: Later local interpretation clock; defaults to the original unit insertion clock.

    Returns:
        The independently rebound actual producer/reader graph.
    """
    async with scenario.source.repository.session() as session:
        return await bind_native_fill_unit_admission(
            session,
            scenario.original_fill,
            evidence,
            scenario.provenance.timestamp if known_at is None else known_at,
        )
