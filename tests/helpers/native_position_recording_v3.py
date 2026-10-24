"""Build actual original native sources before the atomic V3 financial caller creates any targets."""

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

from snapper.application.trade.account_bound_submission_admission import (
    capture_account_submission_admission,
)
from snapper.application.trade.execution_observation_recovery import recoverable_frame_children
from snapper.application.trade.native_position_recording_v3 import NativePositionRecordingRequestV3
from snapper.application.trade.native_position_recording_v3 import RecordedNativePositionV3
from snapper.application.trade.native_position_recording_v3 import record_native_position_v3
from snapper.core.contract_accounting_observation import AccountingObservationScope
from snapper.core.contract_accounting_observation import AccountingWriteProvenance
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.execution_route_authority import client_selection_digest
from snapper.core.execution_unit_admission import ExecutionRouteReference
from snapper.core.financial_account_owner import FinancialAccountOwnerAssignment
from snapper.core.financial_account_owner import FinancialAccountOwnerPolicy
from snapper.core.financial_account_owner import financial_account_owner_key_from_authority
from snapper.core.native_account_authority import NativeAccountAuthorityPayload
from snapper.core.native_fill_unit_admission import NativeUnitRecordReference
from snapper.core.private_frame_ticket import require_authenticated_private_frame
from snapper.data.account_bound_submission_admission_store import (
    get_bound_account_submission_admission,
)
from snapper.data.contract_accounting_capture_session import capture_bound_accounting_observation
from snapper.data.financial_account_owner_policy_writer import FinancialOwnerPolicyChange
from snapper.data.financial_account_owner_policy_writer import write_financial_owner_policy
from snapper.data.native_account_authority_store import NativeAuthorityFreshnessPolicy
from snapper.data.native_position_fill_binding_v2 import BoundOriginalPositionFillV2
from snapper.data.native_position_fill_binding_v2 import OriginalPositionFillRequestV2
from snapper.data.native_position_fill_binding_v2 import bind_original_position_fill_v2
from snapper.data.private_source_ownership_store import capture_private_source_ownership
from snapper.messaging.schemas.data import OrderRequestData
from tests.helpers.account_bound_admission import account_admission_scenario
from tests.helpers.native_account_authority import NativeAuthorityDatabase
from tests.helpers.native_account_authority import NativeAuthorityScenario
from tests.helpers.native_cash_accounting_v2 import CapturedNativeCashV2
from tests.helpers.native_cash_accounting_v2 import fresh_native_cash_authority_v2
from tests.helpers.native_cash_accounting_v2 import native_cash_identity
from tests.helpers.native_cash_accounting_v2 import receive_native_cash_v2
from tests.helpers.native_fill_unit_admission import NativeFillSide
from tests.helpers.native_fill_unit_admission import _observe_current_unit_catalog
from tests.helpers.native_fill_unit_admission import _seed_original_intent
from tests.helpers.native_fill_unit_admission import _wire as native_fill_wire
from tests.helpers.native_fill_unit_admission import profile_bytes
from tests.helpers.unit_admission import admission_id
from tests.helpers.unit_admission import admission_identity
from tests.infrastructure.exchanges.test_authenticated_private_socket import (
    _scenario as socket_scenario,
)


@dataclass(frozen=True, slots=True)
class NativePositionRecordingScenarioV3:
    """Genuine admitted private source pair and explicit owner before any financial target exists."""

    source: NativeAuthorityScenario
    request: NativePositionRecordingRequestV3
    identity: ExecutionEvidenceIdentity
    captured_fill: CapturedNativeCashV2
    captured_position: CapturedNativeCashV2
    authority: NativeAccountAuthorityPayload
    owner_policy: FinancialAccountOwnerPolicy
    order: OrderRequestData


def native_recording_position_wire_v3(original: BoundOriginalPositionFillV2) -> bytes:
    """Provide one real-format position entry naming the exact authenticated original native fill.

    Args:
        original: Actual parser/catalog/UID-bound original private source from its committed raw receipt.

    Returns:
        Original account_log bytes containing a position quantity and no invented cash event/header.
    """
    fill = original.fill
    assert fill.quantity in {1, 2}
    quantity = int(fill.quantity) if fill.side == "buy" else -int(fill.quantity)
    return json.dumps(
        {
            "feed": "account_log",
            "new_entry": {
                "id": 31,
                "booking_uid": "native-recording-v3-position-booking-1",
                "date": (fill.occurred_at + timedelta(milliseconds=10)).isoformat(),
                "asset": fill.provider_symbol,
                "contract": fill.provider_symbol,
                "collateral": "USD",
                "margin_account": "flex",
                "execution": fill.execution_id,
                "info": "futures trade",
                "old_balance": 0,
                "new_balance": quantity,
                "old_average_entry_price": 0,
                "new_average_entry_price": 100,
                "fee": "0.10000000000000001",
            },
        },
        separators=(",", ":"),
    ).encode()


async def _receive_recording_fill(
    source: NativeAuthorityScenario,
    monkeypatch: pytest.MonkeyPatch,
    side: NativeFillSide,
    completion: Literal["partial", "full"],
) -> CapturedNativeCashV2:
    """Use actual encoded SDK subscription and issuer, then retain the original raw child and sidecar."""
    wire = native_fill_wire(side)
    if completion == "full":
        assert wire.count(b'"qty":1') == wire.count(b'"remaining_order_qty":1') == 1
        wire = wire.replace(b'"qty":1', b'"qty":2').replace(
            b'"remaining_order_qty":1', b'"remaining_order_qty":0'
        )
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


@asynccontextmanager
async def native_position_recording_scenario_v3(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    side: NativeFillSide = "buy",
    completion: Literal["partial", "full"] = "partial",
    database: NativeAuthorityDatabase | None = None,
) -> AsyncIterator[NativePositionRecordingScenarioV3]:
    """Own actual original sources while leaving all financial target creation to the real caller.

    Args:
        tmp_path: Actual isolated SQLite namespace owned by this genuine source fixture.
        monkeypatch: Lifetime owner of the local signed HTTP and actual SDK socket transport boundaries.
        side: Direction encoded from the beginning in the actual durable command, order and provider fill.
        completion: Supply one or two actual native contracts and matching provider remaining quantity.
        database: Already migrated source repository with an independent same-database observer.

    Yields:
        Explicit original admission and independently retained fill/log/authority/owner references.
    """
    async with account_admission_scenario(tmp_path, monkeypatch, database) as account:
        source = account.native
        source.policy = NativeAuthorityFreshnessPolicy(timedelta(seconds=60), timedelta(seconds=60))
        await _observe_current_unit_catalog(account, datetime.now(UTC))
        order = await _seed_original_intent(account, side)
        source.adapter.profiles = profile_bytes("unchanged", False)
        admitted_reference, admitted_authority = await fresh_native_cash_authority_v2(source, 7201)
        admission_result = await capture_account_submission_admission(
            source.repository,
            order,
            ExecutionRouteReference(
                authority_public_id=admitted_reference.public_id,
                authority_content_digest=admitted_reference.content_digest,
                scope=admitted_authority.scope,
                route_revision=admitted_authority.route_revision,
            ),
            admission_identity(7202),
            source.policy,
        )
        async with source.repository.session() as session:
            admission = await get_bound_account_submission_admission(
                session, admission_result.public_id
            )
        source.adapter.profiles = profile_bytes("unchanged", True)
        fill_reference, _fill_authority = await fresh_native_cash_authority_v2(source, 7301)
        captured_fill = await _receive_recording_fill(source, monkeypatch, side, completion)
        original = OriginalPositionFillRequestV2(
            wallet_public_id=source.selection.wallet_public_id,
            raw_receipt_public_id=captured_fill.raw.public_id,
            authority_reference=fill_reference,
            ownership_reference=captured_fill.ownership.reference,
        )
        async with source.repository.session() as session:
            actual_fill = await bind_original_position_fill_v2(session, original, datetime.now(UTC))
        captured_position = await receive_native_cash_v2(
            source, monkeypatch, native_recording_position_wire_v3(actual_fill), 7601
        )
        position_reference, authority = await fresh_native_cash_authority_v2(source, 7701)
        owner_policy = FinancialAccountOwnerPolicy(
            assignments=(
                FinancialAccountOwnerAssignment(
                    owner_key=financial_account_owner_key_from_authority(authority),
                    owner_wallet_public_id=authority.scope.wallet_public_id,
                ),
            )
        )
        await write_financial_owner_policy(
            source.repository,
            FinancialOwnerPolicyChange(
                policy=owner_policy, updated_by="isolated-native-recording-v3-owner"
            ),
            source.tracker,
        )
        request = NativePositionRecordingRequestV3(
            **original.model_dump(mode="python"),
            order_public_id=admission_id(7104),
            submission_admission=NativeUnitRecordReference(
                public_id=admission.public_id,
                content_digest=admission.content_digest,
                recorded_at=admission.recorded_at,
            ),
            position_receipt_public_id=captured_position.raw.public_id,
            position_authority_reference=position_reference,
        )
        yield NativePositionRecordingScenarioV3(
            source,
            request,
            native_cash_identity(7801),
            captured_fill,
            captured_position,
            authority,
            owner_policy,
            order,
        )


async def record_native_position_scenario_v3(
    scenario: NativePositionRecordingScenarioV3,
) -> RecordedNativePositionV3:
    """Call the real one-commit financial writer using only the fixture's actual retained source graph.

    Args:
        scenario: Actual original source/admission/log/policy references before or after uncertain commit.

    Returns:
        Original or newly committed Execution/unit/position/notification coordinates.
    """
    return await record_native_position_v3(
        scenario.source.repository, scenario.request, scenario.identity, scenario.source.policy
    )
