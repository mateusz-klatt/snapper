"""Compose actual closed HTTP/SDK/account-log ownership and real native cash accounting dependencies."""

import base64
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from snapper.application.trade.execution_observation_recovery import recoverable_frame_children
from snapper.core.account_authority_reference import AccountAuthorityReference
from snapper.core.contract_accounting_observation import AccountingObservationScope
from snapper.core.contract_accounting_observation import AccountingWriteProvenance
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.execution_route_authority import client_selection_digest
from snapper.core.financial_account_owner import FinancialAccountOwnerAssignment
from snapper.core.financial_account_owner import FinancialAccountOwnerPolicy
from snapper.core.financial_account_owner import financial_account_owner_key_from_authority
from snapper.core.native_account_authority import NativeAccountAuthorityPayload
from snapper.core.private_frame_ticket import require_authenticated_private_frame
from snapper.data.contract_accounting_capture_session import capture_bound_accounting_observation
from snapper.data.contract_accounting_store import RawAccountingWriteResult
from snapper.data.financial_account_owner_policy_writer import FinancialOwnerPolicyChange
from snapper.data.financial_account_owner_policy_writer import write_financial_owner_policy
from snapper.data.native_account_authority_store import NativeAuthorityFreshnessPolicy
from snapper.data.native_account_authority_store import get_bound_native_account_authority
from snapper.data.native_cash_store_v2 import NativeCashAccountingRequestV2
from snapper.data.private_source_ownership_store import BoundPrivateSourceOwnership
from snapper.data.private_source_ownership_store import capture_private_source_ownership
from tests.helpers.native_account_authority import NativeAuthorityScenario
from tests.helpers.native_account_authority import build_native_authority_payload
from tests.helpers.native_account_authority import capture_native_account_read
from tests.helpers.native_account_authority import insert_native_authority
from tests.helpers.native_account_authority import native_authority_scenario
from tests.helpers.native_account_authority import native_authority_uuid
from tests.infrastructure.exchanges.test_authenticated_private_socket import (
    _scenario as _socket_scenario,
)


@dataclass(frozen=True, slots=True)
class CapturedNativeCashV2:
    """Actual raw/hold/sidecar facts from a real encoded account-log subscription and ACK."""

    observation: RawAccountingObservation
    raw: RawAccountingWriteResult
    ownership: BoundPrivateSourceOwnership


@dataclass(frozen=True, slots=True)
class NativeCashScenarioV2:
    """Actual source/policy dependencies, with no synthetic cash book or financial permission flag."""

    source: NativeAuthorityScenario
    captured: CapturedNativeCashV2
    authority: NativeAccountAuthorityPayload
    authority_reference: AccountAuthorityReference
    policy: FinancialAccountOwnerPolicy

    @property
    def request(self) -> NativeCashAccountingRequestV2:
        """Name actual retained source rows without supplying a mirror amount or assumed account.

        Returns:
            Exact native request whose real caller must still pass current financial admission.
        """
        return NativeCashAccountingRequestV2(
            wallet_public_id=self.source.selection.wallet_public_id,
            raw_receipt_public_id=self.captured.raw.public_id,
            authority_reference=self.authority_reference,
        )


def native_cash_identity(number: int) -> ExecutionEvidenceIdentity:
    """Allocate isolated stable trace identity without choosing a recording timestamp.

    Args:
        number: Explicit independent test identity number.

    Returns:
        Stable valid public/session/sequence identity for uncertain-commit replay.
    """
    return ExecutionEvidenceIdentity(
        public_id=native_authority_uuid(number),
        session_id=native_authority_uuid(9000),
        sequence_id=number,
    )


def native_cash_wire_v2(source_id: int, before: Decimal, after: Decimal) -> bytes:
    """Retain exact numeric JSON lexemes instead of routing source quantities through float.

    Args:
        source_id: Actual provider log identifier for this local closed-provider response.
        before: Exact native old USD balance at the fixture's literal flex profile.
        after: Exact native new USD balance in the same source currency.

    Returns:
        Original account_log/new_entry bytes without any fabricated account or UID header.
    """
    fields = json.dumps(
        {
            "booking_uid": f"cash-source-{source_id}",
            "date": "2026-10-23T15:00:00Z",
            "asset": "USD",
            "collateral": "USD",
            "margin_account": "flex",
            "info": "transfer",
        },
        separators=(",", ":"),
    )
    entry = fields[:-1] + f',"id":{source_id},"old_balance":{before},"new_balance":{after}' + "}"
    return ('{"feed":"account_log","new_entry":' + entry + "}").encode()


async def receive_native_cash_v2(
    source: NativeAuthorityScenario,
    monkeypatch: pytest.MonkeyPatch,
    wire: bytes,
    number: int,
    pointer: str = "/new_entry",
) -> CapturedNativeCashV2:
    """Run the actual closed SDK account-log auth path and commit raw/hold/sidecar atomically.

    Args:
        source: Actual isolated repository, selected credential and local signed HTTP provider.
        monkeypatch: Lifetime owner of the real socket helper's local provider bindings.
        wire: Exact original financial reply supplied by that local provider.
        number: Stable distinct raw and sidecar trace namespace for this receive.
        pointer: Actual parser-enumerated child, never a relabelled fills selector.

    Returns:
        Genuine committed source rows and original envelope, retaining all native quantity digits.
    """
    source.clock.current = datetime.now(UTC)
    async with _socket_scenario(
        source,
        monkeypatch,
        "valid",
        subscription_feed="account_log",
        financial_wire=wire.decode(),
    ) as socket:
        task = await socket.start()
        await socket.ws.subscribe(feed="account_log")
        await task
        ticket = socket.tickets[0]
        assert ticket is not None
        frame = require_authenticated_private_frame(ticket, wire)
        assert frame.subscription_feed == "account_log"
        selected = frame.client_selection
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
            channel=f"kraken_futures.ws.{frame.raw_feed}",
            representation="wire_bytes_base64",
            material=base64.b64encode(wire).decode(),
            observed_at=datetime.now(UTC),
        )
        observation = next(
            child
            for child in recoverable_frame_children(envelope)
            if child.target_json_pointer == pointer
        )
        async with source.repository.session() as session:
            await source.repository._begin_execution_insert_transaction(session)
            await source.repository._acquire_execution_fence_lock(
                session, selected.wallet_public_id
            )
            stamp = AccountingWriteProvenance(
                **native_cash_identity(number).model_dump(mode="python"),
                timestamp=datetime.now(UTC),
            )
            raw = await capture_bound_accounting_observation(session, observation, stamp)
            assert raw.created and not raw.conflicted
            ownership = await capture_private_source_ownership(
                session, raw.public_id, ticket, native_cash_identity(number + 100)
            )
            await session.commit()
    return CapturedNativeCashV2(observation, raw, ownership)


async def fresh_native_cash_authority_v2(
    source: NativeAuthorityScenario, number: int
) -> tuple[AccountAuthorityReference, NativeAccountAuthorityPayload]:
    """Run a fresh actual prepared-key HTTP read after private capture and retain its real row clock.

    Args:
        source: Actual isolated signed HTTP provider and source account context.
        number: Stable identity for this independently captured current account authority.

    Returns:
        Explicit native family reference from the actual retained row and its fully source-bound payload.
    """
    source.clock.current = datetime.now(UTC)
    outcome = await capture_native_account_read(source)
    payload = await build_native_authority_payload(source, outcome, "flex", source.policy)
    assert payload.native_read.parser_policy == "kraken_futures_accounts_prepared_key_v2"
    result = await insert_native_authority(source, payload, number)
    async with source.repository.session() as session:
        bound = await get_bound_native_account_authority(session, result.public_id)
    reference = AccountAuthorityReference(
        family="native_accounts_v1",
        public_id=bound.public_id,
        content_digest=bound.content_digest,
        recorded_at=bound.recorded_at,
    )
    return reference, payload


@asynccontextmanager
async def native_cash_scenario_v2(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[NativeCashScenarioV2]:
    """Own a genuine sidecar/current authority/versioned owner-policy dependency graph.

    Args:
        tmp_path: Isolated actual file-backed database namespace for this test.
        monkeypatch: Lifetime owner for source clocks, local HTTP and actual SDK/socket bindings.

    Yields:
        A real source graph with no cash event yet; each financial test calls the actual new appender.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        source.policy = NativeAuthorityFreshnessPolicy(timedelta(seconds=60), timedelta(seconds=60))
        source.clock.current = datetime.now(UTC)
        captured = await receive_native_cash_v2(
            source,
            monkeypatch,
            native_cash_wire_v2(17, Decimal(0), Decimal("100.000000000000000001")),
            1100,
        )
        reference, authority = await fresh_native_cash_authority_v2(source, 1500)
        assert captured.ownership.account_uid == authority.venue_account_id
        assert authority.scope.margin_account == "flex"
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
            FinancialOwnerPolicyChange(policy=policy, updated_by="isolated-native-cash-owner"),
            source.tracker,
        )
        yield NativeCashScenarioV2(source, captured, authority, reference, policy)
