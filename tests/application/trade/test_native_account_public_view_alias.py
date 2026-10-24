"""Actual signed-local-HTTP counterexamples for public model views aliasing trusted source snapshots."""

import base64
import re
from pathlib import Path

import pytest

import snapper.application.trade.native_account_read as producer
from snapper.application.trade.native_private_owner_preparation import prepare_native_private_owner
from snapper.data.accounting_observation_binding import get_bound_accounting_observation
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.native_account_identity_store import find_native_account_identity
from snapper.data.native_account_read_store import BoundNativeAccountReadOutcome
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import (
    GuardedFuturesAccountsWireReceipt,
)
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import (
    require_guarded_accounts_wire,
)
from tests.helpers.native_account_authority import native_authority_scenario
from tests.helpers.native_account_authority import native_authority_uuid


async def test_public_wire_view_cannot_change_original_bytes_before_first_actual_capture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An ordinary public response model view must not be the producer's original receipt snapshot.

    Given: Actual selected-key/account HTTP from the local SDK adapter and no captured raw row yet,
    When: A consumer changes only the returned public model's backing entity view before first capture,
    Then: The original network bytes are retained unchanged rather than the consumer's different quantity.
    """
    actual_capture = producer._capture_response
    original_bytes: list[bytes] = []
    returned_bytes: list[bytes] = []

    async def view_then_capture(
        repository: AccountingRepository,
        operation: producer._ReadOperation,
        receipt: GuardedFuturesAccountsWireReceipt,
    ) -> BoundNativeAccountReadOutcome:
        """Use the real public view and original capture; never edit a private registry or mint a receipt."""
        view, _ = require_guarded_accounts_wire(receipt)
        original_bytes.append(bytes(view.entity_bytes))
        changed = re.sub(rb'("quantity":)[^,}]+', rb"\g<1>9999", view.entity_bytes, count=1)
        assert changed != view.entity_bytes
        view.__dict__["entity_bytes"] = changed
        returned_bytes.append(view.entity_bytes)
        return await actual_capture(repository, operation, receipt)

    monkeypatch.setattr(producer, "_capture_response", view_then_capture)
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        outcome = await producer.read_native_futures_accounts(
            source.repository, source.client, source.tracker
        )
        assert (
            outcome.payload.result.kind == "captured"
            and outcome.payload.source_reference is not None
        )
        async with source.repository.session() as session:
            raw = await get_bound_accounting_observation(
                session, outcome.payload.source_reference.raw_receipt_public_id
            )
        assert len(source.adapter.calls) == 2
        assert len(original_bytes) == len(returned_bytes) == 1
        assert original_bytes[0] != returned_bytes[0]
        assert base64.b64decode(raw.material) == original_bytes[0]


@pytest.mark.parametrize("accessor", ("require", "assert", "bind"))
async def test_public_safe_proof_view_cannot_relabel_the_tokens_retained_reference(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, accessor: str
) -> None:
    """Public safe metadata views must not share mutable storage with the actual committed token reference.

    Given: A real network-issued token bound to its actual retained UID and proof,
    When: A caller changes a returned model view through each public reference accessor,
    Then: Later views retain the actual original safe UID without relabeling the trusted cache.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        prepared = await prepare_native_private_owner(
            source.repository, source.client, source.tracker
        )
        try:
            token = prepared.require_token()
            async with source.repository.session() as session:
                proof = await find_native_account_identity(
                    session, prepared.outcome.payload.attempt_public_id
                )
            user = source.client._user_client
            assert proof is not None and user is not None
            if accessor == "require":
                view = token.require_proof_reference()
            elif accessor == "assert":
                view = token.assert_prepared_key(
                    user, source.selection, user._key, source.clock.current
                )
            else:
                view = token.bind_committed_proof(proof.public_id, proof.payload)
            view.__dict__["account_uid"] = native_authority_uuid(9999)
            assert (
                token.require_proof_reference().account_uid == proof.payload.observation.account_uid
            )
        finally:
            prepared.close()
