"""Replay actual legacy source while requiring prepared-key evidence for current admission.

Legacy rows are explicitly restored using actual signed local HTTP, actual
proof/raw/outcome records and the original canonical domain. That historical
fixture does not invoke the new current writer or claim new runtime eligibility.
"""

import base64
import hashlib
import json
from datetime import timedelta
from pathlib import Path

import pytest

from snapper.application.trade import native_account_read as producer
from snapper.application.trade.native_account_owner_preflight import prove_native_account_owner
from snapper.core.contract_accounting_observation import AccountingObservationScope
from snapper.core.contract_accounting_observation import AccountingWriteProvenance
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.contract_accounting_observation import accounting_observation_digest
from snapper.core.contract_accounting_observation import canonical_accounting_observation
from snapper.core.execution_route_authority import client_selection_digest
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.core.native_account_authority import NativeAccountAuthorityPayload
from snapper.core.native_account_authority import canonical_native_account_authority
from snapper.core.native_account_authority import native_account_authority_digest
from snapper.core.native_account_read import NativeAccountReadCapturedSource
from snapper.data.contract_accounting_store import capture_accounting_observation
from snapper.data.execution_unit_snapshot_store import SnapshotWriteResult
from snapper.data.models import AccountingRawReceipt
from snapper.data.models import NativeAccountAuthority
from snapper.data.native_account_authority_store import NativeAccountAuthorityBindingError
from snapper.data.native_account_authority_store import _authority_projection
from snapper.data.native_account_authority_store import get_bound_native_account_authority
from snapper.data.native_account_read_store import BoundNativeAccountReadOutcome
from snapper.data.native_account_read_store import append_native_account_read_outcome
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import (
    read_futures_accounts_wire,
)
from snapper.infrastructure.exchanges.kraken_rest_egress import futures_sdk_proxy_target
from tests.helpers.native_account_authority import NativeAuthorityScenario
from tests.helpers.native_account_authority import build_native_authority_payload
from tests.helpers.native_account_authority import capture_native_account_read
from tests.helpers.native_account_authority import current_native_authority
from tests.helpers.native_account_authority import insert_native_authority
from tests.helpers.native_account_authority import native_authority_scenario
from tests.helpers.native_account_authority import native_authority_uuid


async def _legacy_read(scenario: NativeAuthorityScenario) -> BoundNativeAccountReadOutcome:
    """Restore an actual original v1 source graph without marking its plain HTTP as guarded v2."""
    user = scenario.client._user_client
    assert user is not None
    selected = scenario.selection
    operation = await producer._start_operation(scenario.repository, selected, scenario.tracker)
    owner = await prove_native_account_owner(
        scenario.repository, scenario.client, operation.attempt, operation.key_identity, user
    )
    assert owner.proof.payload.matches_owner
    assert owner.proof.payload.observation.client_selection == selected
    scenario.clock.current += timedelta(seconds=1)
    try:
        wire = await scenario.client._dispatch_routed_rest(
            operation="restore_original_v1_accounts_source",
            kind="private_idempotent_read",
            target=futures_sdk_proxy_target(user),
            sync_call=lambda: read_futures_accounts_wire(user, selected),
        )
    finally:
        owner.close()
    assert wire.client_selection == selected and wire.status_code == 200
    assert owner.proof.payload.recorded_at <= wire.request_started_at <= wire.observed_at
    scenario.clock.current += timedelta(seconds=1)
    observation = RawAccountingObservation(
        scope=AccountingObservationScope(
            wallet_public_id=selected.wallet_public_id,
            exchange=selected.exchange,
            mode=selected.mode,
            environment=selected.environment,
        ),
        route_revision=client_selection_digest(selected),
        credential_public_id=selected.credential_public_id,
        credential_timestamp=selected.credential_timestamp,
        kind="account_cash",
        channel="kraken_futures.rest.accounts.v1",
        representation="wire_bytes_base64",
        material=base64.b64encode(wire.entity_bytes).decode("ascii"),
        observed_at=wire.observed_at,
        source_identity=operation.attempt.public_id,
        source_identity_kind="unproven",
    )
    stamp = AccountingWriteProvenance(
        **operation.raw_identity.model_dump(), timestamp=scenario.clock.current
    )
    raw = await capture_accounting_observation(scenario.repository, observation, stamp)
    assert raw.public_id == operation.raw_identity.public_id and raw.created and not raw.conflicted
    assert raw.content_digest == accounting_observation_digest(observation)
    async with scenario.repository.session() as session:
        await scenario.repository._begin_execution_insert_transaction(session)
        await scenario.repository._acquire_execution_fence_lock(session, selected.wallet_public_id)
        actual_raw = await session.get(AccountingRawReceipt, raw.id)
        assert actual_raw is not None and actual_raw.public_id == raw.public_id
        assert actual_raw.canonical_payload == canonical_accounting_observation(observation)
        result = await append_native_account_read_outcome(
            session,
            operation.attempt.public_id,
            NativeAccountReadCapturedSource(
                raw_receipt_public_id=raw.public_id,
                raw_content_digest=raw.content_digest,
                request_started_at=wire.request_started_at,
                source_observed_at=wire.observed_at,
                source_recorded_at=actual_raw.timestamp,
            ),
            operation.outcome_identity,
        )
        await session.commit()
    return result


async def _restore_legacy_authority(
    scenario: NativeAuthorityScenario, payload: NativeAccountAuthorityPayload
) -> SnapshotWriteResult:
    """Restore only old immutable history; the current writer must not create this legacy source."""
    stamp = SnapshotWriteProvenance(
        public_id=native_authority_uuid(500),
        session_id=native_authority_uuid(999),
        sequence_id=500,
        timestamp=scenario.clock.current,
    )
    row = NativeAccountAuthority(**stamp.model_dump(), **_authority_projection(payload))
    async with scenario.repository.session() as session:
        session.add(row)
        await session.flush()
        result = SnapshotWriteResult(row.id, row.public_id, row.content_digest, True)
        await session.commit()
    return result


async def test_original_v1_history_replays_identically_but_cannot_become_current_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A new prepared-key requirement cannot rewrite old receipts or retroactively certify them.

    Given: Actual signed original-v1 account bytes and an explicitly restored immutable legacy authority,
    When: The typed historical reader replays it and current admission separately rechecks it,
    Then: Original bytes/domain survive and even a later guarded source cannot certify the old row as current.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        outcome = await _legacy_read(source)
        payload = await build_native_authority_payload(source, outcome)
        assert payload.native_read.parser_policy == "kraken_futures_accounts_v1"
        original_bytes = canonical_native_account_authority(payload)
        original_digest = native_account_authority_digest(payload)
        original_document = payload.model_dump(mode="json")
        assert original_bytes == json.dumps(
            original_document, sort_keys=True, separators=(",", ":")
        )
        assert (
            original_digest
            == hashlib.sha256(
                b"snapper:native-account-authority:v1\x00" + original_bytes.encode()
            ).hexdigest()
        )
        assert payload.owner_proof.observed_at < payload.native_read.request_started_at
        assert payload.native_read.source_observed_at < payload.native_read.source_recorded_at
        assert source.clock.current < payload.valid_until
        restored = await _restore_legacy_authority(source, payload)
        async with source.repository.session() as session:
            historical = await get_bound_native_account_authority(session, restored.public_id)
        assert canonical_native_account_authority(historical.payload) == original_bytes
        assert historical.content_digest == original_digest == restored.content_digest
        with pytest.raises(
            NativeAccountAuthorityBindingError, match="original_prepared_key_unproven"
        ):
            await current_native_authority(source, restored, payload)
        with pytest.raises(
            NativeAccountAuthorityBindingError, match="original_prepared_key_unproven"
        ):
            await insert_native_authority(source, payload, 501)
        later = await capture_native_account_read(source)
        replacement = await build_native_authority_payload(source, later)
        assert replacement.native_read.parser_policy == "kraken_futures_accounts_prepared_key_v2"
        stored = await insert_native_authority(source, replacement, 502)
        assert (
            await current_native_authority(source, stored, replacement)
        ).public_id == stored.public_id
        with pytest.raises(
            NativeAccountAuthorityBindingError, match="original_prepared_key_unproven"
        ):
            await current_native_authority(source, restored, payload)
        async with source.repository.session() as session:
            retained = await get_bound_native_account_authority(session, restored.public_id)
        assert canonical_native_account_authority(retained.payload) == original_bytes
        assert retained.content_digest == original_digest


async def test_actual_guarded_v2_source_can_create_and_revalidate_current_native_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The new real prepared-request source supplies the current identity guarantee without hash migration.

    Given: Actual closed native key/accounts read using the guarded final Requests APIKey,
    When: Its independently loaded source builds and stores the native authority,
    Then: Explicit v2 parser policy is retained and the current fenced reader accepts that exact row.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        outcome = await capture_native_account_read(source)
        payload = await build_native_authority_payload(source, outcome)
        assert payload.native_read.parser_policy == "kraken_futures_accounts_prepared_key_v2"
        stored = await insert_native_authority(source, payload)
        current = await current_native_authority(source, stored, payload)
        assert current.public_id == stored.public_id and current.payload == payload
