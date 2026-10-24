"""Actual closed HTTP source to native authority producer, without external orders or balances."""

from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func
from sqlalchemy import select

import snapper.application.trade.native_account_authority as capture
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import Execution
from snapper.data.models import NativeAccountAuthority
from snapper.data.native_account_authority_store import NativeAccountAuthorityBindingError
from snapper.data.native_account_authority_store import NativeAuthorityFreshnessPolicy
from snapper.data.native_account_authority_store import get_bound_native_account_authority
from snapper.data.native_account_read_store import begin_native_account_read
from tests.helpers.native_account_authority import NativeAuthorityScenario
from tests.helpers.native_account_authority import capture_native_account_read
from tests.helpers.native_account_authority import native_authority_scenario
from tests.helpers.native_account_authority import native_authority_uuid


@pytest.fixture
async def scenario(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[NativeAuthorityScenario]:
    """Own actual SDK signing, local HTTP, source clocks and an isolated native authority database."""
    async with native_authority_scenario(tmp_path, monkeypatch) as value:
        monkeypatch.setattr(capture, "datetime", value.clock)
        yield value


def _identity(number: int) -> ExecutionEvidenceIdentity:
    """Allocate explicit independent synthetic producer identities without supplied row timestamps."""
    return ExecutionEvidenceIdentity(
        public_id=native_authority_uuid(number),
        session_id=native_authority_uuid(999),
        sequence_id=number,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("key", ("cash", "flex", "fi_xbtusd"))
async def test_capture_rebinds_real_native_profile_without_financial_writes(
    scenario: NativeAuthorityScenario, key: str
) -> None:
    """Capture one actual observed account identity without turning its quantities into cash.

    Given: A completed signed key-check and original accounts response from the closed producer,
    When: The native authority producer captures one exact retained profile,
    Then: Its immutable graph and source expiry bind while no execution or financial event exists.
    """
    outcome = await capture_native_account_read(scenario)
    request = capture.NativeAuthorityCaptureRequest(
        read_attempt_public_id=outcome.payload.attempt_public_id, native_account_key=key
    )
    captured = await capture.capture_native_account_authority(
        scenario.repository, request, _identity(500), scenario.policy
    )
    async with scenario.repository.session() as session:
        bound = await get_bound_native_account_authority(session, captured.result.public_id)
        assert await session.scalar(select(func.count()).select_from(Execution)) == 0
        assert await session.scalar(select(func.count()).select_from(ContractAccountingEvent)) == 0
    assert bound.payload == captured.payload
    assert captured.reference.authority_public_id == bound.public_id
    assert captured.reference.authority_content_digest == bound.content_digest
    assert captured.payload.scope.margin_account == key
    assert captured.payload.native_read.attempt_public_id == outcome.payload.attempt_public_id
    assert captured.payload.valid_until == (
        captured.payload.owner_proof.observed_at + scenario.policy.maximum_source_age
    )
    assert captured.payload.observed_at == captured.payload.native_read.source_observed_at
    assert len(scenario.adapter.calls) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("key", ("Flex", "fi-XBTUSD", "unknown"))
async def test_capture_does_not_guess_a_native_wallet_alias(
    scenario: NativeAuthorityScenario, key: str
) -> None:
    """Reject guessed wallet spellings even when another actual native profile exists.

    Given: Successful original accounts bytes containing literal cash, flex and fi_xbtusd keys,
    When: A coordinator asks for a different spelling or missing key,
    Then: Capture refuses without issuing any authority or repeating network I/O.
    """
    outcome = await capture_native_account_read(scenario)
    request = capture.NativeAuthorityCaptureRequest(
        read_attempt_public_id=outcome.payload.attempt_public_id, native_account_key=key
    )
    with pytest.raises(NativeAccountAuthorityBindingError, match="exact_profile_missing"):
        await capture.capture_native_account_authority(
            scenario.repository, request, _identity(500), scenario.policy
        )
    async with scenario.repository.session() as session:
        assert await session.scalar(select(func.count()).select_from(NativeAccountAuthority)) == 0
    assert len(scenario.adapter.calls) == 2


@pytest.mark.asyncio
async def test_empty_native_inventory_cannot_become_an_invented_profile(
    scenario: NativeAuthorityScenario,
) -> None:
    """Preserve an empty source inventory without inventing an account or zero balance.

    Given: An actual successful original accounts response containing an empty profile object,
    When: The producer is asked to capture a flex profile,
    Then: The absent profile refuses and no native authority row is created.
    """
    scenario.adapter.profiles = b"{}"
    outcome = await capture_native_account_read(scenario)
    request = capture.NativeAuthorityCaptureRequest(
        read_attempt_public_id=outcome.payload.attempt_public_id, native_account_key="flex"
    )
    with pytest.raises(NativeAccountAuthorityBindingError, match="exact_profile_missing"):
        await capture.capture_native_account_authority(
            scenario.repository, request, _identity(500), scenario.policy
        )
    async with scenario.repository.session() as session:
        assert await session.scalar(select(func.count()).select_from(NativeAccountAuthority)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("failed", (False, True))
async def test_capture_requires_an_actual_successful_read(
    scenario: NativeAuthorityScenario, failed: bool
) -> None:
    """Keep pending and unsuccessful reads ineligible for source authority.

    Given: Either an actual committed pending attempt or an actual retained HTTP failure,
    When: Native profile capture is requested for that attempt,
    Then: No previous success or fabricated account response supplies the missing evidence.
    """
    if failed:
        scenario.adapter.fail_accounts = True
        outcome = await capture_native_account_read(scenario)
        attempt_id = outcome.payload.attempt_public_id
    else:
        pending = await begin_native_account_read(
            scenario.repository, scenario.selection, _identity(900)
        )
        attempt_id = pending.public_id
    request = capture.NativeAuthorityCaptureRequest(
        read_attempt_public_id=attempt_id, native_account_key="flex"
    )
    with pytest.raises(NativeAccountAuthorityBindingError, match="read_unsuccessful"):
        await capture.capture_native_account_authority(
            scenario.repository, request, _identity(500), scenario.policy
        )
    async with scenario.repository.session() as session:
        assert await session.scalar(select(func.count()).select_from(NativeAccountAuthority)) == 0


@pytest.mark.asyncio
async def test_capture_cannot_supply_a_missing_public_owner_proof(
    scenario: NativeAuthorityScenario, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Refuse when the independently bound safe public proof cannot be obtained.

    Given: Real successful retained accounts bytes but an unavailable owner-proof lookup,
    When: The producer attempts to construct native account authority,
    Then: It refuses instead of accepting caller metadata or inventing an owner UID.
    """
    outcome = await capture_native_account_read(scenario)
    monkeypatch.setattr(capture, "find_native_account_identity", AsyncMock(return_value=None))
    request = capture.NativeAuthorityCaptureRequest(
        read_attempt_public_id=outcome.payload.attempt_public_id, native_account_key="flex"
    )
    with pytest.raises(NativeAccountAuthorityBindingError, match="owner_unproven"):
        await capture.capture_native_account_authority(
            scenario.repository, request, _identity(500), scenario.policy
        )
    async with scenario.repository.session() as session:
        assert await session.scalar(select(func.count()).select_from(NativeAccountAuthority)) == 0


@pytest.mark.asyncio
async def test_capture_retry_keeps_original_row_and_source_window(
    scenario: NativeAuthorityScenario,
) -> None:
    """Keep exact source/profile retry on its original committed identity and knowledge clock.

    Given: A source-bound native authority whose result might have been lost after commit,
    When: The same request is retried later with a different proposed insertion UUID,
    Then: It returns the original row with unchanged validity and no second venue read.
    """
    outcome = await capture_native_account_read(scenario)
    request = capture.NativeAuthorityCaptureRequest(
        read_attempt_public_id=outcome.payload.attempt_public_id, native_account_key="flex"
    )
    first = await capture.capture_native_account_authority(
        scenario.repository, request, _identity(500), scenario.policy
    )
    async with scenario.repository.session() as session:
        original = await get_bound_native_account_authority(session, first.result.public_id)
    scenario.clock.current += timedelta(seconds=1)
    repeated = await capture.capture_native_account_authority(
        scenario.repository, request, _identity(501), scenario.policy
    )
    async with scenario.repository.session() as session:
        rebound = await get_bound_native_account_authority(session, repeated.result.public_id)
    assert first.result.created and not repeated.result.created
    assert first.result.public_id == repeated.result.public_id
    assert first.payload == repeated.payload
    assert original == rebound
    assert len(scenario.adapter.calls) == 2


@pytest.mark.asyncio
async def test_looser_policy_cannot_renew_an_already_issued_source_profile(
    scenario: NativeAuthorityScenario,
) -> None:
    """Require a new read attempt when changed policy would extend previously issued validity.

    Given: A source/profile already issued under a short trusted source-age policy,
    When: The same source is captured again under a longer policy without new I/O,
    Then: The immutable source/profile material conflict refuses rather than extending expiry.
    """
    outcome = await capture_native_account_read(scenario)
    request = capture.NativeAuthorityCaptureRequest(
        read_attempt_public_id=outcome.payload.attempt_public_id, native_account_key="flex"
    )
    short_policy = NativeAuthorityFreshnessPolicy(timedelta(seconds=30), timedelta(seconds=1))
    first = await capture.capture_native_account_authority(
        scenario.repository, request, _identity(500), short_policy
    )
    scenario.clock.current += timedelta(seconds=1)
    with pytest.raises(NativeAccountAuthorityBindingError, match="source_profile_material_changed"):
        await capture.capture_native_account_authority(
            scenario.repository, request, _identity(501), scenario.policy
        )
    async with scenario.repository.session() as session:
        original = await get_bound_native_account_authority(session, first.result.public_id)
    assert original.payload == first.payload
    assert len(scenario.adapter.calls) == 2


@pytest.mark.asyncio
async def test_newer_pending_read_prevents_capture_from_an_older_success(
    scenario: NativeAuthorityScenario,
) -> None:
    """Select actual allocation order before considering an older successful response.

    Given: A successful closed read followed by a newer committed pending attempt,
    When: Capture tries to issue authority from the older successful source,
    Then: Current-source validation refuses and no native authority row is created.
    """
    outcome = await capture_native_account_read(scenario)
    scenario.clock.current += timedelta(seconds=1)
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(900))
    request = capture.NativeAuthorityCaptureRequest(
        read_attempt_public_id=outcome.payload.attempt_public_id, native_account_key="flex"
    )
    with pytest.raises(
        NativeAccountAuthorityBindingError, match="newest_attempt_changed_or_pending"
    ):
        await capture.capture_native_account_authority(
            scenario.repository, request, _identity(500), scenario.policy
        )
    async with scenario.repository.session() as session:
        assert await session.scalar(select(func.count()).select_from(NativeAccountAuthority)) == 0
