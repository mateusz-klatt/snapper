"""Exercise the actual app installer with genuine local signed HTTP, SQLite and issued prepared ownership."""

import asyncio
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Literal

import pytest
from kraken.futures import User

import snapper.infrastructure.exchanges.kraken_futures_key_identity as transport
from snapper.application.trade.native_execution_source_installation import (
    prepare_native_execution_observation_capture,
)
from snapper.application.trade.native_private_owner_preparation import prepare_native_private_owner
from snapper.application.trade.private_source_observation_ingestion import (
    AuthenticatedPrivateObservationSink,
)
from snapper.application.trade.unit_snapshot_route import CapturedExecutionRoute
from snapper.core.native_account_identity import NativeAccountPublicIdentity
from snapper.data.native_account_identity_store import find_native_account_identity
from snapper.data.native_account_read_store import BoundNativeAccountReadOutcome
from snapper.data.native_account_read_store import get_latest_native_account_read
from snapper.infrastructure.exchanges.implementations.paper import PaperExchangeClient
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyToken
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyTokenError
from snapper.messaging.infrastructure.publisher import SequenceTracker
from tests.helpers.native_account_authority import NativeAuthorityScenario
from tests.helpers.native_account_authority import native_authority_scenario
from tests.helpers.native_account_authority import native_authority_uuid


def _observe_actual_issuance(monkeypatch: pytest.MonkeyPatch) -> list[PreparedKeyToken]:
    """Observe real closed transport issuance without substituting a public-shaped token or issuer."""
    issued: list[PreparedKeyToken] = []
    actual = transport._mint_prepared_private_key

    def observe(user: User, key: str, observation: NativeAccountPublicIdentity) -> PreparedKeyToken:
        """Return exactly the token created by the actual source operation after its real key comparison."""
        token = actual(user, key, observation)
        issued.append(token)
        return token

    monkeypatch.setattr(transport, "_mint_prepared_private_key", observe)
    return issued


async def _latest(source: NativeAuthorityScenario) -> BoundNativeAccountReadOutcome:
    """Fetch the real latest persisted attempt/outcome rather than a supplied preparation DTO."""
    async with source.repository.session() as session:
        latest = await get_latest_native_account_read(
            session, source.selection, source.clock.current
        )
    assert latest is not None and latest.outcome is not None
    return latest.outcome


def _require_closed(source: NativeAuthorityScenario, token: PreparedKeyToken) -> None:
    """Check actual runtime revocation while preserving committed public source history."""
    user = source.client._user_client
    assert user is not None
    with pytest.raises(PreparedKeyTokenError, match="closed"):
        token.assert_prepared_key(user, source.selection, "synthetic-key", source.clock.current)


def _no_legacy_authority() -> CapturedExecutionRoute | None:
    """Keep missing nominal route evidence explicit rather than fabricating a legacy authority."""
    return None


def _require_unstarted(source: NativeAuthorityScenario) -> None:
    """A preparation failure must not create a private SDK, receive task or pending owner handoff."""
    assert source.client._private_ws_client is None
    assert source.client._prepared_private_source_token is None
    assert source.client._private_frame_sink_factory is None


async def test_actual_installation_transfers_same_issued_owner_and_frame_factory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Transfer genuine prepared ownership only after actual key proof and account read have committed.

    Given: An actual isolated selected SDK User and local signed HTTP source with no private client.
    When: The application installer completes its real same-operation prepared-key account read.
    Then: The actual issued token and frame factory are installed without starting a private socket.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        source.clock.current = datetime.now(UTC)
        issued = _observe_actual_issuance(monkeypatch)
        calls: list[None] = []

        def authority() -> CapturedExecutionRoute | None:
            """Observe that nominal legacy authority is selected per original frame, not during startup."""
            calls.append(None)
            return None

        await prepare_native_execution_observation_capture(
            source.repository, source.client, authority, source.tracker
        )
        assert len(issued) == 1
        assert source.client._prepared_private_source_token is issued[0]
        assert source.client._private_ws_client is None
        assert calls == []
        factory = source.client._private_frame_sink_factory
        assert factory is not None
        assert isinstance(getattr(factory, "__self__", None), AuthenticatedPrivateObservationSink)
        outcome = await _latest(source)
        assert outcome.payload.result.kind == "captured"
        reference = issued[0].require_proof_reference()
        assert reference.read_attempt_public_id == outcome.payload.attempt_public_id
        async with source.repository.session() as session:
            proof = await find_native_account_identity(session, reference.read_attempt_public_id)
        assert proof is not None and proof.payload.matches_owner
        assert reference.public_id == proof.public_id
        assert reference.account_uid == source.adapter.account_uid
        assert source.adapter.proofs_at_accounts_send == [1]
        user = source.client._user_client
        assert user is not None
        issued[0].assert_prepared_key(user, source.selection, "synthetic-key", datetime.now(UTC))


async def test_futures_installation_requires_actual_repository_before_source_dispatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Refuse missing persistence before any source proof or runtime ownership can be fabricated.

    Given: A real selected Futures client whose local signed HTTP provider has not received a call.
    When: Installation has no actual SQL repository.
    Then: The immediate prerequisite refuses with no HTTP attempt, token, frame writer or private SDK.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        issued = _observe_actual_issuance(monkeypatch)
        with pytest.raises(ValueError, match="native_private_execution_repository_required"):
            await prepare_native_execution_observation_capture(
                None, source.client, _no_legacy_authority, source.tracker
            )
        assert issued == [] and source.adapter.calls == []
        _require_unstarted(source)


async def test_actual_accounts_failure_keeps_safe_history_and_closes_unhanded_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Retain the successful safe key proof while refusing failed account-read runtime installation.

    Given: A real local key check succeeds and the actual signed accounts transport refuses its read.
    When: The installer requests the unsuccessful prepared-owner handoff.
    Then: The real issued token is closed, failed source history remains and no private runtime starts.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        source.clock.current = datetime.now(UTC)
        issued = _observe_actual_issuance(monkeypatch)
        source.adapter.fail_accounts = True
        with pytest.raises(ValueError, match="private_owner_preparation_unsuccessful"):
            await prepare_native_execution_observation_capture(
                source.repository, source.client, _no_legacy_authority, source.tracker
            )
        assert len(issued) == 1
        _require_closed(source, issued[0])
        _require_unstarted(source)
        outcome = await _latest(source)
        assert outcome.payload.result.kind == "failed"
        async with source.repository.session() as session:
            proof = await find_native_account_identity(session, outcome.payload.attempt_public_id)
        assert proof is not None and proof.payload.matches_owner
        assert source.adapter.proofs_at_accounts_send
        assert set(source.adapter.proofs_at_accounts_send) == {1}


async def test_actual_owner_conflict_never_installs_a_new_private_account_namespace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Refuse a later real conflicting UID instead of treating a read-root change as runtime authority.

    Given: Actual source history already pins the selected credential's initial external account UID.
    When: Installation observes another UID through the actual signed key response.
    Then: Its conflicting proof stays historical, accounts GET is skipped and no owner is handed off.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        source.clock.current = datetime.now(UTC)
        original = await prepare_native_private_owner(
            source.repository, source.client, source.tracker
        )
        original.close()
        source.clock.current += timedelta(seconds=1)
        source.adapter.account_uid = native_authority_uuid(999)
        issued = _observe_actual_issuance(monkeypatch)
        with pytest.raises(ValueError, match="private_owner_preparation_unsuccessful"):
            await prepare_native_execution_observation_capture(
                source.repository, source.client, _no_legacy_authority, source.tracker
            )
        assert len(issued) == 1
        _require_closed(source, issued[0])
        _require_unstarted(source)
        outcome = await _latest(source)
        assert outcome.payload.result.kind == "failed"
        async with source.repository.session() as session:
            proof = await find_native_account_identity(session, outcome.payload.attempt_public_id)
        assert proof is not None and not proof.payload.matches_owner
        assert proof.payload.owner_root is not None
        assert source.adapter.proofs_at_accounts_send == [1]
        assert len(source.adapter.calls) == 3


async def test_actual_inflight_read_cancellation_closes_owner_before_private_handoff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cancel genuine account IO after safe proof commitment without leaking its unhanded key owner.

    Given: The actual local accounts request is blocked after real signed key proof and token issuance.
    When: The installer task is cancelled while awaiting that actual transport operation.
    Then: Its real token closes and no private SDK or frame writer starts; the owned worker is released.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        source.clock.current = datetime.now(UTC)
        issued = _observe_actual_issuance(monkeypatch)
        source.adapter.block_accounts = True
        task = asyncio.create_task(
            prepare_native_execution_observation_capture(
                source.repository, source.client, _no_legacy_authority, source.tracker
            )
        )
        try:
            assert await asyncio.to_thread(source.adapter.accounts_entered.wait, 2)
            assert len(issued) == 1
            task.cancel()
            await asyncio.sleep(0)
            source.adapter.accounts_release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            source.adapter.accounts_release.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        _require_closed(source, issued[0])
        _require_unstarted(source)
        outcome = await _latest(source)
        assert outcome.payload.result.kind == "failed"


@pytest.mark.parametrize("stop", ("failure", "cancelled"))
async def test_installation_exception_closes_the_actual_prepared_token_before_handoff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stop: Literal["failure", "cancelled"]
) -> None:
    """Close a genuine successful preparation when the synchronous installation boundary refuses.

    Given: Actual key/account preparation succeeds and no private SDK has been constructed.
    When: Frame-writer installation refuses with an ordinary error or explicit cancellation.
    Then: The application's BaseException cleanup closes the actual issued token before propagation.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        source.clock.current = datetime.now(UTC)
        issued = _observe_actual_issuance(monkeypatch)

        def refuse(_factory: object) -> None:
            """Reject only the application installation boundary, preserving real source issuance."""
            if stop == "cancelled":
                raise asyncio.CancelledError
            raise RuntimeError("isolated_frame_writer_installation_refused")

        monkeypatch.setattr(source.client, "set_private_frame_observation_sink", refuse)
        if stop == "cancelled":
            with pytest.raises(asyncio.CancelledError):
                await prepare_native_execution_observation_capture(
                    source.repository, source.client, _no_legacy_authority, source.tracker
                )
        else:
            with pytest.raises(RuntimeError, match="isolated_frame_writer_installation_refused"):
                await prepare_native_execution_observation_capture(
                    source.repository, source.client, _no_legacy_authority, source.tracker
                )
        assert len(issued) == 1
        _require_closed(source, issued[0])
        _require_unstarted(source)
        assert (await _latest(source)).payload.result.kind == "captured"


async def test_non_futures_client_is_untouched_without_repository_or_source_lookup() -> None:
    """Preserve ordinary non-Futures startup without constructing native ownership or capture state.

    Given: A real Paper client has no native REST identity, repository or private SDK.
    When: The shared installer is called for that non-Futures client.
    Then: It returns without consulting authority or preparing any source and ordinary cleanup remains valid.
    """
    client = PaperExchangeClient()

    def forbidden_authority() -> CapturedExecutionRoute | None:
        """Fail if a no-op client is incorrectly routed into native source preparation."""
        raise AssertionError("Non-Futures installation must not consult native authority")

    try:
        await prepare_native_execution_observation_capture(
            None, client, forbidden_authority, SequenceTracker()
        )
    finally:
        await client.disconnect()
