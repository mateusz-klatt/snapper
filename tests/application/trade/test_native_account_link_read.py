"""Actual same-response IIBAN companion through signed local HTTP and immutable SQLite history."""

import asyncio
import copy
import json
import pickle
from collections.abc import Iterator
from typing import Literal

import pytest
from kraken.futures import User
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

import snapper.application.trade.native_account_link_read as linked
import tests.application.trade.test_native_account_read as native_read_tests
from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.native_account_identity import NativeAccountPublicIdentity
from snapper.core.native_account_read import NativeAccountReadOutcomePayload
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.models import NativeAccountIdentityProof
from snapper.data.models import NativeAccountReadAttempt
from snapper.data.models import NativeAccountReadOutcome
from snapper.data.native_account_identity_store import BoundNativeAccountIdentityProof
from snapper.data.native_account_identity_store import find_native_account_identity
from snapper.data.native_account_read_store import BoundNativeAccountReadOutcome
from snapper.infrastructure.exchanges.prepared_futures_account_link import (
    FuturesAccountLinkSourceError,
)
from snapper.infrastructure.exchanges.prepared_futures_account_link import (
    PreparedFuturesAccountLink,
)
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyTokenError
from tests.application.trade.test_native_account_read import _counts
from tests.application.trade.test_native_account_read import _id
from tests.application.trade.test_native_account_read import _Scenario

scenario = native_read_tests.scenario

_IIBAN = "AA00 TEST ACCT 0001"


def _key_body(source: _Scenario, iiban: object = _IIBAN) -> None:
    """Set original local HTTP key-response bytes before the actual SDK producer reads them."""
    source.adapter.key_body = json.dumps(
        {
            "apiKey": "synthetic-key",
            "accountUid": source.adapter.account_uid,
            "permissions": {"general": "READ_ONLY", "transfer": "NO_ACCESS"},
            "iiban": iiban,
        }
    ).encode()


def _observe_links(monkeypatch: pytest.MonkeyPatch) -> list[PreparedFuturesAccountLink]:
    """Observe only completed actual source issuance without replacing a comparison or issuer."""
    issued: list[PreparedFuturesAccountLink] = []
    original = linked.read_futures_prepared_account_link

    def observe(user: User, selection: ExecutionClientSelection) -> PreparedFuturesAccountLink:
        """Call the original real signed HTTP reader before recording its actual issued result."""
        result = original(user, selection)
        issued.append(result)
        return result

    monkeypatch.setattr(linked, "read_futures_prepared_account_link", observe)
    return issued


@pytest.fixture(autouse=True)
def issued_links(
    scenario: _Scenario, monkeypatch: pytest.MonkeyPatch
) -> Iterator[list[PreparedFuturesAccountLink]]:
    """Close every observed genuine link even when an earlier test setup or assertion fails."""
    issued = _observe_links(monkeypatch)
    try:
        yield issued
    finally:
        scenario.adapter.release.set()
        for source in issued:
            source.close()


async def _drain_linked_read(
    scenario: _Scenario,
    task: asyncio.Task[
        tuple[BoundNativeAccountReadOutcome, linked.PreparedNativeAccountLinkOwner | None]
    ],
) -> None:
    """Release and join the actual source without replacing an earlier assertion with cleanup failure."""
    scenario.adapter.release.set()
    for result in await asyncio.gather(task, return_exceptions=True):
        if isinstance(result, BaseException):
            continue
        owner = result[1]
        if owner is not None:
            owner.close()


async def test_actual_link_uses_one_committed_attempt_and_one_key_accounts_pair(
    scenario: _Scenario,
) -> None:
    """The IIBAN companion preserves the existing source/proof/result lifecycle and v1 payload.

    Given: The actual SDK and local HTTP return a supported same-response UUID and IIBAN,
    When: The companion commits its original proof then reads and retains accounts,
    Then: Both requests share one attempt and the same original source remains owned until close.
    """
    _key_body(scenario)
    outcome, owner = await linked.read_linked_native_futures_accounts(
        scenario.repository, scenario.client, scenario.tracker
    )
    assert outcome.payload.result.kind == "captured" and owner is not None
    link = owner.require_link()
    token = owner.require_token()
    try:
        assert link.token is token
        assert link.observation.internal_account_id == _IIBAN
        assert link.observation.identity == owner.proof.payload.observation
        assert owner.proof.payload.attempt_public_id == outcome.payload.attempt_public_id
        assert token.require_proof_reference().public_id == owner.proof.public_id
        assert scenario.adapter.attempts_seen == [1, 1]
        assert scenario.adapter.proofs_seen == [0, 1]
        assert len(scenario.adapter.calls) == 2
        assert all(request.headers["Authent"] for request in scenario.adapter.calls)
        assert await _counts(scenario) == (1, 1, 1, 0, 0, 0)
        async with scenario.repository.session() as session:
            proof = (await session.execute(select(NativeAccountIdentityProof))).scalar_one()
        assert _IIBAN not in proof.canonical_payload
        assert "synthetic-key" not in proof.canonical_payload
        assert "iiban" not in proof.canonical_payload
        assert _IIBAN not in repr(owner)
        retained_proof = owner.proof
        with pytest.raises(AttributeError, match="immutable"):
            owner.proof = retained_proof
        with pytest.raises(TypeError, match="serialized"):
            pickle.dumps(owner)
        with pytest.raises(TypeError, match="copied"):
            copy.deepcopy(owner)
    finally:
        owner.close()
        owner.close()
    with pytest.raises(ValueError, match="unavailable"):
        owner.require_link()
    with pytest.raises(FuturesAccountLinkSourceError, match="not_transport_issued"):
        _ = link.observation
    with pytest.raises(PreparedKeyTokenError, match="closed"):
        token.require_proof_reference()


@pytest.mark.parametrize("failure", ("http", "invalid_json", "unknown_profile", "transport"))
async def test_actual_accounts_failure_preserves_proof_but_closes_the_whole_link(
    scenario: _Scenario, issued_links: list[PreparedFuturesAccountLink], failure: str
) -> None:
    """An original successful key response cannot leave a live companion after accounts capture fails.

    Given: A genuine IIBAN key response followed by an unsuccessful original accounts response,
    When: The shared account-read lifecycle records the actual terminal result,
    Then: Its unchanged safe proof remains but no live link or token is handed to a caller.
    """
    _key_body(scenario)
    issued = issued_links
    if failure == "http":
        scenario.adapter.status = 503
    elif failure == "invalid_json":
        scenario.adapter.body = b"not JSON"
    elif failure == "unknown_profile":
        scenario.adapter.body = (
            b'{"result":"success","serverTime":"2026-10-23T13:00:00Z",'
            b'"accounts":{"new":{"type":"unknown"}}}'
        )
    else:
        scenario.adapter.fail_accounts = True
    outcome, owner = await linked.read_linked_native_futures_accounts(
        scenario.repository, scenario.client, scenario.tracker
    )
    assert outcome.payload.result.kind == "failed" and owner is None
    assert len(issued) == 1 and len(scenario.adapter.calls) == 2
    with pytest.raises(FuturesAccountLinkSourceError, match="not_transport_issued"):
        _ = issued[0].observation
    async with scenario.repository.session() as session:
        proof = await find_native_account_identity(session, outcome.payload.attempt_public_id)
    assert proof is not None and proof.payload.matches_owner


@pytest.mark.parametrize("iiban", (None, "unsupported", "aa00 TEST ACCT 0001"))
async def test_actual_unavailable_iiban_does_not_begin_an_accounts_read(
    scenario: _Scenario, iiban: object
) -> None:
    """The explicit companion cannot borrow an internal account identifier from another observation.

    Given: An actual matched key response whose original IIBAN is unsupported or null,
    When: The explicit linked account-read operation is performed,
    Then: Its one real key request yields a failed outcome without a proof, accounts read or owner.
    """
    _key_body(scenario, iiban)
    outcome, owner = await linked.read_linked_native_futures_accounts(
        scenario.repository, scenario.client, scenario.tracker
    )
    assert outcome.payload.result.kind == "failed" and owner is None
    assert len(scenario.adapter.calls) == 1
    assert await _counts(scenario) == (1, 0, 1, 0, 0, 0)
    async with scenario.repository.session() as session:
        assert (
            await find_native_account_identity(session, outcome.payload.attempt_public_id) is None
        )


async def test_actual_changed_uid_remains_a_conflict_and_never_replaces_the_first_root(
    scenario: _Scenario, issued_links: list[PreparedFuturesAccountLink]
) -> None:
    """A supported IIBAN does not override the original native account ownership policy.

    Given: One original linked account operation and a later same-key response with another UUID,
    When: The next real operation observes the ownership conflict,
    Then: The original root remains and the new link closes without a second accounts request.
    """
    _key_body(scenario)
    first, initial = await linked.read_linked_native_futures_accounts(
        scenario.repository, scenario.client, scenario.tracker
    )
    assert initial is not None
    initial.close()
    issued_links.clear()
    scenario.adapter.account_uid = _id(990)
    _key_body(scenario)
    issued = issued_links
    conflict, owner = await linked.read_linked_native_futures_accounts(
        scenario.repository, scenario.client, scenario.tracker
    )
    assert conflict.payload.result.kind == "failed" and owner is None
    async with scenario.repository.session() as session:
        first_proof = await find_native_account_identity(session, first.payload.attempt_public_id)
        next_proof = await find_native_account_identity(session, conflict.payload.attempt_public_id)
    assert first_proof is not None and next_proof is not None
    assert not next_proof.payload.matches_owner
    assert next_proof.payload.owner_root is not None
    assert next_proof.payload.owner_root.public_id == first_proof.public_id
    assert len(scenario.adapter.calls) == 3 and len(issued) == 1
    with pytest.raises(FuturesAccountLinkSourceError, match="not_transport_issued"):
        _ = issued[0].observation


async def test_actual_user_change_after_real_proof_commit_closes_unhanded_link(
    scenario: _Scenario,
    monkeypatch: pytest.MonkeyPatch,
    issued_links: list[PreparedFuturesAccountLink],
) -> None:
    """The unchanged v1 proof is historical evidence while an actual User replacement ends handoff.

    Given: One actual link source and the actual committed immutable native proof,
    When: The selected client's User slot changes immediately after that real commit,
    Then: Accounts dispatch is refused and the original link closes while the proof remains.
    """
    _key_body(scenario)
    issued = issued_links
    retain = linked.retain_native_account_identity

    async def replace_after_commit(
        repository: AccountingRepository,
        attempt_public_id: str,
        observation: NativeAccountPublicIdentity,
        identity: ExecutionEvidenceIdentity,
    ) -> BoundNativeAccountIdentityProof:
        """Perform the original actual commit before replacing the real selected SDK User slot."""
        proof = await retain(repository, attempt_public_id, observation, identity)
        monkeypatch.setattr(scenario.client, "_user_client", None)
        return proof

    monkeypatch.setattr(linked, "retain_native_account_identity", replace_after_commit)
    outcome, owner = await linked.read_linked_native_futures_accounts(
        scenario.repository, scenario.client, scenario.tracker
    )
    assert outcome.payload.result.kind == "failed" and owner is None
    assert len(scenario.adapter.calls) == 1 and len(issued) == 1
    async with scenario.repository.session() as session:
        proof = await find_native_account_identity(session, outcome.payload.attempt_public_id)
    assert proof is not None
    with pytest.raises(FuturesAccountLinkSourceError, match="not_transport_issued"):
        _ = issued[0].observation


@pytest.mark.parametrize("phase", ("key", "accounts"))
async def test_actual_cancelled_accounts_worker_joins_before_link_revocation_and_outcome(
    scenario: _Scenario,
    issued_links: list[PreparedFuturesAccountLink],
    phase: Literal["key", "accounts"],
) -> None:
    """Own both late key issuance and the original accounts read through repeated cancellation.

    Given: The actual source worker is blocked inside its original signed local HTTP request,
    When: Its caller receives two cancellation requests before that worker is released,
    Then: No outcome or owner release precedes join and the first actual caller signal survives.
    """
    _key_body(scenario)
    scenario.adapter.block = True
    scenario.adapter.block_accounts = phase == "accounts"
    task = asyncio.create_task(
        linked.read_linked_native_futures_accounts(
            scenario.repository, scenario.client, scenario.tracker
        )
    )
    try:
        assert await asyncio.to_thread(scenario.adapter.entered.wait, 2)
        assert len(issued_links) == int(phase == "accounts")
        task.cancel("synthetic-linked-read-cancelled")
        await asyncio.sleep(0)
        assert not task.done()
        task.cancel("synthetic-linked-read-cancelled-again")
        await asyncio.sleep(0)
        assert not task.done()
        assert await _counts(scenario) == (1, 0, 0, 0, 0, 0)
        if phase == "accounts":
            assert issued_links[0].observation.internal_account_id == _IIBAN
            assert (
                issued_links[0].token.require_proof_reference().account_uid
                == scenario.adapter.account_uid
            )
        scenario.adapter.release.set()
        with pytest.raises(
            asyncio.CancelledError, match="^synthetic-linked-read-cancelled$"
        ) as interrupted:
            await task
        assert interrupted.value.__context__ is None
        assert len(issued_links) == 1
        assert len(scenario.adapter.calls) == (2 if phase == "accounts" else 1)
        assert await _counts(scenario) == (1, 0, 1, 0, 0, 0)
        async with scenario.repository.session() as session:
            outcome = (await session.execute(select(NativeAccountReadOutcome))).scalar_one()
            payload = NativeAccountReadOutcomePayload.model_validate_json(outcome.canonical_payload)
            proof = await find_native_account_identity(session, payload.attempt_public_id)
        assert payload.result.kind == "failed" and payload.result.reason == "cancelled"
        assert (proof is not None) is (phase == "accounts")
        with pytest.raises(FuturesAccountLinkSourceError, match="not_transport_issued"):
            _ = issued_links[0].observation
    finally:
        await _drain_linked_read(scenario, task)


@pytest.mark.parametrize("boundary", ("proof", "outcome"))
async def test_actual_sql_refusal_closes_link_without_inventing_committed_source_or_result(
    scenario: _Scenario,
    issued_links: list[PreparedFuturesAccountLink],
    boundary: Literal["proof", "outcome"],
) -> None:
    """Actual SQL insert refusal preserves the existing transaction boundaries and disposes its link.

    Given: Genuine signed key/accounts HTTP and an isolated trigger refusing one original insert,
    When: The real proof or final outcome writer reaches that database boundary,
    Then: The actual link closes and only the earlier successfully committed history remains.
    """
    _key_body(scenario)
    table = (
        "native_account_identity_proofs" if boundary == "proof" else "native_account_read_outcomes"
    )
    async with scenario.repository.session() as session:
        await session.execute(
            text(
                f"CREATE TRIGGER refuse_link_fixture BEFORE INSERT ON {table} "
                "BEGIN SELECT RAISE(ABORT, 'synthetic-link-insert-refused'); END"
            )
        )
        await session.commit()
    if boundary == "proof":
        outcome, owner = await linked.read_linked_native_futures_accounts(
            scenario.repository, scenario.client, scenario.tracker
        )
        assert outcome.payload.result.kind == "failed"
        assert outcome.payload.result.reason == "transport_error" and owner is None
    else:
        with pytest.raises(IntegrityError, match="synthetic-link-insert-refused"):
            await linked.read_linked_native_futures_accounts(
                scenario.repository, scenario.client, scenario.tracker
            )
    assert await _counts(scenario) == (1, 0, int(boundary == "proof"), 0, 0, 0)
    assert len(scenario.adapter.calls) == (1 if boundary == "proof" else 2)
    assert len(issued_links) == 1
    async with scenario.repository.session() as session:
        attempt = (await session.execute(select(NativeAccountReadAttempt))).scalar_one()
        proof = await find_native_account_identity(session, attempt.public_id)
    assert (proof is not None) is (boundary == "outcome")
    with pytest.raises(FuturesAccountLinkSourceError, match="not_transport_issued"):
        _ = issued_links[0].observation


async def test_actual_closed_link_cannot_be_reopened_through_the_successful_owner(
    scenario: _Scenario,
) -> None:
    """The owner facade must consult original live issuance on every access.

    Given: One successful actual read and its original proof-bound account link,
    When: That real link is explicitly closed while the owner facade still exists,
    Then: The facade cannot revive observation or token authority and close remains idempotent.
    """
    _key_body(scenario)
    outcome, owner = await linked.read_linked_native_futures_accounts(
        scenario.repository, scenario.client, scenario.tracker
    )
    assert outcome.payload.result.kind == "captured" and owner is not None
    try:
        link = owner.require_link()
        token = owner.require_token()
        link.close()
        with pytest.raises(FuturesAccountLinkSourceError, match="not_transport_issued"):
            owner.require_link()
        with pytest.raises(FuturesAccountLinkSourceError, match="not_transport_issued"):
            owner.require_token()
        with pytest.raises(PreparedKeyTokenError, match="closed"):
            token.require_proof_reference()
    finally:
        owner.close()
    with pytest.raises(ValueError, match="unavailable"):
        owner.require_token()
