"""Real signed HTTP state reads with precommitted attempts and atomic source/result persistence."""

import asyncio
import base64
import json
import sqlite3
import threading
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from io import BytesIO
from pathlib import Path
from typing import cast

import pytest
from kraken.futures import User
from requests import ConnectionError as RequestsConnectionError
from requests import PreparedRequest
from requests import Response
from requests import Session
from requests.adapters import BaseAdapter
from sqlalchemy import delete
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy import update
from urllib3.response import HTTPResponse

from snapper.application.trade import native_account_owner_preflight as preflight
from snapper.application.trade import native_account_read as producer
from snapper.application.trade.futures_native_accounts import FuturesNativeAccountsRequest
from snapper.application.trade.futures_native_accounts import get_bound_futures_native_accounts
from snapper.application.trade.native_private_owner_preparation import PreparedPrivateOwnerRead
from snapper.application.trade.native_private_owner_preparation import prepare_native_private_owner
from snapper.config.credentials import CredentialVersion
from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.native_account_identity import NativeAccountIdentityProofPayload
from snapper.core.native_account_identity import canonical_native_account_identity
from snapper.core.native_account_identity import native_account_identity_digest
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.models import AccountingHoldResolution
from snapper.data.models import AccountingRawReceipt
from snapper.data.models import AccountingScopeHold
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import NativeAccountIdentityProof
from snapper.data.models import NativeAccountReadAttempt
from snapper.data.models import NativeAccountReadOutcome
from snapper.data.models import Wallet
from snapper.data.models import WalletCredential
from snapper.data.native_account_identity_store import find_native_account_identity
from snapper.data.native_account_read_store import BoundNativeAccountReadOutcome
from snapper.data.native_account_read_store import NativeAccountReadBindingError
from snapper.data.native_account_read_store import get_latest_native_account_read
from snapper.data.repository import SQLAlchemyRepository
from snapper.infrastructure.exchanges import kraken_rest_egress
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import (
    FuturesAccountsWireReceipt,
)
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import (
    GuardedFuturesAccountsWireReceipt,
)
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import (
    NativeAccountsTransportError,
)
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import (
    read_futures_prepared_accounts_wire,
)
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import (
    require_guarded_accounts_wire,
)
from snapper.infrastructure.exchanges.kraken_rest_egress import futures_sdk_proxy_target
from snapper.messaging.infrastructure.publisher import SequenceTracker

_WIRE = (
    b'{"result":"success","serverTime":"2026-10-23T13:00:00Z","accounts":'
    b'{"flex":{"type":"multiCollateralMarginAccount","currencies":'
    b'{"USD":{"quantity":12.3400000000000000001,"value":999999}}}}}'
)
_KEY_PATH = "/api/auth/v1/api-keys/v3/check"


def _id(number: int) -> str:
    """Separate fixture wallet, credential and producer session identities."""
    return f"00000000-0000-7000-8000-{number:012d}"


class _Adapter(BaseAdapter):
    """Exercise the actual signed requests session using local bytes and a committed database probe."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.body = _WIRE
        self.status = 200
        self.fail = False
        self.fail_accounts = False
        self.block = False
        self.block_accounts = False
        self.account_uid = _id(80)
        self.key_body: bytes | None = None
        self.entered = threading.Event()
        self.release = threading.Event()
        self.calls: list[PreparedRequest] = []
        self.attempts_seen: list[int] = []
        self.proofs_seen: list[int] = []

    def send(self, request: PreparedRequest, *args: object, **kwargs: object) -> Response:
        """Observe a durable attempt from the actual HTTP worker before returning any response."""
        with closing(sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)) as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM native_account_read_attempts"
            ).fetchone()
            proofs = connection.execute(
                "SELECT COUNT(*) FROM native_account_identity_proofs"
            ).fetchone()
        self.attempts_seen.append(int(count[0]))
        self.proofs_seen.append(int(proofs[0]))
        self.calls.append(request)
        is_key = (request.url or "").endswith(_KEY_PATH)
        if is_key != self.block_accounts:
            self.entered.set()
            if self.block and not self.release.wait(4):
                raise TimeoutError("Isolated transport release was not delivered")
        if self.fail or (self.fail_accounts and not is_key):
            raise RequestsConnectionError("Isolated transport failure")
        return self._response(request, is_key)

    def _response(self, request: PreparedRequest, is_key: bool) -> Response:
        """Return independent identity/state bodies while retaining their actual request ordering."""
        body = self.body
        status = self.status
        if is_key:
            status = 200
            body = (
                self.key_body
                if self.key_body is not None
                else json.dumps(
                    {
                        "apiKey": "synthetic-key",
                        "accountUid": self.account_uid,
                        "permissions": {
                            "general": "READ_ONLY",
                            "transfer": "NO_ACCESS",
                        },
                        "iiban": "synthetic-private-extra-must-not-be-stored",
                    }
                ).encode()
            )
        response = Response()
        response.status_code = status
        response.url = request.url or ""
        response.request = request
        response.raw = HTTPResponse(body=BytesIO(body), status=status, preload_content=False)
        return response

    def close(self) -> None:
        """Own no external socket or persistent transport resource."""


@dataclass
class _Scenario:
    """The actual SDK/session, isolated store and public revision used by the end-to-end producer."""

    repository: SQLAlchemyRepository
    client: KrakenFuturesExchangeClient
    selection: ExecutionClientSelection
    adapter: _Adapter
    tracker: SequenceTracker
    prepared_owner: PreparedPrivateOwnerRead | None = None


@pytest.fixture
async def scenario(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[_Scenario]:
    """Install only synthetic credentials and a local adapter; never connect to an external venue."""
    path = tmp_path / "native-account-producer.db"
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{path}")
    client = KrakenFuturesExchangeClient(
        api_key="synthetic-key", api_secret="c3ludGhldGljLXNlY3JldA=="
    )
    stamp = datetime.now(UTC) - timedelta(days=1)
    credential = CredentialVersion(
        public_id=_id(2),
        wallet_public_id=_id(1),
        exchange="kraken_futures",
        credential_type="api_key_secret",
        timestamp=stamp,
    )
    client.bind_execution_identity(_id(1), credential)
    selection = client.execution_client_selection()
    assert selection is not None
    user = client._user_client
    assert user is not None
    session = cast(Session, user._FuturesClient__session)
    adapter = _Adapter(path)
    session.mount("https://", adapter)
    monkeypatch.setattr(kraken_rest_egress, "get_egress_pool", lambda: None)
    value = _Scenario(repository, client, selection, adapter, SequenceTracker())
    try:
        await repository.create_all()
        async with repository.session() as db:
            db.add_all(
                [
                    Wallet(
                        public_id=_id(1),
                        session_id=_id(9),
                        sequence_id=1,
                        timestamp=stamp,
                        label="native-producer-fixture",
                        is_paper=False,
                    ),
                    WalletCredential(
                        public_id=_id(2),
                        session_id=_id(9),
                        sequence_id=2,
                        timestamp=stamp,
                        wallet_public_id=_id(1),
                        exchange="kraken_futures",
                        credential_type="api_key_secret",
                        encrypted_payload="fixture-never-decrypted",
                    ),
                ]
            )
            await db.commit()
        yield value
    finally:
        adapter.release.set()
        if value.prepared_owner is not None:
            value.prepared_owner.close()
        await client.disconnect()
        session.close()
        await repository.engine.dispose()


async def _counts(scenario: _Scenario) -> tuple[int, ...]:
    """Count only the relevant actual state, financial-event and hold tables."""
    async with scenario.repository.session() as session:
        return tuple(
            [
                (await session.execute(select(func.count()).select_from(model))).scalar_one()
                for model in (
                    NativeAccountReadAttempt,
                    AccountingRawReceipt,
                    NativeAccountReadOutcome,
                    AccountingScopeHold,
                    ContractAccountingEvent,
                    AccountingHoldResolution,
                )
            ]
        )


async def test_actual_http_worker_sees_committed_attempt_before_raw_state_and_result_are_written(
    scenario: _Scenario,
) -> None:
    """The complete producer owns network I/O instead of trusting a caller's balance DTO.

    Given: A real signed SDK session using a local HTTP adapter and isolated database,
    When: The native account producer performs one read,
    Then: Its attempt is already durable at send time and exact state/result commit without financial holds.
    """
    outcome = await producer.read_native_futures_accounts(
        scenario.repository, scenario.client, scenario.tracker
    )
    assert outcome.payload.result.kind == "captured"
    assert scenario.adapter.attempts_seen == [1, 1]
    assert scenario.adapter.proofs_seen == [0, 1]
    assert len(scenario.adapter.calls) == 2
    assert await _counts(scenario) == (1, 1, 1, 0, 0, 0)
    source = outcome.payload.source_reference
    assert source is not None
    async with scenario.repository.session() as session:
        raw = (await session.execute(select(AccountingRawReceipt))).scalar_one()
        proof = (await session.execute(select(NativeAccountIdentityProof))).scalar_one()
        assert "synthetic-key" not in proof.canonical_payload
        assert "synthetic-private-extra" not in proof.canonical_payload
        assert proof.account_uid == scenario.adapter.account_uid
        assert proof.owner_root_public_id is None
        assert _WIRE.decode() not in raw.canonical_payload
        latest = await get_latest_native_account_read(
            session, scenario.selection, datetime.now(UTC)
        )
        assert latest is not None and latest.outcome == outcome
        bound = await get_bound_futures_native_accounts(
            session,
            FuturesNativeAccountsRequest(
                raw_receipt_public_id=source.raw_receipt_public_id,
                client_selection=scenario.selection,
            ),
            datetime.now(UTC),
        )
    assert str(bound.snapshot.profiles[0].quantities[0].quantity) == "12.3400000000000000001"


@pytest.mark.parametrize(
    "status,body,reason,retained",
    (
        (503, b"temporarily unavailable", "http_error", False),
        (200, b"not JSON", "source_invalid", False),
        (
            200,
            b'{"result":"success","accounts":{},"nested":{"Authent":"SYNTHETIC_OTHER_AUTHENT"}}',
            "source_invalid",
            False,
        ),
        (200, b'{"result":"error","error":"fixture"}', "source_invalid", False),
        (
            200,
            b'{"result":"success","serverTime":"2026-10-23T13:00:00Z","accounts":{"unknown":{"type":"newShape"}}}',
            "source_invalid",
            True,
        ),
        (
            200,
            b'{"result":"success","accounts":{},"nested":{"aPiKeY":"synthetic-private"}}',
            "source_invalid",
            False,
        ),
        (
            200,
            b'{"result":"success","accounts":{},"nested":[{"signature":"synthetic-private"}]}',
            "source_invalid",
            False,
        ),
        (
            200,
            b'{"result":"success","accounts":{},"nested":{"safe":"synthetic-key","safe":"overwritten"}}',
            "source_invalid",
            False,
        ),
    ),
)
async def test_unclassified_state_is_not_retained_and_unsupported_financial_state_remains_honest(
    scenario: _Scenario,
    status: int,
    body: bytes,
    reason: str,
    retained: bool,
) -> None:
    """Only classified successful financial envelopes can enter durable raw state storage.

    Given: A failed, auth-bearing, malformed or unsupported account response from real local HTTP,
    When: The closed account-read producer classifies it before any raw encoding or digest,
    Then: Unclassified material leaves only fixed failure metadata while safe unsupported state is retained.
    """
    scenario.adapter.status = status
    scenario.adapter.body = body
    outcome = await producer.read_native_futures_accounts(
        scenario.repository, scenario.client, scenario.tracker
    )
    assert outcome.payload.result.kind == "failed"
    assert outcome.payload.result.reason == reason
    assert (outcome.payload.source_reference is not None) is retained
    assert await _counts(scenario) == (1, int(retained), 1, 0, 0, 0)
    async with scenario.repository.session() as session:
        raw = (await session.execute(select(AccountingRawReceipt))).scalar_one_or_none()
    if retained:
        assert raw is not None
        assert base64.b64encode(body).decode() in raw.canonical_payload
    else:
        assert raw is None


async def test_actual_transport_failure_records_no_invented_raw_receipt(
    scenario: _Scenario,
) -> None:
    """A failed network operation has an actual attempt but no successful response material.

    Given: A real selected session whose local adapter raises a connection failure,
    When: The producer performs the authenticated read,
    Then: It retains transport failure without inventing source bytes or financial state.
    """
    scenario.adapter.fail = True
    outcome = await producer.read_native_futures_accounts(
        scenario.repository, scenario.client, scenario.tracker
    )
    assert outcome.payload.result.kind == "failed"
    assert outcome.payload.result.reason == "transport_error"
    assert outcome.payload.source_reference is None
    assert await _counts(scenario) == (1, 0, 1, 0, 0, 0)


async def test_account_transport_failure_preserves_the_previously_committed_public_identity(
    scenario: _Scenario,
) -> None:
    """Owner knowledge survives a failure of the following independent account-state request.

    Given: An actual successful key check followed by a failing accounts transport,
    When: The closed producer records that unsuccessful read,
    Then: Its original owner proof remains durable without inventing a state response.
    """
    scenario.adapter.fail_accounts = True
    outcome = await producer.read_native_futures_accounts(
        scenario.repository, scenario.client, scenario.tracker
    )
    assert outcome.payload.result.kind == "failed"
    assert outcome.payload.result.reason == "transport_error"
    assert scenario.adapter.proofs_seen == [0, 1]
    async with scenario.repository.session() as session:
        proof = await find_native_account_identity(session, outcome.payload.attempt_public_id)
    assert proof is not None and proof.payload.matches_owner
    assert proof.payload.observation.account_uid == scenario.adapter.account_uid
    assert await _counts(scenario) == (1, 0, 1, 0, 0, 0)


async def test_changed_external_owner_is_retained_and_blocks_the_following_accounts_request(
    scenario: _Scenario,
) -> None:
    """A later key observation cannot silently replace the wallet's original external owner.

    Given: One retained owner and account state followed by a key naming a different account UID,
    When: Another read performs its real identity preflight,
    Then: The conflicting proof and failure commit while the accounts endpoint is never called again.
    """
    original = await producer.read_native_futures_accounts(
        scenario.repository, scenario.client, scenario.tracker
    )
    scenario.adapter.account_uid = _id(81)
    conflict = await producer.read_native_futures_accounts(
        scenario.repository, scenario.client, scenario.tracker
    )
    assert conflict.payload.result.kind == "failed"
    assert conflict.payload.result.reason == "source_invalid"
    assert conflict.payload.source_reference is None
    assert len(scenario.adapter.calls) == 3
    assert scenario.adapter.calls[-1].url == "https://futures.kraken.com" + _KEY_PATH
    async with scenario.repository.session() as session:
        root = await find_native_account_identity(session, original.payload.attempt_public_id)
        proof = await find_native_account_identity(session, conflict.payload.attempt_public_id)
        latest = await get_latest_native_account_read(
            session, scenario.selection, datetime.now(UTC)
        )
    assert root is not None and root.payload.observation.account_uid == _id(80)
    assert proof is not None and not proof.payload.matches_owner
    assert (
        proof.payload.owner_root is not None
        and proof.payload.owner_root.public_id == root.public_id
    )
    assert latest is not None and latest.outcome == conflict
    assert await _counts(scenario) == (2, 1, 2, 0, 0, 0)


async def test_invalid_key_response_never_enters_the_original_state_receipt_store(
    scenario: _Scenario,
) -> None:
    """A malformed sensitive identity body cannot be mistaken for ordinary financial source evidence.

    Given: A local key-check response containing a private marker and invalid JSON,
    When: The closed producer refuses its identity,
    Then: Only the failed attempt is retained with no key proof, source bytes or accounts GET.
    """
    scenario.adapter.key_body = b'{"apiKey":"synthetic-private-invalid-body",'
    outcome = await producer.read_native_futures_accounts(
        scenario.repository, scenario.client, scenario.tracker
    )
    assert outcome.payload.result.kind == "failed"
    assert outcome.payload.result.reason == "transport_error"
    async with scenario.repository.session() as session:
        assert (
            await find_native_account_identity(session, outcome.payload.attempt_public_id) is None
        )
    assert len(scenario.adapter.calls) == 1
    assert "synthetic-private-invalid-body" not in outcome.payload.model_dump_json()
    assert await _counts(scenario) == (1, 0, 1, 0, 0, 0)


async def test_public_identity_commit_failure_prevents_the_following_accounts_request(
    scenario: _Scenario, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An in-memory key observation alone cannot permit the second authenticated request.

    Given: A successful real key response whose public proof cannot be committed,
    When: Identity storage fails before account state is requested,
    Then: The read fails with no account bytes, durable proof or second HTTP request.
    """

    async def fail_storage(*args: object, **kwargs: object) -> None:
        raise RuntimeError("Isolated public proof write failure")

    monkeypatch.setattr(preflight, "retain_native_account_identity", fail_storage)
    outcome = await producer.read_native_futures_accounts(
        scenario.repository, scenario.client, scenario.tracker
    )
    assert outcome.payload.result.kind == "failed"
    assert outcome.payload.result.reason == "transport_error"
    assert len(scenario.adapter.calls) == 1
    assert await _counts(scenario) == (1, 0, 1, 0, 0, 0)


@pytest.mark.parametrize("storage_fails", (False, True))
@pytest.mark.parametrize("after_identity", (False, True))
async def test_cancelled_actual_worker_cannot_later_reauthorize_its_discarded_response(
    scenario: _Scenario,
    monkeypatch: pytest.MonkeyPatch,
    storage_fails: bool,
    after_identity: bool,
) -> None:
    """Cancellation retains actual source ownership until the blocking HTTP worker finishes.

    Given: A committed request whose actual worker is waiting for its original response,
    When: Its asyncio owner is cancelled and failure storage may also be unavailable,
    Then: Source ownership is joined before cancellation propagates and state remains unavailable.

    Args:
        scenario: Actual isolated source repository and signed local HTTP producer.
        monkeypatch: Scoped source clocks and observers that retain actual dispatch and issuance.
        storage_fails: Whether cancellation outcome storage is deliberately unavailable.
        after_identity: Whether the blocked source follows a committed actual key proof.
    """

    async def fail_storage(*args: object, **kwargs: object) -> BoundNativeAccountReadOutcome:
        raise RuntimeError("Isolated cancellation storage failure")

    if storage_fails:
        monkeypatch.setattr(producer, "_record_failure", fail_storage)
    scenario.adapter.block = True
    scenario.adapter.block_accounts = after_identity
    task = asyncio.create_task(
        producer.read_native_futures_accounts(
            scenario.repository, scenario.client, scenario.tracker
        )
    )
    try:
        assert await asyncio.to_thread(scenario.adapter.entered.wait, 2)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        scenario.adapter.release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        async with scenario.repository.session() as session:
            latest = await get_latest_native_account_read(
                session, scenario.selection, datetime.now(UTC)
            )
        assert latest is not None
        if storage_fails:
            assert latest.outcome is None
        else:
            assert latest.outcome is not None and latest.outcome.payload.result.kind == "failed"
            assert latest.outcome.payload.result.reason == "cancelled"
        assert (await _counts(scenario))[1] == 0
        async with scenario.repository.session() as session:
            proof = await find_native_account_identity(session, latest.attempt.public_id)
        assert (proof is not None) is after_identity
    finally:
        scenario.adapter.release.set()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_unproven_client_is_refused_before_starting_an_attempt(
    scenario: _Scenario,
) -> None:
    """Missing actual selection cannot be replaced with syntactically plausible source metadata.

    Given: A Futures client without an instantiated authenticated user,
    When: A native account read is requested,
    Then: It refuses before any durable request or transport operation.
    """
    empty = KrakenFuturesExchangeClient()
    try:
        with pytest.raises(NativeAccountReadBindingError, match="actual_client_missing"):
            await producer.read_native_futures_accounts(
                scenario.repository, empty, scenario.tracker
            )
        assert await _counts(scenario) == (0, 0, 0, 0, 0, 0)
    finally:
        await empty.disconnect()


async def test_changed_actual_credential_blocks_http_before_an_attempt_is_committed(
    scenario: _Scenario,
) -> None:
    """Actual current database selection is checked before authenticated I/O.

    Given: A selected client whose durable credential revision has changed,
    When: The native account read is requested,
    Then: No HTTP request or durable attempt is created through the old client.
    """
    async with scenario.repository.session() as session:
        credential = (await session.execute(select(WalletCredential))).scalar_one()
        credential.timestamp += timedelta(seconds=1)
        await session.commit()
    with pytest.raises(NativeAccountReadBindingError, match="selected_credential_changed"):
        await producer.read_native_futures_accounts(
            scenario.repository, scenario.client, scenario.tracker
        )
    assert scenario.adapter.calls == []
    assert await _counts(scenario) == (0, 0, 0, 0, 0, 0)


async def test_last_outcome_failure_rolls_back_source_while_preserving_the_previously_committed_attempt(
    scenario: _Scenario,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Source and outcome form one transaction independently of the pre-network attempt.

    Given: A real HTTP response and a forced failure at the final outcome write,
    When: The capture transaction fails,
    Then: The original attempt remains pending and no half-committed source/result exists.
    """

    async def refuse(*args: object, **kwargs: object) -> BoundNativeAccountReadOutcome:
        raise LookupError("Isolated final-write failure")

    monkeypatch.setattr(producer, "append_native_account_read_outcome", refuse)
    with pytest.raises(LookupError, match="final-write"):
        await producer.read_native_futures_accounts(
            scenario.repository, scenario.client, scenario.tracker
        )
    assert scenario.adapter.attempts_seen == [1, 1]
    assert scenario.adapter.proofs_seen == [0, 1]
    assert await _counts(scenario) == (1, 0, 0, 0, 0, 0)


type _Capture = Callable[
    [AccountingRepository, producer._ReadOperation, GuardedFuturesAccountsWireReceipt],
    Awaitable[BoundNativeAccountReadOutcome],
]


async def _observe_capture(
    scenario: _Scenario,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[
    _Capture,
    producer._ReadOperation,
    GuardedFuturesAccountsWireReceipt,
    BoundNativeAccountReadOutcome,
]:
    """Retain the actual issued operation/response for explicit uncertain-acknowledgement retry tests."""
    original = producer._capture_response
    seen: list[tuple[producer._ReadOperation, GuardedFuturesAccountsWireReceipt]] = []

    async def capture(
        repository: AccountingRepository,
        operation: producer._ReadOperation,
        receipt: GuardedFuturesAccountsWireReceipt,
    ) -> BoundNativeAccountReadOutcome:
        """Observe only an actual transport-issued receipt without inventing its key-use proof."""
        seen.append((operation, receipt))
        return await original(repository, operation, receipt)

    monkeypatch.setattr(producer, "_capture_response", capture)
    scenario.prepared_owner = await prepare_native_private_owner(
        scenario.repository, scenario.client, scenario.tracker
    )
    operation, receipt = seen[0]
    return original, operation, receipt, scenario.prepared_owner.outcome


async def test_retry_after_uncertain_capture_acknowledgement_reuses_the_original_response_and_result(
    scenario: _Scenario,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An uncertain database acknowledgement does not require a second network response.

    Given: One actual committed source and its original operation/response retained by the producer,
    When: The same capture transaction is retried,
    Then: Original source/result identities and clocks survive without repeating either HTTP call.
    """
    capture, operation, receipt, result = await _observe_capture(scenario, monkeypatch)
    assert await capture(scenario.repository, operation, receipt) == result
    assert len(scenario.adapter.calls) == 2
    assert await _counts(scenario) == (1, 1, 1, 0, 0, 0)


async def test_changed_response_identity_commits_conflict_evidence_even_after_an_original_outcome_exists(
    scenario: _Scenario,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A contradictory retry cannot roll back the evidence of its own identity conflict.

    Given: One original committed response and outcome,
    When: A changed body is presented under the same retained capture operation,
    Then: Changed raw evidence and its hold commit while current read eligibility is refused.
    """
    capture, operation, receipt, result = await _observe_capture(scenario, monkeypatch)
    scenario.adapter.body = _WIRE.replace(b"12.3400000000000000001", b"99.9999999999999999999")
    user = scenario.client._user_client
    prepared = scenario.prepared_owner
    assert user is not None and prepared is not None
    token = prepared.require_token()
    changed = await scenario.client._dispatch_routed_rest(
        operation="replay_changed_native_account_response",
        kind="private_idempotent_read",
        target=futures_sdk_proxy_target(user),
        sync_call=lambda: read_futures_prepared_accounts_wire(user, scenario.selection, token),
    )
    with pytest.raises(NativeAccountReadBindingError, match="raw_identity_conflict"):
        await capture(scenario.repository, operation, changed)
    assert await _counts(scenario) == (1, 2, 1, 1, 0, 0)
    async with scenario.repository.session() as session:
        with pytest.raises(NativeAccountReadBindingError, match="source_identity_contested"):
            await get_latest_native_account_read(session, scenario.selection, datetime.now(UTC))
        retained = (await session.execute(select(NativeAccountReadOutcome))).scalar_one()
    assert retained.public_id == result.public_id


@pytest.mark.parametrize("change", ("client", "clock"))
async def test_actual_response_cannot_be_rebound_to_a_different_client_or_pre_attempt_network_start(
    scenario: _Scenario,
    monkeypatch: pytest.MonkeyPatch,
    change: str,
) -> None:
    """The closed producer checks transport provenance independently of syntactically valid receipts.

    Given: An actual original response with a substituted public selection or request start,
    When: Capture tries to bind it to the retained operation,
    Then: It refuses before adding any new raw source or outcome.
    """
    capture, operation, receipt, _ = await _observe_capture(scenario, monkeypatch)
    original_wire, actual_proof = require_guarded_accounts_wire(receipt)
    data = original_wire.model_dump()
    if change == "client":
        data["client_selection"]["wallet_public_id"] = _id(90)
    else:
        data["request_started_at"] = operation.attempt.payload.started_at - timedelta(
            microseconds=1
        )
    changed = GuardedFuturesAccountsWireReceipt(
        FuturesAccountsWireReceipt.model_validate(data), actual_proof
    )
    with pytest.raises(NativeAccountsTransportError, match="guarded_transport_required"):
        await capture(scenario.repository, operation, changed)
    assert await _counts(scenario) == (1, 1, 1, 0, 0, 0)


@pytest.mark.parametrize("state", ("missing", "changed_uid", "after_network"))
async def test_capture_rebinds_the_actual_stored_key_proof_even_after_a_damaged_restore(
    scenario: _Scenario, monkeypatch: pytest.MonkeyPatch, state: str
) -> None:
    """A genuine issued HTTP receipt cannot bypass actual stored-proof/root reconstruction.

    Given: Actual original key/account HTTP and a receipt retained across an uncertain database acknowledgement,
    When: A damaged local restore removes or changes its independently stored safe key proof,
    Then: Capture refuses before adding source rows or changing the original outcome.
    """
    capture, operation, receipt, _ = await _observe_capture(scenario, monkeypatch)
    wire, _ = require_guarded_accounts_wire(receipt)
    async with scenario.repository.session() as session:
        actual = await find_native_account_identity(session, operation.attempt.public_id)
        assert actual is not None
        if state == "missing":
            await session.execute(text("DROP TRIGGER native_account_identity_proofs_reject_delete"))
            await session.execute(
                delete(NativeAccountIdentityProof).where(
                    NativeAccountIdentityProof.public_id == actual.public_id
                )
            )
        else:
            data = actual.payload.model_dump()
            if state == "changed_uid":
                data["observation"]["account_uid"] = _id(81)
            else:
                data["recorded_at"] = wire.request_started_at + timedelta(seconds=1)
            restored = NativeAccountIdentityProofPayload.model_validate(data)
            await session.execute(text("DROP TRIGGER native_account_identity_proofs_reject_update"))
            await session.execute(
                update(NativeAccountIdentityProof)
                .where(NativeAccountIdentityProof.public_id == actual.public_id)
                .values(
                    timestamp=restored.recorded_at,
                    account_uid=restored.observation.account_uid,
                    canonical_payload=canonical_native_account_identity(restored),
                    content_digest=native_account_identity_digest(restored),
                )
            )
        await session.commit()
    with pytest.raises(NativeAccountReadBindingError, match="identity_preflight_unproven"):
        await capture(scenario.repository, operation, receipt)
    assert (await _counts(scenario))[1:] == (1, 1, 0, 0, 0)


@pytest.mark.parametrize("changed", ("missing", "replacement", "selection"))
async def test_selected_sdk_user_and_public_binding_cannot_change_between_read_phases(
    scenario: _Scenario, monkeypatch: pytest.MonkeyPatch, changed: str
) -> None:
    """Original selected client identity survives the await between network and proof persistence.

    Given: The actual instantiated user and immutable selection retained by a read operation,
    When: The SDK user disappears, is replaced, or the reported public binding changes,
    Then: The operation refuses before relying on the replacement for another phase.
    """
    user = scenario.client._user_client
    assert user is not None
    selected = scenario.selection
    expected: User | None = user
    if changed == "missing":
        expected = None
    elif changed == "replacement":
        monkeypatch.setattr(scenario.client, "_user_client", None)
    else:
        selected = ExecutionClientSelection.model_validate(
            {**selected.model_dump(), "wallet_public_id": _id(90)}
        )
    try:
        with pytest.raises(NativeAccountReadBindingError, match="actual_client_changed"):
            preflight.require_actual_native_user(scenario.client, selected, expected)
        assert await _counts(scenario) == (0, 0, 0, 0, 0, 0)
    finally:
        scenario.client._user_client = user


async def test_replaced_sdk_user_during_attempt_commit_cannot_anchor_another_owner(
    scenario: _Scenario, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The originally selected SDK user owns identity preflight across the attempt-commit await.

    Given: User A selected before the actual durable attempt and a valid different User B,
    When: The client replaces A with B while that attempt is being committed,
    Then: The read fails before B can send its key check or anchor its external account UID.
    """
    original_user = scenario.client._user_client
    assert original_user is not None
    replacement = User(key="replacement-fixture-key", secret="c3ludGhldGljLXNlY3JldA==")
    replacement_session = cast(Session, replacement._FuturesClient__session)
    replacement_session.mount("https://", scenario.adapter)
    scenario.adapter.key_body = json.dumps(
        {
            "apiKey": "replacement-fixture-key",
            "accountUid": _id(81),
            "permissions": {"general": "READ_ONLY", "transfer": "NO_ACCESS"},
        }
    ).encode()
    start = producer._start_operation

    async def replace_during_commit(
        repository: AccountingRepository,
        selection: ExecutionClientSelection,
        tracker: SequenceTracker,
    ) -> producer._ReadOperation:
        operation = await start(repository, selection, tracker)
        scenario.client._user_client = replacement
        return operation

    monkeypatch.setattr(producer, "_start_operation", replace_during_commit)
    try:
        outcome = await producer.read_native_futures_accounts(
            scenario.repository, scenario.client, scenario.tracker
        )
        assert outcome.payload.result.kind == "failed"
        assert outcome.payload.result.reason == "transport_error"
        async with scenario.repository.session() as session:
            proof = await find_native_account_identity(session, outcome.payload.attempt_public_id)
        assert proof is None
        assert scenario.adapter.calls == []
        assert await _counts(scenario) == (1, 0, 1, 0, 0, 0)
    finally:
        scenario.client._user_client = original_user
        replacement_session.close()
