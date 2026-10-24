"""Issued-key preparation scope using real SQLite and closed signed HTTP.

The actual SDK encoding test exercises the token API at a local physical send;
it grants no authenticated receive ticket or native financial permission.
"""

import asyncio
import json
import pickle
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Mapping
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any
from typing import cast

import kraken.futures.websocket as futures_websocket
import pytest
from kraken.futures import FuturesWSClient
from kraken.futures import User
from requests import PreparedRequest
from requests import Response
from requests import Session
from requests.auth import AuthBase
from websockets.protocol import State

import snapper.application.trade.native_account_owner_preflight as preflight
import snapper.application.trade.native_account_read as producer
import snapper.infrastructure.exchanges.futures_native_state_privacy as privacy
import snapper.infrastructure.exchanges.kraken_futures_key_identity as transport
from snapper.application.trade.native_account_owner_preflight import PreparedNativeAccountOwner
from snapper.application.trade.native_private_owner_preparation import PreparedPrivateOwnerRead
from snapper.application.trade.native_private_owner_preparation import prepare_native_private_owner
from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.native_account_identity import NativeAccountIdentityProofPayload
from snapper.core.native_account_identity import NativeAccountPublicIdentity
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.native_account_identity_store import find_native_account_identity
from snapper.data.native_account_read_store import BoundNativeAccountReadAttempt
from snapper.data.native_account_read_store import BoundNativeAccountReadOutcome
from snapper.data.native_account_read_store import NativeAccountReadBindingError
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
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
from snapper.infrastructure.exchanges.kraken_futures_key_identity import (
    NativeKeyIdentityTransportError,
)
from snapper.infrastructure.exchanges.kraken_futures_key_identity import (
    PreparedNativeKeyObservation,
)
from snapper.infrastructure.exchanges.kraken_futures_key_identity import read_futures_key_identity
from snapper.infrastructure.exchanges.kraken_futures_private_receive import (
    bind_owned_futures_private_callback,
)
from snapper.infrastructure.exchanges.kraken_rest_egress import futures_sdk_proxy_target
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyToken
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyTokenError
from tests.helpers.native_account_authority import NativeAuthorityScenario
from tests.helpers.native_account_authority import native_authority_scenario
from tests.helpers.native_account_authority import native_authority_uuid

_RUN = "_ConnectFuturesWebsocket__run"
_KEY = "synthetic-key"
_SECRET = "c3ludGhldGljLXNlY3JldA=="
_CHALLENGE = "SYNTHETIC_PREPARED_CHALLENGE"


async def test_actual_preparation_commits_same_attempt_proof_before_accounts_and_handoff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Prepared ownership follows the real network/store path and preserves the original safe root.

    Given: An actual selected User, SQLite repository and local signed key/accounts transport,
    When: The companion owns its committed attempt and completes both actual endpoint reads,
    Then: Its issued token binds the same actual safe proof/root and original selected key.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        prepared = await prepare_native_private_owner(
            source.repository, source.client, source.tracker
        )
        try:
            assert prepared.outcome.payload.result.kind == "captured"
            async with source.repository.session() as session:
                proof = await find_native_account_identity(
                    session, prepared.outcome.payload.attempt_public_id
                )
                assert proof is not None and proof.payload.matches_owner
            user = source.client._user_client
            assert user is not None
            token = prepared.require_token()
            reference = token.assert_prepared_key(
                user, source.selection, _KEY, source.clock.current
            )
            assert reference == token.require_proof_reference()
            assert reference == prepared.require_proof_reference()
            assert reference.public_id == proof.public_id
            assert reference.key_owner_root_public_id == proof.public_id
            assert reference.key_owner_root_content_digest == reference.content_digest
            assert reference.client_selection == source.selection
            assert len(source.adapter.calls) == 2 and source.adapter.proofs_at_accounts_send == [1]
            assert source.client._private_ws_client is None
        finally:
            prepared.close()


async def test_failed_accounts_read_retains_safe_identity_but_closes_unhanded_key_token(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A successful identity response is historical evidence even when its accounts read fails.

    Given: Actual successful key preflight followed by a real local accounts transport failure,
    When: The owner preparation records its failed outcome,
    Then: The safe proof remains retained while no usable prepared token or socket is handed off.
    """
    issued: list[PreparedKeyToken] = []
    actual_mint = transport._mint_prepared_private_key

    def observe_mint(
        user: User, key: str, observation: NativeAccountPublicIdentity
    ) -> PreparedKeyToken:
        """Observe actual issuance without manufacturing a token or replacing its real comparison."""
        token = actual_mint(user, key, observation)
        issued.append(token)
        return token

    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        monkeypatch.setattr(transport, "_mint_prepared_private_key", observe_mint)
        source.adapter.fail_accounts = True
        prepared = await prepare_native_private_owner(
            source.repository, source.client, source.tracker
        )
        assert prepared.outcome.payload.result.kind == "failed"
        with pytest.raises(ValueError, match="unsuccessful"):
            prepared.require_token()
        async with source.repository.session() as session:
            proof = await find_native_account_identity(
                session, prepared.outcome.payload.attempt_public_id
            )
            assert proof is not None and proof.payload.matches_owner
        user = source.client._user_client
        assert user is not None and len(issued) == 1
        with pytest.raises(PreparedKeyTokenError, match="closed"):
            issued[0].assert_prepared_key(user, source.selection, _KEY, source.clock.current)
        assert source.client._private_ws_client is None


async def test_changed_external_uid_is_retained_without_replacing_root_or_reading_accounts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Changed ownership never mints a second account namespace or a usable prepared WS capability.

    Given: One actual committed original owner root and a later key response with another UID,
    When: A new actual preparation observes that conflict,
    Then: The original root is referenced, the conflicting proof remains and accounts GET is skipped.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        first = await prepare_native_private_owner(source.repository, source.client, source.tracker)
        first.close()
        source.clock.current += timedelta(seconds=1)
        source.adapter.account_uid = native_authority_uuid(999)
        conflicting = await prepare_native_private_owner(
            source.repository, source.client, source.tracker
        )
        assert conflicting.outcome.payload.result.kind == "failed"
        with pytest.raises(ValueError, match="unsuccessful"):
            conflicting.require_token()
        async with source.repository.session() as session:
            original = await find_native_account_identity(
                session, first.outcome.payload.attempt_public_id
            )
            observed = await find_native_account_identity(
                session, conflicting.outcome.payload.attempt_public_id
            )
        assert original is not None and observed is not None and not observed.payload.matches_owner
        root = observed.payload.owner_root
        assert root is not None and root.public_id == original.public_id
        assert len(source.adapter.calls) == 3 and source.adapter.proofs_at_accounts_send == [1]


@pytest.mark.parametrize(
    "invalid",
    ("encoded_key", "user_key", "selection", "origin", "naive", "backdated", "closed"),
)
async def test_prepared_token_refuses_changed_key_route_or_dispatch_knowledge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, invalid: str
) -> None:
    """A prepared token remains tied to the exact actual key/revision and its honest dispatch clock.

    Given: An actual network-issued proof-bound token,
    When: A dispatch changes its key, selected route, clock or closed lifecycle,
    Then: The fixed refusal reveals no synthetic key and no additional HTTP request is made.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        prepared = await prepare_native_private_owner(
            source.repository, source.client, source.tracker
        )
        token = prepared.require_token()
        user = source.client._user_client
        assert user is not None
        selected = source.selection
        encoded: object = _KEY
        at = source.clock.current
        if invalid == "encoded_key":
            encoded = "SYNTHETIC_WRONG_ENCODED_KEY"
        elif invalid == "user_key":
            monkeypatch.setattr(user, "_key", "SYNTHETIC_WRONG_CURRENT_KEY")
        elif invalid == "selection":
            selected = ExecutionClientSelection.model_validate(
                selected.model_dump() | {"credential_public_id": native_authority_uuid(999)}
            )
        elif invalid == "origin":
            monkeypatch.setattr(user, "url", "https://different.invalid")
        elif invalid == "naive":
            at = at.replace(tzinfo=None)
        elif invalid == "backdated":
            at -= timedelta(microseconds=1)
        else:
            token.close()
        try:
            with pytest.raises(PreparedKeyTokenError) as caught:
                token.assert_prepared_key(user, selected, encoded, at)
            assert _KEY not in str(caught.value) and len(source.adapter.calls) == 2
        finally:
            prepared.close()


async def test_public_token_constructor_and_generic_traversal_cannot_acquire_prepared_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An ordinary shaped object cannot borrow a real proof or expose an actual issued key token.

    Given: Actual retained proof and a genuine issued token from the closed key operation,
    When: A public token is manually constructed or generic persistence targets the runtime values,
    Then: Manual proof binding refuses and ordinary serializers cannot traverse the secret-bearing token.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        prepared = await prepare_native_private_owner(
            source.repository, source.client, source.tracker
        )
        try:
            async with source.repository.session() as session:
                proof = await find_native_account_identity(
                    session, prepared.outcome.payload.attempt_public_id
                )
                assert proof is not None
            user = source.client._user_client
            assert user is not None
            forged = PreparedKeyToken(user, _KEY, proof.payload.observation)
            with pytest.raises(PreparedKeyTokenError, match="not_transport_issued"):
                forged.bind_committed_proof(proof.public_id, proof.payload)
            for value in (prepared, prepared.require_token()):
                assert _KEY not in repr(value)
                for serialize in (pickle.dumps, deepcopy, vars):
                    with pytest.raises(TypeError):
                        serialize(value)
        finally:
            prepared.close()


class _PreparedSendSocket:
    """Observe the token API on actual SDK encoded private authentication, using a local provider."""

    state = State.OPEN

    def __init__(
        self, source: NativeAuthorityScenario, token: PreparedKeyToken, mutate: bool
    ) -> None:
        """Keep actual safe proof plus only synthetic WS control material in this local socket."""
        self.source = source
        self.token = token
        self.mutate = mutate
        self.incoming: asyncio.Queue[str] = asyncio.Queue()
        self.accepted_events: list[str] = []
        self.ws: FuturesWSClient | None = None

    async def recv(self) -> str:
        """Return the actual queued original control response to the pinned SDK receive loop."""
        return await self.incoming.get()

    async def send(self, source: str) -> None:
        """Guard the actual final encoded key immediately before the local provider accepts its source."""
        document: object = json.loads(source)
        assert isinstance(document, dict)
        user = self.source.client._user_client
        assert user is not None and self.ws is not None
        self.token.assert_prepared_key(
            user, self.source.selection, document.get("api_key"), datetime.now(UTC)
        )
        event = document.get("event")
        assert isinstance(event, str)
        self.accepted_events.append(event)
        if event == "challenge":
            if self.mutate:
                self.ws._key = "SYNTHETIC_CHANGED_DURING_CHALLENGE"
            await self.incoming.put(json.dumps({"event": "challenge", "message": _CHALLENGE}))
            return
        assert event == "subscribe"
        await self.incoming.put(
            json.dumps(
                {
                    "event": "subscribed",
                    "feed": "fills",
                    "api_key": document["api_key"],
                    "original_challenge": document["original_challenge"],
                    "signed_challenge": document["signed_challenge"],
                }
            )
        )


@pytest.mark.parametrize("mutate", (False, True))
async def test_actual_sdk_encoded_auth_uses_the_actual_closed_prepared_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mutate: bool
) -> None:
    """Actual SDK challenge waits cannot substitute a mutable key for the proved expected value.

    Given: A real closed prepared key and a real SDK private subscriber with a local socket,
    When: Its actual challenge wait preserves or changes the encoded authentication key,
    Then: Only the exact prepared key reaches provider acceptance; no receive ownership is claimed.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        prepared = await prepare_native_private_owner(
            source.repository, source.client, source.tracker
        )
        socket = _PreparedSendSocket(source, prepared.require_token(), mutate)

        async def callback(message: Mapping[str, object]) -> None:
            """Stop only after the actual SDK forwards the documented successful subscription ACK."""
            assert message.get("event") == "subscribed"
            ws._conn.keep_alive = False

        ws = FuturesWSClient(key=_KEY, secret=_SECRET, callback=callback)
        socket.ws = ws
        bind_owned_futures_private_callback(ws, callback)

        @asynccontextmanager
        async def connect(*args: object, **kwargs: object) -> AsyncIterator[_PreparedSendSocket]:
            """Preserve the actual pinned receive context without any venue connection."""
            assert args[0] == "wss://futures.kraken.com/ws/v1"
            yield socket

        monkeypatch.setattr(futures_websocket, "connect", connect)
        connected = asyncio.Event()
        run = cast(Callable[[asyncio.Event], Awaitable[None]], getattr(ws._conn, _RUN))

        async def run_connector() -> None:
            """Await the actual SDK receive coroutine through its declared asynchronous interface."""
            await run(connected)

        task: asyncio.Task[None] = asyncio.create_task(run_connector())
        try:
            await connected.wait()
            if mutate:
                with pytest.raises(PreparedKeyTokenError, match="changed"):
                    await ws.subscribe(feed="fills")
                assert socket.accepted_events == ["challenge"]
            else:
                await ws.subscribe(feed="fills")
                await task
                assert socket.accepted_events == ["challenge", "subscribe"]
        finally:
            ws._conn.keep_alive = False
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await ws.close()
            session = getattr(ws, "_FuturesClient__session", None)
            if isinstance(session, Session):
                session.close()
            prepared.close()


@dataclass(frozen=True)
class _ObservedRuntime:
    """Actual closed transport objects observed without manufacturing issuance or replacing their data."""

    prepared: PreparedPrivateOwnerRead
    key: PreparedNativeKeyObservation
    owner: PreparedNativeAccountOwner
    receipt: GuardedFuturesAccountsWireReceipt
    operation: producer._ReadOperation


async def _observe_runtime(
    source: NativeAuthorityScenario, monkeypatch: pytest.MonkeyPatch
) -> _ObservedRuntime:
    """Record actual production handoffs while every key, proof, body and database operation remains real."""
    keys: list[PreparedNativeKeyObservation] = []
    owners: list[PreparedNativeAccountOwner] = []
    captures: list[tuple[producer._ReadOperation, GuardedFuturesAccountsWireReceipt]] = []
    actual_key = preflight.read_futures_prepared_key_identity
    actual_owner = producer.prove_native_account_owner
    actual_capture = producer._capture_response

    def observe_key(
        user: User, selection: ExecutionClientSelection
    ) -> PreparedNativeKeyObservation:
        """Observe real uncommitted issuance and ensure it cannot authorize a pre-commit dispatch."""
        value = actual_key(user, selection)
        with pytest.raises(PreparedKeyTokenError, match="proof_not_committed"):
            value.token.require_proof_reference()
        with pytest.raises(PreparedKeyTokenError, match="proof_not_committed"):
            value.token.assert_prepared_key(user, selection, _KEY, value.observation.observed_at)
        keys.append(value)
        return value

    async def observe_owner(
        repository: AccountingRepository,
        client: KrakenFuturesExchangeClient,
        attempt: BoundNativeAccountReadAttempt,
        identity: ExecutionEvidenceIdentity,
        expected_user: User,
    ) -> PreparedNativeAccountOwner:
        """Observe only the actual owner returned after its real database proof commit."""
        value = await actual_owner(repository, client, attempt, identity, expected_user)
        owners.append(value)
        return value

    async def observe_capture(
        repository: AccountingRepository,
        operation: producer._ReadOperation,
        receipt: GuardedFuturesAccountsWireReceipt,
    ) -> BoundNativeAccountReadOutcome:
        """Record the actual issued response without replacing the real atomic capture operation."""
        captures.append((operation, receipt))
        return await actual_capture(repository, operation, receipt)

    monkeypatch.setattr(preflight, "read_futures_prepared_key_identity", observe_key)
    monkeypatch.setattr(producer, "prove_native_account_owner", observe_owner)
    monkeypatch.setattr(producer, "_capture_response", observe_capture)
    prepared = await prepare_native_private_owner(source.repository, source.client, source.tracker)
    assert prepared.outcome.payload.result.kind == "captured"
    assert len(keys) == len(owners) == len(captures) == 1
    operation, receipt = captures[0]
    return _ObservedRuntime(prepared, keys[0], owners[0], receipt, operation)


async def test_actual_runtime_objects_refuse_relabeling_and_generic_source_traversal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Actual capabilities remain opaque and immutable after their genuine network/database handoff.

    Given: Actual selected-key and account reads, safe committed proof and their real runtime objects,
    When: Generic serialization, copying, mutation or a second owner handoff is attempted,
    Then: No credential/source is exposed and no token can be relabeled or handed out twice.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        observed = await _observe_runtime(source, monkeypatch)
        try:
            values = (
                ("_token", observed.prepared),
                ("observation", observed.key),
                ("proof", observed.owner),
                ("_receipt", observed.receipt),
                ("_user", observed.prepared.require_token()),
            )
            for field_name, value in values:
                assert _KEY not in str(value) and _KEY not in repr(value)
                with pytest.raises(AttributeError):
                    setattr(value, field_name, None)
                for serialize in (pickle.dumps, deepcopy, vars):
                    with pytest.raises(TypeError):
                        serialize(value)
            with pytest.raises(ValueError, match="token_unavailable"):
                observed.owner.require_token()
            observed.owner.close()
            wire, _ = require_guarded_accounts_wire(observed.receipt)
            with pytest.raises(NativeAccountsTransportError, match="guarded_transport_required"):
                require_guarded_accounts_wire(wire)
        finally:
            observed.prepared.close()
            observed.prepared.close()


@pytest.mark.parametrize("invalid", ("", "\ud800"))
async def test_malformed_constructor_key_never_becomes_an_issued_capability(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, invalid: str
) -> None:
    """Caller-supplied key shape errors are refused before a capability can be issued or bound.

    Given: Real safe identity from a completed closed selected-key read,
    When: A public constructor receives empty or unencodable synthetic key text,
    Then: It returns only a fixed error and cannot reconstruct an issued runtime expectation.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        prepared = await prepare_native_private_owner(
            source.repository, source.client, source.tracker
        )
        try:
            async with source.repository.session() as session:
                proof = await find_native_account_identity(
                    session, prepared.outcome.payload.attempt_public_id
                )
            user = source.client._user_client
            assert proof is not None and user is not None
            with pytest.raises(PreparedKeyTokenError, match="text_invalid"):
                PreparedKeyToken(user, invalid, proof.payload.observation)
        finally:
            prepared.close()


async def test_issued_token_cannot_rebind_another_proof_or_accept_malformed_ack_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An actual issued key expectation cannot acquire another account or recording identity.

    Given: A real proof-bound token and its original safe stored observation,
    When: A caller changes public proof identity, UID material or presents a non-string/invalid ACK key,
    Then: Exact retry is stable while every contradictory bind/dispatch is refused without another HTTP call.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        prepared = await prepare_native_private_owner(
            source.repository, source.client, source.tracker
        )
        try:
            async with source.repository.session() as session:
                proof = await find_native_account_identity(
                    session, prepared.outcome.payload.attempt_public_id
                )
            user = source.client._user_client
            assert proof is not None and user is not None
            token = prepared.require_token()
            original = token.require_proof_reference()
            assert token.bind_committed_proof(proof.public_id, proof.payload) == original
            with pytest.raises(PreparedKeyTokenError, match="bound_proof_changed"):
                token.bind_committed_proof(native_authority_uuid(9999), proof.payload)
            data = proof.payload.model_dump()
            data["observation"]["account_uid"] = native_authority_uuid(9998)
            changed = NativeAccountIdentityProofPayload.model_validate(data)
            with pytest.raises(PreparedKeyTokenError, match="bound_proof_mismatch"):
                token.bind_committed_proof(proof.public_id, changed)
            for malformed in (None, "\ud800"):
                with pytest.raises(PreparedKeyTokenError, match="actual_client_or_key_changed"):
                    token.assert_prepared_key(
                        user, source.selection, malformed, source.clock.current
                    )
            assert len(source.adapter.calls) == 2
        finally:
            prepared.close()


async def test_failed_preparation_close_is_idempotent_without_a_handed_token(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Failed accounts source cannot manufacture a runtime token through cleanup or retry.

    Given: A real successful key preflight followed by an actual failed local accounts read,
    When: The failed handoff is closed repeatedly,
    Then: No token appears and its immutable failed outcome remains unchanged.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        source.adapter.fail_accounts = True
        prepared = await prepare_native_private_owner(
            source.repository, source.client, source.tracker
        )
        assert prepared.outcome.payload.result.kind == "failed"
        prepared.close()
        prepared.close()
        with pytest.raises(ValueError, match="unsuccessful"):
            prepared.require_proof_reference()


async def test_two_genuine_reads_cannot_swap_their_actual_capture_operations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Same client/account identity cannot make the response of another committed read interchangeable.

    Given: Two genuine same-owner signed HTTP reads with distinct actual committed attempts,
    When: The second issued response is supplied to the first operation's capture boundary,
    Then: It refuses the cross-attempt source before changing either retained original outcome.
    """
    captures: list[tuple[producer._ReadOperation, GuardedFuturesAccountsWireReceipt]] = []
    actual = producer._capture_response

    async def observe(
        repository: AccountingRepository,
        operation: producer._ReadOperation,
        receipt: GuardedFuturesAccountsWireReceipt,
    ) -> BoundNativeAccountReadOutcome:
        """Observe both real operation/response pairs while retaining the original capture implementation."""
        captures.append((operation, receipt))
        return await actual(repository, operation, receipt)

    monkeypatch.setattr(producer, "_capture_response", observe)
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        first = await prepare_native_private_owner(source.repository, source.client, source.tracker)
        second = await prepare_native_private_owner(
            source.repository, source.client, source.tracker
        )
        try:
            assert len(captures) == 2
            assert (
                first.outcome.payload.attempt_public_id != second.outcome.payload.attempt_public_id
            )
            with pytest.raises(
                NativeAccountReadBindingError, match="actual_transport_attempt_mismatch"
            ):
                await actual(source.repository, captures[0][0], captures[1][1])
            assert len(source.adapter.calls) == 4
        finally:
            first.close()
            second.close()


async def test_tightened_current_source_bound_refuses_new_capture_without_rewriting_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Original receipt issuance cannot bypass the capture boundary's independently applied privacy bound.

    Given: Actual issued source and its committed outcome retained for an uncertain-ack retry,
    When: The local body-inspection bound tightens before that retry reaches raw encoding,
    Then: New capture refuses while the original immutable source outcome remains readable unchanged.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        observed = await _observe_runtime(source, monkeypatch)
        try:
            monkeypatch.setattr(privacy, "_MAX_BODY_BYTES", 1)
            with pytest.raises(NativeAccountReadBindingError, match="unclassified_state_body"):
                await producer._capture_response(
                    source.repository, observed.operation, observed.receipt
                )
            async with source.repository.session() as session:
                actual = await find_native_account_identity(
                    session, observed.operation.attempt.public_id
                )
            assert actual is not None
            assert observed.prepared.outcome.payload.result.kind == "captured"
            assert len(source.adapter.calls) == 2
        finally:
            observed.prepared.close()


async def _read_accounts(
    source: NativeAuthorityScenario, prepared: PreparedPrivateOwnerRead
) -> GuardedFuturesAccountsWireReceipt:
    """Use the real owned dispatcher, selected SDK and actual proof-bound token for one local read."""
    user = source.client._user_client
    assert user is not None
    return await source.client._dispatch_routed_rest(
        operation="test_guarded_original_accounts_boundary",
        kind="private_idempotent_read",
        target=futures_sdk_proxy_target(user),
        sync_call=lambda: read_futures_prepared_accounts_wire(
            user, source.selection, prepared.require_token()
        ),
    )


@pytest.mark.parametrize(
    "fault", ("method", "url", "history", "session_before_send", "session_during_body")
)
async def test_actual_guarded_accounts_rechecks_preparation_response_and_session_ownership(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    """A real token cannot excuse an altered request, response route or selected physical session.

    Given: A completed genuine preparation with the original selected SDK and response adapter,
    When: The actual signing, Session authentication, response or body boundary changes its route/session,
    Then: Guarded capture refuses before returning an authoritative receipt and closes real responses.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        prepared = await prepare_native_private_owner(
            source.repository, source.client, source.tracker
        )
        user = source.client._user_client
        assert user is not None
        session = getattr(user, "_FuturesClient__session", None)
        assert isinstance(session, Session)
        replacement = Session()
        try:
            _inject_transport_fault(user, session, replacement, monkeypatch, fault)
            with pytest.raises(NativeAccountsTransportError):
                await _read_accounts(source, prepared)
            assert all(response.raw.closed for response in source.adapter.responses)
        finally:
            monkeypatch.setattr(user, "_FuturesClient__session", session)
            replacement.close()
            prepared.close()


def _inject_transport_fault(
    user: User,
    session: Session,
    replacement: Session,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    """Change only one actual external SDK/Requests boundary without replacing a proof or guard."""
    if fault == "method":
        actual_prepare = user._prepare_request

        def change_method(**kwargs: Any) -> Any:
            """Run genuine SDK signing before changing its claimed method at the external boundary."""
            values = actual_prepare(**kwargs)
            return "POST", *values[1:]

        monkeypatch.setattr(user, "_prepare_request", change_method)
    elif fault == "session_before_send":

        class ChangeSession(AuthBase):
            """Change the actual SDK session after initial capture and during real auth preparation."""

            def __call__(self, request: PreparedRequest) -> PreparedRequest:
                """Preserve request bytes while replacing the SDK's selected session pointer."""
                monkeypatch.setattr(user, "_FuturesClient__session", replacement)
                return request

        monkeypatch.setattr(session, "auth", ChangeSession())
    elif fault == "session_during_body":
        actual_chunks = Response.iter_content

        def change_after_body(
            response: Response, chunk_size: int = 1, decode_unicode: bool = False
        ) -> Iterator[bytes]:
            """Yield all original entity bytes before the actual SDK session changes at body completion."""
            for chunk in actual_chunks(response, chunk_size=chunk_size, decode_unicode=False):
                assert isinstance(chunk, bytes)
                yield chunk
            monkeypatch.setattr(user, "_FuturesClient__session", replacement)

        monkeypatch.setattr(Response, "iter_content", change_after_body)
    else:
        actual_send = session.send

        def change_response(request: PreparedRequest, **kwargs: object) -> Response:
            """Complete genuine local HTTP before corrupting only its response route/history metadata."""
            response = actual_send(request, **kwargs)
            if fault == "url":
                response.url = "https://different.invalid/accounts"
            else:
                response.history = [Response()]
            return response

        monkeypatch.setattr(session, "send", change_response)


async def test_unknown_session_return_cannot_be_mistaken_for_an_original_http_response(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An external Session extension returning an unknown object cannot manufacture response provenance.

    Given: An actual issued token with its selected original SDK/session,
    When: The external send boundary supplies an unknown object rather than Requests.Response,
    Then: The fixed refusal yields no receipt and performs no additional provider adapter call.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        prepared = await prepare_native_private_owner(
            source.repository, source.client, source.tracker
        )
        user = source.client._user_client
        assert user is not None
        session = getattr(user, "_FuturesClient__session", None)
        assert isinstance(session, Session)

        def unknown_response(request: PreparedRequest, **kwargs: object) -> object:
            """Represent an invalid external Session protocol result without creating financial bytes."""
            return object()

        monkeypatch.setattr(session, "send", unknown_response)
        try:
            with pytest.raises(NativeAccountsTransportError, match="original_response_required"):
                await _read_accounts(source, prepared)
            assert len(source.adapter.calls) == 2
        finally:
            prepared.close()


async def test_actual_key_check_rejects_user_mutation_after_sdk_preparation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The actual final key check compares its signed header with the still-selected original User.

    Given: A real selected SDK and an AuthBase handler retaining the original signed key header,
    When: Auth preparation changes only that User's current key while preserving the request header,
    Then: The key check refuses before physical send and cannot certify a stale key expectation.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        prepared = await prepare_native_private_owner(
            source.repository, source.client, source.tracker
        )
        user = source.client._user_client
        assert user is not None
        session = getattr(user, "_FuturesClient__session", None)
        assert isinstance(session, Session)

        class ChangeUserKey(AuthBase):
            """Exercise only the actual user-key mutation after genuine SDK signing."""

            def __call__(self, request: PreparedRequest) -> PreparedRequest:
                """Leave the signed header untouched while changing the actual selected key."""
                monkeypatch.setattr(user, "_key", "SYNTHETIC_LATER_KEY")
                return request

        monkeypatch.setattr(session, "auth", ChangeUserKey())
        try:
            with pytest.raises(NativeKeyIdentityTransportError):
                await source.client._dispatch_routed_rest(
                    operation="test_final_key_user_drift",
                    kind="private_idempotent_read",
                    target=futures_sdk_proxy_target(user),
                    sync_call=lambda: read_futures_key_identity(user, source.selection),
                )
            assert len(source.adapter.calls) == 2
        finally:
            prepared.close()
