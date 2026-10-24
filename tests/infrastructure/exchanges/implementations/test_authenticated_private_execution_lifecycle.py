"""Actual installed Futures client callback to raw, hold and ownership without venue operations."""

import asyncio
import base64
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any
from typing import Literal
from typing import cast

import pytest
from kraken.futures import FuturesWSClient
from kraken.futures.websocket import ConnectFuturesWebsocket
from sqlalchemy import func
from sqlalchemy import select

import snapper.infrastructure.exchanges.implementations.kraken_futures as client_module
import snapper.infrastructure.exchanges.kraken_futures_authenticated_socket as bridge
from snapper.application.trade.native_private_owner_preparation import prepare_native_private_owner
from snapper.application.trade.private_source_observation_ingestion import (
    AuthenticatedPrivateObservationSink,
)
from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.private_frame_ticket import require_authenticated_private_frame
from snapper.data.accounting_observation_binding import get_bound_accounting_observation
from snapper.data.models import AccountingScopeHold
from snapper.data.native_account_identity_store import find_native_account_identity
from snapper.data.private_source_ownership_store import get_bound_private_source_ownership
from snapper.infrastructure.exchanges.authenticated_private_shutdown import (
    PrivateShutdownUnresolvedError,
)
from snapper.infrastructure.exchanges.contracts import PrivateExecutionObservation
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyTokenError
from tests.helpers.native_account_authority import NativeAuthorityScenario
from tests.helpers.native_account_authority import native_authority_scenario
from tests.helpers.native_account_authority import native_authority_uuid
from tests.infrastructure.exchanges.test_authenticated_private_socket import _BridgeScenario
from tests.infrastructure.exchanges.test_authenticated_private_socket import _scenario

_FILLS = (
    '{"feed":"fills","account":"DemoUser","fills":['
    '{"fill_id":"closed-client-1","qty":0.100000000000000001},'
    '{"fill_id":"closed-client-2","qty":2.000000000000000003}]}'
)
_LOG = (
    '{"feed":"account_log","new_entry":{"id":17,"booking_uid":"closed-cash-17",'
    '"date":"2026-10-23T15:00:00Z","asset":"USD","collateral":"USD",'
    '"margin_account":"flex","info":"transfer","old_balance":0,'
    '"new_balance":100.000000000000000001}}'
)
_BACKOFF = "_ConnectFuturesWebsocket__get_reconnect_wait"


class _InstalledCaptures:
    """Observe the actual installed frame factory and unchanged in-session raw/hold/sidecar writer."""

    def __init__(
        self,
        source: NativeAuthorityScenario,
        monkeypatch: pytest.MonkeyPatch,
        mode: str,
        feed: Literal["fills", "account_log"],
    ) -> None:
        """Keep only local fixture barriers and original source observations."""
        self.source = source
        self.monkeypatch = monkeypatch
        self.mode = mode
        self.expected_count = 2 if feed == "fills" else 1
        self.wire = (_FILLS if feed == "fills" else _LOG).encode()
        self.sink = AuthenticatedPrivateObservationSink(
            source.repository,
            source.client.execution_client_selection,
            lambda: None,
            source.tracker,
        )
        self.raw_ids: list[str] = []
        self.bindings: list[tuple[object | None, bytes | None]] = []
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.done = asyncio.Event()
        self.self_close_errors: list[str] = []

    def bind_frame(
        self, ticket: object | None, wire: bytes | None
    ) -> Callable[[PrivateExecutionObservation], Awaitable[str | None]]:
        """Wrap the genuine bound writer without replacing its ticket or database path."""
        self.bindings.append((ticket, wire))
        original = self.sink.bind_frame(ticket, wire)

        async def capture(source: PrivateExecutionObservation) -> str | None:
            """Keep original raw/hold/sidecar capture behind observable actual callback barriers."""
            self.entered.set()
            if self.mode == "blocked":
                while not self.release.is_set():
                    try:
                        await self.release.wait()
                    except asyncio.CancelledError:
                        continue
            if self.mode in {"self_close", "self_close_child"} and not self.raw_ids:
                actual = self.source.client._private_ws_client
                assert actual is not None
                try:
                    if self.mode == "self_close_child":
                        await asyncio.create_task(actual.close())
                    else:
                        await actual.close()
                except PrivateShutdownUnresolvedError as error:
                    self.self_close_errors.append(str(error))
            raw_id = await original(source)
            self.raw_ids.append(raw_id)
            if self.mode == "rotate" and len(self.raw_ids) == 1:
                changed = ExecutionClientSelection.model_validate(
                    self.source.selection.model_dump(mode="python")
                    | {"credential_public_id": native_authority_uuid(998)}
                )
                self.monkeypatch.setattr(
                    self.source.client, "execution_client_selection", lambda: changed
                )
            if len(self.raw_ids) == self.expected_count:
                actual = self.source.client._private_ws_client
                assert actual is not None
                actual._conn.keep_alive = False
                self.done.set()
            return raw_id

        return capture


@dataclass
class _InstalledRuntime:
    """Actual class-owned SDK instance plus its original provider and durable capture observations."""

    source: NativeAuthorityScenario
    provider: _BridgeScenario
    captures: _InstalledCaptures


@asynccontextmanager
async def _installed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    feed: Literal["fills", "account_log"] = "fills",
) -> AsyncIterator[_InstalledRuntime]:
    """Use the existing real HTTP/provider fixture while the actual client owns fresh SDK construction."""
    wire = _FILLS if feed == "fills" else _LOG
    async with (
        native_authority_scenario(tmp_path, monkeypatch) as source,
        _scenario(source, monkeypatch, "valid", feed, wire) as provider,
    ):
        await provider.ws.close()
        source.client._private_ws_client = None
        provider.prepared = await prepare_native_private_owner(
            source.repository, source.client, source.tracker
        )
        async with source.repository.session() as session:
            proof = await find_native_account_identity(
                session, provider.prepared.outcome.payload.attempt_public_id
            )
        assert proof is not None and proof.payload.matches_owner
        provider.proof_public_id = proof.public_id
        actual_constructor = client_module.FuturesWSClient

        def construct_actual(*args: Any, **kwargs: Any) -> FuturesWSClient:
            """Observe the actual constructor result without changing its installed application callback."""
            actual = actual_constructor(*args, **kwargs)
            provider.ws = actual
            return actual

        def no_provider_backoff(*args: object, **kwargs: object) -> float:
            """Keep only the closed local provider immediately available to the real SDK supervisor."""
            return 0.0

        monkeypatch.setattr(client_module, "FuturesWSClient", construct_actual)
        monkeypatch.setattr(ConnectFuturesWebsocket, _BACKOFF, no_provider_backoff)
        captures = _InstalledCaptures(source, monkeypatch, mode, feed)
        source.client.set_private_frame_observation_sink(captures.bind_frame)
        source.client.bind_prepared_private_owner(provider.prepared.require_token())
        await source.client._ensure_private_ws_connected()
        provider.task = cast(asyncio.Task[None], provider.ws._conn.task)
        try:
            yield _InstalledRuntime(source, provider, captures)
        finally:
            captures.release.set()
            await source.client.disconnect()


async def _assert_actual_committed_children(runtime: _InstalledRuntime) -> None:
    """Rebind actual immutable raw/hold/sidecar records after the installed physical callback."""
    selected = runtime.source.selection
    captures = runtime.captures
    assert len(captures.bindings) == 1
    ticket, wire = captures.bindings[0]
    assert ticket is not None and wire == captures.wire
    original = require_authenticated_private_frame(ticket, captures.wire)
    frames: set[str] = set()
    async with runtime.source.repository.session() as session:
        for raw_id in captures.raw_ids:
            raw = await get_bound_accounting_observation(session, raw_id)
            owned = await get_bound_private_source_ownership(session, raw_id, datetime.now(UTC))
            assert raw.scope.wallet_public_id == selected.wallet_public_id
            assert raw.credential_public_id == selected.credential_public_id
            assert base64.b64decode(raw.material) == captures.wire
            assert owned.account_uid == runtime.source.adapter.account_uid
            assert owned.payload.frame_context == original
            frames.add(owned.payload.frame_context.frame_receive_public_id)
        held = await session.scalar(
            select(func.count())
            .select_from(AccountingScopeHold)
            .where(AccountingScopeHold.raw_receipt_public_id.in_(captures.raw_ids))
        )
    assert held == captures.expected_count
    assert len(frames) == 1
    assert not runtime.source.client._execution_queue.qsize()


@pytest.mark.parametrize("feed", ("fills", "account_log"))
async def test_actual_client_installs_owned_private_callback_and_atomic_child_capture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    feed: Literal["fills", "account_log"],
) -> None:
    """Exercise actual class construction, SDK socket authentication and durable per-child ownership.

    Given: Genuine signed HTTP ownership and an installed original-frame sink before private startup.
    When: The actual client constructs its SDK and receives a real local fills or account-log frame.
    Then: Its physical callback commits raw, hold and sidecar with exact bytes and original ownership.
    """
    async with _installed(tmp_path, monkeypatch, "valid", feed) as runtime:
        assert runtime.source.client._private_ws_client is runtime.provider.ws
        assert runtime.source.client._prepared_private_source_token is None
        await runtime.provider.ws.subscribe(feed=feed)
        await runtime.captures.done.wait()
        await _assert_actual_committed_children(runtime)
        with pytest.raises(RuntimeError, match="before socket startup"):
            runtime.source.client.set_private_frame_observation_sink(runtime.captures.bind_frame)
        with pytest.raises(RuntimeError, match="fresh client"):
            runtime.source.client.bind_prepared_private_owner(
                runtime.provider.prepared.require_token()
            )


async def test_actual_client_keeps_original_frame_selection_across_child_storage_awaits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A new current selection cannot relabel later children of one original authenticated frame.

    Given: The actual installed callback receives two children from one original physical frame.
    When: The actual current-client provider changes after the first child's real database commit.
    Then: Both raw receipts and sidecars keep the original source selection and one receive identity.
    """
    async with _installed(tmp_path, monkeypatch, "rotate") as runtime:
        await runtime.provider.ws.subscribe(feed="fills")
        await runtime.captures.done.wait()
        assert runtime.source.client.execution_client_selection() != runtime.source.selection
        await _assert_actual_committed_children(runtime)


async def test_actual_client_double_cancel_keeps_unresolved_owned_slot_until_callback_joins(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Parent cleanup cannot clear an occupied source owner before the original callback terminates.

    Given: The actual installed callback is paused before its raw/hold/sidecar transaction.
    When: Two actual parent disconnect callers are cancelled before that callback is released.
    Then: The occupied SDK slot and original token remain until joined cleanup actually completes.
    """
    async with _installed(tmp_path, monkeypatch, "blocked") as runtime:
        client = runtime.source.client
        actual = runtime.provider.ws
        await actual.subscribe(feed="fills")
        await runtime.captures.entered.wait()
        first = asyncio.create_task(client.disconnect())
        while actual._conn.keep_alive:
            await asyncio.sleep(0)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        second = asyncio.create_task(client.disconnect())
        await asyncio.sleep(0)
        second.cancel()
        with pytest.raises(asyncio.CancelledError):
            await second
        assert client._private_ws_client is actual
        assert not bridge.authenticated_private_close_resolved(actual)
        token = runtime.provider.prepared.require_token()
        user = client._user_client
        assert user is not None
        token.assert_prepared_key(
            user, runtime.source.selection, "synthetic-key", datetime.now(UTC)
        )
        with pytest.raises(RuntimeError, match="fresh client"):
            client.bind_prepared_private_owner(token)
        runtime.captures.release.set()
        await runtime.captures.done.wait()
        await client.disconnect()
        assert client._private_ws_client is None
        assert bridge.authenticated_private_close_resolved(actual)
        with pytest.raises(PreparedKeyTokenError, match="closed"):
            token.require_proof_reference()
        await _assert_actual_committed_children(runtime)


@pytest.mark.parametrize("delegated", (False, True))
async def test_actual_client_callback_self_close_refuses_the_receive_dependency_cycle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, delegated: bool
) -> None:
    """The actual source receiver cannot wait on cleanup that must first join that same receiver.

    Given: Genuine installed per-child capture is executing inside the original SDK receive task.
    When: The first actual child attempts to close that same SDK before completing its transaction.
    Then: It receives a fixed refusal, commits normally and later external teardown joins safely.
    """
    async with _installed(
        tmp_path, monkeypatch, "self_close_child" if delegated else "self_close"
    ) as runtime:
        await runtime.provider.ws.subscribe(feed="fills")
        await runtime.captures.done.wait()
        assert runtime.captures.self_close_errors == ["private_socket_close_from_receive_task"]
        await _assert_actual_committed_children(runtime)
