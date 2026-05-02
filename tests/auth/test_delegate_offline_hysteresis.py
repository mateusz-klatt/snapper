"""Tests for ``WebSocketAuthManager`` Layer 1 delegate-offline hysteresis.

Covers the three new hooks the WS dispatcher calls around the
delegate-connection lifecycle:

- :meth:`WebSocketAuthManager.on_disconnect` schedules a delayed
  ``bus.delegate_offline`` publish task keyed on
  ``ai_delegates.public_id``.
- :meth:`WebSocketAuthManager.on_authenticate` cancels any pending task
  for the same delegate (flapping reconnect protection) AND bumps
  ``ai_delegates.last_seen_at``.
- :meth:`WebSocketAuthManager._delayed_offline_publish` sleeps the
  configured grace window then publishes the bus event.

All tests run with sub-second grace windows so the deferred path is
deterministic; the ZMQ socket is mocked.
"""

import asyncio
from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.websocket_auth import _BUS_DELEGATE_OFFLINE_TOPIC
from snapper.auth.websocket_auth import WebSocketAuthManager
from snapper.messaging.schemas.data import DelegateOfflineData

TEST_TIMEOUT = 5
_GRACE_SECONDS = 1
"""Sub-second window not allowed; grace must be a positive int. 1s keeps tests
fast while still exercising the deferred-publish path deterministically."""


async def _instant_sleep(_seconds: float) -> None:
    """Patched stand-in for ``asyncio.sleep`` inside the deferred-publish path.

    The monkeypatch in each test rebinds the ``sleep`` attribute on the
    global ``asyncio`` module object — the same object every importer
    references — so calling ``asyncio.sleep(0)`` here would recurse
    indefinitely. Awaiting a pre-resolved :class:`asyncio.Future` yields
    control to the event loop once and immediately resumes, which is
    the semantic the deferred-publish path actually needs.
    """
    fut: asyncio.Future[None] = asyncio.Future()
    fut.set_result(None)
    await fut


@pytest.fixture(autouse=True)
def _clear_singleton() -> Iterator[None]:
    """Reset the WebSocketAuthManager singleton between cases."""
    WebSocketAuthManager.clear_instance()
    yield
    WebSocketAuthManager.clear_instance()


def _delegate_principal(
    *,
    user_public_id: str = "user-1",
    delegate_public_id: str | None = "del-1",
) -> AuthPrincipal:
    """Build an AI_DELEGATE principal carrying ``delegate_public_id``."""
    return AuthPrincipal(
        username="delegate-x",
        role=UserRole.AI_DELEGATE,
        user_public_id=user_public_id,
        operator_public_ids=["op-1"],
        delegate_public_id=delegate_public_id,
    )


def _viewer_principal() -> AuthPrincipal:
    """Build a non-delegate principal (``delegate_public_id`` defaults to None)."""
    return AuthPrincipal(
        username="viewer-x",
        role=UserRole.VIEWER,
        user_public_id="user-2",
        operator_public_ids=["op-1"],
    )


def _mock_publisher() -> MagicMock:
    """Return a publisher whose ``send`` is async-recordable."""
    pub = MagicMock()
    pub.send = AsyncMock()
    return pub


def _mock_repo_factory() -> tuple[MagicMock, MagicMock]:
    """Return ``(repo_factory_callable, repo_mock)`` with async ``update_delegate_last_seen``."""
    repo = MagicMock()
    repo.update_delegate_last_seen = AsyncMock()
    factory = MagicMock(return_value=repo)
    return factory, repo


def _make_manager(
    *,
    publisher: MagicMock | None = None,
    repository_factory: MagicMock | None = None,
    grace_seconds: int = _GRACE_SECONDS,
) -> WebSocketAuthManager:
    """Build a fresh manager with optional wiring + sub-second grace."""
    manager = WebSocketAuthManager()
    if publisher is not None:
        manager.set_msg_publisher(publisher)
    if repository_factory is not None:
        manager.set_wiring(
            connection_manager=None, zmq_bridge=None, repository_factory=repository_factory
        )
    manager.set_delegate_offline_grace_seconds(grace_seconds)
    return manager


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_set_grace_seconds_rejects_non_positive() -> None:
    """``set_delegate_offline_grace_seconds`` raises on zero / negative input.

    Defends against flipped-sign bugs at call sites: a non-positive
    grace window would either fire instantly (=0) or never (<0), both
    silently breaking the hysteresis contract.

    Given a fresh WebSocketAuthManager,
    When set_delegate_offline_grace_seconds is invoked with 0 or -1,
    Then ValueError is raised with a "must be positive" message.
    """
    manager = WebSocketAuthManager()
    with pytest.raises(ValueError, match="must be positive"):
        manager.set_delegate_offline_grace_seconds(0)
    with pytest.raises(ValueError, match="must be positive"):
        manager.set_delegate_offline_grace_seconds(-1)


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_on_disconnect_no_op_for_non_delegate_principal() -> None:
    """A VIEWER (non-delegate) disconnect schedules nothing.

    Given a configured manager and a VIEWER principal,
    When on_disconnect is invoked,
    Then no offline task is scheduled and no publish is emitted.
    """
    publisher = _mock_publisher()
    manager = _make_manager(publisher=publisher)
    ws = MagicMock()
    await manager.on_disconnect(ws, _viewer_principal())
    assert manager._pending_offline_tasks == {}
    publisher.send.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_on_disconnect_schedules_pending_task_for_delegate() -> None:
    """Delegate disconnect creates a pending task keyed by ``delegate_public_id``.

    The task is recorded in ``_pending_offline_tasks`` and is not done
    immediately — it sleeps the grace window before publishing.

    Given a configured manager and a delegate principal,
    When on_disconnect is invoked,
    Then a not-yet-done task is registered under the delegate's public_id.
    """
    publisher = _mock_publisher()
    manager = _make_manager(publisher=publisher)
    principal = _delegate_principal()
    ws = MagicMock()
    await manager.on_disconnect(ws, principal)
    pending = manager._pending_offline_tasks["del-1"]
    assert not pending.done()
    pending.cancel()


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_on_disconnect_cancels_prior_pending_then_schedules_new() -> None:
    """A second disconnect for the same delegate cancels the prior pending task.

    Hysteresis: only the LATEST disconnect timestamp must fire, so a
    stale task from a previous flap is cancelled before the new one
    is scheduled.

    Given a manager with one pending offline task already scheduled,
    When on_disconnect is invoked again for the same delegate,
    Then the prior task is cancelled and a fresh task replaces it.
    """
    publisher = _mock_publisher()
    manager = _make_manager(publisher=publisher)
    principal = _delegate_principal()
    ws = MagicMock()
    await manager.on_disconnect(ws, principal)
    first_task = manager._pending_offline_tasks["del-1"]
    await manager.on_disconnect(ws, principal)
    second_task = manager._pending_offline_tasks["del-1"]
    assert first_task is not second_task
    assert first_task.cancelled()
    assert not second_task.done()
    second_task.cancel()


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_delayed_offline_publish_fires_after_grace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pending task publishes ``bus.delegate_offline`` once the grace elapses.

    Patches ``asyncio.sleep`` to a fast no-op so the deferred-publish
    branch runs without wall-clock waits, then awaits the scheduled
    task and asserts the publisher saw exactly one ``DelegateOfflineData``
    message on the right topic.

    Given a manager wired with a publisher and a patched zero-duration sleep,
    When on_disconnect schedules the task and the test awaits it,
    Then the publisher receives one DelegateOfflineData message on the
    bus.delegate_offline topic carrying the right user + delegate ids.
    """
    publisher = _mock_publisher()
    manager = _make_manager(publisher=publisher)

    monkeypatch.setattr("snapper.auth.websocket_auth.asyncio.sleep", _instant_sleep)
    principal = _delegate_principal()
    ws = MagicMock()
    await manager.on_disconnect(ws, principal)
    task = manager._pending_offline_tasks["del-1"]
    await task
    publisher.send.assert_awaited_once()
    args, _ = publisher.send.await_args
    topic, payload = args
    assert topic == _BUS_DELEGATE_OFFLINE_TOPIC
    assert isinstance(payload, DelegateOfflineData)
    assert payload.delegate_public_id == "del-1"
    assert payload.user_public_id == "user-1"


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_delayed_offline_publish_cancellation_skips_publish() -> None:
    """Cancelling the task before grace elapses skips the publish call.

    A flapping reconnect within the grace window cancels the pending
    task so subscribers never see a phantom-offline event.

    Given a manager with a long grace window and a scheduled task,
    When the test cancels the task before the sleep finishes,
    Then awaiting the task raises CancelledError and the publisher is never called.
    """
    publisher = _mock_publisher()
    manager = _make_manager(publisher=publisher, grace_seconds=10)
    ws = MagicMock()
    await manager.on_disconnect(ws, _delegate_principal())
    task = manager._pending_offline_tasks["del-1"]
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    publisher.send.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_delayed_offline_publish_logs_when_publisher_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing publisher degrades to a logged warning instead of raising.

    Mirrors :class:`ScopeGrantService` best-effort semantics: a
    singleton spun up before the FastAPI lifespan attached a publisher
    must still tolerate the call without crashing the asyncio task.

    Given a manager without a wired publisher and a patched zero-sleep,
    When the deferred publish path runs to completion,
    Then the task ends cleanly (done) without raising.
    """
    manager = _make_manager()

    monkeypatch.setattr("snapper.auth.websocket_auth.asyncio.sleep", _instant_sleep)
    ws = MagicMock()
    await manager.on_disconnect(ws, _delegate_principal())
    task = manager._pending_offline_tasks["del-1"]
    await task
    assert task.done()


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_delayed_offline_publish_swallows_publisher_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Transient publish failures are caught + logged, not propagated.

    A broken broker connection must not leak into the asyncio task
    graph as an unhandled exception; the column-level last_seen_at is
    the source of truth so the Layer 2 scanner remains the
    correctness backstop.

    Given a publisher whose send() raises RuntimeError,
    When the deferred publish path runs to completion,
    Then the task finishes with no exception observable on .exception().
    """
    publisher = _mock_publisher()
    publisher.send = AsyncMock(side_effect=RuntimeError("broker down"))
    manager = _make_manager(publisher=publisher)

    monkeypatch.setattr("snapper.auth.websocket_auth.asyncio.sleep", _instant_sleep)
    ws = MagicMock()
    await manager.on_disconnect(ws, _delegate_principal())
    task = manager._pending_offline_tasks["del-1"]
    await task
    assert task.exception() is None


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_on_authenticate_no_op_for_non_delegate_principal() -> None:
    """A VIEWER authenticate does not call the repository or cancel tasks.

    Given a manager wired with a repository and a VIEWER principal,
    When on_authenticate is invoked,
    Then update_delegate_last_seen is never called.
    """
    factory, repo = _mock_repo_factory()
    manager = _make_manager(repository_factory=factory)
    ws = MagicMock()
    await manager.on_authenticate(ws, _viewer_principal())
    repo.update_delegate_last_seen.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_on_authenticate_cancels_pending_offline_task() -> None:
    """A delegate reconnect cancels the prior on_disconnect task.

    Fast-path: a flapping reconnect within the grace window cancels
    the pending publish so subscribers never observe the
    phantom-offline transition. The task slot is also cleared from
    ``_pending_offline_tasks`` so a follow-up disconnect schedules
    cleanly.

    Given a manager with a pending offline task scheduled for a delegate,
    When on_authenticate is invoked for the same delegate,
    Then the pending task is cancelled, removed from the registry, and
    the publisher is never called.
    """
    publisher = _mock_publisher()
    factory, _repo = _mock_repo_factory()
    manager = _make_manager(publisher=publisher, repository_factory=factory, grace_seconds=10)
    principal = _delegate_principal()
    ws = MagicMock()
    await manager.on_disconnect(ws, principal)
    pending = manager._pending_offline_tasks["del-1"]
    assert not pending.done()
    await manager.on_authenticate(ws, principal)
    assert pending.cancelled()
    assert "del-1" not in manager._pending_offline_tasks
    publisher.send.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_on_authenticate_updates_delegate_last_seen() -> None:
    """Successful authenticate path bumps ``ai_delegates.last_seen_at``.

    The wall-clock passed to the repository is the live ``datetime.now(UTC)``
    so cross-instance Layer 2 scanners observe the freshness in their
    next tick.

    Given a manager wired with a repository and a delegate principal,
    When on_authenticate is invoked,
    Then repo.update_delegate_last_seen is awaited once with the
    delegate id and a wall-clock falling within [before, after].
    """
    factory, repo = _mock_repo_factory()
    manager = _make_manager(repository_factory=factory)
    ws = MagicMock()
    before = datetime.now(UTC)
    await manager.on_authenticate(ws, _delegate_principal())
    after = datetime.now(UTC)
    repo.update_delegate_last_seen.assert_awaited_once()
    args, _kwargs = repo.update_delegate_last_seen.await_args
    delegate_id, last_seen_at = args
    assert delegate_id == "del-1"
    assert before <= last_seen_at <= after


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_on_authenticate_logs_when_repository_missing() -> None:
    """Missing repository factory degrades the last_seen_at update to a warning.

    Mirrors :class:`ScopeGrantService` resilience: a manager spun up
    before lifespan wired the repository must still tolerate the call
    so test fixtures and dev servers without full DI graphs don't
    crash.

    Given a manager without a wired repository factory,
    When on_authenticate is invoked for a delegate principal,
    Then the call returns cleanly without raising.
    """
    publisher = _mock_publisher()
    manager = _make_manager(publisher=publisher)
    ws = MagicMock()
    await manager.on_authenticate(ws, _delegate_principal())


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_publish_payload_carries_session_and_sequence() -> None:
    """Sequence id increments per delegate-offline publish on the same topic.

    The ``_delegate_offline_tracker`` is a private :class:`SequenceTracker`
    so consumers can rely on per-topic monotonic ordering without
    depending on the broker.

    Given a manager with a wired publisher and two distinct delegate principals,
    When _publish_delegate_offline is invoked for each in order,
    Then both messages share session_id while sequence_id strictly
    increases between them.
    """
    publisher = _mock_publisher()
    manager = _make_manager(publisher=publisher)
    last_seen = datetime.now(UTC)
    await manager._publish_delegate_offline(
        user_public_id="user-a",
        delegate_public_id="del-a",
        last_seen_at=last_seen,
    )
    await manager._publish_delegate_offline(
        user_public_id="user-b",
        delegate_public_id="del-b",
        last_seen_at=last_seen,
    )
    assert publisher.send.await_count == 2
    payloads: list[Any] = [call.args[1] for call in publisher.send.await_args_list]
    assert payloads[0].delegate_public_id == "del-a"
    assert payloads[1].delegate_public_id == "del-b"
    assert payloads[0].session_id == payloads[1].session_id
    assert payloads[1].sequence_id > payloads[0].sequence_id


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_concurrent_on_disconnect_does_not_orphan_pending_task() -> None:
    """Same-delegate concurrent disconnects never orphan a pending publish task.

    Hysteresis: if call A and call B both run on_disconnect for the
    same delegate, both must serialise so only the latest scheduled
    task lives in the registry and any earlier task is cancelled.
    Without the per-delegate lock, A's `await existing` yields and B
    can install a new task that A then overwrites, leaving an orphan
    that wakes up and publishes anyway.

    Given a manager with one already-pending offline task,
    When two on_disconnect coroutines race for the same delegate via
    asyncio.gather,
    Then exactly one task remains in the registry, every prior task is
    cancelled (or done), and a single subsequent on_authenticate
    suppresses the offline publish entirely.
    """
    publisher = _mock_publisher()
    factory, _repo = _mock_repo_factory()
    manager = _make_manager(publisher=publisher, repository_factory=factory, grace_seconds=10)
    principal = _delegate_principal()
    ws = MagicMock()
    await manager.on_disconnect(ws, principal)
    await asyncio.gather(
        manager.on_disconnect(ws, principal),
        manager.on_disconnect(ws, principal),
        manager.on_disconnect(ws, principal),
    )
    assert len(manager._pending_offline_tasks) == 1
    surviving = manager._pending_offline_tasks["del-1"]
    assert not surviving.done()
    await manager.on_authenticate(ws, principal)
    assert "del-1" not in manager._pending_offline_tasks
    assert surviving.cancelled()
    publisher.send.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_cancel_pending_offline_tasks_skips_already_done(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Already-completed task in the registry is skipped, not cancelled.

    Given a manager whose pending task already ran to completion (the
    delayed publish fired and the task is done),
    When cancel_pending_offline_tasks runs,
    Then the loop sees ``task.done() is True`` for that entry and skips
    the cancel, the registry is cleared, and no exception leaks.
    """
    publisher = _mock_publisher()
    manager = _make_manager(publisher=publisher, grace_seconds=10)
    monkeypatch.setattr("snapper.auth.websocket_auth.asyncio.sleep", _instant_sleep)
    ws = MagicMock()
    await manager.on_disconnect(ws, _delegate_principal())
    task = manager._pending_offline_tasks["del-1"]
    await task
    assert task.done()
    await manager.cancel_pending_offline_tasks()
    assert manager._pending_offline_tasks == {}


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_cancel_pending_offline_tasks_drains_registry() -> None:
    """Shutdown helper cancels every in-flight delayed task and clears the dict.

    FastAPI lifespan calls this on shutdown so a closing process does
    not leave deferred-publish tasks sleeping against torn-down wiring.

    Given a manager with multiple pending tasks for distinct delegates,
    When cancel_pending_offline_tasks runs,
    Then the registry is empty, every task is done (cancelled), and the
    publisher was never called.
    """
    publisher = _mock_publisher()
    manager = _make_manager(publisher=publisher, grace_seconds=10)
    ws = MagicMock()
    await manager.on_disconnect(ws, _delegate_principal(delegate_public_id="d-1"))
    await manager.on_disconnect(ws, _delegate_principal(delegate_public_id="d-2"))
    await manager.on_disconnect(ws, _delegate_principal(delegate_public_id="d-3"))
    pending_snapshot = list(manager._pending_offline_tasks.values())
    assert len(pending_snapshot) == 3
    await manager.cancel_pending_offline_tasks()
    assert manager._pending_offline_tasks == {}
    for task in pending_snapshot:
        assert task.done()
    publisher.send.assert_not_called()
    await manager.cancel_pending_offline_tasks()
