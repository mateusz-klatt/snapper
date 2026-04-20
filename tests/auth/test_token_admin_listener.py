"""Tests for Day 3d-C admin-bus listener on :class:`TokenManager`.

Covers the cross-instance cache-eviction half of plan §3.6.1 Day 3
deliverable 1a: on receipt of ``admin.user_deactivated`` the token
manager walks its 30-second LRU and drops every entry whose cached
``user_public_id`` matches the deactivated user.

The ZMQ socket layer is mocked (same rationale as Day 3c's
`test_admin_listener.py`): the recv + dispatch halves are factored
into helpers (`_admin_recv_one_frame` + `_admin_dispatch_frame`) that
are individually testable without a running broker.
"""

import asyncio
from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.auth.tokens import TokenManager
from snapper.auth.tokens import _VerifyCacheEntry
from snapper.messaging.schemas.data import UserDeactivatedData


def _fresh_manager() -> TokenManager:
    """Return a freshly-initialised singleton with cleared verify cache."""
    TokenManager.clear_instance()
    TokenManager._initialized = False
    manager = TokenManager()
    manager._blacklisted_tokens.clear()
    manager._verify_cache.clear()
    return manager


def _seed_cache_entry(manager: TokenManager, token_hash: str, user_public_id: str) -> None:
    """Plant one active positive-verdict entry for ``user_public_id``."""
    now_ts = datetime.now(UTC).timestamp()
    manager._verify_cache[token_hash] = _VerifyCacheEntry(
        is_valid=True,
        user_is_active=True,
        user_public_id=user_public_id,
        expires_at_ts=now_ts + 900,
        cached_at_ts=now_ts,
    )


def _make_user_deactivated_payload(user_public_id: str) -> UserDeactivatedData:
    """Build a canonical ``UserDeactivatedData`` event payload."""
    now = datetime.now(UTC)
    return UserDeactivatedData(
        public_id=f"evt-{user_public_id}",
        timestamp=now,
        session_id="t-sid",
        sequence_id=1,
        user_public_id=user_public_id,
        deactivated_at=now,
        reason=None,
    )


class TestHandleUserDeactivated:
    """`_handle_user_deactivated` wires event → `invalidate_user_cache`."""

    def test_evicts_matching_entries_only(self) -> None:
        """Entries for the deactivated user drop; others stay."""
        manager = _fresh_manager()
        _seed_cache_entry(manager, "hash-target-1", "target-user")
        _seed_cache_entry(manager, "hash-target-2", "target-user")
        _seed_cache_entry(manager, "hash-bystander", "bystander-user")
        manager._handle_user_deactivated(_make_user_deactivated_payload("target-user"))
        assert "hash-bystander" in manager._verify_cache
        assert "hash-target-1" not in manager._verify_cache
        assert "hash-target-2" not in manager._verify_cache

    def test_unknown_user_is_noop(self) -> None:
        """Events for users with no cached entries don't error."""
        manager = _fresh_manager()
        _seed_cache_entry(manager, "hash-bystander", "bystander-user")
        manager._handle_user_deactivated(_make_user_deactivated_payload("never-cached"))
        assert "hash-bystander" in manager._verify_cache


class TestAdminDispatchFrame:
    """Dispatch routes topics → typed handlers and swallows errors."""

    @pytest.mark.asyncio
    async def test_user_deactivated_topic_dispatches_to_handler(self) -> None:
        """``admin.user_deactivated`` → ``_handle_user_deactivated``."""
        manager = _fresh_manager()
        _seed_cache_entry(manager, "hash-x", "user-x")
        payload = _make_user_deactivated_payload("user-x").to_json()
        await manager._admin_dispatch_frame("admin.user_deactivated", payload)
        assert "hash-x" not in manager._verify_cache

    @pytest.mark.asyncio
    async def test_unknown_topic_is_noop(self) -> None:
        """Unrecognised topics don't touch the cache."""
        manager = _fresh_manager()
        _seed_cache_entry(manager, "hash-stay", "stay-user")
        await manager._admin_dispatch_frame("admin.scope_revoked", "{}")
        assert "hash-stay" in manager._verify_cache

    @pytest.mark.asyncio
    async def test_handler_exception_is_swallowed(self) -> None:
        """A malformed payload logs + continues — never unwinds the loop."""
        manager = _fresh_manager()
        await manager._admin_dispatch_frame("admin.user_deactivated", "not valid json")


class TestAdminRecvOneFrame:
    """Recv helper handles transport errors + non-UTF-8 bytes."""

    @pytest.mark.asyncio
    async def test_decode_failure_returns_none_with_backoff(self) -> None:
        """Invalid UTF-8 bytes → None + backoff, loop continues."""
        manager = _fresh_manager()
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(return_value=(b"\xff\xfe\x00", b"payload"))
        with patch(
            "snapper.auth.tokens.asyncio.sleep", new=AsyncMock(return_value=None)
        ) as mock_sleep:
            result = await manager._admin_recv_one_frame(subscriber)
        assert result is None
        mock_sleep.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_recv_error_returns_none_with_backoff(self) -> None:
        """Transport errors → None + backoff; loop stays alive."""
        manager = _fresh_manager()
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=RuntimeError("broker down"))
        with patch(
            "snapper.auth.tokens.asyncio.sleep", new=AsyncMock(return_value=None)
        ) as mock_sleep:
            result = await manager._admin_recv_one_frame(subscriber)
        assert result is None
        mock_sleep.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_cancelled_error_propagates(self) -> None:
        """``CancelledError`` from stop_admin_listener unwinds cleanly."""
        manager = _fresh_manager()
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=asyncio.CancelledError())
        with pytest.raises(asyncio.CancelledError):
            await manager._admin_recv_one_frame(subscriber)


class TestStartStopAdminListener:
    """Lifecycle + idempotency + restart-after-failure."""

    @pytest.mark.asyncio
    async def test_empty_endpoint_skips_listener(self) -> None:
        """Empty XPUB address → listener NOT started (test-mode / single-instance)."""
        manager = _fresh_manager()
        await manager.start_admin_listener("")
        assert manager._admin_listen_task is None
        assert manager._admin_subscriber is None

    @pytest.mark.asyncio
    async def test_second_start_is_noop_while_listener_healthy(self) -> None:
        """Idempotency: running + not-done → second start returns immediately."""
        manager = _fresh_manager()
        loop_invocations: list[int] = []

        async def _never() -> None:
            loop_invocations.append(1)
            await asyncio.Event().wait()

        with (
            patch.object(manager, "_admin_listen_loop", side_effect=_never),
            patch("snapper.auth.tokens.zmq.asyncio.Context") as mock_ctx,
            patch("snapper.auth.tokens.ValidatedSubscriber") as mock_sub,
        ):
            mock_ctx.return_value.socket.return_value = MagicMock()
            mock_ctx.return_value.term = MagicMock()
            mock_sub.return_value = MagicMock()
            await manager.start_admin_listener("tcp://127.0.0.1:7501")
            await asyncio.sleep(0)
            first_task = manager._admin_listen_task
            await manager.start_admin_listener("tcp://127.0.0.1:7501")
            assert manager._admin_listen_task is first_task
            assert len(loop_invocations) == 1
        await manager.stop_admin_listener()

    @pytest.mark.asyncio
    async def test_start_after_task_done_reaps_and_restarts(self) -> None:
        """Dead task is reaped and replaced; kill-switch stays live."""
        manager = _fresh_manager()
        invocations: list[str] = []

        async def _finish_quickly() -> None:
            invocations.append("done")
            await asyncio.sleep(0)

        async def _never() -> None:
            invocations.append("alive")
            await asyncio.Event().wait()

        with (
            patch("snapper.auth.tokens.zmq.asyncio.Context") as mock_ctx,
            patch("snapper.auth.tokens.ValidatedSubscriber") as mock_sub,
        ):
            mock_ctx.return_value.socket.return_value = MagicMock()
            mock_ctx.return_value.term = MagicMock()
            mock_sub.return_value = MagicMock()
            with patch.object(manager, "_admin_listen_loop", side_effect=_finish_quickly):
                await manager.start_admin_listener("tcp://127.0.0.1:7501")
                dead_task = manager._admin_listen_task
                assert dead_task is not None
                await dead_task
                assert dead_task.done()
            with patch.object(manager, "_admin_listen_loop", side_effect=_never):
                await manager.start_admin_listener("tcp://127.0.0.1:7501")
                await asyncio.sleep(0)
                fresh_task = manager._admin_listen_task
                assert fresh_task is not None
                assert fresh_task is not dead_task
                assert not fresh_task.done()
        assert invocations == ["done", "alive"]
        await manager.stop_admin_listener()

    @pytest.mark.asyncio
    async def test_stop_without_start_is_idempotent(self) -> None:
        """``stop_admin_listener`` on a cold manager does not raise."""
        manager = _fresh_manager()
        await manager.stop_admin_listener()


class TestAdminListenLoop:
    """End-to-end loop behaviour: recv → dispatch → continue on None."""

    @pytest.mark.asyncio
    async def test_loop_returns_early_when_subscriber_is_none(self) -> None:
        """``_admin_listen_loop`` guards against the no-subscriber case."""
        manager = _fresh_manager()
        manager._admin_subscriber = None
        await manager._admin_listen_loop()

    @pytest.mark.asyncio
    async def test_loop_exits_cleanly_when_running_flag_flipped_false(self) -> None:
        """``stop_admin_listener`` flips the flag → loop exits on next check."""
        manager = _fresh_manager()
        subscriber = MagicMock()
        manager._admin_subscriber = subscriber
        manager._admin_running = False
        await manager._admin_listen_loop()

    @pytest.mark.asyncio
    async def test_loop_continues_past_none_frames_then_dispatches_valid(self) -> None:
        """Loop body handles both None frames (continue) and valid frames.

        Given: a subscriber that yields (a) a recv failure → None
            frame, then (b) a valid ``admin.user_deactivated`` frame,
            then (c) asyncio.CancelledError to unwind cleanly,
        When: the loop runs,
        Then: the invalid frame is skipped, the valid frame dispatches
            through ``_handle_user_deactivated`` (the seeded cache
            entry is evicted), and ``CancelledError`` propagates.
        """
        manager = _fresh_manager()
        _seed_cache_entry(manager, "hash-evicted", "target-user")
        payload = _make_user_deactivated_payload("target-user").to_json()
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(
            side_effect=[
                RuntimeError("first call fails"),
                (b"admin.user_deactivated", payload.encode("utf-8")),
                asyncio.CancelledError(),
            ]
        )
        manager._admin_subscriber = subscriber
        manager._admin_running = True
        with (
            patch("snapper.auth.tokens.asyncio.sleep", new=AsyncMock(return_value=None)),
            pytest.raises(asyncio.CancelledError),
        ):
            await manager._admin_listen_loop()
        assert "hash-evicted" not in manager._verify_cache


class TestAdminRecvHappyPath:
    """Happy path for recv helper — proven bytes → decoded tuple."""

    @pytest.mark.asyncio
    async def test_valid_bytes_decode_to_tuple(self) -> None:
        """Two valid UTF-8 byte strings decode to ``(topic, payload)``."""
        manager = _fresh_manager()
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(
            return_value=(b"admin.user_deactivated", b'{"user_public_id": "u"}')
        )
        result = await manager._admin_recv_one_frame(subscriber)
        assert result == ("admin.user_deactivated", '{"user_public_id": "u"}')


class TestAdminDispatchCancelPropagation:
    """Dispatch must re-raise `CancelledError` instead of swallowing it."""

    @pytest.mark.asyncio
    async def test_cancelled_during_handler_propagates(self) -> None:
        """Handler raising CancelledError unwinds the dispatch frame cleanly."""
        manager = _fresh_manager()
        payload = _make_user_deactivated_payload("user-x").to_json()

        def _cancelled_handler(_data: UserDeactivatedData) -> None:
            raise asyncio.CancelledError()

        with (
            patch.object(manager, "_handle_user_deactivated", side_effect=_cancelled_handler),
            pytest.raises(asyncio.CancelledError),
        ):
            await manager._admin_dispatch_frame("admin.user_deactivated", payload)
