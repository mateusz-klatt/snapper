"""Tests for the AiReviewService bus listener.

Covers :meth:`AiReviewService.start_bus_listener` /
:meth:`AiReviewService.stop_bus_listener` + the recv / dispatch
helpers that drain ``bus.delegate_offline`` (layer-1 fast-path)
and ``bus.caps_violation_after_ai_approve`` (fanout)
into the per-message handlers.

Mirrors the admin-listener test pattern used by ``WebSocketAuthManager``
+ ``TokenManager`` (`tests/auth/test_token_admin_listener.py`): the
ZMQ socket layer is mocked so the recv + dispatch halves can be
exercised without a running broker.
"""

import asyncio
from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch
from uuid import uuid7

import pytest
from loguru import logger

from snapper.application.ai_review.service import AiReviewService
from snapper.messaging.schemas.data import AiReviewDecisionData
from snapper.messaging.schemas.data import CapsViolationAfterAiApproveData
from snapper.messaging.schemas.data import DelegateOfflineData


@pytest.fixture(autouse=True)
def _clear_singleton() -> Iterator[None]:
    """Reset the AiReviewService singleton between cases."""
    AiReviewService.clear_instance()
    yield
    AiReviewService.clear_instance()


def _delegate_offline_payload() -> str:
    """Build a canonical ``DelegateOfflineData`` JSON payload."""
    now = datetime.now(UTC)
    return DelegateOfflineData(
        public_id=str(uuid7()),
        timestamp=now,
        session_id="t-sid",
        sequence_id=1,
        user_public_id="user-1",
        delegate_public_id="del-1",
        last_seen_at=now,
    ).to_json()


def _decision_payload(
    *,
    review_public_id: str = "rev-1",
    decision: str = "approve",
    new_status: str = "resolved_approved",
    resolution_mode: str = "pick_one_primary",
    dispatch_version: int = 1,
) -> str:
    """Build a canonical ``AiReviewDecisionData`` JSON payload."""
    return AiReviewDecisionData(
        public_id=str(uuid7()),
        timestamp=datetime.now(UTC),
        session_id="t-sid",
        sequence_id=1,
        review_public_id=review_public_id,
        responding_delegate_public_id="del-1",
        decision=decision,
        new_status=new_status,
        resolution_mode=resolution_mode,
        dispatch_version=dispatch_version,
    ).to_json()


def _caps_violation_payload() -> str:
    """Build a canonical ``CapsViolationAfterAiApproveData`` JSON payload."""
    now = datetime.now(UTC)
    return CapsViolationAfterAiApproveData(
        public_id=str(uuid7()),
        timestamp=now,
        session_id="t-sid",
        sequence_id=1,
        review_public_id="rev-1",
        user_public_id="user-1",
        strategy_public_id="strat-1",
        wallet_public_id="wal-1",
        instrument_public_id="inst-1",
        cap_type="max_open_orders",
        attempted=11.0,
        limit=10.0,
        dispatch_version=1,
    ).to_json()


class TestSetRepositoryFactory:
    """`set_repository_factory` injects + clears the per-tick factory seam."""

    def test_factory_can_be_set_and_cleared(self) -> None:
        """Inject a callable, observe it, then clear with None."""
        svc = AiReviewService.get_instance()
        repo_marker = MagicMock()

        def _factory() -> object:
            return repo_marker

        svc.set_repository_factory(_factory)
        assert svc._repository_factory is _factory
        svc.set_repository_factory(None)
        assert svc._repository_factory is None


class TestBusDispatchFrame:
    """`_bus_dispatch_frame` routes topics → typed handlers + swallows errors.

    The dispatch helper is the surface a bus publisher (e.g.
    TradingCapsEnforcer for caps_violation, WebSocketAuthManager for
    delegate_offline) hits at production time.
    """

    @pytest.mark.asyncio
    async def test_caps_violation_topic_dispatches_to_handler(self) -> None:
        """``bus.caps_violation_after_ai_approve`` → ``handle_caps_violation_bus_message``."""
        svc = AiReviewService.get_instance()
        with patch.object(
            svc, "handle_caps_violation_bus_message", new=AsyncMock(return_value=True)
        ) as mock_handler:
            await svc._bus_dispatch_frame(
                "bus.caps_violation_after_ai_approve", _caps_violation_payload()
            )
        mock_handler.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_ai_review_decision_topic_dispatches_to_handler(self) -> None:
        """``bus.ai_review_decision`` → ``handle_ai_review_decision_bus_message``.

        Fast-path: the listener wakes registered Futures so
        the strategy primitive's await loop bypasses the DB-poll
        interval. The dispatch must route to the synchronous handler
        without requiring repository_factory wiring (the handler is
        in-memory only).
        """
        svc = AiReviewService.get_instance()
        with patch.object(
            svc, "handle_ai_review_decision_bus_message", return_value=True
        ) as mock_handler:
            await svc._bus_dispatch_frame("bus.ai_review_decision", _decision_payload())
        mock_handler.assert_called_once()

    @pytest.mark.asyncio
    async def test_delegate_offline_dispatches_with_repo_from_factory(self) -> None:
        """``bus.delegate_offline`` → handler invoked with a fresh repo from the factory.

        The handler accepts a ``repo: Repository`` keyword; the
        listener-level dispatch must
        thread a fresh repo per dispatch so the per-tick UnitOfWork is
        bounded + does not bleed connections across handler calls.
        """
        svc = AiReviewService.get_instance()
        repo_marker = MagicMock(name="per_dispatch_repo")
        factory = MagicMock(return_value=repo_marker)
        svc.set_repository_factory(factory)
        with patch.object(
            svc, "handle_delegate_offline_bus_message", new=AsyncMock(return_value=0)
        ) as mock_handler:
            await svc._bus_dispatch_frame("bus.delegate_offline", _delegate_offline_payload())
        factory.assert_called_once_with()
        mock_handler.assert_awaited_once()
        await_args = mock_handler.await_args
        assert await_args is not None
        assert await_args.kwargs["repo"] is repo_marker

    @pytest.mark.asyncio
    async def test_delegate_offline_without_factory_logs_warning_and_skips(self) -> None:
        """Missing ``repository_factory`` -> warn + no-op (graceful degradation).

        Mirrors the ``ScopeGrantService`` best-effort contract: a
        singleton spun up before the FastAPI lifespan attached the
        factory must still tolerate the call without crashing the
        listener loop.
        """
        svc = AiReviewService.get_instance()
        svc.set_repository_factory(None)
        with patch.object(
            svc, "handle_delegate_offline_bus_message", new=AsyncMock(return_value=0)
        ) as mock_handler:
            await svc._bus_dispatch_frame("bus.delegate_offline", _delegate_offline_payload())
        mock_handler.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unknown_topic_is_noop(self) -> None:
        """Topics outside the AI-review subscription set are ignored."""
        svc = AiReviewService.get_instance()
        with (
            patch.object(svc, "handle_caps_violation_bus_message", new=AsyncMock()) as caps_mock,
            patch.object(svc, "handle_delegate_offline_bus_message", new=AsyncMock()) as off_mock,
        ):
            await svc._bus_dispatch_frame("admin.scope_revoked", "{}")
        caps_mock.assert_not_awaited()
        off_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_handler_exception_is_swallowed(self) -> None:
        """A malformed payload logs + continues — never unwinds the loop."""
        svc = AiReviewService.get_instance()
        await svc._bus_dispatch_frame("bus.caps_violation_after_ai_approve", "not valid json")

    @pytest.mark.asyncio
    async def test_cancelled_error_propagates_from_handler(self) -> None:
        """``CancelledError`` from the handler must unwind the loop cleanly."""
        svc = AiReviewService.get_instance()
        caps_violation_frame = _caps_violation_payload()
        with (
            patch.object(
                svc,
                "handle_caps_violation_bus_message",
                new=AsyncMock(side_effect=asyncio.CancelledError()),
            ),
            pytest.raises(asyncio.CancelledError),
        ):
            await svc._bus_dispatch_frame(
                "bus.caps_violation_after_ai_approve", caps_violation_frame
            )


class TestBusRecvOneFrame:
    """Recv helper handles transport errors + non-UTF-8 bytes."""

    @pytest.mark.asyncio
    async def test_decode_failure_returns_none_with_backoff(self) -> None:
        """Invalid UTF-8 bytes → None + backoff, loop continues."""
        svc = AiReviewService.get_instance()
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(return_value=(b"\xff\xfe\x00", b"payload"))
        with patch(
            "snapper.application.ai_review.service.asyncio.sleep",
            new=AsyncMock(return_value=None),
        ) as mock_sleep:
            result = await svc._bus_recv_one_frame(subscriber)
        assert result is None
        mock_sleep.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_recv_error_returns_none_with_backoff(self) -> None:
        """Transport errors → None + backoff; loop stays alive."""
        svc = AiReviewService.get_instance()
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=RuntimeError("broker down"))
        with patch(
            "snapper.application.ai_review.service.asyncio.sleep",
            new=AsyncMock(return_value=None),
        ) as mock_sleep:
            result = await svc._bus_recv_one_frame(subscriber)
        assert result is None
        mock_sleep.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_cancelled_error_propagates(self) -> None:
        """``CancelledError`` from stop_bus_listener unwinds cleanly."""
        svc = AiReviewService.get_instance()
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=asyncio.CancelledError())
        with pytest.raises(asyncio.CancelledError):
            await svc._bus_recv_one_frame(subscriber)

    @pytest.mark.asyncio
    async def test_valid_bytes_decode_to_tuple(self) -> None:
        """Happy path: bytes pair decodes to a (topic, payload) tuple."""
        svc = AiReviewService.get_instance()
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(
            return_value=(b"bus.caps_violation_after_ai_approve", b'{"k": "v"}')
        )
        result = await svc._bus_recv_one_frame(subscriber)
        assert result == ("bus.caps_violation_after_ai_approve", '{"k": "v"}')


class TestStartStopBusListener:
    """Lifecycle + idempotency + restart-after-failure."""

    @pytest.mark.asyncio
    async def test_empty_endpoint_skips_listener(self) -> None:
        """Empty XPUB address → listener NOT started (test-mode / API-only)."""
        svc = AiReviewService.get_instance()
        await svc.start_bus_listener("")
        assert svc._bus_listen_task is None
        assert svc._bus_subscriber is None

    @pytest.mark.asyncio
    async def test_second_start_is_noop_while_listener_healthy(self) -> None:
        """Idempotency: running + not-done → second start returns immediately."""
        svc = AiReviewService.get_instance()
        loop_invocations: list[int] = []

        async def _never() -> None:
            loop_invocations.append(1)
            await asyncio.Event().wait()

        with (
            patch.object(svc, "_bus_listen_loop", side_effect=_never),
            patch("snapper.application.ai_review.service.zmq.asyncio.Context") as mock_ctx,
            patch("snapper.application.ai_review.service.ValidatedSubscriber") as mock_sub,
        ):
            mock_ctx.return_value.socket.return_value = MagicMock()
            mock_ctx.return_value.term = MagicMock()
            mock_sub.return_value = MagicMock()
            await svc.start_bus_listener("tcp://127.0.0.1:7501")
            await asyncio.sleep(0)
            first_task = svc._bus_listen_task
            await svc.start_bus_listener("tcp://127.0.0.1:7501")
            assert svc._bus_listen_task is first_task
            assert len(loop_invocations) == 1
        await svc.stop_bus_listener()

    @pytest.mark.asyncio
    async def test_start_after_task_done_reaps_and_restarts(self) -> None:
        """Dead task is reaped and replaced — fanout stays live across single failures."""
        svc = AiReviewService.get_instance()
        invocations: list[str] = []

        async def _finish_quickly() -> None:
            invocations.append("done")
            await asyncio.sleep(0)

        async def _never() -> None:
            invocations.append("alive")
            await asyncio.Event().wait()

        with (
            patch("snapper.application.ai_review.service.zmq.asyncio.Context") as mock_ctx,
            patch("snapper.application.ai_review.service.ValidatedSubscriber") as mock_sub,
        ):
            mock_ctx.return_value.socket.return_value = MagicMock()
            mock_ctx.return_value.term = MagicMock()
            mock_sub.return_value = MagicMock()
            with patch.object(svc, "_bus_listen_loop", side_effect=_finish_quickly):
                await svc.start_bus_listener("tcp://127.0.0.1:7501")
                dead_task = svc._bus_listen_task
                assert dead_task is not None
                await dead_task
                assert dead_task.done()
            with patch.object(svc, "_bus_listen_loop", side_effect=_never):
                await svc.start_bus_listener("tcp://127.0.0.1:7501")
                await asyncio.sleep(0)
                fresh_task = svc._bus_listen_task
                assert fresh_task is not None
                assert fresh_task is not dead_task
                assert not fresh_task.done()
        assert invocations == ["done", "alive"]
        await svc.stop_bus_listener()

    @pytest.mark.asyncio
    async def test_subscribes_to_three_topics(self) -> None:
        """Start subscribes to delegate_offline + caps_violation + ai_review_decision."""
        svc = AiReviewService.get_instance()

        async def _never() -> None:
            await asyncio.Event().wait()

        with (
            patch.object(svc, "_bus_listen_loop", side_effect=_never),
            patch("snapper.application.ai_review.service.zmq.asyncio.Context") as mock_ctx,
            patch("snapper.application.ai_review.service.ValidatedSubscriber") as mock_sub,
        ):
            mock_ctx.return_value.socket.return_value = MagicMock()
            mock_ctx.return_value.term = MagicMock()
            sub_instance = MagicMock()
            mock_sub.return_value = sub_instance
            await svc.start_bus_listener("tcp://127.0.0.1:7501")
            subscribed = {call.args[0] for call in sub_instance.subscribe.call_args_list}
        assert subscribed == {
            "bus.delegate_offline",
            "bus.caps_violation_after_ai_approve",
            "bus.ai_review_decision",
        }
        await svc.stop_bus_listener()

    @pytest.mark.asyncio
    async def test_stop_without_start_is_idempotent(self) -> None:
        """``stop_bus_listener`` on a cold service does not raise."""
        svc = AiReviewService.get_instance()
        await svc.stop_bus_listener()

    @pytest.mark.asyncio
    async def test_topics_param_restricts_subscription_to_decision_only(self) -> None:
        """Subprocess strategies pass topics=(decision,) only.

        The subprocess listener wired by ``snapper.server.process_runner`` must
        subscribe ONLY to ``bus.ai_review_decision`` so the strategy primitive's
        in-subprocess Future fast-path can resolve. The other 2 topics
        (``bus.delegate_offline`` + ``bus.caps_violation_after_ai_approve``)
        belong exclusively to the FastAPI server's lifespan-wired listener;
        a subprocess that subscribed to them would either skip-with-warning
        on every event (delegate_offline needs a repository factory) OR
        N-duplicate the external WS frame (caps_violation re-publishes
        unconditionally when ShardOwnership is unwired in the subprocess).
        """
        svc = AiReviewService.get_instance()

        async def _never() -> None:
            await asyncio.Event().wait()

        with (
            patch.object(svc, "_bus_listen_loop", side_effect=_never),
            patch("snapper.application.ai_review.service.zmq.asyncio.Context") as mock_ctx,
            patch("snapper.application.ai_review.service.ValidatedSubscriber") as mock_sub,
        ):
            mock_ctx.return_value.socket.return_value = MagicMock()
            mock_ctx.return_value.term = MagicMock()
            sub_instance = MagicMock()
            mock_sub.return_value = sub_instance
            await svc.start_bus_listener(
                "tcp://127.0.0.1:7501",
                topics=("bus.ai_review_decision",),
            )
            subscribed = {call.args[0] for call in sub_instance.subscribe.call_args_list}
        assert subscribed == {"bus.ai_review_decision"}
        assert svc._bus_subscribed_topics == ("bus.ai_review_decision",)
        await svc.stop_bus_listener()

    @pytest.mark.asyncio
    async def test_topic_widening_logs_warning_and_does_not_re_subscribe(self) -> None:
        """Restricted-then-full second start logs a warning + leaves restricted in place.

        Topic widening between calls is NOT supported in place — the second
        ``start_bus_listener`` call observes the running task + returns
        without re-subscribing (the existing idempotency guard). The new
        topics simply do not get added. Callers that need to widen MUST
        explicitly :meth:`stop_bus_listener` first. We log a warning when
        the second call's topic set is wider than the running set so the
        misuse is visible in operations.

        The warning assertion uses a loguru sink (``caplog`` does not
        intercept loguru) so a future refactor that silently drops the
        warning is caught by CI.
        """
        svc = AiReviewService.get_instance()
        warnings: list[str] = []

        def _sink(message: object) -> None:
            warnings.append(str(message))

        handler_id = logger.add(_sink, level="WARNING")

        async def _never() -> None:
            await asyncio.Event().wait()

        try:
            with (
                patch.object(svc, "_bus_listen_loop", side_effect=_never),
                patch("snapper.application.ai_review.service.zmq.asyncio.Context") as mock_ctx,
                patch("snapper.application.ai_review.service.ValidatedSubscriber") as mock_sub,
            ):
                mock_ctx.return_value.socket.return_value = MagicMock()
                mock_ctx.return_value.term = MagicMock()
                sub_instance = MagicMock()
                mock_sub.return_value = sub_instance
                await svc.start_bus_listener(
                    "tcp://127.0.0.1:7501",
                    topics=("bus.ai_review_decision",),
                )
                await asyncio.sleep(0)
                initial_subscribe_count = sub_instance.subscribe.call_count
                await svc.start_bus_listener("tcp://127.0.0.1:7501")
                assert sub_instance.subscribe.call_count == initial_subscribe_count
            assert svc._bus_subscribed_topics == ("bus.ai_review_decision",)
            assert any("widening requires explicit stop_bus_listener" in msg for msg in warnings), (
                f"expected topic-widening warning to fire on the second start_bus_listener call; "
                f"captured warnings: {warnings!r}"
            )
        finally:
            logger.remove(handler_id)
            await svc.stop_bus_listener()

    @pytest.mark.asyncio
    async def test_stop_clears_subscribed_topics_so_restart_can_widen(self) -> None:
        """After ``stop_bus_listener``, ``_bus_subscribed_topics`` resets to empty.

        Closes the topic-widening footgun: a caller that wants to widen the
        topic set MUST stop first. After stop the tracking attribute is
        empty so a follow-up start cleanly applies the new (wider) topic
        set.
        """
        svc = AiReviewService.get_instance()

        async def _never() -> None:
            await asyncio.Event().wait()

        with (
            patch.object(svc, "_bus_listen_loop", side_effect=_never),
            patch("snapper.application.ai_review.service.zmq.asyncio.Context") as mock_ctx,
            patch("snapper.application.ai_review.service.ValidatedSubscriber") as mock_sub,
        ):
            mock_ctx.return_value.socket.return_value = MagicMock()
            mock_ctx.return_value.term = MagicMock()
            mock_sub.return_value = MagicMock()
            await svc.start_bus_listener(
                "tcp://127.0.0.1:7501",
                topics=("bus.ai_review_decision",),
            )
            assert svc._bus_subscribed_topics == ("bus.ai_review_decision",)
            await svc.stop_bus_listener()
        assert svc._bus_subscribed_topics == ()

    @pytest.mark.asyncio
    async def test_default_topics_param_subscribes_to_all_three(self) -> None:
        """``topics=None`` (default) preserves the 3-topic FastAPI lifespan behaviour.

        Regression pin: the FastAPI lifespan continues to call
        ``start_bus_listener(zmq_broker_xpub)`` without a ``topics`` argument
        and must keep subscribing to the full 3-topic set. The
        ``test_subscribes_to_three_topics`` covers the same invariant
        through a different path; this test pins it via the explicit
        ``None`` default.
        """
        svc = AiReviewService.get_instance()

        async def _never() -> None:
            await asyncio.Event().wait()

        with (
            patch.object(svc, "_bus_listen_loop", side_effect=_never),
            patch("snapper.application.ai_review.service.zmq.asyncio.Context") as mock_ctx,
            patch("snapper.application.ai_review.service.ValidatedSubscriber") as mock_sub,
        ):
            mock_ctx.return_value.socket.return_value = MagicMock()
            mock_ctx.return_value.term = MagicMock()
            sub_instance = MagicMock()
            mock_sub.return_value = sub_instance
            await svc.start_bus_listener("tcp://127.0.0.1:7501", topics=None)
            subscribed = {call.args[0] for call in sub_instance.subscribe.call_args_list}
        assert subscribed == {
            "bus.delegate_offline",
            "bus.caps_violation_after_ai_approve",
            "bus.ai_review_decision",
        }
        assert set(svc._bus_subscribed_topics) == subscribed
        await svc.stop_bus_listener()


class TestBusListenLoop:
    """End-to-end loop behaviour: recv → dispatch → continue on None."""

    @pytest.mark.asyncio
    async def test_loop_returns_early_when_subscriber_is_none(self) -> None:
        """``_bus_listen_loop`` guards against the no-subscriber case."""
        svc = AiReviewService.get_instance()
        svc._bus_subscriber = None
        await svc._bus_listen_loop()

    @pytest.mark.asyncio
    async def test_loop_exits_cleanly_when_running_flag_flipped_false(self) -> None:
        """``stop_bus_listener`` flips the flag → loop exits on next check."""
        svc = AiReviewService.get_instance()
        subscriber = MagicMock()
        svc._bus_subscriber = subscriber
        svc._bus_running = False
        await svc._bus_listen_loop()

    @pytest.mark.asyncio
    async def test_loop_continues_past_none_frames_then_dispatches_valid(self) -> None:
        """Loop body handles both None frames (continue) and valid frames.

        Given: a subscriber that yields (a) a recv failure → None
            frame, then (b) a valid ``bus.caps_violation_after_ai_approve``
            frame, then (c) ``CancelledError`` to unwind cleanly,
        When: the loop runs,
        Then: the invalid frame is skipped, the valid frame dispatches
            through ``handle_caps_violation_bus_message``, and
            ``CancelledError`` propagates.
        """
        svc = AiReviewService.get_instance()
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(
            side_effect=[
                RuntimeError("first call fails"),
                (b"bus.caps_violation_after_ai_approve", _caps_violation_payload().encode("utf-8")),
                asyncio.CancelledError(),
            ]
        )
        svc._bus_subscriber = subscriber
        svc._bus_running = True
        with (
            patch.object(
                svc, "handle_caps_violation_bus_message", new=AsyncMock(return_value=True)
            ) as mock_handler,
            patch(
                "snapper.application.ai_review.service.asyncio.sleep",
                new=AsyncMock(return_value=None),
            ),
            pytest.raises(asyncio.CancelledError),
        ):
            await svc._bus_listen_loop()
        mock_handler.assert_awaited_once()
