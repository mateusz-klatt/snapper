"""Unit tests for core AiReviewService process-local behavior.

Covers the singleton lifecycle, the in-memory ``asyncio.Future`` registry that
backs the strategy-side await primitive, and the bus-publisher injection seam.
Dedicated modules cover create-review, submit-decision, reaper, and bus-listener
state-machine paths.
"""

import asyncio
from collections.abc import Iterator
from typing import cast
from unittest.mock import MagicMock

import pytest

from snapper.application.ai_review.service import AiReviewService
from snapper.application.ai_review.service import DelegateBusyError
from snapper.application.ai_review.service import NoLiveDelegateError
from snapper.application.ai_review.service import get_ai_review_service
from snapper.messaging.infrastructure.publisher import MessagePublisher


@pytest.fixture(autouse=True)
def _clear_singleton() -> Iterator[None]:
    """Reset the singleton both before and after each test.

    Required because the AiReviewService is process-global: a test that
    populates the futures registry would otherwise leak handles into the next
    test and break isolation.
    """
    AiReviewService.clear_instance()
    yield
    AiReviewService.clear_instance()


class TestAiReviewServiceSingleton:
    """Lifecycle invariants for the AiReviewService singleton.

    ``get_instance`` is idempotent across calls and survives clearing only via
    the explicit reset seam exposed for tests.
    """

    def test_get_instance_returns_same_object(self) -> None:
        """``get_instance`` returns identical references across calls.

        WS-listener and strategy paths must share one futures registry, so any
        regression here would silently strand awaiting strategies.
        """
        first = AiReviewService.get_instance()
        second = AiReviewService.get_instance()
        assert first is second

    def test_clear_instance_forces_fresh_singleton(self) -> None:
        """``clear_instance`` is the only supported reset path.

        Used by tests and (defensively) by lifespan teardown to drop
        bus-publisher refs that point at closed sockets.
        """
        first = AiReviewService.get_instance()
        AiReviewService.clear_instance()
        second = AiReviewService.get_instance()
        assert first is not second

    def test_module_accessor_returns_singleton(self) -> None:
        """``get_ai_review_service`` is the public-facing accessor.

        Used by FastAPI ``Depends`` and MCP tool registration so both
        transports observe the same instance.
        """
        instance = AiReviewService.get_instance()
        assert get_ai_review_service() is instance

    def test_direct_construction_returns_singleton(self) -> None:
        """Constructing via ``AiReviewService()`` yields the singleton too.

        Guards against test code that instantiates directly bypassing
        ``get_instance``.
        """
        first = AiReviewService.get_instance()
        second = AiReviewService()
        assert first is second

    def test_repeat_init_is_idempotent(self) -> None:
        """Calling ``__init__`` again on the same instance must not reset state.

        The singleton-via-``__new__`` pattern relies on this so subsequent
        ``__init__`` invocations don't clobber injected publisher refs.
        """
        instance = AiReviewService.get_instance()
        publisher = cast(MessagePublisher, MagicMock(spec=MessagePublisher))
        instance.set_msg_publisher(publisher)
        instance.__init__()
        assert instance.msg_publisher is publisher


class TestPublisherInjection:
    """The bus publisher is injected by the FastAPI lifespan; tests stub it."""

    def test_set_publisher_then_read(self) -> None:
        """``set_msg_publisher`` round-trips through ``msg_publisher``.

        Callers verify wiring before the first publish, so the read path must
        return the exact reference passed in.
        """
        instance = AiReviewService.get_instance()
        assert instance.msg_publisher is None
        publisher = cast(MessagePublisher, MagicMock(spec=MessagePublisher))
        instance.set_msg_publisher(publisher)
        assert instance.msg_publisher is publisher

    def test_set_publisher_to_none_clears(self) -> None:
        """Passing ``None`` clears the slot.

        Used by the lifespan teardown path so the singleton doesn't hold a
        closed socket reference between hot reloads.
        """
        instance = AiReviewService.get_instance()
        publisher = cast(MessagePublisher, MagicMock(spec=MessagePublisher))
        instance.set_msg_publisher(publisher)
        instance.set_msg_publisher(None)
        assert instance.msg_publisher is None


class TestFutureRegistry:
    """The in-process channel between WS-listener and strategy-await paths.

    Both sides MUST share the same singleton; otherwise the strategy would
    hang on a future the listener can't see.
    """

    @pytest.mark.asyncio
    async def test_register_then_get(self) -> None:
        """A registered future is retrievable by id.

        This is the contract the WS-listener relies on when a decision arrives
        and it needs to resolve the awaiting strategy.
        """
        instance = AiReviewService.get_instance()
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[object] = loop.create_future()
        instance.register_future("rev-1", fut)
        assert instance.get_future("rev-1") is fut

    def test_get_unknown_id_returns_none(self) -> None:
        """Unknown ids yield ``None``.

        Bus listeners treat this as a no-op — the review may have resolved on
        a peer instance, so a missing future is expected.
        """
        instance = AiReviewService.get_instance()
        assert instance.get_future("does-not-exist") is None

    @pytest.mark.asyncio
    async def test_register_replaces_prior(self) -> None:
        """Re-registering the same id replaces the previous future.

        Only a reconnecting strategy legitimately re-registers,
        and the most-recent waiter wins.
        """
        instance = AiReviewService.get_instance()
        loop = asyncio.get_running_loop()
        first: asyncio.Future[object] = loop.create_future()
        second: asyncio.Future[object] = loop.create_future()
        instance.register_future("rev-1", first)
        instance.register_future("rev-1", second)
        assert instance.get_future("rev-1") is second

    @pytest.mark.asyncio
    async def test_unregister_removes_future(self) -> None:
        """``unregister_future`` drops the entry.

        Strategy await-loops call this in their ``finally`` clause to avoid
        leaking handles after either normal resolution or timeout.
        """
        instance = AiReviewService.get_instance()
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[object] = loop.create_future()
        instance.register_future("rev-1", fut)
        instance.unregister_future("rev-1")
        assert instance.get_future("rev-1") is None

    def test_unregister_missing_id_is_idempotent(self) -> None:
        """Unregistering an unknown id must not raise.

        Reaper + supersede paths call this defensively as a safety net even
        when the strategy has already cleaned up.
        """
        instance = AiReviewService.get_instance()
        instance.unregister_future("never-registered")


class TestExceptionTypes:
    """Admission-control exception types.

    The two outcomes ``NoLiveDelegateError`` and ``DelegateBusyError`` let
    strategy code branch deterministically on retry policy.
    """

    def test_no_live_delegate_is_exception(self) -> None:
        """``NoLiveDelegateError`` propagates as a regular Exception.

        Strategy code can ``except`` it directly without importing
        ``BaseException``.
        """
        with pytest.raises(NoLiveDelegateError):
            raise NoLiveDelegateError("none live")

    def test_delegate_busy_is_exception(self) -> None:
        """``DelegateBusyError`` is the second admission-control outcome.

        Retryable, distinct from the live-zero case so strategy code can pick
        a backoff that's appropriate to "everyone is busy" vs "nobody is here".
        """
        with pytest.raises(DelegateBusyError):
            raise DelegateBusyError("all busy")
