"""Tests for the subprocess process-runner entry point.

Verifies the decision-only AiReviewService bus listener wiring around
the subprocess target invocation. The listener is wired in
:func:`snapper.server.process_runner._ai_review_decision_listener`
and consumed by both the async-method and awaitable code paths so any
strategy that subscribes to the strategy primitive's await loop gets
the fast-path even when launched via
:class:`ProcessLauncherService` in ``ProcessModeEnum.PROCESS``.
"""

import asyncio
import signal
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Coroutine
from collections.abc import Iterator
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.application.ai_review.service import AiReviewService
from snapper.server import process_runner
from snapper.server.process_runner import _ai_review_decision_listener
from snapper.server.process_runner import _await_result_with_listener
from snapper.server.process_runner import _run_async_method_with_listener
from snapper.server.process_runner import _run_with_sigterm_drain


@pytest.fixture(autouse=True)
def _clear_singleton() -> Iterator[None]:
    """Reset the AiReviewService singleton between cases."""
    AiReviewService.clear_instance()
    yield
    AiReviewService.clear_instance()


@pytest.mark.asyncio
async def test_decision_listener_starts_with_decision_topic_only() -> None:
    """Listener subscribes to ``bus.ai_review_decision`` only.

    Subprocess strategies must NOT subscribe to ``bus.delegate_offline``
    (handler needs a repository factory the subprocess lacks) or
    ``bus.caps_violation_after_ai_approve`` (the FastAPI server with
    ShardOwnership gating is the SOLE re-publisher of the external WS
    frame; a subprocess re-publish would N-duplicate the frame).

    Given a non-empty ``zmq_broker_xpub`` setting,
    When the ``_ai_review_decision_listener`` context manager enters,
    Then ``start_bus_listener`` fires with the decision-topic-only tuple
    and ``stop_bus_listener`` fires on exit.
    """
    fake_settings = MagicMock(zmq_broker_xpub="tcp://127.0.0.1:7501")
    fake_service = MagicMock(
        start_bus_listener=AsyncMock(),
        stop_bus_listener=AsyncMock(),
    )
    inside_block_state: dict[str, bool] = {}
    with (
        patch("snapper.server.process_runner.get_settings", return_value=fake_settings),
        patch("snapper.server.process_runner.get_ai_review_service", return_value=fake_service),
    ):
        async with _ai_review_decision_listener():
            inside_block_state["entered"] = True
    assert inside_block_state == {"entered": True}
    fake_service.start_bus_listener.assert_awaited_once_with(
        "tcp://127.0.0.1:7501",
        topics=("bus.ai_review_decision",),
    )
    fake_service.stop_bus_listener.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_decision_listener_skips_when_broker_xpub_is_empty() -> None:
    """Empty ``zmq_broker_xpub`` short-circuits — no listener start, no stop.

    Test fixtures + ``SERVER_API_ONLY`` boots set the broker XPUB to an
    empty string. The listener wiring must respect that so unit tests
    can exercise the wrapper without touching the real ZMQ stack.

    Given an empty ``zmq_broker_xpub`` setting,
    When the ``_ai_review_decision_listener`` context manager enters,
    Then neither ``start_bus_listener`` nor ``stop_bus_listener`` fire.
    """
    fake_settings = MagicMock(zmq_broker_xpub="")
    fake_service = MagicMock(
        start_bus_listener=AsyncMock(),
        stop_bus_listener=AsyncMock(),
    )
    inside_block_state: dict[str, bool] = {}
    with (
        patch("snapper.server.process_runner.get_settings", return_value=fake_settings),
        patch("snapper.server.process_runner.get_ai_review_service", return_value=fake_service),
    ):
        async with _ai_review_decision_listener():
            inside_block_state["entered"] = True
    assert inside_block_state == {"entered": True}
    fake_service.start_bus_listener.assert_not_awaited()
    fake_service.stop_bus_listener.assert_not_awaited()


@pytest.mark.asyncio
async def test_decision_listener_stops_listener_on_exception() -> None:
    """Listener stop fires even when the wrapped block raises.

    Cleanup invariant: a strategy that crashes mid-await MUST NOT leak
    the subprocess's ZMQ subscriber socket. The asynccontextmanager
    ``finally`` clause guarantees the stop call regardless of whether
    the wrapped block returned normally or raised.

    Given a wrapped block that raises ``RuntimeError``,
    When the ``_ai_review_decision_listener`` context manager exits via
    the exception path,
    Then ``stop_bus_listener`` still fires once and the exception
    propagates to the caller unchanged.
    """
    fake_settings = MagicMock(zmq_broker_xpub="tcp://127.0.0.1:7501")
    fake_service = MagicMock(
        start_bus_listener=AsyncMock(),
        stop_bus_listener=AsyncMock(),
    )
    strategy_error = RuntimeError("strategy boom")
    with (
        patch("snapper.server.process_runner.get_settings", return_value=fake_settings),
        patch("snapper.server.process_runner.get_ai_review_service", return_value=fake_service),
        pytest.raises(RuntimeError, match="strategy boom"),
    ):
        async with _ai_review_decision_listener():
            raise strategy_error
    fake_service.stop_bus_listener.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_run_async_method_with_listener_drives_method_inside_seam() -> None:
    """``_run_async_method_with_listener`` invokes the target while listener active.

    Pin the ordering: start_bus_listener fires BEFORE the target method
    runs (so the strategy primitive's ``register_future`` has a live
    listener to resolve into) and stop_bus_listener fires AFTER the
    target method completes (so any in-flight bus dispatch can finish).

    Given an async target method,
    When ``_run_async_method_with_listener`` is awaited on it,
    Then start fires first, target runs, stop fires last, and the
    target's return value is returned to the caller.
    """
    invocation_order: list[str] = []
    fake_settings = MagicMock(zmq_broker_xpub="tcp://127.0.0.1:7501")
    fake_service = MagicMock(
        start_bus_listener=AsyncMock(side_effect=lambda *a, **kw: invocation_order.append("start")),
        stop_bus_listener=AsyncMock(side_effect=lambda: invocation_order.append("stop")),
    )

    async def _target() -> str:
        invocation_order.append("target")
        await asyncio.sleep(0)
        return "ok"

    with (
        patch("snapper.server.process_runner.get_settings", return_value=fake_settings),
        patch("snapper.server.process_runner.get_ai_review_service", return_value=fake_service),
    ):
        result = await _run_async_method_with_listener(_target)
    assert result == "ok"
    assert invocation_order == ["start", "target", "stop"]


@pytest.mark.asyncio
async def test_await_result_with_listener_drives_awaitable_inside_seam() -> None:
    """``_await_result_with_listener`` drives an existing awaitable inside the seam.

    Mirror of the async-method case for the rare branch where
    ``target_method()`` is sync but returns an awaitable. Same
    start-before/stop-after ordering invariant.

    Given an awaitable produced by a sync target method,
    When ``_await_result_with_listener`` is awaited on it,
    Then start fires first, awaitable runs, stop fires last, and the
    awaitable's resolved value is returned to the caller.
    """
    invocation_order: list[str] = []
    fake_settings = MagicMock(zmq_broker_xpub="tcp://127.0.0.1:7501")
    fake_service = MagicMock(
        start_bus_listener=AsyncMock(side_effect=lambda *a, **kw: invocation_order.append("start")),
        stop_bus_listener=AsyncMock(side_effect=lambda: invocation_order.append("stop")),
    )

    async def _coro() -> int:
        invocation_order.append("awaitable")
        await asyncio.sleep(0)
        return 42

    with (
        patch("snapper.server.process_runner.get_settings", return_value=fake_settings),
        patch("snapper.server.process_runner.get_ai_review_service", return_value=fake_service),
    ):
        result = await _await_result_with_listener(_coro())
    assert result == 42
    assert invocation_order == ["start", "awaitable", "stop"]


@pytest.mark.parametrize(
    ("process_role", "expected"),
    [
        (None, True),
        ("strategy", True),
        ("core", False),
        ("task", False),
        ("backtest", False),
    ],
)
def test_ai_review_listener_is_limited_to_strategy_roles(
    process_role: object,
    expected: bool,
) -> None:
    """Role metadata gates strategy-only subprocess services.

    Given: Legacy, strategy, and explicit non-strategy process roles,
    When: The subprocess runner selects its optional decision listener,
    Then: Only legacy and strategy workloads receive that listener.
    """
    assert process_runner._uses_ai_review_listener(process_role) is expected


def test_run_event_loop_uses_uvloop_when_supported(monkeypatch: pytest.MonkeyPatch) -> None:
    """POSIX subprocesses keep the uvloop event-loop runner.

    Given the subprocess runner is on a uvloop-capable platform,
    When an awaitable is executed through the event-loop helper,
    Then uvloop is imported and used to drive the awaitable.
    """
    calls: list[str] = []

    async def _target() -> str:
        return "ok"

    class _FakeUvloop:
        @staticmethod
        def run(awaitable: Awaitable[str]) -> str:
            calls.append("run")
            return asyncio.run(awaitable)

    def _import_module(name: str) -> object:
        calls.append(name)
        return _FakeUvloop

    monkeypatch.setattr(process_runner, "_use_asyncio_runner", lambda: False)
    monkeypatch.setattr(process_runner.importlib, "import_module", _import_module)
    assert process_runner._run_event_loop(_target()) == "ok"
    assert calls == ["uvloop", "run"]


def test_run_event_loop_uses_asyncio_when_uvloop_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Windows subprocesses use the asyncio event-loop runner.

    Given the subprocess runner is on a platform without uvloop support,
    When an awaitable is executed through the event-loop helper,
    Then asyncio drives the awaitable without importing uvloop.
    """

    async def _target() -> str:
        return "ok"

    def _import_module(name: str) -> object:
        raise AssertionError(f"Unexpected import of {name}")

    monkeypatch.setattr(process_runner, "_use_asyncio_runner", lambda: True)
    monkeypatch.setattr(process_runner.importlib, "import_module", _import_module)
    assert process_runner._run_event_loop(_target()) == "ok"


def test_main_async_path_uses_listener_wrapper(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin the integration: ``main()`` async path delegates to the listener wrapper.

    Regression guard: if a future refactor inlines the asyncio.run call
    or skips the wrapper, the subprocess strategy primitive's fast-path
    silently falls back to DB-poll without a CI signal. This test
    verifies the wrapper is the only async-invocation surface.

    Given a fake strategy class with an async ``start`` method,
    When ``main()`` resolves and invokes the target,
    Then ``_run_async_method_with_listener`` is called exactly once
    (proving the wrapper sits between asyncio.run and the strategy
    method).
    """
    captured: dict[str, object] = {}

    async def _wrapper(method: Callable[[], Awaitable[object]]) -> None:
        captured["wrapper_called_with"] = method
        await asyncio.sleep(0)

    class _Instance:
        async def start(self) -> None:
            captured["instance_start_called"] = True
            await asyncio.sleep(0)

    class _FakeUvloop:
        @staticmethod
        def run(awaitable: Awaitable[None]) -> None:
            return asyncio.run(awaitable)

    def _import_module(name: str) -> object:
        if name == "fake.module":
            return fake_module
        if name == "uvloop":
            return _FakeUvloop
        raise AssertionError(f"Unexpected import of {name}")

    fake_module = MagicMock()
    fake_module.FakeStrategy = _Instance
    config = (
        '{"name": "fake", "class_path": "fake.module.FakeStrategy", '
        '"method": "start", "parameters": {}, "role": "strategy"}'
    )
    monkeypatch.setattr("sys.argv", ["process_runner", "--config", config])
    with (
        patch(
            "snapper.server.process_runner._run_async_method_with_listener",
            side_effect=_wrapper,
        ) as mock_wrapper,
        patch("snapper.server.process_runner.setup_logging"),
        patch(
            "snapper.server.process_runner.importlib.import_module",
            side_effect=_import_module,
        ),
    ):
        exit_code = process_runner.main()
    assert exit_code == 0
    assert captured.get("instance_start_called") is None
    mock_wrapper.assert_awaited_once()


def test_main_core_async_path_skips_listener_wrapper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-strategy subprocesses use only generic target invocation.

    Given: A core-role process with an asynchronous start method,
    When: The subprocess runner invokes it,
    Then: The target runs without entering the strategy decision listener.
    """
    captured: dict[str, bool] = {}

    class _Instance:
        async def start(self) -> None:
            captured["started"] = True
            await asyncio.sleep(0)

    def _run(awaitable: Awaitable[object]) -> object:
        return asyncio.run(awaitable)

    config = (
        '{"name": "fake", "class_path": "fake.module.CoreProcess", '
        '"method": "start", "parameters": {}, "role": "core"}'
    )
    monkeypatch.setattr("sys.argv", ["process_runner", "--config", config])
    with (
        patch("snapper.server.process_runner.setup_logging"),
        patch("snapper.server.process_runner.log_kraken_sdk_patches_status"),
        patch("snapper.server.process_runner._resolve_process_class", return_value=_Instance),
        patch("snapper.server.process_runner._run_event_loop", side_effect=_run),
        patch(
            "snapper.server.process_runner._run_async_method_with_listener",
            new_callable=AsyncMock,
        ) as mock_wrapper,
    ):
        exit_code = process_runner.main()
    assert exit_code == 0
    assert captured == {"started": True}
    mock_wrapper.assert_not_awaited()


def test_main_core_sync_awaitable_path_skips_listener_wrapper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-strategy synchronous factories await results without strategy services.

    Given: A core-role process whose synchronous start method returns an awaitable,
    When: The subprocess runner drives that returned awaitable,
    Then: The generic await path runs without entering the decision listener wrapper.
    """
    captured: dict[str, bool] = {}

    class _Instance:
        def start(self) -> Awaitable[None]:
            async def _start() -> None:
                captured["started"] = True
                await asyncio.sleep(0)

            return _start()

    def _run(awaitable: Awaitable[object]) -> object:
        return asyncio.run(awaitable)

    async def _listener_wrapper(awaitable: Awaitable[None]) -> None:
        await awaitable

    config = (
        '{"name": "fake", "class_path": "fake.module.CoreProcess", '
        '"method": "start", "parameters": {}, "role": "core"}'
    )
    monkeypatch.setattr("sys.argv", ["process_runner", "--config", config])
    with (
        patch("snapper.server.process_runner.setup_logging"),
        patch("snapper.server.process_runner.log_kraken_sdk_patches_status"),
        patch("snapper.server.process_runner._resolve_process_class", return_value=_Instance),
        patch("snapper.server.process_runner._run_event_loop", side_effect=_run),
        patch(
            "snapper.server.process_runner._await_result_with_listener",
            side_effect=_listener_wrapper,
        ) as mock_wrapper,
    ):
        exit_code = process_runner.main()
    assert exit_code == 0
    assert captured == {"started": True}
    mock_wrapper.assert_not_awaited()


def test_main_logs_to_container_subprocess_logfile(monkeypatch: pytest.MonkeyPatch) -> None:
    """Spec — process_runner.main routes logging to the container's file.

    Given ``SNAPPER_LOG_FILE`` points at a container's dedicated file
        (as the feed container exports before spawning publishers),
    When ``process_runner.main`` runs,
    Then setup_logging is called with that resolved logfile so the
        spawned publisher logs to its container's file rather than the
        API service logfile.
    """
    captured: dict[str, object] = {}

    def _fake_setup_logging(*args: object, **kwargs: object) -> None:
        captured.update(kwargs)

    expected_logfile = "data/log/snapper-feed/snapper-feed.log"
    monkeypatch.setenv("SNAPPER_LOG_FILE", expected_logfile)
    monkeypatch.setattr("snapper.server.process_runner.setup_logging", _fake_setup_logging)
    monkeypatch.setattr("snapper.server.process_runner.log_kraken_sdk_patches_status", lambda: None)
    monkeypatch.setattr("sys.argv", ["process_runner", "--config", "not-json"])
    assert process_runner.main() == 1
    assert captured["logfile"] == expected_logfile


def test_main_sigterm_drains_and_reports_rows_left_at_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SIGTERM invokes publisher drain and reports rows left after its deadline.

    Given: A publisher whose drain cannot finish within the runner deadline,
    When: The subprocess runner receives SIGTERM,
    Then: It invokes stop and warns with the seven undrained trade rows.
    """
    state: dict[str, bool] = {}
    registered_signals: list[int] = []

    class _Instance:
        async def start(self) -> None:
            await asyncio.Event().wait()

        async def stop(self) -> None:
            state["stop_called"] = True
            await asyncio.Event().wait()

        def pending_trade_write_rows(self) -> int:
            return 7

    def _run(awaitable: Awaitable[object]) -> object | None:
        async def _drive() -> object | None:
            loop = asyncio.get_running_loop()

            def _add_signal_handler(
                signal_number: int,
                callback: Callable[[], None],
            ) -> None:
                registered_signals.append(signal_number)
                callback()

            with (
                patch.object(loop, "add_signal_handler", side_effect=_add_signal_handler),
                patch.object(loop, "remove_signal_handler", return_value=True),
            ):
                try:
                    return await asyncio.wait_for(awaitable, timeout=0.1)
                except TimeoutError:
                    return None

        return asyncio.run(_drive())

    config = (
        '{"name": "fake", "class_path": "fake.module.Publisher", '
        '"method": "start", "parameters": {}, "role": "core"}'
    )
    monkeypatch.setattr("sys.argv", ["process_runner", "--config", config])
    monkeypatch.setattr(process_runner, "_SIGTERM_DRAIN_TIMEOUT_S", 0.001, raising=False)
    with (
        patch("snapper.server.process_runner.setup_logging"),
        patch("snapper.server.process_runner.log_kraken_sdk_patches_status"),
        patch("snapper.server.process_runner._resolve_process_class", return_value=_Instance),
        patch("snapper.server.process_runner._run_event_loop", side_effect=_run),
        patch.object(process_runner.logger, "warning") as warning_mock,
    ):
        assert process_runner.main() == 0

    warning_messages = [str(call.args[0]) for call in warning_mock.call_args_list]
    assert registered_signals == [signal.SIGTERM]
    assert state == {"stop_called": True}
    assert any("undrained_trade_rows=7" in message for message in warning_messages)


@pytest.mark.asyncio
async def test_sigterm_drain_fallback_returns_awaitable_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unsupported signal handlers preserve the target awaitable's result.

    Given: An event loop that cannot install a SIGTERM handler.
    When: The runner drives a target awaitable.
    Then: It returns the target result unchanged without starting a drain.
    """
    loop = asyncio.get_running_loop()
    drainable = MagicMock()

    async def _target() -> object:
        return drainable

    monkeypatch.setattr(
        loop,
        "add_signal_handler",
        MagicMock(side_effect=NotImplementedError),
    )

    assert await _run_with_sigterm_drain(drainable, _target()) is drainable
    drainable.stop.assert_not_called()


@pytest.mark.asyncio
async def test_sigterm_drain_completion_cancels_target_without_leaking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A completed SIGTERM drain cancels the still-running target task.

    Given: A pending target and a drain that completes immediately.
    When: The registered SIGTERM callback fires.
    Then: The drain completes and the target task observes cancellation.
    """
    loop = asyncio.get_running_loop()
    target_started = asyncio.Event()
    target_cancelled = asyncio.Event()
    stop_completed = asyncio.Event()
    signal_callback: Callable[[], None] | None = None

    class _Drainable:
        async def stop(self) -> None:
            stop_completed.set()

        def pending_trade_write_rows(self) -> int:
            return 0

    async def _target() -> None:
        target_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            target_cancelled.set()

    def _add_signal_handler(
        signal_number: int,
        callback: Callable[[], None],
    ) -> None:
        nonlocal signal_callback
        assert signal_number == signal.SIGTERM
        signal_callback = callback

    monkeypatch.setattr(loop, "add_signal_handler", _add_signal_handler)
    monkeypatch.setattr(loop, "remove_signal_handler", MagicMock(return_value=True))
    runner = asyncio.create_task(_run_with_sigterm_drain(_Drainable(), _target()))
    await target_started.wait()
    assert signal_callback is not None
    signal_callback()

    await runner
    assert runner.result() is None
    assert stop_completed.is_set()
    assert target_cancelled.is_set()


@pytest.mark.asyncio
async def test_sigterm_drain_deadline_reports_rows_and_cancels_pending_tasks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An expired drain deadline reports retained rows and cancels both jobs.

    Given: Pending target and drain tasks with seven retained trade rows.
    When: SIGTERM fires and the patched zero-second deadline expires.
    Then: The warning reports seven rows and both pending jobs are cancelled.
    """
    loop = asyncio.get_running_loop()
    target_started = asyncio.Event()
    target_cancelled = asyncio.Event()
    drain_started = asyncio.Event()
    drain_cancelled = asyncio.Event()
    signal_callback: Callable[[], None] | None = None

    class _Drainable:
        async def stop(self) -> None:
            drain_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                drain_cancelled.set()

        def pending_trade_write_rows(self) -> int:
            return 7

    async def _target() -> None:
        target_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            target_cancelled.set()

    def _add_signal_handler(
        signal_number: int,
        callback: Callable[[], None],
    ) -> None:
        nonlocal signal_callback
        assert signal_number == signal.SIGTERM
        signal_callback = callback

    monkeypatch.setattr(loop, "add_signal_handler", _add_signal_handler)
    monkeypatch.setattr(loop, "remove_signal_handler", MagicMock(return_value=True))
    monkeypatch.setattr(process_runner, "_SIGTERM_DRAIN_TIMEOUT_S", 0.0)
    warning_mock = MagicMock()
    monkeypatch.setattr(process_runner.logger, "warning", warning_mock)
    runner = asyncio.create_task(_run_with_sigterm_drain(_Drainable(), _target()))
    await target_started.wait()
    assert signal_callback is not None
    signal_callback()

    await runner
    assert runner.result() is None
    assert drain_started.is_set()
    assert target_cancelled.is_set()
    assert drain_cancelled.is_set()
    warning_mock.assert_called_once()
    assert "undrained_trade_rows=7" in str(warning_mock.call_args.args[0])


@pytest.mark.asyncio
async def test_target_completion_cancels_pending_signal_wait(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ordinary target completion cancels the unused SIGTERM waiter.

    Given: A target that completes before any SIGTERM arrives.
    When: The SIGTERM-aware runner returns its result.
    Then: The unused signal waiter ends cancelled instead of leaking.
    """
    loop = asyncio.get_running_loop()
    created_tasks: list[asyncio.Task[object]] = []
    original_create_task = asyncio.create_task

    class _Drainable:
        async def stop(self) -> None:
            raise AssertionError("Ordinary completion must not drain")

        def pending_trade_write_rows(self) -> int:
            return 0

    async def _target() -> str:
        return "complete"

    def _create_task(
        coroutine: Coroutine[object, object, object],
    ) -> asyncio.Task[object]:
        task: asyncio.Task[object] = original_create_task(coroutine)
        created_tasks.append(task)
        return task

    monkeypatch.setattr(loop, "add_signal_handler", MagicMock())
    monkeypatch.setattr(loop, "remove_signal_handler", MagicMock(return_value=True))
    monkeypatch.setattr(process_runner.asyncio, "create_task", _create_task)

    assert await _run_with_sigterm_drain(_Drainable(), _target()) == "complete"
    assert len(created_tasks) == 1
    assert created_tasks[0].cancelled()
