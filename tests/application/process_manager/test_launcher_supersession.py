"""Supersession-safety tests for the process launcher.

Regression coverage for the 2026-07-08 zombie-strategy incident: rapid
desired-state churn (restart + config change + stop within seconds) let a
stale completion callback evict the SUCCESSOR's tracking, a stale watchdog
respawn double-start the name, and the per-name map overwrites drop the
only handles to a still-running instance — unreachable by every stop path.

These tests pin the repaired invariant: at most one live instance per
logical process name, every live instance stays reachable by a stop path,
and a superseded completion may only finalize its OWN run row.
"""

import asyncio
import contextlib
from datetime import UTC
from datetime import datetime
from types import SimpleNamespace
from typing import cast
from unittest import mock
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.process_manager import launcher as launcher_module
from snapper.application.process_manager.launcher import ProcessLauncherService
from snapper.application.process_manager.launcher import _DesiredState
from snapper.application.process_manager.models import ProcessConfigModel
from snapper.application.process_manager.models import ProcessInstanceInfo
from snapper.application.process_manager.run_recorder import ProcessRunRecorder
from snapper.config.app import AppSettings
from snapper.core.types import ProcessLifecycleEnum
from snapper.core.types import ProcessRoleEnum
from snapper.core.types import ProcessRunStatusEnum
from snapper.core.types import StopProcessStatusEnum


class _StubSettings(SimpleNamespace):
    """Namespace settings stub carrying only the db_url the launcher stores."""

    db_url: str = "sqlite:///:memory:"


class _LongRunningProcess:
    """Process stub whose start() blocks until cancelled."""

    async def start(self) -> None:
        """Wait forever; unwinds cleanly on task cancellation."""
        await asyncio.Event().wait()

    async def stop(self) -> None:
        """No-op stop matching the launcher's instance-stop contract."""
        return None


def _make_factory() -> ProcessLauncherService:
    """Build a launcher with a mocked run recorder (no DB access)."""
    factory = ProcessLauncherService(settings=cast(AppSettings, _StubSettings()))
    factory._run_recorder = cast(
        ProcessRunRecorder,
        SimpleNamespace(update_run_record=AsyncMock()),
    )
    return factory


def _make_config(name: str) -> ProcessConfigModel:
    """Build a minimal thread-mode LONG_RUNNING config for tests."""
    return ProcessConfigModel(
        name=name,
        enabled=True,
        mode="thread",
        class_path="tests.dummy.LongRunning",
        method="start",
        parameters={},
        note=None,
        lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
        role=ProcessRoleEnum.CORE,
        tags=(),
        parameters_schema=None,
    )


async def _finished_task() -> asyncio.Task[None]:
    """Return a task that already completed successfully."""
    task = asyncio.create_task(asyncio.sleep(0))
    await task
    return task


async def _cancelled_task() -> asyncio.Task[None]:
    """Return a task that already completed as cancelled."""
    task = asyncio.create_task(asyncio.sleep(60))
    await asyncio.sleep(0)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    return task


@pytest.mark.asyncio
async def test_stale_task_completion_leaves_successor_tracking_untouched() -> None:
    """A completed task that lost its slot must not evict the successor.

    Given: A successor task owns ``process_tasks[name]`` and a successor
        instance owns ``started_processes[name]``,
    When: The superseded predecessor's completion handler runs,
    Then: Neither map is mutated, the watchdog is not driven, the by-name
        finalize is not called, and only the predecessor's own run row is
        finalized via ``_finalize_superseded_run``.
    """
    factory = _make_factory()
    old_task = await _cancelled_task()
    successor_task = asyncio.create_task(asyncio.sleep(60))
    successor_instance = object()
    factory.process_tasks["s"] = successor_task
    factory.started_processes["s"] = successor_instance
    factory.process_lifecycles["s"] = ProcessLifecycleEnum.LONG_RUNNING
    factory.process_roles["s"] = ProcessRoleEnum.CORE
    finalize_by_name = AsyncMock()
    finalize_superseded = AsyncMock()
    restart_mock = AsyncMock()
    with (
        mock.patch.object(factory, "_finalize_process_run", finalize_by_name),
        mock.patch.object(factory, "_finalize_superseded_run", finalize_superseded),
        mock.patch.object(factory, "_maybe_schedule_restart", restart_mock),
    ):
        await factory._handle_task_completion("s", old_task, "run-old")
    finalize_by_name.assert_not_awaited()
    restart_mock.assert_not_awaited()
    finalize_superseded.assert_awaited_once_with(
        "s", "run-old", ProcessRunStatusEnum.CANCELLED, error=None
    )
    assert factory.process_tasks.get("s") is successor_task
    assert factory.started_processes.get("s") is successor_instance
    successor_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await successor_task


@pytest.mark.asyncio
async def test_stale_completion_detected_via_started_processes_after_mode_switch() -> None:
    """A task completion with no task slot but a live instance is superseded.

    Given: ``process_tasks`` has no entry (a stop popped the predecessor)
        while ``started_processes`` holds a native successor after a
        thread-to-process mode switch,
    When: The predecessor's completion handler runs,
    Then: It takes the superseded path — no by-name finalize, no map pops.
    """
    factory = _make_factory()
    old_task = await _finished_task()
    successor_instance = object()
    factory.started_processes["m"] = successor_instance
    finalize_by_name = AsyncMock()
    finalize_superseded = AsyncMock()
    restart_mock = AsyncMock()
    with (
        mock.patch.object(factory, "_finalize_process_run", finalize_by_name),
        mock.patch.object(factory, "_finalize_superseded_run", finalize_superseded),
        mock.patch.object(factory, "_maybe_schedule_restart", restart_mock),
    ):
        await factory._handle_task_completion("m", old_task, "run-old")
    finalize_by_name.assert_not_awaited()
    restart_mock.assert_not_awaited()
    finalize_superseded.assert_awaited_once()
    assert factory.started_processes.get("m") is successor_instance


@pytest.mark.asyncio
async def test_finalize_superseded_run_is_idempotent_and_skips_none() -> None:
    """The per-run finalizer no-ops on None ids and already-closed rows.

    Given: A launcher whose finalized-run set already holds ``run-x``,
    When: ``_finalize_superseded_run`` runs for None, for ``run-x``, and
        for a fresh ``run-y``,
    Then: Only ``run-y`` reaches the run recorder and joins the set.
    """
    factory = _make_factory()
    recorder = cast(SimpleNamespace, factory._run_recorder)
    factory._mark_run_finalized("run-x")
    await factory._finalize_superseded_run("p", None, ProcessRunStatusEnum.CANCELLED)
    await factory._finalize_superseded_run("p", "run-x", ProcessRunStatusEnum.CANCELLED)
    recorder.update_run_record.assert_not_awaited()
    await factory._finalize_superseded_run(
        "p", "run-y", ProcessRunStatusEnum.CANCELLED, error="late"
    )
    recorder.update_run_record.assert_awaited_once_with(
        "run-y", ProcessRunStatusEnum.CANCELLED, error="late"
    )
    assert "run-y" in factory._finalized_run_ids


@pytest.mark.asyncio
async def test_finalize_process_run_marks_run_id_finalized() -> None:
    """The by-name finalizer records the popped run id as finalized.

    Given: ``active_runs`` maps a name to a run id,
    When: ``_finalize_process_run`` closes it,
    Then: The id lands in the finalized-run set, so a later superseded
        finalize of the same id is a no-op.
    """
    factory = _make_factory()
    factory.active_runs["p"] = "run-1"
    factory.active_run_started_at["p"] = datetime.now(UTC)
    await factory._finalize_process_run("p", ProcessRunStatusEnum.CANCELLED)
    assert "run-1" in factory._finalized_run_ids
    recorder = cast(SimpleNamespace, factory._run_recorder)
    recorder.update_run_record.reset_mock()
    await factory._finalize_superseded_run("p", "run-1", ProcessRunStatusEnum.CANCELLED)
    recorder.update_run_record.assert_not_awaited()


def test_mark_run_finalized_evicts_oldest_past_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    """The finalized-run set stays bounded by evicting insertion order.

    Given: A cap of 3,
    When: Five run ids are marked finalized,
    Then: Only the newest three remain.
    """
    monkeypatch.setattr(launcher_module, "_FINALIZED_RUN_IDS_CAP", 3)
    factory = _make_factory()
    for index in range(5):
        factory._mark_run_finalized(f"run-{index}")
    assert list(factory._finalized_run_ids) == ["run-2", "run-3", "run-4"]


@pytest.mark.asyncio
async def test_start_process_reaps_live_predecessor_before_tracking() -> None:
    """A start with a live predecessor reaps it instead of overwriting.

    Given: A live predecessor task + instance + active run for the name,
    When: ``start_process`` launches a successor,
    Then: The predecessor task is cancelled, its run row is finalized as
        CANCELLED with the supersession error, and the maps hold exactly
        the successor.
    """
    factory = _make_factory()
    old_task: asyncio.Task[None] = asyncio.create_task(asyncio.sleep(60))
    await asyncio.sleep(0)
    old_instance = SimpleNamespace(stop=AsyncMock())
    factory.process_tasks["x"] = cast(asyncio.Task[object], old_task)
    factory.started_processes["x"] = old_instance
    factory.active_runs["x"] = "run-A"
    factory.active_run_started_at["x"] = datetime.now(UTC)
    factory._create_process_run_record = AsyncMock(return_value="run-B")
    factory.import_class = lambda path, name=None, template=None: _LongRunningProcess
    await factory.start_process(_make_config("x"))
    assert old_task.cancelled()
    old_instance.stop.assert_awaited_once()
    recorder = cast(SimpleNamespace, factory._run_recorder)
    assert (
        mock.call(
            "run-A",
            ProcessRunStatusEnum.CANCELLED,
            result=None,
            error="superseded by a newer start",
        )
        in recorder.update_run_record.await_args_list
    )
    assert factory.active_runs.get("x") == "run-B"
    assert factory.process_tasks.get("x") is not None
    assert factory.process_tasks["x"] is not old_task
    assert isinstance(factory.started_processes.get("x"), _LongRunningProcess)
    await factory.stop_process_by_name("x")
    await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_start_process_refuses_when_predecessor_survives_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cancellation-absorbing predecessor blocks the successor start.

    Given: A predecessor task that swallows cancellation and keeps
        running, with the reap timeout shrunk for the test,
    When: ``start_process`` is called for the same name,
    Then: It raises RuntimeError and RETAINS the predecessor's handles,
        so the wedged instance stays stoppable and is never double-run.
    """
    monkeypatch.setattr(launcher_module, "_REAP_PREDECESSOR_TIMEOUT_S", 0.05)
    factory = _make_factory()
    release = asyncio.Event()

    async def _stubborn() -> None:
        """Absorb cancellation until the release event is set."""
        while True:
            try:
                await release.wait()
                return
            except asyncio.CancelledError:
                if release.is_set():
                    raise

    stubborn_task: asyncio.Task[None] = asyncio.create_task(_stubborn())
    await asyncio.sleep(0)
    factory.process_tasks["w"] = cast(asyncio.Task[object], stubborn_task)
    successor_config = _make_config("w")
    with pytest.raises(RuntimeError, match="survived cancellation"):
        await factory.start_process(successor_config)
    assert factory.process_tasks.get("w") is stubborn_task
    release.set()
    await stubborn_task


@pytest.mark.asyncio
async def test_reap_restores_task_slot_when_reap_itself_is_cancelled() -> None:
    """An outer cancellation of the reap await re-attaches the predecessor.

    Given: A cancellation-absorbing predecessor task and a reap running
        inside a wrapper task (the reconcile ``wait_for`` shape),
    When: The wrapper is cancelled while the reap awaits the predecessor,
    Then: The task slot is restored so the still-live predecessor stays
        reachable by a stop, and the cancellation propagates.
    """
    factory = _make_factory()
    release = asyncio.Event()

    async def _stubborn() -> None:
        """Absorb cancellation until the release event is set."""
        while True:
            try:
                await release.wait()
                return
            except asyncio.CancelledError:
                if release.is_set():
                    raise

    stubborn_task: asyncio.Task[None] = asyncio.create_task(_stubborn())
    await asyncio.sleep(0)
    factory.process_tasks["rc"] = cast(asyncio.Task[object], stubborn_task)
    reap_task = asyncio.create_task(factory._reap_superseded_instance("rc"))
    await asyncio.sleep(0.05)
    reap_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await reap_task
    assert factory.process_tasks.get("rc") is stubborn_task
    release.set()
    await stubborn_task


@pytest.mark.asyncio
async def test_reap_restores_instance_when_stop_is_cancelled() -> None:
    """An outer cancellation during instance.stop() re-attaches the instance.

    Given: A tracked instance whose ``stop`` raises ``CancelledError``
        (the outer reconcile timeout landing inside the stop await),
    When: ``_reap_superseded_instance`` runs,
    Then: The instance entry is restored and the cancellation propagates
        unwrapped.
    """
    factory = _make_factory()

    async def _cancelled_stop() -> None:
        raise asyncio.CancelledError()

    instance = SimpleNamespace(stop=_cancelled_stop)
    factory.started_processes["ic"] = instance
    with pytest.raises(asyncio.CancelledError):
        await factory._reap_superseded_instance("ic")
    assert factory.started_processes.get("ic") is instance


@pytest.mark.asyncio
async def test_start_process_refuses_when_predecessor_stop_fails() -> None:
    """A predecessor instance whose stop() raises blocks the successor.

    Given: A tracked instance whose ``stop`` raises,
    When: ``start_process`` is called for the same name,
    Then: It raises RuntimeError and the instance stays tracked.
    """
    factory = _make_factory()
    broken = SimpleNamespace(stop=AsyncMock(side_effect=RuntimeError("wedged")))
    factory.started_processes["b"] = broken
    successor_config = _make_config("b")
    with pytest.raises(RuntimeError, match="failed to stop"):
        await factory.start_process(successor_config)
    assert factory.started_processes.get("b") is broken


@pytest.mark.asyncio
async def test_delayed_restart_noops_when_instance_already_live() -> None:
    """A stale watchdog respawn must not double-start a live name.

    Given: desired-RUNNING and a live instance (or a live task) already
        tracked for the name,
    When: ``_delayed_restart`` fires,
    Then: ``start_process`` is never called.
    """
    factory = _make_factory()
    start_mock = AsyncMock()
    factory._desired_state["d"] = _DesiredState.RUNNING
    factory.started_processes["d"] = object()
    with mock.patch.object(factory, "start_process", start_mock):
        await factory._delayed_restart("d", 0)
    start_mock.assert_not_awaited()
    factory.started_processes.pop("d")
    live_task: asyncio.Task[None] = asyncio.create_task(asyncio.sleep(60))
    await asyncio.sleep(0)
    factory.process_tasks["d"] = cast(asyncio.Task[object], live_task)
    with mock.patch.object(factory, "start_process", start_mock):
        await factory._delayed_restart("d", 0)
    start_mock.assert_not_awaited()
    live_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await live_task


@pytest.mark.asyncio
async def test_stop_process_by_name_stops_task_only_survivor() -> None:
    """A live task without a started_processes entry is still stoppable.

    Given: A live task in ``process_tasks`` and NO ``started_processes``
        entry (the 2026-07-08 zombie shape),
    When: ``stop_process_by_name`` runs,
    Then: It cancels the task and reports SUCCESS instead of NOT_RUNNING.
    """
    factory = _make_factory()
    zombie: asyncio.Task[None] = asyncio.create_task(asyncio.sleep(60))
    await asyncio.sleep(0)
    factory.process_tasks["z"] = cast(asyncio.Task[object], zombie)
    finalize_mock = AsyncMock()
    with mock.patch.object(factory, "_finalize_process_run", finalize_mock):
        result = await factory.stop_process_by_name("z")
    assert result.status is StopProcessStatusEnum.SUCCESS
    assert zombie.cancelled()
    assert "z" not in factory.process_tasks
    finalize_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_try_chain_result_discards_stale_task_and_coroutine() -> None:
    """Stale chained results are discarded, never re-tracked.

    Given: A successor task owns the slot,
    When: A superseded completion offers a chained task and a chained
        coroutine,
    Then: Both are refused; the task is cancelled and the coroutine
        closed, and the successor's slot is untouched.
    """
    factory = _make_factory()
    successor: asyncio.Task[None] = asyncio.create_task(asyncio.sleep(60))
    old = await _finished_task()
    await asyncio.sleep(0)
    factory.process_tasks["c"] = cast(asyncio.Task[object], successor)
    stale_inner: asyncio.Task[None] = asyncio.create_task(asyncio.sleep(60))
    await asyncio.sleep(0)
    assert factory._try_chain_result("c", stale_inner, old, "run-old") is False
    await asyncio.sleep(0)
    assert stale_inner.cancelled()
    stale_coro = asyncio.sleep(60)
    assert factory._try_chain_result("c", stale_coro, old, "run-old") is False
    assert factory.process_tasks.get("c") is successor
    successor.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await successor


@pytest.mark.asyncio
async def test_handle_process_completion_superseded_finalizes_own_run_only() -> None:
    """A superseded native completion never touches the successor.

    Given: ``started_processes[name]`` holds a successor info while a
        dead predecessor info completes,
    When: ``_handle_process_completion`` runs for the predecessor,
    Then: Only the predecessor's own run row is finalized; the name-keyed
        spawner cleanup is NOT invoked and the successor entry survives.
    """
    factory = _make_factory()
    factory.spawner = MagicMock()
    successor_info = MagicMock()
    factory.started_processes["n"] = successor_info
    predecessor = MagicMock()
    predecessor.run_public_id = "run-old"
    predecessor.process = MagicMock()
    predecessor.process.returncode = -15
    finalize_superseded = AsyncMock()
    with mock.patch.object(factory, "_finalize_superseded_run", finalize_superseded):
        await factory._handle_process_completion("n", predecessor)
    finalize_superseded.assert_awaited_once_with(
        "n",
        "run-old",
        ProcessRunStatusEnum.CANCELLED,
        error="superseded instance exited (code -15)",
    )
    factory.spawner.cleanup.assert_not_called()
    assert factory.started_processes.get("n") is successor_info


@pytest.mark.asyncio
async def test_handle_process_completion_finally_guard_spares_replacer() -> None:
    """The completion's final pops skip a successor registered mid-await.

    Given: An owned native completion whose awaited finalize step lets a
        reap+start replace ``started_processes[name]``,
    When: The handler's ``finally`` block runs,
    Then: The successor's tracking entries are left intact.
    """
    factory = _make_factory()
    factory.spawner = MagicMock()
    proc_info = MagicMock()
    proc_info.process = MagicMock()
    proc_info.process.returncode = 0
    successor_info = MagicMock()
    factory.started_processes["r"] = proc_info
    factory.process_lifecycles["r"] = ProcessLifecycleEnum.LONG_RUNNING
    factory.process_roles["r"] = ProcessRoleEnum.CORE

    async def _replace_during_finalize(
        name: str,
        status: ProcessRunStatusEnum,
        result: object = None,
        error: str | None = None,
        exit_code: int | None = None,
    ) -> None:
        """Simulate a reap + successor start landing inside the await."""
        del name, status, result, error, exit_code
        factory.started_processes["r"] = successor_info

    with (
        mock.patch.object(factory, "_finalize_process_run", _replace_during_finalize),
        mock.patch.object(factory, "_maybe_schedule_restart", AsyncMock()),
    ):
        await factory._handle_process_completion("r", proc_info)
    assert factory.started_processes.get("r") is successor_info
    assert factory.process_lifecycles.get("r") is ProcessLifecycleEnum.LONG_RUNNING
    assert factory.process_roles.get("r") is ProcessRoleEnum.CORE


def _make_instance_info(spawner: object) -> ProcessInstanceInfo:
    """Build a ProcessInstanceInfo bound to the given spawner stub."""
    return ProcessInstanceInfo(
        name="x",
        pid=123,
        started_at=datetime.now(UTC),
        config={},
        process=MagicMock(),
        spawner=spawner,
    )


@pytest.mark.asyncio
async def test_instance_stop_skips_spawner_when_successor_registered() -> None:
    """A stale info must not terminate a successor's re-registered child.

    Given: The spawner registry maps the name to a DIFFERENT info,
    When: The stale info's ``stop`` runs,
    Then: Neither terminate nor cleanup is called.
    """
    spawner = SimpleNamespace(processes={}, terminate=MagicMock(), cleanup=MagicMock())
    stale = _make_instance_info(spawner)
    spawner.processes["x"] = _make_instance_info(spawner)
    await stale.stop()
    spawner.terminate.assert_not_called()
    spawner.cleanup.assert_not_called()


@pytest.mark.asyncio
async def test_instance_stop_terminates_when_it_owns_the_registration() -> None:
    """The registered owner's stop terminates and cleans exactly once.

    Given: The spawner registry maps the name to this exact info,
    When: ``stop`` runs twice,
    Then: terminate/cleanup fire once; the second call is a no-op.
    """
    spawner = SimpleNamespace(processes={}, terminate=MagicMock(), cleanup=MagicMock())
    info = _make_instance_info(spawner)
    spawner.processes["x"] = info
    await info.stop()
    await info.stop()
    spawner.terminate.assert_called_once_with("x")
    spawner.cleanup.assert_called_once_with("x")


@pytest.mark.asyncio
async def test_instance_stop_proceeds_when_registry_entry_absent() -> None:
    """An already-cleaned registration still allows the harmless stop.

    Given: The spawner registry has no entry for the name,
    When: ``stop`` runs,
    Then: terminate/cleanup are called (both are no-ops for an unknown
        name in the real spawner).
    """
    spawner = SimpleNamespace(processes={}, terminate=MagicMock(), cleanup=MagicMock())
    info = _make_instance_info(spawner)
    await info.stop()
    spawner.terminate.assert_called_once_with("x")
    spawner.cleanup.assert_called_once_with("x")


@pytest.mark.asyncio
async def test_reap_handles_task_only_predecessor() -> None:
    """A live task without an instance object is reaped cleanly.

    Given: Only ``process_tasks`` holds a live predecessor (no
        ``started_processes`` entry),
    When: ``_reap_superseded_instance`` runs,
    Then: The task is cancelled, its run finalized, and the slot cleared.
    """
    factory = _make_factory()
    lone: asyncio.Task[None] = asyncio.create_task(asyncio.sleep(60))
    await asyncio.sleep(0)
    factory.process_tasks["t"] = cast(asyncio.Task[object], lone)
    factory.active_runs["t"] = "run-T"
    await factory._reap_superseded_instance("t")
    assert lone.cancelled()
    assert "t" not in factory.process_tasks
    assert "run-T" in factory._finalized_run_ids
    await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_reap_tolerates_completion_racing_the_instance_stop() -> None:
    """A completion popping the task slot mid-reap does not double-pop.

    Given: A DONE predecessor task still tracked plus an instance whose
        ``stop`` simulates the completion callback clearing the slot,
    When: ``_reap_superseded_instance`` runs,
    Then: The reap finishes without touching the slot again and clears
        the instance tracking.
    """
    factory = _make_factory()
    done_task = await _finished_task()
    factory.process_tasks["r2"] = cast(asyncio.Task[object], done_task)

    async def _stop_and_clear_slot() -> None:
        """Simulate the done-callback popping the task slot mid-stop."""
        factory.process_tasks.pop("r2", None)

    factory.started_processes["r2"] = SimpleNamespace(stop=_stop_and_clear_slot)
    await factory._reap_superseded_instance("r2")
    assert "r2" not in factory.process_tasks
    assert "r2" not in factory.started_processes


@pytest.mark.asyncio
async def test_cleanup_task_tracking_returns_early_on_foreign_task() -> None:
    """Cleanup with a foreign stored task must not pop the successor.

    Given: ``process_tasks`` holds a successor task,
    When: ``_cleanup_task_tracking`` runs for a different task,
    Then: Every tracking entry survives (defense in depth behind the
        superseded guard).
    """
    factory = _make_factory()
    successor: asyncio.Task[None] = asyncio.create_task(asyncio.sleep(60))
    old = await _finished_task()
    await asyncio.sleep(0)
    factory.process_tasks["f"] = cast(asyncio.Task[object], successor)
    factory.started_processes["f"] = object()
    factory.process_lifecycles["f"] = ProcessLifecycleEnum.LONG_RUNNING
    factory.process_roles["f"] = ProcessRoleEnum.CORE
    factory._cleanup_task_tracking("f", old)
    assert factory.process_tasks.get("f") is successor
    assert "f" in factory.started_processes
    assert "f" in factory.process_lifecycles
    assert "f" in factory.process_roles
    successor.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await successor


@pytest.mark.asyncio
async def test_superseded_generator_exit_skips_even_own_run_finalize() -> None:
    """A superseded GeneratorExit completion finalizes nothing.

    Given: A superseded predecessor task that raised ``GeneratorExit``
        (the should-not-finalize family),
    When: Its completion handler runs,
    Then: Neither the by-name nor the per-run finalizer is invoked.
    """
    factory = _make_factory()

    async def _gen_exit() -> None:
        raise GeneratorExit()

    old = asyncio.create_task(_gen_exit())
    with contextlib.suppress(GeneratorExit):
        await old
    successor: asyncio.Task[None] = asyncio.create_task(asyncio.sleep(60))
    await asyncio.sleep(0)
    factory.process_tasks["g"] = cast(asyncio.Task[object], successor)
    finalize_by_name = AsyncMock()
    finalize_superseded = AsyncMock()
    with (
        mock.patch.object(factory, "_finalize_process_run", finalize_by_name),
        mock.patch.object(factory, "_finalize_superseded_run", finalize_superseded),
    ):
        await factory._handle_task_completion("g", old, "run-old")
    finalize_by_name.assert_not_awaited()
    finalize_superseded.assert_not_awaited()
    assert factory.process_tasks.get("g") is successor
    successor.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await successor


@pytest.mark.asyncio
async def test_current_strategy_task_completion_emits_strategy_list() -> None:
    """An owned STRATEGY task completion refreshes the strategy list.

    Given: A completed strategy task that still owns its tracking slot,
    When: Its completion handler runs the current (non-superseded) path,
    Then: The strategy-list snapshot emit fires after cleanup.
    """
    factory = _make_factory()
    task = await _finished_task()
    factory.process_tasks["st"] = cast(asyncio.Task[object], task)
    factory.started_processes["st"] = object()
    factory.process_lifecycles["st"] = ProcessLifecycleEnum.LONG_RUNNING
    factory.process_roles["st"] = ProcessRoleEnum.STRATEGY
    strategy_emit = AsyncMock()
    with (
        mock.patch.object(factory, "_finalize_process_run", AsyncMock()),
        mock.patch.object(factory, "_maybe_schedule_restart", AsyncMock()),
        mock.patch.object(factory, "_emit_strategy_list_snapshot", strategy_emit),
    ):
        await factory._handle_task_completion("st", task, "run-st")
    strategy_emit.assert_awaited_once()
    assert "st" not in factory.process_tasks


@pytest.mark.asyncio
async def test_current_completion_logs_unexpected_handler_error() -> None:
    """An exception inside the owned completion path is logged, not raised.

    Given: A current-path completion whose restart decision raises,
    When: ``_handle_task_completion`` runs,
    Then: The handler swallows and logs the error instead of propagating
        (the raise inside ``finally`` skips the tracking cleanup, so the
        slot deliberately stays for a later stop to reap).
    """
    factory = _make_factory()
    task = await _finished_task()
    factory.process_tasks["x"] = cast(asyncio.Task[object], task)
    factory.started_processes["x"] = object()
    factory.process_lifecycles["x"] = ProcessLifecycleEnum.LONG_RUNNING
    factory.process_roles["x"] = ProcessRoleEnum.CORE
    with (
        mock.patch.object(factory, "_finalize_process_run", AsyncMock()),
        mock.patch.object(
            factory, "_maybe_schedule_restart", AsyncMock(side_effect=RuntimeError("boom"))
        ),
    ):
        await factory._handle_task_completion("x", task, "run-x")
    assert factory.process_tasks.get("x") is task


@pytest.mark.asyncio
async def test_try_chain_result_refuses_when_active_run_belongs_to_successor() -> None:
    """Run identity blocks chaining even while the old task owns the slot.

    Given: A done predecessor still holding ``process_tasks[name]`` while
        ``active_runs[name]`` already carries the successor's run id,
    When: The predecessor's chained task and coroutine results arrive,
    Then: Both are refused and cancelled/closed; nothing is re-tracked.
    """
    factory = _make_factory()
    old = await _finished_task()
    factory.process_tasks["cw"] = cast(asyncio.Task[object], old)
    factory.active_runs["cw"] = "run-B"
    stale_inner: asyncio.Task[None] = asyncio.create_task(asyncio.sleep(60))
    await asyncio.sleep(0)
    assert factory._try_chain_result("cw", stale_inner, old, "run-A") is False
    await asyncio.sleep(0)
    assert stale_inner.cancelled()
    stale_coro = asyncio.sleep(60)
    assert factory._try_chain_result("cw", stale_coro, old, None) is False
    assert factory.process_tasks.get("cw") is old


@pytest.mark.asyncio
async def test_reap_detaches_done_tracked_slot_without_instance() -> None:
    """A done-but-tracked slot is detached even with no instance present.

    Given: Only a DONE task occupies ``process_tasks[name]`` (its pending
        done-callback could otherwise chain into the successor's window),
    When: ``_reap_superseded_instance`` runs,
    Then: The slot is cleared, nothing is cancelled, and the dangling run
        row is finalized.
    """
    factory = _make_factory()
    done_task = await _finished_task()
    factory.process_tasks["dd"] = cast(asyncio.Task[object], done_task)
    factory.active_runs["dd"] = "run-D"
    await factory._reap_superseded_instance("dd")
    assert "dd" not in factory.process_tasks
    assert "dd" not in factory.active_runs
    assert "run-D" in factory._finalized_run_ids


@pytest.mark.asyncio
async def test_reap_clears_done_but_tracked_task_alongside_instance() -> None:
    """A done-but-still-tracked task is cleared by the tail pops.

    Given: A DONE predecessor task still tracked plus a healthy instance,
    When: ``_reap_superseded_instance`` runs,
    Then: Both slots are cleared without cancelling anything.
    """
    factory = _make_factory()
    done_task = await _finished_task()
    factory.process_tasks["dt"] = cast(asyncio.Task[object], done_task)
    factory.started_processes["dt"] = SimpleNamespace(stop=AsyncMock())
    await factory._reap_superseded_instance("dt")
    assert "dt" not in factory.process_tasks
    assert "dt" not in factory.started_processes


@pytest.mark.asyncio
async def test_maybe_schedule_restart_drops_stale_decision_for_live_successor() -> None:
    """A stale death decision is dropped when a successor owns the name.

    Given: desired-RUNNING plus a restartable config, and a LIVE successor
        task (then instance) tracked under the name,
    When: ``_maybe_schedule_restart`` runs on behalf of the dead
        predecessor lineage,
    Then: No delayed restart is scheduled and no watchdog state is
        touched, while the same call passing the successor identities
        does schedule one.
    """
    factory = _make_factory()
    config = _make_config("n")
    factory._desired_state["n"] = _DesiredState.RUNNING
    factory._restart_configs["n"] = config
    old_task = await _cancelled_task()
    successor: asyncio.Task[None] = asyncio.create_task(asyncio.sleep(60))
    await asyncio.sleep(0)
    factory.process_tasks["n"] = cast(asyncio.Task[object], successor)
    await factory._maybe_schedule_restart("n", ProcessRunStatusEnum.FAILED, completed_task=old_task)
    assert "n" not in factory._restart_tasks
    factory.process_tasks.pop("n")
    successor_instance = object()
    factory.started_processes["n"] = successor_instance
    await factory._maybe_schedule_restart(
        "n", ProcessRunStatusEnum.FAILED, completed_task=old_task, completed_instance=None
    )
    assert "n" not in factory._restart_tasks
    await factory._maybe_schedule_restart(
        "n",
        ProcessRunStatusEnum.FAILED,
        completed_task=None,
        completed_instance=successor_instance,
    )
    assert "n" in factory._restart_tasks
    pending = factory._restart_tasks.pop("n")
    pending.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await pending
    successor.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await successor


@pytest.mark.asyncio
async def test_done_tracked_task_callback_in_successor_record_window_is_superseded() -> None:
    """Run identity outranks slot identity in supersession detection.

    Given: A done predecessor task STILL holding ``process_tasks[name]``
        while ``active_runs[name]`` already carries the successor's run
        id (the window between the successor's run-record insert and its
        task-slot overwrite),
    When: The predecessor's completion handler runs,
    Then: It is classified superseded — the successor's fresh run row is
        not finalized by name and the slot state is untouched.
    """
    factory = _make_factory()
    old_task = await _finished_task()
    factory.process_tasks["w2"] = cast(asyncio.Task[object], old_task)
    factory.active_runs["w2"] = "run-B"
    finalize_by_name = AsyncMock()
    finalize_superseded = AsyncMock()
    restart_mock = AsyncMock()
    with (
        mock.patch.object(factory, "_finalize_process_run", finalize_by_name),
        mock.patch.object(factory, "_finalize_superseded_run", finalize_superseded),
        mock.patch.object(factory, "_maybe_schedule_restart", restart_mock),
    ):
        await factory._handle_task_completion("w2", old_task, "run-A")
    finalize_by_name.assert_not_awaited()
    restart_mock.assert_not_awaited()
    finalize_superseded.assert_awaited_once()
    assert factory.active_runs.get("w2") == "run-B"
    assert factory.process_tasks.get("w2") is old_task


@pytest.mark.asyncio
async def test_record_failed_predecessor_callback_cannot_finalize_successor_run() -> None:
    """A lineage with no persisted run is superseded by any populated run.

    Given: A done predecessor task still in the slot whose own run record
        FAILED to persist (run id None), while ``active_runs[name]``
        already carries the successor's run id,
    When: The predecessor's completion handler runs,
    Then: It is classified superseded and the successor's row survives.
    """
    factory = _make_factory()
    old_task = await _finished_task()
    factory.process_tasks["w3"] = cast(asyncio.Task[object], old_task)
    factory.active_runs["w3"] = "run-B"
    finalize_by_name = AsyncMock()
    finalize_superseded = AsyncMock()
    with (
        mock.patch.object(factory, "_finalize_process_run", finalize_by_name),
        mock.patch.object(factory, "_finalize_superseded_run", finalize_superseded),
        mock.patch.object(factory, "_maybe_schedule_restart", AsyncMock()),
    ):
        await factory._handle_task_completion("w3", old_task, None)
    finalize_by_name.assert_not_awaited()
    assert factory.active_runs.get("w3") == "run-B"


@pytest.mark.asyncio
async def test_stale_generation_cannot_tombstone_failed_successor_config() -> None:
    """A reaped lineage's late completion cannot outlive a newer launch.

    Given: desired-RUNNING with the (failed) successor's ON_FAILURE
        config armed, NO live tracking (the successor start failed
        before installing any), and a watchdog call carrying the
        predecessor's OLD launch generation,
    When: ``_maybe_schedule_restart`` runs,
    Then: The decision is dropped — no terminal-no-restart tombstone, no
        watchdog-state clear — while the CURRENT generation still
        proceeds normally.
    """
    factory = _make_factory()
    config = _make_config("fg")
    factory._desired_state["fg"] = _DesiredState.RUNNING
    factory._restart_configs["fg"] = config
    factory._launch_generation["fg"] = 2
    await factory._maybe_schedule_restart("fg", ProcessRunStatusEnum.CANCELLED, launch_generation=1)
    assert "fg" not in factory._terminal_no_restart_generation
    assert factory._desired_state.get("fg") is _DesiredState.RUNNING
    await factory._maybe_schedule_restart("fg", ProcessRunStatusEnum.CANCELLED, launch_generation=2)
    assert "fg" in factory._terminal_no_restart_generation


@pytest.mark.asyncio
async def test_launch_generation_increments_per_start_and_stamps_lineage() -> None:
    """Every start attempt bumps the per-name generation and stamps it.

    Given: Two consecutive starts of the same name,
    When: Each start registers its completion,
    Then: The per-name counter advances so the first lineage's
        generation is stale for the second.
    """
    factory = _make_factory()
    factory._create_process_run_record = AsyncMock(side_effect=["run-1", "run-2"])
    factory.import_class = lambda path, name=None, template=None: _LongRunningProcess
    config = _make_config("lg")
    await factory.start_process(config)
    assert factory._launch_generation["lg"] == 1
    await factory.stop_process_by_name("lg")
    await factory.start_process(config)
    assert factory._launch_generation["lg"] == 2
    await factory.stop_process_by_name("lg")
    await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_stale_generation_skips_cleanup_sparing_native_successor() -> None:
    """A generation gone stale mid-finalize cannot evict a native successor.

    Given: A completion that passed the entry guard (nothing tracked,
        empty active runs) whose awaited finalize lets a PROCESS-mode
        successor register (bumping the launch generation) — a successor
        with NO task slot,
    When: The handler's finally block runs,
    Then: The tracking cleanup is skipped and the successor's
        ``started_processes`` entry survives.
    """
    factory = _make_factory()
    old_task = await _finished_task()
    factory._launch_generation["nv"] = 1
    successor_instance = object()

    async def _successor_lands_mid_finalize(
        name: str,
        status: ProcessRunStatusEnum,
        result: object = None,
        error: str | None = None,
        exit_code: int | None = None,
    ) -> None:
        """Simulate a native successor start landing inside the await."""
        del name, status, result, error, exit_code
        factory._launch_generation["nv"] = 2
        factory.started_processes["nv"] = successor_instance

    with (
        mock.patch.object(factory, "_finalize_process_run", _successor_lands_mid_finalize),
        mock.patch.object(factory, "_maybe_schedule_restart", AsyncMock()),
    ):
        await factory._handle_task_completion("nv", old_task, None, 1)
    assert factory.started_processes.get("nv") is successor_instance


@pytest.mark.asyncio
async def test_finalize_process_run_skips_already_finalized_id() -> None:
    """The by-name finalizer no-ops on a row another path already closed.

    Given: ``active_runs`` points at a run id already in the finalized
        set (a superseded finalize won the race),
    When: ``_finalize_process_run`` runs,
    Then: The slot is cleared but no second DB update or event fires.
    """
    factory = _make_factory()
    factory.active_runs["p"] = "run-9"
    factory._mark_run_finalized("run-9")
    await factory._finalize_process_run("p", ProcessRunStatusEnum.CANCELLED)
    assert "p" not in factory.active_runs
    recorder = cast(SimpleNamespace, factory._run_recorder)
    recorder.update_run_record.assert_not_awaited()


@pytest.mark.asyncio
async def test_finalizers_unmark_run_id_when_update_fails() -> None:
    """A failed durable update releases the finalized-id claim.

    Given: A run recorder whose update raises,
    When: Either finalizer runs,
    Then: The exception propagates and the id leaves the finalized set,
        so a later retry is not suppressed.
    """
    factory = _make_factory()
    recorder = cast(SimpleNamespace, factory._run_recorder)
    recorder.update_run_record.side_effect = RuntimeError("db down")
    factory.active_runs["p"] = "run-a"
    with pytest.raises(RuntimeError, match="db down"):
        await factory._finalize_process_run("p", ProcessRunStatusEnum.CANCELLED)
    assert "run-a" not in factory._finalized_run_ids
    with pytest.raises(RuntimeError, match="db down"):
        await factory._finalize_superseded_run("p", "run-b", ProcessRunStatusEnum.CANCELLED)
    assert "run-b" not in factory._finalized_run_ids


@pytest.mark.asyncio
async def test_rapid_bounce_keeps_exactly_one_reachable_instance() -> None:
    """End-to-end bounce regression for the 2026-07-08 zombie incident.

    Given: A real start → stop → start sequence through the launcher's
        own wiring (done-callbacks included),
    When: The predecessor's late completion callback fires after the
        successor started,
    Then: The successor's tracking and active run survive, and a final
        stop reaches the successor (SUCCESS, not NOT_RUNNING) leaving no
        live instance behind.
    """
    factory = _make_factory()
    factory._create_process_run_record = AsyncMock(side_effect=["run-A", "run-B"])
    factory.import_class = lambda path, name=None, template=None: _LongRunningProcess
    config = _make_config("e")
    await factory.start_process(config)
    task_a = factory.process_tasks["e"]
    await factory.stop_process_by_name("e")
    assert task_a.cancelled()
    await factory.start_process(config)
    task_b = factory.process_tasks["e"]
    assert task_b is not task_a
    await asyncio.sleep(0.05)
    assert factory.process_tasks.get("e") is task_b
    assert isinstance(factory.started_processes.get("e"), _LongRunningProcess)
    assert factory.active_runs.get("e") == "run-B"
    result = await factory.stop_process_by_name("e")
    assert result.status is StopProcessStatusEnum.SUCCESS
    assert "e" not in factory.started_processes
    assert "e" not in factory.process_tasks
    await asyncio.sleep(0.05)
