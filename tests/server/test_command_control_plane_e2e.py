"""End-to-end cross-slice verification of the HMAC control-plane nudge/ack path.

Each control-plane slice is unit-tested in isolation against its own assumptions;
this test wires the API side (:class:`ProcessCommandAckRegistry`, P2.4) to the
coordinator side (:class:`ProcessCommandListener`, P2.3) in-process and drives one
full round trip, so a drift in ANY inter-slice contract fails here:

- the shared signing key (both derive ``command_signing_key(master_password)`` — the
  single env secret every container has),
- the canonical signed form (API signs ``model_dump(mode="json")``; coordinator
  verifies ``json.loads(payload)`` — both must exclude ``signature`` + ``topic``),
- the topic slug (API publishes ``processes.commands.{coordinator}``; coordinator
  self-filters on its signed ``coordinator`` == own slug),
- the ``command_id`` round trip (minted by the API, echoed in the coordinator's ack,
  matched back to the pending future),
- the signed ack (coordinator signs; API verifies before resolving the future).

The in-process wiring mirrors production faithfully: the API is one container
(coord-0) and the coordinator another (coord-2), but both share ``master_password``
via env, exactly as modelled here.
"""

import asyncio
from datetime import UTC
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from uuid import uuid7

import pytest

from snapper.application.process_manager.command_listener import ProcessCommandListener
from snapper.application.process_manager.launcher import ProcessLauncherService
from snapper.application.process_manager.models import ProcessConfigModel
from snapper.config.app import AppSettings
from snapper.config.bootstrap import BootstrapSettingsLoader
from snapper.core.types import ProcessRoleEnum
from snapper.messaging.schemas.data import ProcessCommandData
from snapper.messaging.security.command_signing import command_signing_key
from snapper.messaging.security.command_signing import sign_command_payload
from snapper.server.command_ack_registry import ProcessCommandAckRegistry

_MASTER = "shared-master-secret"


class _DummySettingsService:
    """Stub settings service that returns defaults for any key."""

    def get_setting(self, key: str, default: Any) -> Any:
        """Return the provided default for any key.

        Args:
            key: Setting key (ignored).
            default: Value to return.

        Returns:
            The provided default.
        """
        return default


def _coordinator_launcher(reconcile: AsyncMock) -> MagicMock:
    """Build a mock coordinator launcher (slug coord-2, wired publisher).

    Args:
        reconcile: The reconcile coroutine mock to observe.

    Returns:
        A MagicMock standing in for the coordinator's ProcessLauncherService.
    """
    launcher = MagicMock()
    launcher.coordinator_topic_slug.return_value = "coord-2"
    launcher.settings.master_password = _MASTER
    launcher.reconcile_desired_state = reconcile
    tracker = MagicMock()
    tracker.session_id = "coord-session"
    tracker.next_sequence.return_value = 7
    publisher = MagicMock()
    publisher.tracker = tracker
    launcher.message_publisher = publisher

    return launcher


@pytest.mark.asyncio
async def test_full_nudge_reconcile_ack_roundtrip_resolves_the_patch_future() -> None:
    """A signed nudge reconciles the coordinator and its signed ack resolves the future.

    Given: an API ack-registry and a coordinator listener sharing the master
        password, wired so the API's publish delivers to the coordinator and the
        coordinator's ack delivers back to the registry,
    When: the API nudges the coordinator for a restart,
    Then: the coordinator verified it, reconciled once, and its signed ack
        resolved the API's pending future with 'applied' for the same command id.
    """
    reconcile = AsyncMock()
    launcher = _coordinator_launcher(reconcile)
    listener = ProcessCommandListener(launcher)
    registry = ProcessCommandAckRegistry(command_signing_key(_MASTER), ack_timeout_s=0.05)

    async def _coordinator_publishes_ack(topic: str, ack: object) -> None:
        assert topic == "processes.events.command_ack.coord-2"
        registry._resolve_ack(ack.to_json())

    launcher.message_publisher.send = AsyncMock(side_effect=_coordinator_publishes_ack)

    async def _api_publishes_command(topic: str, command: object) -> None:
        assert topic == "processes.commands.coord-2"
        await listener._handle_frame(command.to_json())

    api_publisher = MagicMock()
    api_tracker = MagicMock()
    api_tracker.session_id = "api-session"
    api_tracker.next_sequence.return_value = 1
    api_publisher.tracker = api_tracker
    api_publisher.send = AsyncMock(side_effect=_api_publishes_command)

    ack = await registry.nudge(
        api_publisher,
        coordinator="coord-2",
        process_name="strategy_macd",
        action="restart",
        issued_by="alice",
    )

    reconcile.assert_awaited_once()
    assert ack is not None
    assert ack.status == "applied"
    assert ack.coordinator == "coord-2"
    assert ack.process_name == "strategy_macd"


@pytest.mark.asyncio
async def test_nudge_for_the_wrong_coordinator_is_rejected_and_times_out() -> None:
    """A nudge whose slug the coordinator does not own is dropped, so the API times out.

    Given: the same wiring but the API addresses coord-9 (not the listener's coord-2),
    When: the API nudges,
    Then: the coordinator drops it (no reconcile, no ack) and the API's short-timeout
        nudge returns None (reconcile-pending fallback).
    """
    reconcile = AsyncMock()
    launcher = _coordinator_launcher(reconcile)
    listener = ProcessCommandListener(launcher)
    registry = ProcessCommandAckRegistry(command_signing_key(_MASTER))
    launcher.message_publisher.send = AsyncMock()

    async def _api_publishes_command(_topic: str, command: object) -> None:
        await listener._handle_frame(command.to_json())

    api_publisher = MagicMock()
    api_tracker = MagicMock()
    api_tracker.session_id = "api-session"
    api_tracker.next_sequence.return_value = 1
    api_publisher.tracker = api_tracker
    api_publisher.send = AsyncMock(side_effect=_api_publishes_command)

    ack = await registry.nudge(
        api_publisher,
        coordinator="coord-9",
        process_name="strategy_macd",
        action="restart",
        issued_by="alice",
    )

    assert ack is None
    reconcile.assert_not_awaited()
    launcher.message_publisher.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_nudge_signed_with_a_different_master_is_rejected() -> None:
    """A key mismatch between the API and the coordinator fails verification, so it times out.

    Given: the API registry keyed by a DIFFERENT master password than the coordinator,
    When: the API nudges,
    Then: the coordinator's signature verification fails, it drops the command, and the
        API times out to None — proving the HMAC gate actually blocks a wrong key.
    """
    reconcile = AsyncMock()
    launcher = _coordinator_launcher(reconcile)
    listener = ProcessCommandListener(launcher)
    registry = ProcessCommandAckRegistry(
        command_signing_key("a-different-master"), ack_timeout_s=0.05
    )
    launcher.message_publisher.send = AsyncMock()

    async def _api_publishes_command(_topic: str, command: object) -> None:
        await listener._handle_frame(command.to_json())

    api_publisher = MagicMock()
    api_tracker = MagicMock()
    api_tracker.session_id = "api-session"
    api_tracker.next_sequence.return_value = 1
    api_publisher.tracker = api_tracker
    api_publisher.send = AsyncMock(side_effect=_api_publishes_command)

    ack = await registry.nudge(
        api_publisher,
        coordinator="coord-2",
        process_name="strategy_macd",
        action="restart",
        issued_by="alice",
    )

    assert ack is None
    reconcile.assert_not_awaited()


@pytest.mark.asyncio
async def test_dropped_nudge_is_converged_by_a_reconcile_pass() -> None:
    """§8 dropped-nudge: a nudge that times out to None is still applied by a reconcile pass.

    Given: an API nudge whose publisher delivers nowhere, so no ack ever arrives,
    When: the short-timeout nudge returns None and a reconcile pass then runs on
        a REAL launcher holding an enabled-but-not-started config,
    Then: the nudge is dropped (None) yet the reconcile pass starts the process,
        so the dropped nudge still converges via the periodic safety net.
    """
    registry = ProcessCommandAckRegistry(command_signing_key(_MASTER), ack_timeout_s=0.01)
    api_publisher = MagicMock()
    api_tracker = MagicMock()
    api_tracker.session_id = "api-session"
    api_tracker.next_sequence.return_value = 1
    api_publisher.tracker = api_tracker
    api_publisher.send = AsyncMock()
    dropped = await registry.nudge(
        api_publisher,
        coordinator="coord-2",
        process_name="p",
        action="restart",
        issued_by="api",
    )
    assert dropped is None

    launcher = ProcessLauncherService(
        AppSettings(BootstrapSettingsLoader(), _DummySettingsService())
    )
    launcher.start_process = AsyncMock()
    launcher._start_native_process_monitoring = MagicMock()
    launcher.autostart_includes = MagicMock(return_value=True)
    cfg = ProcessConfigModel(
        name="p",
        enabled=True,
        mode="thread",
        class_path="x.Y",
        method="start",
        parameters={},
        role=ProcessRoleEnum.CORE,
        tags=(),
    )
    launcher.get_process_configs = AsyncMock(return_value=[cfg])
    await launcher.reconcile_desired_state()
    launcher.start_process.assert_awaited_once()


def _core_config(
    name: str, *, enabled: bool, restart_nonce: str | None = None
) -> ProcessConfigModel:
    """Build a minimal CORE-role config for the reconcile safety-net tests.

    CORE keeps :meth:`_prepare_owned_config_for_start` out of the strategy
    scope resolver (which would otherwise reach for a real repository).

    Args:
        name: Process name.
        enabled: Persisted desired-state enabled flag.
        restart_nonce: Optional persisted operator restart nonce.

    Returns:
        A populated ProcessConfigModel.
    """
    return ProcessConfigModel(
        name=name,
        enabled=enabled,
        mode="thread",
        class_path="x.Y",
        method="start",
        parameters={},
        role=ProcessRoleEnum.CORE,
        tags=(),
        restart_nonce=restart_nonce,
    )


def _real_launcher(config: ProcessConfigModel) -> ProcessLauncherService:
    """Build a REAL launcher owning ``config`` with the spawn primitives stubbed.

    The launcher keeps its real reconcile machinery (locks, nonce ledger,
    desired-state matrix); only the process-spawning edges are mocked so a
    pass is observable without launching anything.

    Args:
        config: The single config the desired-state read returns.

    Returns:
        A ProcessLauncherService ready for reconcile-pass tests.
    """
    launcher = ProcessLauncherService(
        AppSettings(BootstrapSettingsLoader(), _DummySettingsService())
    )
    launcher.start_process = AsyncMock()
    launcher._start_native_process_monitoring = MagicMock()
    launcher.autostart_includes = MagicMock(return_value=True)
    launcher.get_process_configs = AsyncMock(return_value=[config])

    return launcher


def _lossy_nudge_publisher() -> MagicMock:
    """Build an API-side publisher whose send delivers NOWHERE (a dropped frame).

    Returns:
        A MagicMock publisher with a tracker and a no-op async send.
    """
    api_publisher = MagicMock()
    api_tracker = MagicMock()
    api_tracker.session_id = "api-session"
    api_tracker.next_sequence.return_value = 1
    api_publisher.tracker = api_tracker
    api_publisher.send = AsyncMock()

    return api_publisher


@pytest.mark.asyncio
async def test_tick_and_concurrent_nudge_spawn_once_and_ack_applied() -> None:
    """§8 tick-vs-nudge: a tick pass and a concurrent signed nudge spawn exactly once.

    Given: a REAL launcher whose delayed spawn registers the process mid-pass,
        a REAL listener bound to that launcher's own signing key and slug, and
        a signed fresh restart command addressed to this coordinator,
    When: the periodic tick pass and the nudge frame are driven concurrently
        under a hard deadline (a lock-ordering regression would deadlock here),
    Then: both entry points complete, TWO reconcile passes ran (the desired
        state is re-read once per pass, so get_process_configs is awaited
        exactly twice — a listener that acks without reconciling fails here),
        the process is spawned exactly once, the nudge still acks 'applied'
        (not 'rejected') on the coordinator's ack topic, and both the
        reconcile lock and the per-name lock are released.
    """
    launcher = _real_launcher(_core_config("p", enabled=True))

    async def _delayed_spawn(config: ProcessConfigModel) -> None:
        await asyncio.sleep(0.02)
        launcher.started_processes[config.name] = MagicMock()

    launcher.start_process = AsyncMock(side_effect=_delayed_spawn)
    ack_tracker = MagicMock()
    ack_tracker.session_id = "coord-session"
    ack_tracker.next_sequence.return_value = 1
    ack_publisher = MagicMock()
    ack_publisher.tracker = ack_tracker
    ack_publisher.send = AsyncMock()
    launcher.set_msg_publisher(ack_publisher)
    listener = ProcessCommandListener(launcher)
    key = command_signing_key(launcher.settings.master_password)
    command = ProcessCommandData(
        session_id="api-session",
        sequence_id=1,
        public_id=str(uuid7()),
        timestamp=datetime.now(UTC),
        command_id=str(uuid7()),
        coordinator=launcher.coordinator_topic_slug(),
        process_name="p",
        action="restart",
        issued_by="api",
        issued_at=datetime.now(UTC),
        signature="",
    )
    signed = command.model_copy(
        update={"signature": sign_command_payload(command.model_dump(mode="json"), key)}
    )

    await asyncio.wait_for(
        asyncio.gather(
            launcher.reconcile_desired_state(), listener._handle_frame(signed.to_json())
        ),
        timeout=2.0,
    )

    assert launcher.get_process_configs.await_count == 2
    assert launcher.start_process.await_count == 1
    ack_publisher.send.assert_awaited_once()
    topic, ack = ack_publisher.send.await_args.args
    assert topic == f"processes.events.command_ack.{launcher.coordinator_topic_slug()}"
    assert ack.status == "applied"
    assert launcher._reconcile_lock.locked() is False
    assert launcher._restart_lock_for("p").locked() is False
    assert "p" in launcher.started_processes


@pytest.mark.asyncio
async def test_dropped_restart_nudge_is_converged_by_a_single_reconcile_pass() -> None:
    """§8 dropped-nudge (restart): a lost restart nudge is bounced by one periodic tick.

    Given: a restart nudge whose frame delivers nowhere (the API times out to
        None) and a REAL launcher running 'p' whose DB restart nonce advanced
        beyond the recorded applied baseline,
    When: exactly ONE reconcile pass runs with the listener never invoked,
    Then: the pass stops then restarts the process and records the new nonce
        as applied — the dropped nudge's action lands within a single tick.
    """
    registry = ProcessCommandAckRegistry(command_signing_key(_MASTER), ack_timeout_s=0.01)
    dropped = await registry.nudge(
        _lossy_nudge_publisher(),
        coordinator="coord-2",
        process_name="p",
        action="restart",
        issued_by="api",
    )
    assert dropped is None

    launcher = _real_launcher(_core_config("p", enabled=True, restart_nonce="n-new"))
    launcher.started_processes["p"] = MagicMock()
    launcher._last_applied_restart_nonce["p"] = "n-old"

    async def _stop(name: str) -> None:
        launcher.started_processes.pop(name, None)

    launcher.stop_process_by_name = AsyncMock(side_effect=_stop)

    await launcher.reconcile_desired_state()

    launcher.stop_process_by_name.assert_awaited_once_with("p")
    launcher.start_process.assert_awaited_once()
    assert launcher._last_applied_restart_nonce["p"] == "n-new"


@pytest.mark.asyncio
async def test_dropped_disable_nudge_is_converged_by_a_single_reconcile_pass() -> None:
    """§8 dropped-nudge (disable): a lost disable nudge is stopped by one periodic tick.

    Given: a disable nudge whose frame delivers nowhere (the API times out to
        None) and a REAL launcher still running 'q' while the DB desired-state
        says disabled,
    When: exactly ONE reconcile pass runs with the listener never invoked,
    Then: the pass stops the process and starts nothing — the dropped nudge's
        action lands within a single tick.
    """
    registry = ProcessCommandAckRegistry(command_signing_key(_MASTER), ack_timeout_s=0.01)
    dropped = await registry.nudge(
        _lossy_nudge_publisher(),
        coordinator="coord-2",
        process_name="q",
        action="disable",
        issued_by="api",
    )
    assert dropped is None

    launcher = _real_launcher(_core_config("q", enabled=False))
    launcher.started_processes["q"] = MagicMock()

    async def _stop(name: str) -> None:
        launcher.started_processes.pop(name, None)

    launcher.stop_process_by_name = AsyncMock(side_effect=_stop)

    await launcher.reconcile_desired_state()

    launcher.stop_process_by_name.assert_awaited_once_with("q")
    launcher.start_process.assert_not_awaited()
