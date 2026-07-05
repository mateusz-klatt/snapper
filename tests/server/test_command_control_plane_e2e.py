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

from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.process_manager.command_listener import ProcessCommandListener
from snapper.messaging.security.command_signing import command_signing_key
from snapper.server.command_ack_registry import ProcessCommandAckRegistry

_MASTER = "shared-master-secret"


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
