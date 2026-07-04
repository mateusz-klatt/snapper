"""Tests for the API-side process-command ack registry (control plane P2.4)."""

import asyncio
import json
from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch
from uuid import uuid7

import pytest

from snapper.messaging.schemas.data import ProcessCommandAckData
from snapper.messaging.security.command_signing import command_signing_key
from snapper.messaging.security.command_signing import sign_command_payload
from snapper.messaging.security.command_signing import verify_command_payload
from snapper.server import command_ack_registry as mod
from snapper.server.command_ack_registry import ProcessCommandAckRegistry

_KEY = command_signing_key("master")


def _signed_ack(
    *,
    command_id: str = "cmd-1",
    status: str = "applied",
    coordinator: str = "coord-2",
    process_name: str = "strategy_x",
) -> str:
    """Build a signed ProcessCommandAckData JSON payload.

    Args:
        command_id: The acked command id.
        status: The ack status.
        coordinator: The acking coordinator slug.
        process_name: The acked process name.

    Returns:
        The signed ack serialized as JSON.
    """
    ack = ProcessCommandAckData(
        session_id="s",
        sequence_id=1,
        public_id=str(uuid7()),
        timestamp=datetime.now(UTC),
        command_id=command_id,
        coordinator=coordinator,
        process_name=process_name,
        status=status,
        signature="",
    )
    signed = ack.model_copy(
        update={"signature": sign_command_payload(ack.model_dump(mode="json"), _KEY)}
    )

    return signed.to_json()


class TestRegistry:
    """Tests for register / unregister and the saturation bound."""

    @pytest.mark.asyncio
    async def test_register_returns_a_future(self) -> None:
        """Register returns an awaitable future for the command id."""
        registry = ProcessCommandAckRegistry(_KEY)
        future = registry.register("cmd-1", coordinator="coord-2", process_name="strategy_x")
        assert isinstance(future, asyncio.Future)
        assert not future.done()

    @pytest.mark.asyncio
    async def test_register_returns_none_when_saturated(self) -> None:
        """Register returns None once the pending bound is reached."""
        registry = ProcessCommandAckRegistry(_KEY)
        for index in range(mod._MAX_PENDING):
            registry.register(f"cmd-{index}", coordinator="coord-2", process_name="strategy_x")
        overflow = registry.register("overflow", coordinator="coord-2", process_name="strategy_x")
        assert overflow is None

    @pytest.mark.asyncio
    async def test_unregister_drops_the_future(self) -> None:
        """Unregister removes the pending future so a late ack cannot resolve it."""
        registry = ProcessCommandAckRegistry(_KEY)
        registry.register("cmd-1", coordinator="coord-2", process_name="strategy_x")
        registry.unregister("cmd-1")
        registry._resolve_ack(_signed_ack(command_id="cmd-1"))


class TestResolveAck:
    """Tests for signature-verified ack resolution."""

    @pytest.mark.asyncio
    async def test_valid_ack_resolves_the_matching_future(self) -> None:
        """A signed ack for a registered command resolves its future with the ack."""
        registry = ProcessCommandAckRegistry(_KEY)
        future = registry.register("cmd-1", coordinator="coord-2", process_name="strategy_x")
        assert future is not None

        registry._resolve_ack(_signed_ack(command_id="cmd-1", status="applied"))

        result = await future
        assert isinstance(result, ProcessCommandAckData)
        assert result.status == "applied"
        assert result.command_id == "cmd-1"

    @pytest.mark.asyncio
    async def test_ack_for_unknown_command_is_ignored(self) -> None:
        """An ack for a command id with no pending future is a no-op."""
        registry = ProcessCommandAckRegistry(_KEY)

        registry._resolve_ack(_signed_ack(command_id="unknown"))

    @pytest.mark.asyncio
    async def test_second_ack_does_not_re_resolve(self) -> None:
        """A second ack for an already-resolved future is ignored."""
        registry = ProcessCommandAckRegistry(_KEY)
        future = registry.register("cmd-1", coordinator="coord-2", process_name="strategy_x")
        assert future is not None
        registry._resolve_ack(_signed_ack(command_id="cmd-1"))
        await future

        registry._resolve_ack(_signed_ack(command_id="cmd-1", status="rejected"))

        assert (await future).status == "applied"

    @pytest.mark.asyncio
    async def test_malformed_json_ack_dropped(self) -> None:
        """A non-JSON ack does not resolve anything."""
        registry = ProcessCommandAckRegistry(_KEY)
        future = registry.register("cmd-1", coordinator="coord-2", process_name="strategy_x")
        assert future is not None

        registry._resolve_ack("not json{")

        assert not future.done()

    @pytest.mark.asyncio
    async def test_non_object_ack_dropped(self) -> None:
        """A JSON-array ack does not resolve anything."""
        registry = ProcessCommandAckRegistry(_KEY)
        future = registry.register("cmd-1", coordinator="coord-2", process_name="strategy_x")
        assert future is not None

        registry._resolve_ack("[1, 2, 3]")

        assert not future.done()

    @pytest.mark.asyncio
    async def test_bad_signature_ack_dropped(self) -> None:
        """An ack signed with the wrong key does not resolve the future."""
        registry = ProcessCommandAckRegistry(_KEY)
        future = registry.register("cmd-1", coordinator="coord-2", process_name="strategy_x")
        assert future is not None
        wrong = ProcessCommandAckData(
            session_id="s",
            sequence_id=1,
            public_id=str(uuid7()),
            timestamp=datetime.now(UTC),
            command_id="cmd-1",
            coordinator="coord-2",
            process_name="strategy_x",
            status="applied",
            signature="",
        )
        forged = wrong.model_copy(
            update={
                "signature": sign_command_payload(
                    wrong.model_dump(mode="json"), command_signing_key("attacker")
                )
            }
        )

        registry._resolve_ack(forged.to_json())

        assert not future.done()

    @pytest.mark.asyncio
    async def test_schema_invalid_ack_dropped(self) -> None:
        """A signed but schema-invalid ack does not resolve the future."""
        registry = ProcessCommandAckRegistry(_KEY)
        future = registry.register("cmd-1", coordinator="coord-2", process_name="strategy_x")
        assert future is not None
        raw = {"type": "process_command_ack", "command_id": "cmd-1", "signature": ""}
        raw["signature"] = sign_command_payload(raw, _KEY)

        registry._resolve_ack(json.dumps(raw))

        assert not future.done()


def _publisher() -> MagicMock:
    """Build a mock bus publisher with a tracker.

    Returns:
        A MagicMock publisher with a tracker and an async ``send``.
    """
    tracker = MagicMock()
    tracker.session_id = "s"
    tracker.next_sequence.return_value = 1
    publisher = MagicMock()
    publisher.tracker = tracker
    publisher.send = AsyncMock()

    return publisher


class TestResolveAckIdentity:
    """A signed ack resolves ONLY the nudge whose identity it matches."""

    @pytest.mark.asyncio
    async def test_ack_with_wrong_coordinator_is_dropped(self) -> None:
        """An ack whose coordinator differs from the pinned one never resolves.

        The registry subscribes to every coordinator's ack topic, so a
        bare command_id match would let any first-party key holder
        resolve any pending PATCH; the identity pin closes that.
        """
        registry = ProcessCommandAckRegistry(_KEY)
        future = registry.register("cmd-1", coordinator="coord-2", process_name="strategy_x")
        assert future is not None

        registry._resolve_ack(_signed_ack(command_id="cmd-1", coordinator="coord-9"))

        assert not future.done()

    @pytest.mark.asyncio
    async def test_ack_with_wrong_process_name_is_dropped(self) -> None:
        """An ack for a different process than the nudge targeted never resolves."""
        registry = ProcessCommandAckRegistry(_KEY)
        future = registry.register("cmd-1", coordinator="coord-2", process_name="strategy_x")
        assert future is not None

        registry._resolve_ack(_signed_ack(command_id="cmd-1", process_name="strategy_other"))

        assert not future.done()

    @pytest.mark.asyncio
    async def test_matching_identity_still_resolves(self) -> None:
        """The exact pinned coordinator+process pair resolves the future."""
        registry = ProcessCommandAckRegistry(_KEY)
        future = registry.register("cmd-1", coordinator="coord-2", process_name="strategy_x")
        assert future is not None

        registry._resolve_ack(
            _signed_ack(command_id="cmd-1", coordinator="coord-2", process_name="strategy_x")
        )

        assert (await future).command_id == "cmd-1"


class TestNudge:
    """Tests for the publish-and-await-ack nudge path."""

    @pytest.mark.asyncio
    async def test_nudge_returns_none_without_publisher(self) -> None:
        """A None publisher yields None (caller reconcile-pends)."""
        registry = ProcessCommandAckRegistry(_KEY)

        result = await registry.nudge(
            None, coordinator="coord-2", process_name="p", action="restart", issued_by="api"
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_nudge_publishes_signed_command_and_returns_ack(self) -> None:
        """Nudge publishes a signed command and returns the coordinator's ack.

        The mock publisher resolves the just-published command's future with a
        signed ack, so nudge returns it; the published command is verifiable.
        """
        registry = ProcessCommandAckRegistry(_KEY)
        publisher = _publisher()

        async def _send(_topic: str, command: ProcessCommandAckData) -> None:
            registry._resolve_ack(_signed_ack(command_id=command.command_id, status="applied"))

        publisher.send = AsyncMock(side_effect=_send)

        ack = await registry.nudge(
            publisher,
            coordinator="coord-2",
            process_name="strategy_x",
            action="restart",
            issued_by="alice",
        )

        assert ack is not None
        assert ack.status == "applied"
        topic, command = publisher.send.await_args.args
        assert topic == "processes.commands.coord-2"
        assert command.coordinator == "coord-2"
        assert command.action == "restart"
        command_dict = json.loads(command.to_json())
        command_dict["topic"] = topic
        assert verify_command_payload(command_dict, _KEY) is True

    @pytest.mark.asyncio
    async def test_nudge_times_out_to_none(self) -> None:
        """A nudge with no ack in time returns None."""
        registry = ProcessCommandAckRegistry(_KEY)

        result = await registry.nudge(
            _publisher(),
            coordinator="coord-2",
            process_name="p",
            action="restart",
            issued_by="api",
            timeout=0.01,
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_nudge_returns_none_on_publish_failure(self) -> None:
        """A publish failure is swallowed and yields None."""
        registry = ProcessCommandAckRegistry(_KEY)
        publisher = _publisher()
        publisher.send = AsyncMock(side_effect=RuntimeError("send boom"))

        result = await registry.nudge(
            publisher, coordinator="coord-2", process_name="p", action="restart", issued_by="api"
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_nudge_returns_none_when_saturated(self) -> None:
        """A saturated registry yields None without publishing."""
        registry = ProcessCommandAckRegistry(_KEY)
        for index in range(mod._MAX_PENDING):
            registry.register(f"cmd-{index}", coordinator="coord-2", process_name="strategy_x")
        publisher = _publisher()

        result = await registry.nudge(
            publisher, coordinator="coord-2", process_name="p", action="restart", issued_by="api"
        )

        assert result is None
        publisher.send.assert_not_awaited()


class TestLifecycle:
    """Tests for the subscriber lifecycle (ZMQ mocked)."""

    @pytest.mark.asyncio
    async def test_start_skips_on_empty_xpub(self) -> None:
        """An empty XPUB skips the listener entirely."""
        registry = ProcessCommandAckRegistry(_KEY)

        await registry.start("")

        assert registry._listen_task is None

    @pytest.mark.asyncio
    async def test_start_subscribes_and_stop_tears_down(self) -> None:
        """Start subscribes to the ack prefix; stop releases all resources."""
        registry = ProcessCommandAckRegistry(_KEY)
        fake_context = MagicMock()
        fake_context.socket.return_value = MagicMock()
        fake_subscriber = MagicMock()
        with (
            patch.object(mod.zmq.asyncio, "Context", return_value=fake_context),
            patch.object(mod, "apply_hwm"),
            patch.object(mod, "ValidatedSubscriber", return_value=fake_subscriber),
        ):
            await registry.start("tcp://broker:7501")
            fake_subscriber.subscribe.assert_called_once_with("processes.events.command_ack.")

            await registry.stop()
            fake_subscriber.close.assert_called_once()
            fake_context.term.assert_called_once()

    @pytest.mark.asyncio
    async def test_start_is_idempotent_while_running(self) -> None:
        """A second start while running is a no-op."""
        registry = ProcessCommandAckRegistry(_KEY)
        fake_context = MagicMock()
        fake_context.socket.return_value = MagicMock()
        with (
            patch.object(mod.zmq.asyncio, "Context", return_value=fake_context) as ctx,
            patch.object(mod, "apply_hwm"),
            patch.object(mod, "ValidatedSubscriber", return_value=MagicMock()),
        ):
            await registry.start("tcp://broker:7501")
            first = registry._listen_task
            await registry.start("tcp://broker:7501")
            assert registry._listen_task is first
            assert ctx.call_count == 1
            await registry.stop()

    @pytest.mark.asyncio
    async def test_start_reaps_and_reraises_on_setup_error(self) -> None:
        """A socket-setup error reaps partial resources and propagates."""
        registry = ProcessCommandAckRegistry(_KEY)
        fake_context = MagicMock()
        fake_context.socket.return_value = MagicMock()
        with (
            patch.object(mod.zmq.asyncio, "Context", return_value=fake_context),
            patch.object(mod, "apply_hwm"),
            patch.object(mod, "ValidatedSubscriber", side_effect=RuntimeError("socket boom")),
            pytest.raises(RuntimeError),
        ):
            await registry.start("tcp://broker:7501")
        assert registry._listen_task is None
        assert registry._subscriber is None

    @pytest.mark.asyncio
    async def test_recv_one_decodes_payload(self) -> None:
        """_recv_one decodes the payload bytes of a received frame."""
        registry = ProcessCommandAckRegistry(_KEY)
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(return_value=("topic", b'{"ok": 1}'))

        assert await registry._recv_one(subscriber) == '{"ok": 1}'

    @pytest.mark.asyncio
    async def test_recv_one_backs_off_on_error(self) -> None:
        """A recv error backs off and returns None."""
        registry = ProcessCommandAckRegistry(_KEY)
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=RuntimeError("recv boom"))
        with patch.object(mod.asyncio, "sleep", AsyncMock()) as sleep:
            assert await registry._recv_one(subscriber) is None
            sleep.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_recv_one_reraises_cancellation(self) -> None:
        """Cancellation during recv propagates."""
        registry = ProcessCommandAckRegistry(_KEY)
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=asyncio.CancelledError)
        with pytest.raises(asyncio.CancelledError):
            await registry._recv_one(subscriber)

    @pytest.mark.asyncio
    async def test_listen_loop_resolves_frames_until_cancelled(self) -> None:
        """The loop resolves a received ack, skips a None frame, then unwinds on cancel."""
        registry = ProcessCommandAckRegistry(_KEY)
        registry._subscriber = MagicMock()
        registry._running = True
        registry._resolve_ack = MagicMock()
        results: list[str | None | asyncio.CancelledError] = [
            None,
            "payload",
            asyncio.CancelledError(),
        ]

        async def _recv(_subscriber: object) -> str | None:
            item = results.pop(0)
            if isinstance(item, asyncio.CancelledError):
                raise item

            return item

        registry._recv_one = AsyncMock(side_effect=_recv)
        with pytest.raises(asyncio.CancelledError):
            await registry._listen_loop()
        registry._resolve_ack.assert_called_once_with("payload")

    @pytest.mark.asyncio
    async def test_listen_loop_returns_when_subscriber_absent(self) -> None:
        """The loop returns immediately when no subscriber is set."""
        registry = ProcessCommandAckRegistry(_KEY)
        registry._subscriber = None

        await registry._listen_loop()

    @pytest.mark.asyncio
    async def test_start_reaps_a_completed_task_before_restart(self) -> None:
        """A start after the prior listen task finished reaps it and starts fresh."""
        registry = ProcessCommandAckRegistry(_KEY)

        async def _done() -> None:
            return None

        registry._listen_task = asyncio.create_task(_done())
        await asyncio.sleep(0)
        fake_context = MagicMock()
        fake_context.socket.return_value = MagicMock()
        with (
            patch.object(mod.zmq.asyncio, "Context", return_value=fake_context),
            patch.object(mod, "apply_hwm"),
            patch.object(mod, "ValidatedSubscriber", return_value=MagicMock()),
        ):
            await registry.start("tcp://broker:7501")
            assert registry._listen_task is not None
            await registry.stop()

    @pytest.mark.asyncio
    async def test_stop_is_a_noop_when_never_started(self) -> None:
        """Stop on a never-started registry releases nothing and does not raise."""
        registry = ProcessCommandAckRegistry(_KEY)

        await registry.stop()

        assert registry._listen_task is None

    @pytest.mark.asyncio
    async def test_listen_loop_exits_when_not_running(self) -> None:
        """The loop returns normally (no recv) when running is already false."""
        registry = ProcessCommandAckRegistry(_KEY)
        registry._subscriber = MagicMock()
        registry._running = False

        await registry._listen_loop()
