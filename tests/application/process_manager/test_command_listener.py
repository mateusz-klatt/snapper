"""Tests for the coordinator process-command listener (control plane P2.3)."""

import asyncio
import json
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch
from uuid import uuid7

import pytest

from snapper.application.process_manager import command_listener as mod
from snapper.application.process_manager.command_listener import ProcessCommandListener
from snapper.messaging.schemas.data import ProcessCommandData
from snapper.messaging.security.command_signing import command_signing_key
from snapper.messaging.security.command_signing import sign_command_payload
from snapper.messaging.security.command_signing import verify_command_payload


def _launcher(slug: str = "coord-0", master: str = "master") -> MagicMock:
    """Build a mock launcher with slug, master password, reconcile, and a publisher.

    Args:
        slug: The coordinator's own topic slug.
        master: The master password (drives the signing key).

    Returns:
        A configured MagicMock standing in for ProcessLauncherService.
    """
    launcher = MagicMock()
    launcher.coordinator_topic_slug.return_value = slug
    launcher.settings.master_password = master
    launcher.reconcile_desired_state = AsyncMock()
    tracker = MagicMock()
    tracker.session_id = "s"
    tracker.next_sequence.return_value = 1
    publisher = MagicMock()
    publisher.tracker = tracker
    publisher.send = AsyncMock()
    launcher.message_publisher = publisher

    return launcher


def _signed_command(
    master: str = "master",
    *,
    coordinator: str = "coord-0",
    command_id: str = "cmd-1",
    issued_at: datetime | None = None,
) -> str:
    """Build a signed ProcessCommandData JSON payload.

    Args:
        master: Master password to derive the signing key.
        coordinator: Target coordinator slug (signed).
        command_id: The command id (signed).
        issued_at: Issue time; defaults to now.

    Returns:
        The signed command serialized as JSON.
    """
    key = command_signing_key(master)
    command = ProcessCommandData(
        session_id="s",
        sequence_id=1,
        public_id=str(uuid7()),
        timestamp=datetime(2026, 7, 4, tzinfo=UTC),
        command_id=command_id,
        coordinator=coordinator,
        process_name="strategy_x",
        action="restart",
        issued_by="api",
        issued_at=issued_at if issued_at is not None else datetime.now(UTC),
        signature="",
    )
    signed = command.model_copy(
        update={"signature": sign_command_payload(command.model_dump(mode="json"), key)}
    )

    return signed.to_json()


class TestHandleFrame:
    """Tests for the verify -> reconcile -> ack frame handler."""

    @pytest.mark.asyncio
    async def test_valid_command_reconciles_and_acks_applied(self) -> None:
        """A valid, fresh, correctly-addressed command reconciles and acks 'applied'.

        Given: a signed command for this coordinator,
        When: the frame is handled,
        Then: reconcile runs once and a signed 'applied' ack is published.
        """
        launcher = _launcher()
        listener = ProcessCommandListener(launcher)

        await listener._handle_frame(_signed_command())

        launcher.reconcile_desired_state.assert_awaited_once()
        launcher.message_publisher.send.assert_awaited_once()
        topic, ack = launcher.message_publisher.send.await_args.args
        assert topic == "processes.events.command_ack.coord-0"
        assert ack.status == "applied"
        assert ack.command_id == "cmd-1"
        assert ack.coordinator == "coord-0"
        assert ack.signature

    @pytest.mark.asyncio
    async def test_malformed_json_dropped(self) -> None:
        """A non-JSON payload is dropped without reconcile or ack.

        Given: a payload that is not valid JSON,
        When: the frame is handled,
        Then: nothing is reconciled or acked.
        """
        launcher = _launcher()
        listener = ProcessCommandListener(launcher)

        await listener._handle_frame("not json{")

        launcher.reconcile_desired_state.assert_not_awaited()
        launcher.message_publisher.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_non_object_payload_dropped(self) -> None:
        """A JSON array (non-object) payload is dropped.

        Given: a JSON payload that decodes to a list,
        When: the frame is handled,
        Then: nothing is reconciled or acked.
        """
        launcher = _launcher()
        listener = ProcessCommandListener(launcher)

        await listener._handle_frame("[1, 2, 3]")

        launcher.reconcile_desired_state.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_bad_signature_dropped(self) -> None:
        """A command signed with the wrong key fails verification and is dropped.

        Given: a command signed by a key derived from a different master password,
        When: the frame is handled by a listener with the real key,
        Then: it is dropped (no reconcile, no ack).
        """
        launcher = _launcher(master="master")
        listener = ProcessCommandListener(launcher)

        await listener._handle_frame(_signed_command(master="attacker"))

        launcher.reconcile_desired_state.assert_not_awaited()
        launcher.message_publisher.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_schema_validation_failure_dropped(self) -> None:
        """A correctly-signed but schema-invalid payload is dropped.

        Given: a raw dict missing required command fields, signed with the real key,
        When: the frame is handled,
        Then: it passes the signature check but fails schema validation and is dropped.
        """
        launcher = _launcher()
        listener = ProcessCommandListener(launcher)
        raw = {"type": "process_command", "command_id": "x", "signature": ""}
        raw["signature"] = sign_command_payload(raw, command_signing_key("master"))

        await listener._handle_frame(json.dumps(raw))

        launcher.reconcile_desired_state.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_command_for_other_coordinator_dropped(self) -> None:
        """A validly-signed command addressed to a different coordinator is dropped.

        Given: a command whose signed 'coordinator' is not this node's slug,
        When: the frame is handled,
        Then: it is dropped even though the signature is valid.
        """
        launcher = _launcher(slug="coord-0")
        listener = ProcessCommandListener(launcher)

        await listener._handle_frame(_signed_command(coordinator="coord-9"))

        launcher.reconcile_desired_state.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stale_command_dropped(self) -> None:
        """A command outside the freshness window is dropped.

        Given: a validly-signed command issued well before now,
        When: the frame is handled,
        Then: it is dropped.
        """
        launcher = _launcher()
        listener = ProcessCommandListener(launcher)
        stale = datetime.now(UTC) - timedelta(seconds=120)

        await listener._handle_frame(_signed_command(issued_at=stale))

        launcher.reconcile_desired_state.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_duplicate_command_re_acks_without_reconciling(self) -> None:
        """A repeated command id re-acks 'applied' but does not reconcile again.

        Given: the same signed command handled twice,
        When: the second frame is handled,
        Then: reconcile ran only once but both were acked (so a retried nudge resolves).
        """
        launcher = _launcher()
        listener = ProcessCommandListener(launcher)
        payload = _signed_command(command_id="cmd-dup")

        await listener._handle_frame(payload)
        await listener._handle_frame(payload)

        assert launcher.reconcile_desired_state.await_count == 1
        assert launcher.message_publisher.send.await_count == 2
        _topic, ack = launcher.message_publisher.send.await_args.args
        assert ack.detail == "duplicate"

    @pytest.mark.asyncio
    async def test_stale_duplicate_is_dropped_before_dedup(self) -> None:
        """§8 hardening: a stale re-send of a seen command id is dropped, not re-acked.

        Given: a command id already handled, remembered, and acked once,
        When: the SAME command id arrives again with an issued_at outside the
            freshness window,
        Then: the freshness gate (which runs BEFORE dedup) drops it silently —
            no duplicate re-ack is published and no second reconcile runs, so
            replay beyond the seen-cache stays bounded by freshness alone.
        """
        launcher = _launcher()
        listener = ProcessCommandListener(launcher)

        await listener._handle_frame(_signed_command(command_id="cmd-stale-dup"))
        assert launcher.message_publisher.send.await_count == 1
        stale = datetime.now(UTC) - timedelta(seconds=120)
        await listener._handle_frame(_signed_command(command_id="cmd-stale-dup", issued_at=stale))

        assert launcher.reconcile_desired_state.await_count == 1
        assert launcher.message_publisher.send.await_count == 1

    @pytest.mark.asyncio
    async def test_reconcile_failure_acks_rejected(self) -> None:
        """A reconcile exception acks 'rejected' with the error detail.

        Given: a valid command but a reconcile that raises,
        When: the frame is handled,
        Then: a 'rejected' ack carrying the error is published.
        """
        launcher = _launcher()
        launcher.reconcile_desired_state = AsyncMock(side_effect=RuntimeError("boom"))
        listener = ProcessCommandListener(launcher)

        await listener._handle_frame(_signed_command())

        _topic, ack = launcher.message_publisher.send.await_args.args
        assert ack.status == "rejected"
        assert ack.detail is not None
        assert "boom" in ack.detail

    @pytest.mark.asyncio
    async def test_failed_reconcile_is_not_remembered_so_retry_reattempts(self) -> None:
        """A command whose reconcile failed is retried, not falsely acked 'applied'.

        Given: a command whose first reconcile raises then a retry that succeeds,
        When: the same command id is handled twice,
        Then: reconcile is attempted BOTH times (the failure was not remembered)
            and the retry acks 'applied'.
        """
        launcher = _launcher()
        launcher.reconcile_desired_state = AsyncMock(side_effect=[RuntimeError("boom"), None])
        listener = ProcessCommandListener(launcher)
        payload = _signed_command(command_id="cmd-retry")

        await listener._handle_frame(payload)
        await listener._handle_frame(payload)

        assert launcher.reconcile_desired_state.await_count == 2
        _topic, ack = launcher.message_publisher.send.await_args.args
        assert ack.status == "applied"
        assert ack.detail is None

    @pytest.mark.asyncio
    async def test_published_ack_verifies_even_with_a_stamped_topic(self) -> None:
        """The published ack verifies under the shared key after a topic is stamped.

        Given: a valid command handled to completion,
        When: the ack is serialized and a transport ``topic`` field is stamped
            onto it (as the publisher does),
        Then: it still verifies with the master-derived key.
        """
        launcher = _launcher()
        listener = ProcessCommandListener(launcher)

        await listener._handle_frame(_signed_command())

        _topic, ack = launcher.message_publisher.send.await_args.args
        ack_dict = json.loads(ack.to_json())
        ack_dict["topic"] = "processes.events.command_ack.coord-0"
        assert verify_command_payload(ack_dict, command_signing_key("master")) is True

    @pytest.mark.asyncio
    async def test_ack_noops_without_publisher(self) -> None:
        """With no wired publisher the reconcile still runs and no ack is attempted.

        Given: a launcher whose message_publisher is None,
        When: a valid command is handled,
        Then: reconcile runs and no send is attempted (no crash).
        """
        launcher = _launcher()
        launcher.message_publisher = None
        listener = ProcessCommandListener(launcher)

        await listener._handle_frame(_signed_command())

        launcher.reconcile_desired_state.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_ack_publish_failure_swallowed(self) -> None:
        """A failing ack publish is logged and swallowed, not raised.

        Given: a publisher whose send raises,
        When: a valid command is handled,
        Then: the handler does not propagate the error.
        """
        launcher = _launcher()
        launcher.message_publisher.send = AsyncMock(side_effect=RuntimeError("send boom"))
        listener = ProcessCommandListener(launcher)

        await listener._handle_frame(_signed_command())

        launcher.reconcile_desired_state.assert_awaited_once()


class TestFreshnessAndDedup:
    """Tests for the freshness window and the bounded seen-cache."""

    def test_fresh_aware_within_window(self) -> None:
        """An aware datetime within the window is fresh."""
        listener = ProcessCommandListener(_launcher())
        assert listener._is_fresh(datetime.now(UTC)) is True

    def test_stale_aware_outside_window(self) -> None:
        """An aware datetime beyond the window is not fresh."""
        listener = ProcessCommandListener(_launcher())
        assert listener._is_fresh(datetime.now(UTC) - timedelta(seconds=120)) is False

    def test_naive_datetime_treated_as_utc(self) -> None:
        """A naive datetime is treated as UTC rather than raising."""
        listener = ProcessCommandListener(_launcher())
        naive = datetime.now(UTC).replace(tzinfo=None)
        assert listener._is_fresh(naive) is True

    def test_future_skew_accepted_inside_abs_window_rejected_beyond(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """§8 hardening: the abs() freshness gate admits bounded FUTURE clock skew.

        The listener's wall-clock read is FROZEN via a datetime stub, so
        the probes deterministically pin the exact ±30s window edge —
        no deschedule between building the timestamp and the comparison
        can move a probe across the boundary, and a runtime widening of
        _FRESHNESS_WINDOW fails the +31s rejection.

        Given: a frozen listener clock and the default ±30s abs() window,
        When: freshness is evaluated for issue times +29s and +31s in
            the future relative to the frozen instant,
        Then: +29s is accepted (a skewed-but-honest clock still nudges)
            and +31s is rejected (the window bounds replay in both
            directions).
        """
        frozen = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)

        class _FrozenDatetime:
            """Datetime stand-in pinning the listener's now() read."""

            @staticmethod
            def now(tz: object = None) -> datetime:
                """Return the frozen instant regardless of tz argument."""
                del tz
                return frozen

        monkeypatch.setattr(mod, "datetime", _FrozenDatetime)
        listener = ProcessCommandListener(_launcher())
        assert listener._is_fresh(frozen + timedelta(seconds=29)) is True
        assert listener._is_fresh(frozen + timedelta(seconds=31)) is False

    def test_remember_evicts_oldest_past_bound(self) -> None:
        """The seen-cache stays bounded, evicting the oldest id first."""
        listener = ProcessCommandListener(_launcher())
        for index in range(mod._SEEN_MAX + 5):
            listener._remember(f"cmd-{index}")
        assert len(listener._seen) == mod._SEEN_MAX
        assert "cmd-0" not in listener._seen
        assert f"cmd-{mod._SEEN_MAX + 4}" in listener._seen


class TestLifecycle:
    """Tests for start / stop / recv / loop wiring (ZMQ mocked)."""

    @pytest.mark.asyncio
    async def test_start_skips_on_empty_xpub(self) -> None:
        """An empty XPUB endpoint skips the listener entirely.

        Given: an empty broker XPUB,
        When: start is called,
        Then: no context/task is created.
        """
        listener = ProcessCommandListener(_launcher())

        await listener.start("")

        assert listener._task is None
        assert listener._context is None

    @pytest.mark.asyncio
    async def test_start_subscribes_and_stop_tears_down(self) -> None:
        """Start allocates a subscriber + task; stop cancels and closes everything.

        Given: a mocked ZMQ context/subscriber,
        When: start then stop run,
        Then: the command topic is subscribed and all resources are released.
        """
        listener = ProcessCommandListener(_launcher(slug="coord-1"))
        fake_context = MagicMock()
        fake_context.socket.return_value = MagicMock()
        fake_subscriber = MagicMock()
        with (
            patch.object(mod.zmq.asyncio, "Context", return_value=fake_context),
            patch.object(mod, "apply_hwm"),
            patch.object(mod, "ValidatedSubscriber", return_value=fake_subscriber),
        ):
            await listener.start("tcp://broker:7501")
            assert listener._task is not None
            fake_subscriber.subscribe.assert_called_once_with("processes.commands.coord-1")

            await listener.stop()
            assert listener._task is None
            fake_subscriber.close.assert_called_once()
            fake_context.term.assert_called_once()

    @pytest.mark.asyncio
    async def test_start_is_idempotent_while_running(self) -> None:
        """A second start while already running is a no-op (same task kept).

        Given: an already-running listener,
        When: start is called again,
        Then: the running task is preserved and no new context is built.
        """
        listener = ProcessCommandListener(_launcher())
        fake_context = MagicMock()
        fake_context.socket.return_value = MagicMock()
        with (
            patch.object(mod.zmq.asyncio, "Context", return_value=fake_context) as ctx,
            patch.object(mod, "apply_hwm"),
            patch.object(mod, "ValidatedSubscriber", return_value=MagicMock()),
        ):
            await listener.start("tcp://broker:7501")
            first_task = listener._task
            await listener.start("tcp://broker:7501")
            assert listener._task is first_task
            assert ctx.call_count == 1
            await listener.stop()

    @pytest.mark.asyncio
    async def test_start_reaps_a_completed_task_before_restart(self) -> None:
        """A start after a prior task has finished reaps it and starts fresh.

        Given: a listener whose previous listen task has completed,
        When: start is called again,
        Then: the completed task is reaped and a new task is created.
        """
        listener = ProcessCommandListener(_launcher())

        async def _done() -> None:
            return None

        listener._task = asyncio.create_task(_done())
        await asyncio.sleep(0)
        fake_context = MagicMock()
        fake_context.socket.return_value = MagicMock()
        with (
            patch.object(mod.zmq.asyncio, "Context", return_value=fake_context),
            patch.object(mod, "apply_hwm"),
            patch.object(mod, "ValidatedSubscriber", return_value=MagicMock()),
        ):
            await listener.start("tcp://broker:7501")
            assert listener._task is not None
            await listener.stop()

    @pytest.mark.asyncio
    async def test_listen_loop_exits_when_not_running(self) -> None:
        """The loop returns normally (no recv) when running is already false.

        Given: a subscriber is set but running is false,
        When: the loop runs,
        Then: it exits immediately without receiving.
        """
        listener = ProcessCommandListener(_launcher())
        listener._subscriber = MagicMock()
        listener._running = False

        await listener._listen_loop()

    @pytest.mark.asyncio
    async def test_stop_is_a_noop_when_never_started(self) -> None:
        """Stop on a never-started listener releases nothing and does not raise.

        Given: a listener that was never started,
        When: stop is called,
        Then: it completes cleanly.
        """
        listener = ProcessCommandListener(_launcher())

        await listener.stop()

        assert listener._task is None

    @pytest.mark.asyncio
    async def test_recv_one_decodes_payload(self) -> None:
        """_recv_one decodes the payload bytes of a received frame."""
        listener = ProcessCommandListener(_launcher())
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(
            return_value=("processes.commands.coord-0", b'{"hi": 1}')
        )

        assert await listener._recv_one(subscriber) == '{"hi": 1}'

    @pytest.mark.asyncio
    async def test_recv_one_backs_off_and_returns_none_on_error(self) -> None:
        """A recv error backs off and returns None so the loop continues."""
        listener = ProcessCommandListener(_launcher())
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=RuntimeError("recv boom"))
        with patch.object(mod.asyncio, "sleep", AsyncMock()) as sleep:
            assert await listener._recv_one(subscriber) is None
            sleep.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_recv_one_reraises_cancellation(self) -> None:
        """Cancellation during recv propagates (loop teardown)."""
        listener = ProcessCommandListener(_launcher())
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=asyncio.CancelledError)
        with pytest.raises(asyncio.CancelledError):
            await listener._recv_one(subscriber)

    @pytest.mark.asyncio
    async def test_listen_loop_handles_frames_until_cancelled(self) -> None:
        """The loop dispatches a received frame, skips a None frame, then unwinds on cancel."""
        listener = ProcessCommandListener(_launcher())
        listener._subscriber = MagicMock()
        listener._running = True
        listener._handle_frame = AsyncMock()
        recv_results = [None, "payload", asyncio.CancelledError()]

        async def _recv(_subscriber: object) -> str | None:
            result = recv_results.pop(0)
            if isinstance(result, asyncio.CancelledError):
                raise result

            return result

        listener._recv_one = AsyncMock(side_effect=_recv)
        with pytest.raises(asyncio.CancelledError):
            await listener._listen_loop()
        listener._handle_frame.assert_awaited_once_with("payload")

    @pytest.mark.asyncio
    async def test_listen_loop_returns_when_subscriber_absent(self) -> None:
        """The loop returns immediately when no subscriber is set."""
        listener = ProcessCommandListener(_launcher())
        listener._subscriber = None

        await listener._listen_loop()
