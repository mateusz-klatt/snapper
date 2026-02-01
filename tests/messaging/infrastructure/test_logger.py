"""Tests for ZmqMessageLogger internal methods."""

import asyncio
import json
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any
from typing import cast
from unittest.mock import AsyncMock

import pytest
import zmq

from snapper.messaging.infrastructure.logger import ZmqMessageLogger


@pytest.mark.asyncio
async def test_log_message_metadata_only() -> None:
    """Test _log_message in metadata-only mode.

    Given: Logger with log_payload=False,
    When: Logging a message,
    Then: No error, metadata logged without payload.
    """
    logger = ZmqMessageLogger(log_to_file=False, log_payload=False)
    payload = b"hello"
    await logger._log_message("topic", payload)


@pytest.mark.asyncio
async def test_log_message_truncates_and_writes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Test _log_message truncates payload per max_payload_length.

    Given: Logger with max_payload_length=3 and log_payload=True,
    When: Logging payload longer than limit,
    Then: Entry written to audit file with truncated payload.
    """
    audit_file = tmp_path / "audit.jsonl"
    logger = ZmqMessageLogger(
        log_to_file=True, log_payload=True, max_payload_length=3, audit_file=str(audit_file)
    )
    monkeypatch.setattr(
        asyncio.get_event_loop(),
        "run_in_executor",
        lambda _pool, func, *args: func(*args),
    )
    long_payload = b"abcdef"
    await logger._log_message("topic", long_payload)
    assert audit_file.exists()
    content = audit_file.read_text().strip()
    assert content
    entry = json.loads(content)
    assert entry["topic"] == "topic"


@pytest.mark.asyncio
async def test_write_to_audit_file_handles_exception(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Test _write_to_audit_file handles file write errors.

    Given: Logger with _append_to_file that raises,
    When: Calling _write_to_audit_file,
    Then: No exception propagates (error logged internally).
    """
    logger = ZmqMessageLogger(
        log_to_file=True, log_payload=False, audit_file=str(tmp_path / "audit.jsonl")
    )

    def boom(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("fail")

    monkeypatch.setattr(logger, "_append_to_file", boom)
    await logger._write_to_audit_file(datetime.now(tz=UTC), "t", 1, "p")


def test_append_to_file_writes_json(tmp_path: Path) -> None:
    """Test _append_to_file writes JSONL entry.

    Given: Logger with audit file path,
    When: Calling _append_to_file with dict,
    Then: File contains JSON entry.
    """
    log_file = tmp_path / "audit.jsonl"
    logger = ZmqMessageLogger(log_to_file=True, audit_file=str(log_file))
    entry = {"foo": "bar"}
    logger._append_to_file(entry)
    assert json.loads(log_file.read_text())["foo"] == "bar"


@pytest.mark.asyncio
async def test_logging_loop_handles_eterm(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test _logging_loop handles ETERM for graceful shutdown.

    Given: Logger with subscriber that raises ZMQError(ETERM),
    When: Running _logging_loop,
    Then: Loop exits cleanly without error.
    """
    logger = ZmqMessageLogger(log_to_file=False)
    logger.running = True

    class DummySubscriber:
        def close(self) -> None:
            return None

        async def recv_multipart(self) -> tuple[bytes, bytes]:
            raise zmq.ZMQError(zmq.ETERM)

    logger.subscriber = DummySubscriber()
    logger.context = type("Ctx", (), {"term": lambda self: None})()
    await logger._logging_loop()


@pytest.mark.asyncio
async def test_logging_loop_processes_message(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test _logging_loop processes received messages.

    Given: Logger with subscriber returning messages,
    When: Running _logging_loop,
    Then: Message count incremented.
    """
    logger = ZmqMessageLogger(log_to_file=False)
    logger.running = True

    class DummySubscriber:
        def __init__(self) -> None:
            self.calls = 0

        def close(self) -> None:
            return None

        async def recv_multipart(self) -> tuple[bytes, bytes]:
            self.calls += 1
            if self.calls > 1:
                logger.running = False
            return (b"topic", b"payload")

    logger.subscriber = DummySubscriber()
    logger.context = type("Ctx", (), {"term": lambda self: None})()
    await logger._logging_loop()
    stats = logger.get_statistics()
    assert stats["message_count"] >= 1


@pytest.mark.asyncio
async def test_stop_logs_stats(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test stop closes socket with linger and logs stats.

    Given: Running logger with subscriber,
    When: Calling stop,
    Then: Socket setsockopt(LINGER, 0) called, close called.
    """
    logger = ZmqMessageLogger(log_to_file=False)
    logger.running = True
    logger.message_count = 2
    logger.bytes_received = 10
    logger.topic_counts = {"a": 2, "b": 1}
    setsockopt_calls: list[tuple[int, int]] = []
    close_called = False

    class StubSubscriber:
        def setsockopt(self, opt: int, val: int) -> None:
            setsockopt_calls.append((opt, val))

        def close(self) -> None:
            nonlocal close_called
            close_called = True

    logger.subscriber = StubSubscriber()
    logger.context = type("C", (), {"term": lambda self: None})()
    await logger.stop()
    assert logger.running is False
    assert setsockopt_calls == [(zmq.LINGER, 0)]
    assert close_called


@pytest.mark.asyncio
async def test_start_handles_logging_loop_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test start propagates _logging_loop exceptions.

    Given: Logger with _logging_loop that raises,
    When: Calling start,
    Then: Exception propagates.
    """
    logger = ZmqMessageLogger(log_to_file=True, audit_file="audit.jsonl")

    class DummySocket:
        def connect(self, *_args: Any, **_kwargs: Any) -> None:
            return None

        def setsockopt(self, *_args: Any, **_kwargs: Any) -> None:
            return None

        def close(self) -> None:
            return None

    class DummyCtx:
        def socket(self, *_args: Any, **_kwargs: Any) -> DummySocket:
            return DummySocket()

        def term(self) -> None:
            return None

    monkeypatch.setattr("snapper.messaging.infrastructure.logger.zmq.asyncio.Context", DummyCtx)
    monkeypatch.setattr(logger, "_logging_loop", AsyncMock(side_effect=RuntimeError("boom")))
    with pytest.raises(RuntimeError):
        await logger.start()


@pytest.mark.asyncio
async def test_start_without_file_logging(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test start with log_to_file=False.

    Given: Logger with file logging disabled,
    When: Starting and cancelling,
    Then: CancelledError propagates after cleanup.
    """
    logger = ZmqMessageLogger(log_to_file=False, log_payload=False)

    class DummySocket:
        def connect(self, *_args: Any, **_kwargs: Any) -> None:
            return None

        def setsockopt(self, *_args: Any, **_kwargs: Any) -> None:
            return None

        def close(self) -> None:
            return None

    class DummyCtx:
        def socket(self, *_args: Any, **_kwargs: Any) -> DummySocket:
            return DummySocket()

        def term(self) -> None:
            return None

    monkeypatch.setattr("snapper.messaging.infrastructure.logger.zmq.asyncio.Context", DummyCtx)
    monkeypatch.setattr(logger, "_logging_loop", AsyncMock(side_effect=asyncio.CancelledError()))
    with pytest.raises(asyncio.CancelledError):
        await logger.start()
    assert not logger.running


@pytest.mark.asyncio
async def test_logging_loop_logs_non_eterm_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test _logging_loop handles non-ETERM ZMQ errors.

    Given: Subscriber raising ZMQError(EAGAIN),
    When: Running _logging_loop,
    Then: Error logged, loop continues.
    """
    logger = ZmqMessageLogger(log_to_file=False)
    logger.running = True

    class DummySubscriber:
        def close(self) -> None:
            return None

        async def recv_multipart(self) -> tuple[bytes, bytes]:
            logger.running = False
            raise zmq.ZMQError(zmq.EAGAIN)

    logger.subscriber = DummySubscriber()
    logger.context = type("Ctx", (), {"term": lambda self: None})()
    await logger._logging_loop()
    assert not logger.running


@pytest.mark.asyncio
async def test_log_message_handles_decode_exception() -> None:
    """Test _log_message handles payload decode errors.

    Given: Payload that raises on decode,
    When: Logging with log_payload=True,
    Then: No exception propagates.
    """
    logger = ZmqMessageLogger(log_payload=True, log_to_file=False)

    class BadPayload:
        def __len__(self) -> int:
            return 3

        def decode(self, *_args: Any, **_kwargs: Any) -> str:
            raise ValueError("boom")

    await logger._log_message("topic", cast(Any, BadPayload()))


@pytest.mark.asyncio
async def test_logging_loop_recovers_from_generic_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test _logging_loop recovers from non-ZMQ errors.

    Given: Subscriber that raises ValueError once,
    When: Running _logging_loop,
    Then: Loop continues and processes next message.
    """
    logger = ZmqMessageLogger(log_to_file=False)
    logger.running = True
    calls = 0

    class DummySubscriber:
        def close(self) -> None:
            return None

        async def recv_multipart(self) -> tuple[bytes, bytes]:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise ValueError("fail once")
            logger.running = False
            return (b"topic", b"payload")

    logger.subscriber = DummySubscriber()
    logger.context = type("Ctx", (), {"term": lambda self: None})()
    await logger._logging_loop()
    assert calls >= 2


@pytest.mark.asyncio
async def test_log_message_handles_binary_payload() -> None:
    """Test _log_message handles non-UTF8 binary payload.

    Given: Binary payload with invalid UTF-8 bytes,
    When: Logging with log_payload=True,
    Then: No decode error.
    """
    logger = ZmqMessageLogger(log_payload=True, log_to_file=False)
    payload = b"\xff\xfe\xfd"
    await logger._log_message("topic", payload)
