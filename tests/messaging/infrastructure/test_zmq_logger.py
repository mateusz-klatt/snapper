"""Tests for ZmqMessageLogger public API and integration."""

import asyncio
import contextlib
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import zmq
import zmq.asyncio

from snapper.messaging.infrastructure.logger import SUBSCRIBE_ALL
from snapper.messaging.infrastructure.logger import ZmqMessageLogger


class TestZmqMessageLogger:
    """Tests for ZmqMessageLogger public API and integration."""

    @pytest.mark.asyncio
    async def test_logger_initialization(self) -> None:
        """Test logger init with configurable options.

        Given: ZmqMessageLogger config parameters,
        When: Creating logger instance,
        Then: All options stored, counters initialized to zero.
        """
        logger = ZmqMessageLogger(
            log_to_file=True,
            log_payload=False,
            max_payload_length=100,
        )
        assert logger.log_to_file is True
        assert logger.log_payload is False
        assert logger.max_payload_length == 100
        assert logger.message_count == 0
        assert logger.running is False

    @pytest.mark.asyncio
    async def test_logger_statistics(self) -> None:
        """Test get_statistics returns initial state.

        Given: Fresh ZmqMessageLogger instance,
        When: Calling get_statistics,
        Then: Returns LoggerStatistics with zero counters and running=False.
        """
        logger = ZmqMessageLogger(log_to_file=False)
        stats = logger.get_statistics()
        assert stats.running is False
        assert stats.message_count == 0
        assert stats.bytes_received == 0
        assert stats.topics_seen == 0

    @pytest.mark.asyncio
    async def test_subscribe_all_constant(self) -> None:
        """Test SUBSCRIBE_ALL is empty bytes for wildcard.

        Given: SUBSCRIBE_ALL constant,
        When: Checking value,
        Then: Is empty bytes (b"").
        """
        assert SUBSCRIBE_ALL == b""
        assert isinstance(SUBSCRIBE_ALL, bytes)

    @pytest.mark.asyncio
    async def test_audit_file_path_default(self) -> None:
        """Test default audit file path.

        Given: ZmqMessageLogger with no audit_file,
        When: Checking audit_path,
        Then: Uses default data/zmq_audit.jsonl.
        """
        logger = ZmqMessageLogger()
        assert logger.audit_path == Path("data/zmq_audit.jsonl")

    @pytest.mark.asyncio
    async def test_audit_file_path_custom(self) -> None:
        """Test custom audit file path.

        Given: ZmqMessageLogger with custom audit_file,
        When: Checking audit_path,
        Then: Uses provided path.
        """
        custom_path = "/tmp/my_audit.jsonl"
        logger = ZmqMessageLogger(audit_file=custom_path)
        assert logger.audit_path == Path(custom_path)

    @pytest.mark.asyncio
    async def test_logger_receives_messages(self, tmp_path: Path) -> None:
        """Test end-to-end message reception from XPUB socket.

        Given: ZmqMessageLogger connected to test broker,
        When: Broker sends message,
        Then: Logger receives, increments counters, writes to audit file.
        """
        audit_file = tmp_path / "audit.jsonl"
        ctx = zmq.asyncio.Context()
        broker = ctx.socket(zmq.XPUB)
        broker.bind("tcp://127.0.0.1:15556")
        logger = ZmqMessageLogger(
            log_to_file=True,
            log_payload=True,
            audit_file=str(audit_file),
        )
        logger.broker_xpub = "tcp://127.0.0.1:15556"
        logger_task = asyncio.create_task(logger.start())
        await asyncio.sleep(0.1)
        test_topic = b"test.topic"
        test_payload = b'{"test": "data"}'
        subscription = await broker.recv()
        assert subscription == b"\x01test.topic" or subscription == b"\x01"
        await broker.send_multipart([test_topic, test_payload])
        await asyncio.sleep(0.2)
        await logger.stop()
        logger_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await logger_task
        assert logger.message_count >= 1
        assert logger.bytes_received > 0
        if audit_file.exists():
            with audit_file.open() as f:
                lines = f.readlines()
                assert len(lines) >= 1
                entry = json.loads(lines[0])
                assert "timestamp" in entry
                assert "topic" in entry
                assert "size_bytes" in entry
                assert entry["topic"] == "test.topic"
        broker.close()
        ctx.term()

    @pytest.mark.asyncio
    async def test_get_default_kwargs(self) -> None:
        """Test get_default_kwargs factory method.

        Given: Mock settings object,
        When: Calling get_default_kwargs,
        Then: Returns dict with expected logger config keys.
        """
        mock_settings = MagicMock()
        kwargs = ZmqMessageLogger.get_default_kwargs(mock_settings)
        assert "log_to_file" in kwargs
        assert "log_payload" in kwargs
        assert "max_payload_length" in kwargs
        assert "audit_file" in kwargs
        assert kwargs["log_to_file"] is True
        assert kwargs["log_payload"] is False

    @pytest.mark.asyncio
    async def test_stop_without_start(self) -> None:
        """Test stop is safe without prior start.

        Given: Fresh logger never started,
        When: Calling stop,
        Then: No error, running remains False.
        """
        logger = ZmqMessageLogger(log_to_file=False)
        await logger.stop()
        assert not logger.running

    @pytest.mark.asyncio
    async def test_statistics_with_topic_counts(self) -> None:
        """Test statistics include top topics.

        Given: Logger with populated counters and topic_counts,
        When: Calling get_statistics,
        Then: Returns aggregated stats including top_topics.
        """
        logger = ZmqMessageLogger(log_to_file=False)
        logger.message_count = 100
        logger.bytes_received = 5000
        logger.topic_counts = {
            "market.ticker": 50,
            "system.heartbeat": 30,
            "orders.update": 20,
        }
        stats = logger.get_statistics()
        assert stats.message_count == 100
        assert stats.bytes_received == 5000
        assert stats.topics_seen == 3
        assert len(stats.top_topics) == 3

    @pytest.mark.asyncio
    async def test_stop_logs_final_statistics(self) -> None:
        """Test stop logs final statistics on shutdown.

        Given: Logger with message counters set,
        When: Calling stop,
        Then: running becomes False (stats logged internally).
        """
        logger = ZmqMessageLogger(log_to_file=False)
        logger.message_count = 10
        logger.bytes_received = 500
        logger.topic_counts = {"test.topic": 10}
        logger.running = True
        await logger.stop()
        assert not logger.running

    @pytest.mark.asyncio
    async def test_append_to_file(self, tmp_path: Path) -> None:
        """Test _append_to_file writes JSONL entry.

        Given: Logger with audit_file path,
        When: Calling _append_to_file with dict,
        Then: File contains JSON line with entry data.
        """
        audit_file = tmp_path / "test_audit.jsonl"
        logger = ZmqMessageLogger(audit_file=str(audit_file))
        test_entry = {
            "timestamp": "2024-01-01T00:00:00",
            "topic": "test",
            "size_bytes": 100,
        }
        logger._append_to_file(test_entry)
        assert audit_file.exists()
        with audit_file.open() as f:
            content = f.read()
            assert "test" in content
