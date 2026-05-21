"""ZMQ message logger for auditing and debugging.

This module provides a message logging service that subscribes to ALL messages
flowing through the ZMQ broker, creating an audit trail for debugging
compliance, and analysis.
The logger writes messages to a JSONL (JSON Lines) file with configurable
payload logging. It tracks statistics including message counts, byte counts
and per-topic breakdowns.
Classes
ZmqMessageLogger
    RegisterableProcess that logs all broker messages.
Configuration
The logger supports several configuration options
log_to_file: Enable/disable file output
log_payload: Include message payload preview in logs
max_payload_length: Truncate long payloads at this length
audit_file: Custom path for JSONL audit file
Output Format
Each line in the audit file is a JSON object
    {"timestamp": "...", "topic": "...", "size_bytes": N, "message_number": M}.

Example:
Enable message logging for debugging
    logger = ZmqMessageLogger(
        log_to_file=True
        log_payload=True
        max_payload_length=500
        audit_file="data/debug_audit.jsonl"
    await logger.start()
    # Check statistics
    stats = logger.get_statistics()
    print(f"Messages: {stats.message_count}")
    print(f"Top topics: {stats.top_topics}")
"""

import asyncio
import json
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any

import zmq
import zmq.asyncio
from loguru import logger

from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.process_parameters import LoggerParameters
from snapper.application.process_manager.registry import register_process
from snapper.config.settings import AppSettings
from snapper.config.settings import get_bootstrap_settings
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRoleEnum
from snapper.messaging.infrastructure.validated_socket import HWM_AUDIT
from snapper.messaging.infrastructure.validated_socket import apply_hwm

SUBSCRIBE_ALL = b""


@dataclass
class LoggerStatistics:
    """ZMQ message logger statistics snapshot.

    Attributes:
        running: Whether the logger is actively subscribing.
        message_count: Total messages received since start.
        bytes_received: Total bytes received since start.
        topics_seen: Number of unique topics encountered.
        top_topics: Top topics by message count (up to 10).
    """

    running: bool
    message_count: int
    bytes_received: int
    topics_seen: int
    top_topics: dict[str, int]


@register_process(
    "zmq_message_logger",
    description="ZMQ message logger",
    priority=15,
    role=ProcessRoleEnum.CORE,
    tags=("zmq", "logging", "audit", "debugging"),
    parameters_model=LoggerParameters,
    enabled=False,
    mode=ProcessModeEnum.THREAD,
)
class ZmqMessageLogger(RegisterableProcess):
    """ZMQ message logger that subscribes to all broker traffic.

    This service connects to the broker's XPUB endpoint and subscribes with
    an empty filter (b""), receiving ALL messages for logging and statistics.

    Messages are logged to both console (via loguru) and an optional JSONL
    audit file. The logger tracks cumulative statistics that can be queried
    via get_statistics().

    Disabled by default in process registration to avoid performance impact
    in production.

    Attributes:
        log_to_file: Whether to write audit entries to file.
        log_payload: Whether to include payload preview in logs.
        max_payload_length: Maximum payload preview length.
        audit_path: Path to JSONL audit file.
        message_count: Total messages received.
        bytes_received: Total bytes received.
        topic_counts: Per-topic message counts.
        running: Whether logger is active.

    Example:
        ::

            logger = ZmqMessageLogger(log_payload=True)
            await logger.start()
            # ... let it run ...
            stats = logger.get_statistics()
            await logger.stop()
    """

    def __init__(
        self,
        log_to_file: bool = True,
        log_payload: bool = False,
        max_payload_length: int = 200,
        audit_file: str | None = None,
    ) -> None:
        """Initialize the message logger.

        Args:
            log_to_file: Whether to write to JSONL audit file.
            log_payload: Whether to include payload preview in output.
            max_payload_length: Maximum characters for payload preview.
            audit_file: Custom path for audit file. Defaults to data/zmq_audit.jsonl.
        """
        self.log_to_file = log_to_file
        self.log_payload = log_payload
        self.max_payload_length = max_payload_length
        if audit_file:
            self.audit_path = Path(audit_file)
        else:
            self.audit_path = Path("data/zmq_audit.jsonl")
        self.audit_path.parent.mkdir(parents=True, exist_ok=True)
        self.context: zmq.asyncio.Context | None = None
        self.subscriber: zmq.asyncio.Socket | None = None
        self.running = False
        self.message_count = 0
        self.bytes_received = 0
        self.topic_counts: dict[str, int] = {}
        bootstrap = get_bootstrap_settings()
        self.broker_xpub = bootstrap.zmq_broker_xpub

    @staticmethod
    def get_default_parameters(settings: AppSettings) -> dict[str, Any]:
        """Get default parameters from application settings.

        Args:
            settings: Application settings (not used, defaults are hardcoded).

        Returns:
            Dictionary with default configuration values.
        """
        return {
            "log_to_file": True,
            "log_payload": False,
            "max_payload_length": 200,
            "audit_file": None,
        }

    async def start(self) -> None:
        """Start the message logger and begin receiving messages.

        Connects to the broker's XPUB endpoint and subscribes to all
        messages (empty filter). Runs the logging loop until stopped
        or cancelled.

        Raises:
            Exception: If connection or logging loop fails.
        """
        logger.info("Starting ZMQ Message Logger")
        logger.info(f"  Broker XPUB: {self.broker_xpub}")
        logger.info(f"  Log to file: {self.log_to_file}")
        if self.log_to_file:
            logger.info(f"  Audit file: {self.audit_path}")
        logger.info(f"  Log payload: {self.log_payload}")
        logger.info(f"  Max payload length: {self.max_payload_length}")
        logger.info('  Subscribing to: ALL MESSAGES (b"")')
        self.running = True
        self.context = zmq.asyncio.Context()
        self.subscriber = self.context.socket(zmq.SUB)
        apply_hwm(self.subscriber, rcvhwm=HWM_AUDIT)
        self.subscriber.connect(self.broker_xpub)
        self.subscriber.setsockopt(zmq.SUBSCRIBE, SUBSCRIBE_ALL)
        try:
            await self._logging_loop()
        except asyncio.CancelledError:
            logger.info("ZMQ Message Logger cancelled")
            raise
        except Exception as e:
            logger.error(f"ZMQ Message Logger error: {e}")
            raise
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Stop the logger and output final statistics.

        Logs summary statistics (total messages, bytes, top topics)
        before closing the socket and terminating the context.
        """
        logger.info("Stopping ZMQ Message Logger")
        self.running = False
        logger.info("Final Statistics:")
        logger.info(f"  Total messages: {self.message_count}")
        logger.info(f"  Total bytes: {self.bytes_received:,}")
        logger.info(f"  Topics seen: {len(self.topic_counts)}")
        if self.topic_counts:
            top_topics = sorted(self.topic_counts.items(), key=lambda x: x[1], reverse=True)[:10]
            logger.info("  Top topics:")
            for topic, count in top_topics:
                logger.info(f"    {topic}: {count} messages")
        if self.subscriber:
            self.subscriber.setsockopt(zmq.LINGER, 0)
            self.subscriber.close()
            self.subscriber = None
        if self.context:
            self.context.term()
            self.context = None

    async def _logging_loop(self) -> None:
        """Main loop receiving and logging messages.

        Continuously receives multipart messages from the subscriber,
        updates statistics, and calls _log_message for each message.
        Handles ZMQ errors gracefully.
        """
        logger.info("ZMQ Message Logger running - press Ctrl+C to stop")
        assert self.subscriber is not None
        while self.running:
            try:
                topic_bytes, payload_bytes = await self.subscriber.recv_multipart()
                topic = topic_bytes.decode("utf-8", errors="replace")
                self.message_count += 1
                self.bytes_received += len(topic_bytes) + len(payload_bytes)
                self.topic_counts[topic] = self.topic_counts.get(topic, 0) + 1
                await self._log_message(topic, payload_bytes)
            except zmq.ZMQError as e:
                if e.errno == zmq.ETERM:
                    break
                logger.error(f"ZMQ error in logger: {e}")
            except Exception as e:
                logger.error(f"Error logging ZMQ message: {e}")

    async def _log_message(self, topic: str, payload_bytes: bytes) -> None:
        """Log a single message to console and optionally file.

        Args:
            topic: Message topic string.
            payload_bytes: Raw message payload.
        """
        timestamp = datetime.now(UTC)
        payload_size = len(payload_bytes)
        try:
            payload_str = payload_bytes.decode("utf-8", errors="replace")
            if len(payload_str) > self.max_payload_length:
                payload_preview = payload_str[: self.max_payload_length] + "..."
            else:
                payload_preview = payload_str
        except Exception:
            payload_preview = f"<binary data, {payload_size} bytes>"
        if self.log_payload:
            logger.debug(f"ZMQ [{topic}] ({payload_size}b): {payload_preview}")
        else:
            logger.debug(f"ZMQ [{topic}] {payload_size}b")
        if self.log_to_file:
            await self._write_to_audit_file(timestamp, topic, payload_size, payload_preview)

    async def _write_to_audit_file(
        self,
        timestamp: datetime,
        topic: str,
        payload_size: int,
        payload_preview: str,
    ) -> None:
        """Write an audit entry to the JSONL file.

        Args:
            timestamp: When message was received.
            topic: Message topic string.
            payload_size: Size of payload in bytes.
            payload_preview: Truncated payload string for logging.
        """
        try:
            audit_entry = {
                "timestamp": timestamp.isoformat(),
                "topic": topic,
                "size_bytes": payload_size,
                "message_number": self.message_count,
            }
            if self.log_payload:
                audit_entry["payload_preview"] = payload_preview
            await asyncio.get_event_loop().run_in_executor(
                None,
                self._append_to_file,
                audit_entry,
            )
        except Exception as e:
            logger.error(f"Failed to write to audit file: {e}")

    def _append_to_file(self, audit_entry: dict[str, Any]) -> None:
        """Synchronous file append operation.

        Args:
            audit_entry: Dictionary to serialize and append.
        """
        with self.audit_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(audit_entry) + "\n")

    def get_statistics(self) -> LoggerStatistics:
        """Get current logger statistics.

        Returns:
            LoggerStatistics snapshot with counters and top topics.
        """
        return LoggerStatistics(
            running=self.running,
            message_count=self.message_count,
            bytes_received=self.bytes_received,
            topics_seen=len(self.topic_counts),
            top_topics=dict(
                sorted(self.topic_counts.items(), key=lambda x: x[1], reverse=True)[:10]
            ),
        )
