"""Strategy health monitoring and heartbeat emission.

Provides health status classification, feed health aggregation,
and periodic heartbeat publishing for trading strategies.
"""

import asyncio
import logging
import time
from datetime import UTC
from datetime import datetime
from typing import Any
from uuid import uuid7

from snapper.interface.websocket.schemas import HealthStatus
from snapper.messaging.schemas.data import HeartbeatData
from snapper.messaging.topics.builders import heartbeat_topic_from_component

logger = logging.getLogger(__name__)


class StrategyHealthMonitor:
    """Monitors strategy health and emits periodic heartbeats.

    Reads strategy state to classify health, build feed health
    summaries, and publish heartbeat envelopes via ZMQ.

    Attributes:
        strategy: Reference to the owning strategy instance.
    """

    def __init__(self, strategy: Any) -> None:
        """Initialize the health monitor.

        Args:
            strategy: The BaseStrategy instance to monitor.
        """
        self.strategy = strategy

    @staticmethod
    def classify_health_status(lag_ms: int) -> HealthStatus:
        """Classify health status based on data lag.

        Args:
            lag_ms: Milliseconds since last data received.

        Returns:
            Health status string.
        """
        if lag_ms < 2000:
            return "healthy"
        if lag_ms < 10000:
            return "warning"
        return "error"

    def build_feed_health(self) -> dict[str, dict[str, Any]] | None:
        """Build feed health summary from cached heartbeats.

        Returns:
            Feed health dict or None if no heartbeats collected.
        """
        if not self.strategy._feed_heartbeats:
            return None
        current_time = time.time()
        feed_health: dict[str, dict[str, Any]] = {}
        for feed_key, heartbeat in self.strategy._feed_heartbeats.items():
            age_ms = int((current_time - heartbeat["timestamp"]) * 1000)
            feed_health[feed_key] = {
                "status": heartbeat["status"],
                "lag_ms": heartbeat["lag_ms"],
                "heartbeat_age_ms": age_ms,
                "healthy": age_ms < 5000,
            }
        return feed_health

    def build_heartbeat_envelope(self, lag_ms: int, topic: str) -> HeartbeatData:
        """Build a heartbeat envelope with current strategy state.

        Args:
            lag_ms: Milliseconds since last data received.
            topic: Heartbeat ZMQ topic for provenance allocation.

        Returns:
            Complete heartbeat data with provenance set.
        """
        tracker = self.strategy.msg_publisher.tracker if self.strategy.msg_publisher else None
        return HeartbeatData(
            public_id=str(uuid7()),
            timestamp=datetime.now(UTC),
            session_id=tracker.session_id if tracker else "",
            sequence_id=tracker.next_sequence(topic) if tracker else 0,
            component=f"strategy.{self.strategy.name}",
            sequence=self.strategy.heartbeat_seq,
            status=self.classify_health_status(lag_ms),
            lag_ms=lag_ms,
            meta={
                "inputs": self.strategy.inputs,
                "outputs": self.strategy.outputs,
                "output_topics": self.strategy.output_topics,
                "running": self.strategy._running,
                "feed_health": self.build_feed_health(),
            },
        )

    async def heartbeat_loop(self) -> None:
        """Background loop for emitting strategy heartbeats."""
        await asyncio.sleep(1.0)
        try:
            while self.strategy._running:
                await asyncio.sleep(2.0)
                try:
                    self.strategy.heartbeat_seq += 1
                    lag_ms = int((time.time() - self.strategy.last_data_timestamp) * 1000)
                    component = f"strategy.{self.strategy.name}"
                    hb_topic = heartbeat_topic_from_component(component)
                    hb_msg = self.build_heartbeat_envelope(lag_ms, hb_topic)
                    if self.strategy.msg_publisher:
                        await self.strategy.msg_publisher.send(hb_topic, hb_msg)
                except Exception as e:
                    logger.error(f"Strategy {self.strategy.name}: Heartbeat error: {e}")
        except asyncio.CancelledError:
            logger.info(f"Strategy {self.strategy.name}: Heartbeat loop cancelled")
            raise
