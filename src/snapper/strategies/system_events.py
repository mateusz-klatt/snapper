"""System message routing for trading strategies.

Routes incoming ZMQ system messages (settings updates, heartbeats,
symbol mappings, replay control) to appropriate handlers.
"""

import logging
import time
from typing import Any

from snapper.application.services.settings import SettingsService
from snapper.infrastructure.symbols.functions import _get_db_mapper
from snapper.messaging.schemas.data import HeartbeatData
from snapper.messaging.schemas.data import ReplayEndData
from snapper.messaging.schemas.data import ReplayStartData
from snapper.messaging.schemas.data import SettingChangedData

logger = logging.getLogger(__name__)


class SystemMessageRouter:
    """Routes system messages to strategy-specific handlers.

    Handles settings updates, feed heartbeats, symbol mapping
    refreshes, and replay lifecycle events.

    Attributes:
        strategy: Reference to the owning strategy instance.
    """

    def __init__(self, strategy: Any) -> None:
        """Initialize the system message router.

        Args:
            strategy: The BaseStrategy instance to route messages for.
        """
        self.strategy = strategy

    def handle_settings_update(self, envelope: SettingChangedData) -> None:
        """Handle dynamic settings update from ZMQ.

        Args:
            envelope: Settings change data with key and value.
        """
        try:
            settings_service = SettingsService.get_instance()
            if settings_service:
                parsed_value = settings_service._parse_value(envelope.value)
                settings_service._cache[envelope.key] = parsed_value
                logger.info(
                    f"Strategy {self.strategy.name}: Setting {envelope.key} updated via ZMQ event"
                )
        except (ValueError, TypeError, KeyError, RuntimeError) as e:
            logger.exception(f"Strategy {self.strategy.name}: Error handling settings update: {e}")

    def handle_system_heartbeat(self, topic_str: str, payload_str: str) -> None:
        """Handle feed heartbeat system message.

        Args:
            topic_str: The ZMQ topic string.
            payload_str: The JSON payload string.
        """
        exchange = topic_str.replace("system.heartbeats.feed.", "")
        heartbeat = HeartbeatData.from_json(payload_str)
        self.strategy._feed_heartbeats[exchange] = {
            "timestamp": time.time(),
            "status": heartbeat.status,
            "lag_ms": heartbeat.lag_ms,
            "component": heartbeat.component,
            "symbol_count": heartbeat.meta.get("symbol_count", 0),
        }
        logger.debug(
            f"Strategy {self.strategy.name}: Received heartbeat from feed.{exchange}, "
            f"status={heartbeat.status}, lag={heartbeat.lag_ms}ms, "
            f"symbols={heartbeat.meta.get('symbol_count', 0)}"
        )

    def handle_symbol_aliases_update(self) -> None:
        """Handle symbol_aliases system message by refreshing cache."""
        logger.info(
            f"Strategy {self.strategy.name}: Received symbol_aliases update, refreshing cache"
        )
        _get_db_mapper().trigger_cache_invalidation(fail_fast=False)

    async def handle_replay_start(self, payload_str: str) -> None:
        """Handle replay start system message.

        Args:
            payload_str: The JSON payload string.
        """
        replay_envelope = ReplayStartData.from_json(payload_str)
        logger.info(f"Strategy {self.strategy.name}: Replay started, resetting state")
        await self.strategy.reset()
        self.strategy._last_data_ts = (
            replay_envelope.started_at.timestamp() if replay_envelope.started_at else None
        )

    def handle_replay_end(self, payload_str: str) -> None:
        """Handle replay end system message.

        Args:
            payload_str: The JSON payload string.
        """
        ReplayEndData.from_json(payload_str)
        logger.info(f"Strategy {self.strategy.name}: Replay ended")
        self.strategy._last_data_ts = None
