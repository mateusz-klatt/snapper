"""Periodic AI-research round trigger.

The API lifespan owns one instance. Each tick atomically replaces the
pending research round through :meth:`Repository.create_ai_research_round`
and then emits a best-effort WebSocket wake. Persistence is the primary
contract: publisher failures never undo or invalidate the new round.

Configuration:

* ``AI_RESEARCH_TRIGGER_INTERVAL_SECONDS`` controls the periodic cadence.
  The default is 30 minutes and values below one minute are clamped.
"""

import asyncio
import contextlib
import logging
import os
from datetime import UTC
from datetime import datetime
from typing import Final
from typing import Protocol
from uuid import uuid7

from snapper.data.repository import Repository
from snapper.data.repository_types import AiResearchRoundInsertRow
from snapper.messaging.schemas.data import AiResearchRequestFrameData

logger = logging.getLogger(__name__)

DEFAULT_INTERVAL_SECONDS: Final = 1800.0
MIN_INTERVAL_SECONDS: Final = 60.0
PERIODIC_TRIGGER: Final = "periodic"

_INTERVAL_ENV_VAR: Final = "AI_RESEARCH_TRIGGER_INTERVAL_SECONDS"

ENV_VARS: Final[frozenset[str]] = frozenset({_INTERVAL_ENV_VAR})
"""Public allowlist of environment variables owned by this service."""


class _ResearchSequenceTracker(Protocol):
    """Subset of ``SequenceTracker`` needed for wake provenance."""

    @property
    def session_id(self) -> str:
        """Return the publisher session identifier."""
        ...

    def next_sequence(self, stream: str) -> int:
        """Return the next transport sequence for a topic."""
        ...


class _ResearchPublisher(Protocol):
    """Subset of ``MessagePublisher`` needed by the trigger."""

    @property
    def tracker(self) -> _ResearchSequenceTracker:
        """Return the publisher's shared sequence tracker."""
        ...

    async def send(self, stream_key: str, data: AiResearchRequestFrameData) -> None:
        """Send one complete research wake frame."""
        ...


def _resolve_interval(env_value: str | None) -> float:
    """Parse the cadence, falling back to the default on invalid input.

    Args:
        env_value: Raw environment value, or ``None`` when unset.

    Returns:
        Positive cadence in seconds, clamped to
        :data:`MIN_INTERVAL_SECONDS`.
    """
    if env_value is None:
        return DEFAULT_INTERVAL_SECONDS
    try:
        parsed = float(env_value)
    except ValueError:
        return DEFAULT_INTERVAL_SECONDS
    if parsed <= 0:
        return DEFAULT_INTERVAL_SECONDS
    return max(parsed, MIN_INTERVAL_SECONDS)


class AiResearchTriggerService:
    """Create latest-wins research rounds and wake the researcher."""

    def __init__(
        self,
        *,
        repo: Repository,
        msg_publisher: _ResearchPublisher | None = None,
        interval_seconds: float | None = None,
    ) -> None:
        """Wire dependencies and resolve the periodic cadence.

        Args:
            repo: Repository owning atomic supersede-before-create.
            msg_publisher: Shared publisher for auxiliary wake frames.
            interval_seconds: Explicit cadence override; ``None`` reads
                ``AI_RESEARCH_TRIGGER_INTERVAL_SECONDS``.
        """
        if interval_seconds is None:
            interval_seconds = _resolve_interval(os.environ.get(_INTERVAL_ENV_VAR))
        self._repo = repo
        self._msg_publisher = msg_publisher
        self._interval_seconds = interval_seconds
        self._stopping = asyncio.Event()
        self._loop_task: asyncio.Task[None] | None = None

    @property
    def interval_seconds(self) -> float:
        """Return the configured periodic cadence.

        Returns:
            Seconds between periodic ticks.
        """
        return self._interval_seconds

    async def start(self) -> None:
        """Take one defensive eager tick and spawn the periodic loop."""
        await self._guarded_tick()
        self._stopping.clear()
        self._loop_task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        """Signal, cancel, and await the loop when it exists."""
        self._stopping.set()
        task = self._loop_task
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._loop_task = None

    async def _loop(self) -> None:
        """Sleep the configured cadence and tick until stopped."""
        while not self._stopping.is_set():
            await asyncio.sleep(self._interval_seconds)
            if self._stopping.is_set():
                return
            await self._guarded_tick()

    async def _guarded_tick(self) -> None:
        """Run one tick and preserve the loop after any failure."""
        try:
            await self._tick()
        except Exception:
            logger.exception("AiResearchTriggerService tick failed; continuing on next tick")

    async def _tick(self, now: datetime | None = None) -> None:
        """Persist one periodic round and emit its best-effort wake.

        Args:
            now: Creation timestamp override for deterministic tests.
        """
        created_at = now if now is not None else datetime.now(UTC)
        row: AiResearchRoundInsertRow = {
            "trigger": PERIODIC_TRIGGER,
            "created_at": created_at,
        }
        round_public_id = await self._repo.create_ai_research_round(row)
        logger.info(
            "AiResearchTriggerService created research round %s",
            round_public_id,
        )
        await self._publish_request_frame(
            round_public_id=round_public_id,
            trigger=PERIODIC_TRIGGER,
            created_at=created_at,
        )

    async def _publish_request_frame(
        self,
        *,
        round_public_id: str,
        trigger: str,
        created_at: datetime,
    ) -> None:
        """Best-effort publish a wake for one committed research round.

        Args:
            round_public_id: Identifier returned after atomic creation.
            trigger: Stable reason persisted on the round.
            created_at: Timestamp shared by the round and frame.
        """
        publisher = self._msg_publisher
        if publisher is None:
            logger.warning(
                "AI-research wake not broadcast for round %s: publisher unavailable",
                round_public_id,
            )
            return
        try:
            topic = f"ai_research.{round_public_id}.request"
            tracker = publisher.tracker
            frame = AiResearchRequestFrameData(
                public_id=str(uuid7()),
                timestamp=created_at,
                session_id=tracker.session_id,
                sequence_id=tracker.next_sequence(topic),
                round_public_id=round_public_id,
                trigger=trigger,
            )
            await publisher.send(topic, frame)
        except Exception:
            logger.exception(
                "AiResearchTriggerService failed to broadcast wake for committed round %s",
                round_public_id,
            )
