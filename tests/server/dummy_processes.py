"""Test doubles for process spawning tests."""

import asyncio
from collections.abc import Awaitable

CALL_LOG: list[str] = []


def reset_call_log() -> None:
    """Clear the call log for test isolation."""
    CALL_LOG.clear()


class SyncProcess:
    """Test double for synchronous process execution."""

    def __init__(self, identifier: str) -> None:
        """Initialize the instance."""
        self.identifier = identifier

    def start(self) -> str:
        """Execute synchronous start and log call."""
        CALL_LOG.append(f"sync:{self.identifier}")
        return "done"


class AsyncProcess:
    """Test double for asynchronous process execution."""

    def __init__(self, identifier: str) -> None:
        """Initialize the instance."""
        self.identifier = identifier

    async def start(self) -> None:
        """Execute asynchronous start and log call."""
        CALL_LOG.append(f"async:{self.identifier}")
        await asyncio.sleep(0)


class SyncReturnsAwaitableProcess:
    """Test double for sync method returning awaitable."""

    def __init__(self, identifier: str) -> None:
        """Initialize the instance."""
        self.identifier = identifier

    def start(self) -> Awaitable[None]:
        """Return awaitable coroutine for awaited execution."""

        async def _inner() -> None:
            CALL_LOG.append(f"awaitable:{self.identifier}")
            await asyncio.sleep(0)

        return _inner()


class FailingProcess:
    """Test double for process that raises exception on start."""

    def __init__(self, identifier: str) -> None:
        """Initialize the instance."""
        self.identifier = identifier

    def start(self) -> None:
        """Raise RuntimeError to simulate process failure."""
        raise RuntimeError(f"boom:{self.identifier}")
