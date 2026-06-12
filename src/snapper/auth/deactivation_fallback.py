"""Shared DB-backed user deactivation fallback helpers."""

import asyncio
import contextlib
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Coroutine
from typing import Final

from loguru import logger

from snapper.data.repository import Repository

DEACTIVATION_FALLBACK_SCAN_INTERVAL_S: Final[float] = 5.0
"""Seconds between DB fallback scans for inactive users."""

_deactivation_fallback_sleep = asyncio.sleep


def start_deactivation_fallback_task(
    current_task: asyncio.Task[None] | None,
    loop_factory: Callable[[], Coroutine[object, object, None]],
) -> asyncio.Task[None]:
    """Return a running fallback task, reusing a healthy existing one.

    Args:
        current_task: Existing scan task, if one has already been created.
        loop_factory: Callable that builds the fallback loop coroutine.

    Returns:
        The still-running existing task, or a newly-created task.
    """
    if current_task is not None and not current_task.done():
        return current_task
    return asyncio.create_task(loop_factory())


async def stop_deactivation_fallback_task(
    current_task: asyncio.Task[None] | None,
) -> None:
    """Cancel and await a fallback task if it exists.

    Args:
        current_task: Existing scan task to stop.
    """
    if current_task is None:
        return
    current_task.cancel()
    with contextlib.suppress(asyncio.CancelledError, Exception):
        await current_task


async def run_deactivation_fallback_loop(
    scan_once: Callable[[], Awaitable[None]],
    *,
    component_name: str,
) -> None:
    """Periodically run one deactivation fallback scan until cancelled.

    Args:
        scan_once: Callback that performs a single fallback scan.
        component_name: Name included in cancellation logs.
    """
    try:
        while True:
            await scan_once()
            await _deactivation_fallback_sleep(DEACTIVATION_FALLBACK_SCAN_INTERVAL_S)
    except asyncio.CancelledError:
        logger.info("{}: deactivation fallback scan loop cancelled", component_name)
        raise


async def list_inactive_user_public_ids(
    repository_factory: Callable[[], Repository] | None,
    user_public_ids: list[str],
    *,
    component_name: str,
) -> list[str]:
    """Return inactive user ids from repository lookup, tolerating fallback failures.

    Args:
        repository_factory: Callable that creates repositories for fallback lookups.
        user_public_ids: Candidate user ids to check.
        component_name: Name included in warning logs.

    Returns:
        Inactive candidate ids, or an empty list when the lookup is disabled or fails.
    """
    if repository_factory is None or not user_public_ids:
        return []
    try:
        return await repository_factory().list_inactive_user_public_ids(user_public_ids)
    except AttributeError:
        return []
    except Exception as exc:
        logger.warning("{} deactivation fallback scan failed: {}", component_name, exc)
        return []
