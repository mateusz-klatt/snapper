"""Per-frame provenance context propagation (PnL Phase 1 S4).

Pins the ContextVar mechanics the replay guard depends on: a detached
task inherits the provenance of the frame that spawned it and stays
immune to later frames mutating the var.
"""

import asyncio
from datetime import UTC
from datetime import datetime

import pytest

from snapper.strategies.frame_context import get_frame_provenance
from snapper.strategies.frame_context import restore_frame_provenance
from snapper.strategies.frame_context import set_frame_provenance
from snapper.strategies.frame_context import snapshot_frame_provenance


@pytest.mark.asyncio
async def test_default_provenance_is_live() -> None:
    """An untouched context reads as live.

    Given: a fresh task context with no frame recorded,
    When: the provenance is read,
    Then: the live default returns.
    """

    async def _fresh_read() -> tuple[str, datetime | None, datetime | None]:
        """Read the provenance from a fresh task context.

        Returns:
            The recorded provenance tuple.
        """
        return get_frame_provenance()

    origin, start, end = await asyncio.create_task(_fresh_read())
    assert origin == "live"
    assert start is None
    assert end is None


@pytest.mark.asyncio
async def test_detached_task_inherits_frame_provenance() -> None:
    """A detached task keeps the provenance of its spawning frame.

    Given: a replay frame recorded, a detached task created, and the
        var then mutated by a LATER live frame,
    When: the detached task reads the provenance after the mutation,
    Then: it still sees the replay provenance captured at creation —
        the consult pattern's detached emit stays correctly stamped.
    """
    window_start = datetime(2026, 7, 1, tzinfo=UTC)
    window_end = datetime(2026, 7, 2, tzinfo=UTC)
    set_frame_provenance("replay", window_start, window_end)
    release = asyncio.Event()

    async def _detached_read() -> tuple[str, datetime | None, datetime | None]:
        """Wait for the mutation, then read the inherited provenance.

        Returns:
            The provenance visible inside the detached task.
        """
        await release.wait()
        return get_frame_provenance()

    task = asyncio.create_task(_detached_read())
    set_frame_provenance("live", None, None)
    release.set()
    origin, start, end = await task
    assert origin == "replay"
    assert start == window_start
    assert end == window_end
    assert get_frame_provenance() == ("live", None, None)


@pytest.mark.asyncio
async def test_snapshot_restore_scopes_provenance_to_frame() -> None:
    """A frame-scoped window restores the pre-frame provenance.

    Given: a snapshot taken before a replay frame records its
        provenance and a detached task spawned inside the window,
    When: the window is restored,
    Then: the parent context reads live again while the detached task
        keeps the replay copy — a replay frame never sticks to the
        long-lived listener task.
    """
    release = asyncio.Event()

    async def _detached_read() -> tuple[str, datetime | None, datetime | None]:
        """Wait for the restore, then read the inherited provenance.

        Returns:
            The provenance visible inside the detached task.
        """
        await release.wait()
        return get_frame_provenance()

    token = snapshot_frame_provenance()
    set_frame_provenance("replay", datetime(2026, 7, 1, tzinfo=UTC), None)
    task = asyncio.create_task(_detached_read())
    restore_frame_provenance(token)
    release.set()
    origin, start, _end = await task
    assert origin == "replay"
    assert start == datetime(2026, 7, 1, tzinfo=UTC)
    assert get_frame_provenance() == ("live", None, None)
