"""Read-only operator inspection of Android push configuration CAS state."""

import asyncio
import json
from dataclasses import asdict

import typer

from snapper.config.settings import get_bootstrap_settings
from snapper.data.repository import AndroidConfigurationStore
from snapper.data.repository import get_repository


def android_push_state() -> None:
    """Print the opaque revision required by the next runtime configuration decision."""

    async def read_state() -> None:
        """Read the current active or disabled head without loading sender credentials."""
        repo = get_repository(get_bootstrap_settings().db_url)
        state = await AndroidConfigurationStore(repo).revision()
        typer.echo(json.dumps(asdict(state), sort_keys=True))

    asyncio.run(read_state())
