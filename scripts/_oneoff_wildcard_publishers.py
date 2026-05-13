"""One-off: flip Kraken publishers to enabled=true + symbols=["*"].

Night-shift snapshot 2026-05-13: capture every instrument's ticks during
the off-hours window. Uses ``close_and_insert`` to keep the bitemporal
``settings`` row history intact (SCD2). Not part of the deployed code
path; safe to delete after the night run.
"""

import asyncio
import json
from datetime import UTC
from datetime import datetime

from snapper.config.settings import get_bootstrap_settings
from snapper.data.models import Setting
from snapper.data.repository import close_and_insert
from snapper.data.repository import get_repository

_TARGET_KEYS: tuple[str, ...] = (
    "process_kraken_feed_publisher",
    "process_kraken_futures_feed_publisher",
    "process_kraken_equities_feed_publisher",
    "process_walutomat_feed_publisher",
)


async def _flip_publisher(session: object, key: str, now: datetime) -> dict[str, object]:
    """Read the active row for ``key`` and re-insert with wildcard symbols."""
    from sqlalchemy import select

    result = await session.execute(
        select(Setting).where(
            Setting.key == key,
            Setting.timestamp <= now,
            Setting.known_to > now,
        )
    )
    existing = result.scalar_one_or_none()
    if existing is None:
        return {"key": key, "status": "missing"}
    config = json.loads(existing.value)
    before = {
        "enabled": config.get("enabled"),
        "symbols": config.get("parameters", {}).get("symbols"),
    }
    config["enabled"] = True
    parameters = dict(config.get("parameters") or {})
    parameters["symbols"] = ["*"]
    config["parameters"] = parameters
    new_value = json.dumps(config, indent=4)
    await close_and_insert(
        session=session,
        model=Setting,
        match_filters=[Setting.key == key],
        new_values={
            "key": key,
            "value": new_value,
            "category": existing.category,
            "description": existing.description,
            "is_encrypted": existing.is_encrypted,
            "updated_by": "night_shift_wildcard",
            "session_id": existing.session_id,
            "sequence_id": existing.sequence_id + 1,
        },
        bus_time=now,
    )
    return {"key": key, "status": "updated", "before": before}


async def main() -> None:
    settings = get_bootstrap_settings()
    repository = get_repository(settings.db_url)
    now = datetime.now(UTC)
    async with repository.session() as session:
        results = [await _flip_publisher(session, key, now) for key in _TARGET_KEYS]
        await session.commit()
    for entry in results:
        print(json.dumps(entry))


if __name__ == "__main__":
    asyncio.run(main())
