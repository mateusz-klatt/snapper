"""Preserve observation payload text through the actual executor and database boundary."""

from collections.abc import AsyncIterator
from pathlib import Path
from typing import Literal

import pytest
from sqlalchemy import select

from snapper.application.engine.service import compute_shard_key
from snapper.data.models import VenueEvent
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import RecordVenueEventParams
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.messaging.executors.base import ExchangeExecutorService

_WALLET = "00000000-0000-7000-8000-000000000211"
_INSTRUMENT = "BTC-USD-PERP"
_PAYLOAD = (
    ' {\n  "quantity_unit": "unknown", "rebase_multiplier": null, '
    '"last_qty": "1.000000000000000001",\n'
    '  "future_extension": { "price_unit": "unknown", "raw": "01.2300" }\n}\n'
)


class PayloadExecutor(ExchangeExecutorService[ExchangeClientBase]):
    """Exercise only observation persistence without starting a transport or service."""

    def _create_exchange_client(self) -> ExchangeClientBase:
        """Refuse accidental transport startup in this persistence-only fixture."""
        raise AssertionError("Observation payload tests must not start an exchange client")

    def _get_exchange_name(self) -> Literal["kraken_futures"]:
        """Return the actual fixture account's execution venue."""
        return "kraken_futures"


@pytest.fixture
async def payload_repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create and dispose a private SQLite schema without any production access."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'payload.db'}")
    await repository.create_all()
    try:
        yield repository
    finally:
        await repository.engine.dispose()


@pytest.mark.parametrize(
    ("supplied", "payload"),
    [(False, None), (True, None), (True, ""), (True, _PAYLOAD)],
    ids=["omitted", "null", "empty-text", "unknown-units-verbatim"],
)
async def test_executor_payload_survives_database_storage_and_repository_read_verbatim(
    payload_repository: SQLAlchemyRepository, supplied: bool, payload: str | None
) -> None:
    """Payload preservation must not parse, normalize, or invent missing unit evidence.

    Given: An optional observation payload containing unknown units and exact numeric strings,
    When: The real executor writer inserts it and the repository reads the stored event,
    Then: Database and public row expose the original text byte for byte or honest null.
    """
    executor = PayloadExecutor(wallet_public_id=_WALLET)
    executor.repository = payload_repository
    params: RecordVenueEventParams = {
        "event_type": "order_accepted",
        "exchange_name": "kraken_futures",
        "instrument": _INSTRUMENT,
        "exchange_order_id": "payload-venue-order",
        "client_order_id": "payload-client-order",
    }
    if supplied:
        params["payload_json"] = payload
    await executor._record_venue_event(params)
    shard_key = compute_shard_key(
        instrument=_INSTRUMENT,
        exchange="kraken_futures",
        mode="live",
        wallet_public_id=_WALLET,
        strategy_tag=None,
    )
    rows = await payload_repository.get_venue_events_after(shard_key, 0)
    assert len(rows) == 1
    assert rows[0]["payload_json"] == payload
    assert rows[0]["event_type"] == "order_accepted"
    assert rows[0]["wallet_public_id"] == _WALLET
    assert rows[0]["fill_size"] is None
    async with payload_repository.session() as session:
        stored = (await session.execute(select(VenueEvent.payload_json))).scalar_one()
    assert stored == payload
