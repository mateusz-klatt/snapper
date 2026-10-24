"""Real source-event/execution atomicity before later accounting or publication writes."""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from pathlib import Path

import pytest
from sqlalchemy import select

from snapper.data.models import Execution
from snapper.data.models import VenueEvent
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import VenueEventInsertRow
from tests.data.test_execution_scope_ingest_repository import _seed_lineage
from tests.data.test_execution_scope_ingest_repository import _transaction_fill

_NOW = datetime(2026, 10, 23, 9, tzinfo=UTC)


@pytest.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create an isolated actual schema and order lineage for the transactional writers."""
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'atomic.db'}")
    await repo.create_all()
    await _seed_lineage(repo)
    try:
        yield repo
    finally:
        await repo.engine.dispose()


def _event() -> VenueEventInsertRow:
    """Provide an actual source observation envelope for the isolated fill."""
    fill = _transaction_fill()
    return {
        "wallet_public_id": fill["wallet_public_id"],
        "event_type": "fill_observed",
        "shard_key": "isolated-wallet.walutomat.live.USD-PLN",
        "exchange": "walutomat",
        "instrument": "USD-PLN",
        "mode": "live",
        "received_at": _NOW,
        "timestamp": _NOW,
        "session_id": fill["session_id"],
        "sequence_id": 911,
        "fill_price": fill["price"],
        "fill_size": fill["size"],
        "fee": fill["fee"],
        "fee_asset": fill["fee_asset"],
        "side": fill["side"],
    }


@pytest.mark.parametrize("commit", (True, False))
async def test_source_event_and_execution_follow_only_the_outer_financial_commit(
    repository: SQLAlchemyRepository, commit: bool
) -> None:
    """Both facts follow it.

    Given: a composed financial write,
    When: it commits or aborts,
    Then: both facts follow it.
    """
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        event = await repository.insert_venue_event_in_transaction(session, _event())
        execution = await repository.insert_execution_in_transaction(session, _transaction_fill())
        assert event.public_id and execution.public_id
        assert event.wallet_public_id == execution.wallet_public_id
        if commit:
            await session.commit()
        else:
            await session.rollback()
    async with repository.session() as session:
        events = list((await session.execute(select(VenueEvent))).scalars())
        executions = list((await session.execute(select(Execution))).scalars())
    assert len(events) == len(executions) == int(commit)


async def test_source_event_does_not_start_an_implicit_financial_transaction(
    repository: SQLAlchemyRepository,
) -> None:
    """Source insertion is refused.

    Given: no explicit isolation opener,
    When: called,
    Then: source insertion is refused.
    """
    async with repository.session() as session:
        with pytest.raises(RuntimeError, match="requires_caller_transaction"):
            await repository.insert_venue_event_in_transaction(session, _event())
        assert not session.in_transaction()


async def test_transactional_event_refuses_an_unscoped_legacy_wallet(
    repository: SQLAlchemyRepository,
) -> None:
    """No sentinel is used.

    Given: a missing wallet,
    When: financial insertion is attempted,
    Then: no sentinel is used.
    """
    row = _event()
    row.pop("wallet_public_id")
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        with pytest.raises(ValueError):
            await repository.insert_venue_event_in_transaction(session, row)
        await session.rollback()
    async with repository.session() as session:
        assert (await session.execute(select(VenueEvent))).first() is None
