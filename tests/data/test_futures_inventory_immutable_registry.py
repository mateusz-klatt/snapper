"""Inventory history remains immutable through generic writers and demo normalization."""

from datetime import UTC
from datetime import datetime

import pytest
from sqlalchemy import column
from sqlalchemy import inspect
from sqlalchemy import table
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from sqlalchemy.orm import aliased

from scripts.seed_demo import _immutable_ledger_table_names
from snapper.data.models import FuturesInventoryReadAttempt
from snapper.data.models import FuturesInventoryReadOutcome
from snapper.data.repository import IMMUTABLE_LEDGER_TABLE_NAMES
from snapper.data.repository import ExecutionPhysicalMutationError
from snapper.data.repository import _guard_immutable_ledger
from snapper.data.repository import close_and_insert
from snapper.data.repository import close_and_insert_sync

type _InventoryEvidence = FuturesInventoryReadAttempt | FuturesInventoryReadOutcome

_MODELS = (FuturesInventoryReadAttempt, FuturesInventoryReadOutcome)
_NOW = datetime(2026, 10, 24, 0, 0, tzinfo=UTC)


def test_inventory_registry_also_excludes_demo_wallet_normalization() -> None:
    """Both inventory histories share the existing central mutation and seeder exclusion registry.

    Given: The actual two inventory models and the demo normalizer's existing registry reader,
    When: Their physical names are compared with both protection sets,
    Then: Neither inventory history is eligible for generic mutation or wallet normalization.
    """
    names = {model.__tablename__ for model in _MODELS}
    assert names <= IMMUTABLE_LEDGER_TABLE_NAMES
    assert names <= _immutable_ledger_table_names()


@pytest.mark.parametrize("model", _MODELS)
def test_inventory_history_refuses_every_generic_physical_spelling(
    model: type[_InventoryEvidence],
) -> None:
    """Aliases and alternative SQLAlchemy targets cannot bypass inventory immutability.

    Given: An actual immutable inventory attempt or outcome model,
    When: Its class, alias, mapper, instance, table and both physical-name forms are guarded,
    Then: Every representation refuses before any generic physical mutation can run.
    """
    for target in (
        model,
        aliased(model),
        inspect(model),
        model(),
        model.__table__,
        model.__tablename__,
        table(model.__tablename__, column("id")),
    ):
        with pytest.raises(ExecutionPhysicalMutationError, match="inventory evidence mutation"):
            _guard_immutable_ledger(target, "inventory evidence mutation")


@pytest.mark.parametrize("model", _MODELS)
def test_sync_temporal_writer_refuses_inventory_before_database_access(
    model: type[_InventoryEvidence],
) -> None:
    """A synchronous generic temporal writer cannot supersede inventory history.

    Given: A real unbound SQLAlchemy session with no database or inventory tables,
    When: The generic synchronous temporal replacement targets an inventory model,
    Then: The immutable-ledger error occurs before any database bind or query is required.
    """
    with (
        Session() as session,
        pytest.raises(ExecutionPhysicalMutationError, match="supersede is refused"),
    ):
        close_and_insert_sync(session, model, [], {}, _NOW)


@pytest.mark.parametrize("model", _MODELS)
async def test_async_temporal_writer_refuses_inventory_before_database_access(
    model: type[_InventoryEvidence],
) -> None:
    """An asynchronous generic temporal writer cannot supersede inventory history.

    Given: A real unbound asynchronous SQLAlchemy session with no database or tables,
    When: The generic asynchronous temporal replacement targets an inventory model,
    Then: The immutable-ledger error occurs before any database bind or query is required.
    """
    async with AsyncSession() as session:
        with pytest.raises(ExecutionPhysicalMutationError, match="supersede is refused"):
            await close_and_insert(session, model, [], {}, _NOW)
