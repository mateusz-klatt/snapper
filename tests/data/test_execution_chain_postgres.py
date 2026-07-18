"""Opt-in live-PostgreSQL dual-dialect proof for the execution chain tip.

The pure ``execution_chain`` module is the backend-independent reference. The
SQLite repository test proves ``repo_tip == pure_tip`` on ``aiosqlite``; this
module proves the same equality on ``postgresql+asyncpg``. Together they
witness that the canonical serialization reads back identically on both
dialects (UUID case, timestamptz microseconds, decimal text, absent optionals),
so a certified tip never differs by backend.

Every test SKIPS unless the session database URL (``DB_URL``, the Makefile's
``TEST_DB_URL`` pass-through) is a PostgreSQL URL, so the default SQLite
``make test`` / ``make check-all`` runs are undisturbed. Opt in against the
docker-compose ``postgres:16`` dev profile with a migrated scratch database::

    make test TEST_DB_URL=postgresql+asyncpg://snapper:example@localhost:5432/snapper_scope_proof

Tests write only executions keyed by per-test random wallet and order
identities; executions are physically immutable (the immutability triggers
refuse DELETE/TRUNCATE), so they are never deleted and per-test isolation comes
from the random keys. Never point it at production.
"""

import os
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

from snapper.application.portfolio.execution_chain import ExecutionChainRecord
from snapper.application.portfolio.execution_chain import execution_chain_genesis
from snapper.application.portfolio.execution_chain import extend_execution_chain
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Execution
from snapper.data.repository import SQLAlchemyRepository

_EXCHANGE = "walutomat"
_MODE = "live"
_SESSION = "00000000-0000-7000-8000-000000000501"
_TS = datetime(2026, 7, 17, 8, 0, 0, 123456, tzinfo=UTC)


def _configured_backend_name() -> str:
    """Return the backend name of the session-configured database URL, or ''."""
    url = os.environ.get("DB_URL", "")
    if not url:
        return ""
    try:
        return make_url(url).get_backend_name()
    except ArgumentError:
        return ""


pytestmark = pytest.mark.skipif(
    _configured_backend_name() != "postgresql",
    reason=(
        "live-PostgreSQL dual-dialect proof: opt in via "
        "make test TEST_DB_URL=postgresql+asyncpg://... (see module docstring)"
    ),
)


@pytest.fixture()
async def repository() -> AsyncIterator[SQLAlchemyRepository]:
    """Create one repository engine against the live PostgreSQL database."""
    result = SQLAlchemyRepository(os.environ["DB_URL"])
    try:
        yield result
    finally:
        await result.engine.dispose()


def _pair(
    sequence: int,
    wallet: str,
    *,
    operator_public_id: str | None = "00000000-0000-7000-8000-000000000401",
    exec_id: str | None = None,
    trade_id: str | None = None,
    price_decimal: str | None = "1.25",
    size_decimal: str | None = "2.0",
    fee_decimal: str | None = "0.1",
    numeric_provenance: str | None = "venue_raw",
    executed_at: datetime | None = _TS + timedelta(seconds=1),
) -> tuple[Execution, ExecutionChainRecord]:
    """Build an ORM execution and its expected chain record from one set of values."""
    public_id = str(uuid4())
    order_public_id = str(uuid4())
    execution = Execution(
        scope_sequence=sequence,
        public_id=public_id,
        order_public_id=order_public_id,
        wallet_public_id=wallet,
        operator_public_id=operator_public_id,
        exchange=_EXCHANGE,
        mode=_MODE,
        exec_id=exec_id,
        trade_id=trade_id,
        side="buy",
        status="filled",
        price=1.25,
        size=2.0,
        fee=0.1,
        fee_asset="PLN",
        price_decimal=price_decimal,
        size_decimal=size_decimal,
        fee_decimal=fee_decimal,
        numeric_provenance=numeric_provenance,
        liquidity_role="maker",
        session_id=_SESSION,
        sequence_id=sequence,
        timestamp=_TS,
        executed_at=executed_at,
        known_to=KNOWN_TO_MAX,
    )
    record = ExecutionChainRecord(
        scope_sequence=sequence,
        public_id=public_id,
        order_public_id=order_public_id,
        wallet_public_id=wallet,
        operator_public_id=operator_public_id,
        exchange=_EXCHANGE,
        mode=_MODE,
        exec_id=exec_id,
        trade_id=trade_id,
        side="buy",
        status="filled",
        fee_asset="PLN",
        price_decimal=price_decimal,
        size_decimal=size_decimal,
        fee_decimal=fee_decimal,
        numeric_provenance=numeric_provenance,
        liquidity_role="maker",
        timestamp=_TS,
        executed_at=executed_at,
    )
    return execution, record


async def test_chain_tip_matches_the_pure_fold_on_postgresql(
    repository: SQLAlchemyRepository,
) -> None:
    """The tip read from PostgreSQL equals the backend-independent pure fold."""
    wallet = str(uuid4())
    pairs = [_pair(sequence, wallet, exec_id=f"E-{sequence}") for sequence in (1, 2, 3)]
    async with repository.session() as session:
        session.add_all([execution for execution, _ in pairs])
        await session.commit()
    genesis = execution_chain_genesis(wallet, _EXCHANGE, _MODE)
    expected = extend_execution_chain(genesis, [record for _, record in pairs])
    actual = await repository.get_spot_execution_chain_tip(wallet, _EXCHANGE, _MODE, 0, genesis, 3)
    assert actual == expected


async def test_absent_optionals_round_trip_identically_on_postgresql(
    repository: SQLAlchemyRepository,
) -> None:
    """A row with every optional absent reads back to the same tip on PostgreSQL."""
    wallet = str(uuid4())
    execution, record = _pair(
        1,
        wallet,
        operator_public_id=None,
        exec_id=None,
        trade_id=None,
        price_decimal=None,
        size_decimal=None,
        fee_decimal=None,
        numeric_provenance=None,
        executed_at=None,
    )
    async with repository.session() as session:
        session.add(execution)
        await session.commit()
    genesis = execution_chain_genesis(wallet, _EXCHANGE, _MODE)
    expected = extend_execution_chain(genesis, [record])
    actual = await repository.get_spot_execution_chain_tip(wallet, _EXCHANGE, _MODE, 0, genesis, 1)
    assert actual == expected
