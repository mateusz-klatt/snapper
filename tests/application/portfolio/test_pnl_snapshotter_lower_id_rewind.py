"""Prove durable rewind when a late commit changes membership below a saved fill ID."""

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy.engine import make_url

from snapper.application.portfolio import pnl_snapshotter
from snapper.application.portfolio.execution_chain import ExecutionChainError
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Execution
from snapper.data.models import PortfolioPnlPoint
from snapper.data.models import VenueEvent
from snapper.data.models import WalletCredential
from snapper.data.repository import PortfolioPnlSampleScope
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import PortfolioPnlSampleRow
from tests.application.portfolio import test_pnl_snapshotter_continuation as restart
from tests.application.portfolio import test_pnl_snapshotter_idless_restart as idless
from tests.application.portfolio import test_pnl_timeline_service as timeline
from tests.data import test_pnl_crypto_usd_plane_repository as prices
from tests.data import test_pnl_timeline_execution_prefix_repository as prefix
from tests.data import test_pnl_timeline_sample_repository as samples


@pytest.fixture(params=("sqlite", "postgresql"))
async def histories(
    request: pytest.FixtureRequest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[tuple[str, str]]:
    """Give replay and control independent databases, opting into PostgreSQL explicitly."""
    monkeypatch.setattr(pnl_snapshotter, "MAX_CORRECTION_CHUNK_MINUTES", 2)
    monkeypatch.delenv("PNL_FX_SHADOW_PINNING_ENABLED", raising=False)
    if request.param == "sqlite":
        yield tuple(
            f"sqlite+aiosqlite:///{tmp_path / name}" for name in ("replay.db", "control.db")
        )
        return
    configured = os.environ.get("PNL_TEST_POSTGRES_URL")
    if configured is None:
        pytest.skip("requires isolated PNL_TEST_POSTGRES_URL")
    base = make_url(configured)
    assert base.get_backend_name() == "postgresql"
    names = (f"pnl_lower_id_{uuid4().hex}", f"pnl_lower_id_{uuid4().hex}")
    maintenance = SQLAlchemyRepository(configured)
    created: list[str] = []
    try:
        async with maintenance.engine.connect() as connection:
            admin = await connection.execution_options(isolation_level="AUTOCOMMIT")
            for name in names:
                await admin.exec_driver_sql(f'CREATE DATABASE "{name}"')
                created.append(name)
        yield tuple(base.set(database=name).render_as_string(hide_password=False) for name in names)
    finally:
        async with maintenance.engine.connect() as connection:
            admin = await connection.execution_options(isolation_level="AUTOCOMMIT")
            for name in created:
                await admin.exec_driver_sql(f'DROP DATABASE "{name}"')
        await maintenance.engine.dispose()


@asynccontextmanager
async def _repository(url: str) -> AsyncIterator[SQLAlchemyRepository]:
    """Close each pool so a later writer reopens only committed database state."""
    repository = SQLAlchemyRepository(url)
    try:
        yield repository
    finally:
        await repository.engine.dispose()


def _fill(number: int) -> tuple[Execution, VenueEvent]:
    """Reserve ID 1 for the later commit while keeping both ID-less fills matched."""
    minute = samples._T0 + number * timedelta(minutes=1)
    execution = prefix._execution("kraken", number, minute)
    event = prefix._fill_event(prefix._CLIENT_ORDER, prefix._KRAKEN_SHARD, sequence_id=number)
    execution.public_id = f"00000000-0000-7000-8000-{number + 100:012x}"
    event.public_id = f"00000000-0000-7000-8000-{number + 200:012x}"
    event.id = 3 - number
    event.timestamp = event.received_at = event.venue_timestamp = minute
    event.cum_fill_size = float(number)
    event.status = "partially_filled"
    execution.status = "partial"
    event.exec_id = event.trade_id = execution.exec_id = execution.trade_id = None
    return execution, event


async def _seed(url: str, *, complete: bool) -> PortfolioPnlSampleScope:
    """Seed complete valuation evidence with one or both committed matched fills."""
    anchor = timeline._empty_anchor(prefix._WALLET, "live", "USD", samples._T0)
    async with _repository(url) as repository:
        await repository.create_all()
        async with repository.session() as session:
            session.add(PortfolioPnlPoint(**anchor, known_to=KNOWN_TO_MAX))
            session.add(
                WalletCredential(
                    wallet_public_id=prefix._WALLET,
                    exchange="kraken",
                    credential_type="api_key_secret",
                    encrypted_payload="isolated-replay-fixture",
                    timestamp=samples._T0,
                    session_id=samples._SESSION,
                    sequence_id=1,
                )
            )
            session.add_all((prefix._symbol(), prefix._instrument(), prefix._order()))
            session.add(
                prices._spec(prefix._INSTRUMENT, ver=prices._Ver(timestamp=prefix._EARLIER))
            )
            session.add_all(_fill(1))
            if complete:
                session.add_all(_fill(2))
            for number, minute in enumerate(restart._MINUTES, start=1):
                observation = samples._observation(
                    timestamp=minute,
                    balances_json='[{"currency":"USD","total":1000},{"currency":"BTC","total":2}]',
                )
                observation.sequence_id = number
                observation.public_id = f"00000000-0000-7000-8000-{number + 300:012x}"
                session.add(observation)
                session.add(
                    prices._candle(
                        prefix._INSTRUMENT,
                        minute - timedelta(minutes=1),
                        100.0 + number,
                        spec=prices._CandleSpec(
                            timestamp=minute,
                            public_id=f"00000000-0000-7000-8000-{number + 400:012x}",
                        ),
                    )
                )
            await session.commit()
    return samples._scope(epoch=anchor["epoch_public_id"])


@asynccontextmanager
async def _delayed_commit(repository: SQLAlchemyRepository) -> AsyncIterator[None]:
    """Hold the lower row uncommitted on PG; reproduce its visibility order on SQLite.

    PostgreSQL permits the lower-ID transaction to flush while a separate writer
    freezes the already-committed higher bound. SQLite serializes writes, so its
    reserved-ID append happens after that writer commits its first chunk.
    """
    if repository.dialect_name == "postgresql":
        async with repository.session() as session:
            session.add_all(_fill(2))
            await session.flush()
            yield
            await session.commit()
    else:
        yield
        async with repository.session() as session:
            session.add_all(_fill(2))
            await session.commit()


async def _finish(url: str, scope: PortfolioPnlSampleScope) -> list[PortfolioPnlSampleRow]:
    """Finish five minutes in three chunks with a fresh pool for each writer."""
    for _ in range(3):
        async with _repository(url) as repository:
            await restart._snapshotter(repository)._tick_once()
    async with _repository(url) as repository:
        frontier = await idless._frontier(repository, scope)
        assert frontier.next_minute is None and frontier.tip == restart._TIP
        rows = await repository.get_portfolio_pnl_samples(
            samples._query(epoch=scope.epoch_public_id), restart._FIRST, restart._TIP
        )
    assert len(rows) == 5
    assert all(row["valuation_status"] == "complete" for row in rows)
    return rows


async def test_lower_id_membership_rewinds_to_uninterrupted_financial_output(
    histories: tuple[str, str],
) -> None:
    """Rewind a committed chunk after a smaller physical fill ID becomes visible.

    Given a frozen generation that has committed two minutes with witness ID 2.
    When an independent transaction publishes matched ID-less fill ID 1 inside its bound.
    Then real certification rejects the old digest, the writer rewinds without editing
    settled rows, and fresh writers persist the uninterrupted control's complete series.
    """
    replay_url, control_url = histories
    scope = await _seed(replay_url, complete=False)
    control_scope = await _seed(control_url, complete=True)
    async with (
        _repository(replay_url) as appender,
        _delayed_commit(appender),
        _repository(replay_url) as first,
    ):
        await restart._snapshotter(first)._tick_once()
        frozen = await idless._frontier(first, scope)
        versions = await idless._versions(first)
        assert frozen.next_minute == restart._THIRD
        assert frozen.venue_event_watermarks == {prefix._KRAKEN_SHARD: 2}
        assert frozen.raw_execution_watermarks == {"kraken": 1}
        assert len(versions) == 2
    async with _repository(replay_url) as reopened:
        fresh = await reopened.get_pnl_timeline_execution_prefix(
            prefix._WALLET, "live", frozen.as_of, capture_fill_witness_cut=True
        )
        witness = fresh["fill_witness_cut"]
        assert witness["watermarks"] == frozen.venue_event_watermarks
        assert witness["digest"] != frozen.fill_witness_digest
        assert frozen.fill_witness_digest is not None
        with pytest.raises(ExecutionChainError, match="changed_execution_fill_witness_cut"):
            await reopened.get_pnl_timeline_execution_prefix_at_watermarks(
                prefix._WALLET,
                "live",
                {"kraken": 1},
                frozen.as_of,
                fill_witness_cut={
                    "watermarks": witness["watermarks"],
                    "digest": frozen.fill_witness_digest,
                },
            )
        await restart._snapshotter(reopened)._tick_once()
        rewound = await idless._frontier(reopened, scope)
        assert rewound.generation == frozen.generation + 1
        assert rewound.revision == frozen.revision + 1
        assert rewound.window_start == rewound.next_minute == restart._FIRST
        assert rewound.raw_execution_watermarks == {"kraken": 2}
        assert rewound.venue_event_watermarks == frozen.venue_event_watermarks
        assert rewound.fill_witness_digest == witness["digest"]
        assert await idless._versions(reopened) == versions
    actual = await _finish(replay_url, scope)
    expected = await _finish(control_url, control_scope)
    assert [row["fee_pnl"] for row in actual] == [-0.25, -0.5, -0.5, -0.5, -0.5]
    assert restart._economics(actual) == restart._economics(expected)
