"""Tests for the capture-first exact P&L execution-prefix repository read."""

import math
import os
from collections.abc import AsyncIterator
from collections.abc import Iterator
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Literal
from typing import TypedDict
from typing import Unpack
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import PropertyMock
from unittest.mock import patch
from uuid import uuid7

import pytest
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.portfolio.execution_chain import ExecutionChainError
from snapper.core.wallet_short import compute_wallet_short
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Execution
from snapper.data.models import ExecutionAnnulment
from snapper.data.models import Instrument
from snapper.data.models import Order
from snapper.data.models import Symbol
from snapper.data.models import VenueEvent
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import _PnlTimelineExecutionCandidate
from snapper.data.repository import _PnlTimelineExecutionPrefixSource
from snapper.data.repository import _PnlTimelineFillAssignmentState
from snapper.data.repository import _PnlTimelineFillIndex
from snapper.data.repository import _PnlTimelineFillNativeScopeKey
from snapper.data.repository import _PnlTimelineIndexedFill
from snapper.data.repository import _PnlTimelineNativeExactFillKey
from snapper.data.repository import _PnlTimelineScopeExactFillKey
from snapper.data.repository import _PnlTimelineScopeExecTradeFillKey
from snapper.data.repository_types import PnlTimelineExecutionPrefix
from snapper.data.repository_types import PnlTimelineOpeningExecutionRow

_AS_OF = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)
_EARLIER = _AS_OF - timedelta(minutes=1)
_LATER = _AS_OF + timedelta(minutes=1)
_SESSION = "00000000-0000-7000-8000-000000000901"
_WALLET = "0000face-0000-7000-8000-0000000000a1"
_FOREIGN_WALLET = "0000face-0000-7000-8000-0000000000a2"
_SYMBOL = "00000000-0000-7000-8000-000000000a01"
_COLLIDING_SYMBOL = "00000000-0000-7000-8000-000000000a02"
_INSTRUMENT = "00000000-0000-7000-8000-000000000b01"
_ZONDA_INSTRUMENT = "00000000-0000-7000-8000-000000000b02"
_COLLIDING_INSTRUMENT = "00000000-0000-7000-8000-000000000b03"
_ORDER = "00000000-0000-7000-8000-000000000c01"
_ZONDA_ORDER = "00000000-0000-7000-8000-000000000c02"
_COLLIDING_ORDER = "00000000-0000-7000-8000-000000000c03"
_CLIENT_ORDER = "client-kraken"
_ZONDA_CLIENT_ORDER = "client-zonda"
_KRAKEN_SHARD = "kraken.BTC-USD.live"
_ZONDA_SHARD = "zonda.BTC-USD.live"


def _configured_postgresql_url() -> str | None:
    """Return the opt-in live PostgreSQL URL or ``None`` for default runs."""
    database_url = os.environ.get("PNL_TEST_POSTGRES_URL")
    if database_url is None:
        return None
    try:
        if make_url(database_url).get_backend_name() == "postgresql":
            return database_url
    except ArgumentError:
        return None
    return None


class _CountingIndexedFillList(list[_PnlTimelineIndexedFill]):
    """Count actual resolver iterations over one indexed fill bucket."""

    def __init__(
        self,
        values: list[_PnlTimelineIndexedFill],
        counter: list[int],
    ) -> None:
        """Retain one mutable aggregate counter shared by all buckets."""
        super().__init__(values)
        self._counter = counter

    def __iter__(self) -> Iterator[_PnlTimelineIndexedFill]:
        """Yield rows while incrementing the aggregate visit counter."""
        for value in super().__iter__():
            self._counter[0] += 1
            yield value


class _CountingOrderPartitionDict(
    dict[_PnlTimelineNativeExactFillKey, list[_PnlTimelineIndexedFill]]
):
    """Count actual atomic-partition membership probes."""

    def __init__(
        self,
        values: dict[_PnlTimelineNativeExactFillKey, list[_PnlTimelineIndexedFill]],
        counter: list[int],
    ) -> None:
        """Retain one aggregate probe counter across the indexed mapping."""
        super().__init__(values)
        self._counter = counter

    def __contains__(self, key: object) -> bool:
        """Count one direct atomic-key lookup."""
        self._counter[0] += 1
        return super().__contains__(key)


type _ExactIndexKey = _PnlTimelineScopeExactFillKey | _PnlTimelineScopeExecTradeFillKey
type _ExactPartitionMap = dict[
    _PnlTimelineFillNativeScopeKey,
    list[_PnlTimelineIndexedFill],
]


class _CountingExactIndexMapping(Mapping[_ExactIndexKey, _ExactPartitionMap]):
    """Count direct complete-key probes against one exact reverse index."""

    def __init__(
        self,
        values: dict[_ExactIndexKey, _ExactPartitionMap],
        counter: list[int],
    ) -> None:
        """Retain indexed groups and one shared probe counter."""
        self._values = values
        self._counter = counter

    def __getitem__(self, key: _ExactIndexKey) -> _ExactPartitionMap:
        """Count and return one direct exact-key lookup."""
        self._counter[0] += 1
        return self._values[key]

    def __iter__(self) -> Iterator[_ExactIndexKey]:
        """Iterate group keys without changing direct-lookup accounting."""
        return iter(self._values)

    def __len__(self) -> int:
        """Return the number of exact identity groups."""
        return len(self._values)


class _CountingExactPartitionMapping(
    Mapping[_PnlTimelineFillNativeScopeKey, list[_PnlTimelineIndexedFill]]
):
    """Count actual native partitions visited inside exact identity groups."""

    def __init__(
        self,
        values: _ExactPartitionMap,
        counter: list[int],
    ) -> None:
        """Retain native partitions and one shared visit counter."""
        self._values = values
        self._counter = counter

    def __getitem__(
        self,
        key: _PnlTimelineFillNativeScopeKey,
    ) -> list[_PnlTimelineIndexedFill]:
        """Return one native partition by exact key."""
        return self._values[key]

    def __iter__(self) -> Iterator[_PnlTimelineFillNativeScopeKey]:
        """Yield and count each actual native partition once."""
        for key in self._values:
            self._counter[0] += 1
            yield key

    def __len__(self) -> int:
        """Return the number of actual native partitions."""
        return len(self._values)


class _CountingQuantityNodeList(list[tuple[int, int]]):
    """Count actual resolver iterations over fixed-depth quantity nodes."""

    def __init__(
        self,
        values: list[tuple[int, int]],
        counter: list[int],
    ) -> None:
        """Retain one mutable aggregate counter shared by all node lists."""
        super().__init__(values)
        self._counter = counter

    def __iter__(self) -> Iterator[tuple[int, int]]:
        """Yield nodes while incrementing the aggregate visit counter."""
        for value in super().__iter__():
            self._counter[0] += 1
            yield value


@dataclass(slots=True)
class _SharedAliasResolverCounters:
    """Aggregate explicit complexity counters for the shared-alias oracle."""

    alias_freeze_calls: int = 0
    alias_copy_visits: int = 0
    native_presence_probes: int = 0
    order_partition_membership_probes: list[int] = field(default_factory=lambda: [0])
    order_pool_build_calls: int = 0
    prepared_partition_counts: list[int] = field(default_factory=list)
    indexed_row_visits: list[int] = field(default_factory=lambda: [0])
    quantity_path_node_visits: list[int] = field(default_factory=lambda: [0])
    quantity_range_node_visits: list[int] = field(default_factory=lambda: [0])
    shared_alias_reference_ids: set[int] = field(default_factory=set)
    alias_reference_counts: list[int] = field(default_factory=list)


@dataclass(slots=True)
class _ExactResolverCounters:
    """Aggregate direct-work counters for exact resolver complexity oracles."""

    alias_freeze_calls: int = 0
    alias_copy_visits: int = 0
    native_presence_probes: int = 0
    native_owner_alias_visits: int = 0
    exact_key_probes: list[int] = field(default_factory=lambda: [0])
    exact_partition_visits: list[int] = field(default_factory=lambda: [0])
    indexed_row_visits: list[int] = field(default_factory=lambda: [0])
    exact_cache_entries: int = 0


class _OrderOptions(TypedDict, total=False):
    """Optional fields accepted by the Order test-row builder."""

    wallet_public_id: str
    mode: str
    instrument_public_id: str
    client_order_id: str | None
    exchange_order_id: str | None


class _ExecutionOptions(TypedDict, total=False):
    """Optional fields accepted by the Execution test-row builder."""

    known_to: datetime
    wallet_public_id: str
    mode: str
    exec_id: str | None
    trade_id: str | None


class _FillEventOptions(TypedDict, total=False):
    """Optional fields accepted by the VenueEvent test-row builder."""

    mode: str
    sequence_id: int
    execution_scope_sequence: int | None
    instrument: str
    exchange_order_id: str | None
    known_to: datetime


def _order(
    public_id: str = _ORDER,
    **options: Unpack[_OrderOptions],
) -> Order:
    """Build one sentinel-current Order lineage row."""
    return Order(
        public_id=public_id,
        instrument_public_id=options.get("instrument_public_id", _INSTRUMENT),
        wallet_public_id=options.get("wallet_public_id", _WALLET),
        mode=options.get("mode", "live"),
        client_order_id=options.get("client_order_id", _CLIENT_ORDER),
        exchange_order_id=options.get("exchange_order_id"),
        created_at=_EARLIER,
        timestamp=_EARLIER,
        side="buy",
        order_type="limit",
        price=100.0,
        size=10.0,
        status="filled",
        session_id=_SESSION,
        sequence_id=1,
        known_to=KNOWN_TO_MAX,
    )


def _instrument(
    exchange: str = "kraken",
    public_id: str = _INSTRUMENT,
    symbol_public_id: str = _SYMBOL,
) -> Instrument:
    """Build one sentinel-current Instrument lineage row."""
    return Instrument(
        public_id=public_id,
        symbol_public_id=symbol_public_id,
        exchange=exchange,
        timestamp=_EARLIER,
        known_to=KNOWN_TO_MAX,
        session_id=_SESSION,
        sequence_id=1,
    )


def _symbol(
    native_symbol: str = "BTC-USD",
    public_id: str = _SYMBOL,
    timestamp: datetime = _EARLIER,
    known_to: datetime = KNOWN_TO_MAX,
    sequence_id: int = 1,
) -> Symbol:
    """Build one version of the stable symbol identity."""
    return Symbol(
        public_id=public_id,
        native_symbol=native_symbol,
        base="BTC",
        quote="USD",
        asset_type="crypto",
        created_at=_EARLIER,
        timestamp=timestamp,
        known_to=known_to,
        session_id=_SESSION,
        sequence_id=sequence_id,
    )


def _symbol_reuse_collision_lineage(
    exchange_order_id: str | None = None,
) -> list[Symbol | Instrument | Order]:
    """Build a second active Order whose symbol reuses the target's old spelling."""
    return [
        _symbol(
            "XBT-USD",
            timestamp=_EARLIER - timedelta(days=1),
            known_to=_EARLIER,
            sequence_id=0,
        ),
        _symbol(
            "XBT-USD",
            public_id=_COLLIDING_SYMBOL,
            sequence_id=2,
        ),
        _instrument(
            public_id=_COLLIDING_INSTRUMENT,
            symbol_public_id=_COLLIDING_SYMBOL,
        ),
        _order(
            _COLLIDING_ORDER,
            instrument_public_id=_COLLIDING_INSTRUMENT,
            client_order_id=_CLIENT_ORDER,
            exchange_order_id=exchange_order_id,
        ),
    ]


def _execution(
    exchange: str,
    scope_sequence: int,
    timestamp: datetime = _EARLIER,
    order_public_id: str = _ORDER,
    **options: Unpack[_ExecutionOptions],
) -> Execution:
    """Build one execution with explicit immutable scope coordinates."""
    exec_id = options.get("exec_id")
    trade_id = options.get("trade_id")
    return Execution(
        order_public_id=order_public_id,
        wallet_public_id=options.get("wallet_public_id", _WALLET),
        operator_public_id=None,
        exchange=exchange,
        mode=options.get("mode", "live"),
        scope_sequence=scope_sequence,
        exec_id=exec_id if exec_id is not None else f"exec-{exchange}-{scope_sequence}",
        trade_id=trade_id if trade_id is not None else f"trade-{exchange}-{scope_sequence}",
        side="buy",
        status="filled",
        price=100.0,
        size=1.0,
        fee=0.25,
        fee_asset="USD",
        executed_at=None,
        liquidity_role="maker",
        timestamp=timestamp,
        known_to=options.get("known_to", KNOWN_TO_MAX),
        session_id=_SESSION,
        sequence_id=scope_sequence,
    )


def _fill_event(
    client_order_id: str,
    shard_key: str,
    exchange: str = "kraken",
    wallet_public_id: str = _WALLET,
    **options: Unpack[_FillEventOptions],
) -> VenueEvent:
    """Build one append-only fill event carrying exact durable shard lineage."""
    sequence_id = options.get("sequence_id", 1)
    execution_scope_sequence = options.get("execution_scope_sequence")
    fill_scope_sequence = (
        sequence_id if execution_scope_sequence is None else execution_scope_sequence
    )
    return VenueEvent(
        event_type="fill_observed",
        shard_key=shard_key,
        wallet_public_id=wallet_public_id,
        command_public_id=None,
        exchange=exchange,
        instrument=options.get("instrument", "BTC-USD"),
        mode=options.get("mode", "live"),
        exchange_order_id=options.get("exchange_order_id"),
        client_order_id=client_order_id,
        venue_client_id=client_order_id,
        side="buy",
        status="filled",
        fill_price=100.0,
        fill_size=1.0,
        cum_fill_size=1.0,
        fee=0.25,
        fee_asset="USD",
        exec_id=f"exec-{exchange}-{fill_scope_sequence}",
        trade_id=f"trade-{exchange}-{fill_scope_sequence}",
        error=None,
        venue_timestamp=_EARLIER,
        received_at=_EARLIER,
        payload_json=None,
        liquidity_role="maker",
        paired_group_id=None,
        timestamp=_EARLIER,
        known_to=options.get("known_to", KNOWN_TO_MAX),
        session_id=_SESSION,
        sequence_id=sequence_id,
    )


class _PrefixSourceOptions(TypedDict, total=False):
    """Optional inputs of the staged effective-prefix certification."""

    annulment_rows: list[ExecutionAnnulment]


def _certified_prefix(
    watermarks: dict[str, int],
    source_rows: list[tuple[Execution, Order | None, Instrument | None]],
    fill_rows: list[VenueEvent],
    native_symbols_by_symbol_public_id: dict[str, set[str]],
    order_instrument_ids_by_scope: dict[str, set[str]],
    **options: Unpack[_PrefixSourceOptions],
) -> list[PnlTimelineOpeningExecutionRow]:
    """Certify one in-memory ``_WALLET``/``live`` prefix and return its effective rows."""
    executions, _ = SQLAlchemyRepository._validate_pnl_timeline_execution_prefix(
        _PnlTimelineExecutionPrefixSource(
            wallet_public_id=_WALLET,
            mode="live",
            watermarks=watermarks,
            source_rows=source_rows,
            fill_rows=fill_rows,
            native_symbols_by_symbol_public_id=native_symbols_by_symbol_public_id,
            order_instrument_ids_by_scope=order_instrument_ids_by_scope,
            annulment_rows=options.get("annulment_rows", []),
        )
    )
    return executions


@pytest.fixture()
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create an isolated repository containing the exact replay lineage tables."""
    db_path = tmp_path / "pnl-execution-prefix.db"
    schema_engine = create_engine(f"sqlite:///{db_path}")
    Symbol.__table__.create(schema_engine)
    Instrument.__table__.create(schema_engine)
    Order.__table__.create(schema_engine)
    Execution.__table__.create(schema_engine)
    ExecutionAnnulment.__table__.create(schema_engine)
    VenueEvent.__table__.create(schema_engine)
    schema_engine.dispose()
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    try:
        async with repo.session() as s:
            s.add(_symbol())
            await s.commit()
        yield repo
    finally:
        await repo.engine.dispose()


async def test_prefix_captures_per_exchange_then_replays_exact_ranges(
    repository: SQLAlchemyRepository,
) -> None:
    """Clock skew retains each complete prefix while later suffixes stay out."""
    async with repository.session() as s:
        s.add_all(
            [
                _instrument(),
                _instrument("zonda", _ZONDA_INSTRUMENT),
                _order(),
                _order(
                    _ZONDA_ORDER,
                    instrument_public_id=_ZONDA_INSTRUMENT,
                    client_order_id=_ZONDA_CLIENT_ORDER,
                ),
                _execution("kraken", 1, _LATER),
                _execution("kraken", 2, _EARLIER),
                _execution("kraken", 3, _LATER + timedelta(minutes=1)),
                _execution("zonda", 1, _EARLIER, _ZONDA_ORDER),
                _execution("zonda", 2, _LATER, _ZONDA_ORDER),
                _fill_event(_CLIENT_ORDER, _KRAKEN_SHARD),
                _fill_event(_CLIENT_ORDER, _KRAKEN_SHARD, sequence_id=2),
                _fill_event(
                    _ZONDA_CLIENT_ORDER,
                    _ZONDA_SHARD,
                    exchange="zonda",
                    sequence_id=3,
                    execution_scope_sequence=1,
                ),
            ]
        )
        await s.commit()

    prefix = await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)

    assert prefix["watermarks"] == {"kraken": 2, "zonda": 1}
    assert [(row["exchange"], row["scope_sequence"]) for row in prefix["executions"]] == [
        ("kraken", 1),
        ("kraken", 2),
        ("zonda", 1),
    ]
    assert [row["instrument_public_id"] for row in prefix["executions"]] == [
        _INSTRUMENT,
        _INSTRUMENT,
        _ZONDA_INSTRUMENT,
    ]
    assert [(row["client_order_id"], row["shard_key"]) for row in prefix["executions"]] == [
        (_CLIENT_ORDER, _KRAKEN_SHARD),
        (_CLIENT_ORDER, _KRAKEN_SHARD),
        (_ZONDA_CLIENT_ORDER, _ZONDA_SHARD),
    ]


async def test_bundle_refuses_orphan_equal_quantity_at_activation_cut(
    repository: SQLAlchemyRepository,
) -> None:
    """A later proper fill cannot prove an earlier cut with another identity."""
    orphan_fill = _fill_event(
        _CLIENT_ORDER,
        _KRAKEN_SHARD,
        execution_scope_sequence=2,
    )
    async with repository.session() as s:
        s.add_all(
            [
                _instrument(),
                _order(),
                _execution("kraken", 1, _EARLIER),
                orphan_fill,
            ]
        )
        await s.commit()

    proper_fill = _fill_event(
        _CLIENT_ORDER,
        _KRAKEN_SHARD,
        sequence_id=2,
        execution_scope_sequence=1,
    )
    proper_fill.timestamp = _LATER
    async with repository.session() as s:
        s.add(proper_fill)
        await s.commit()

    request_prefix = await repository.get_pnl_timeline_execution_prefix(
        _WALLET,
        "live",
        _LATER,
    )
    assert [row["shard_key"] for row in request_prefix["executions"]] == [_KRAKEN_SHARD]

    with pytest.raises(ExecutionChainError, match="missing_execution_fill_identity_lineage"):
        await repository.get_pnl_timeline_execution_prefix_bundle(
            _WALLET,
            "live",
            _LATER,
            _AS_OF,
        )


async def test_bundle_returns_distinct_valid_prefix_cuts(
    repository: SQLAlchemyRepository,
) -> None:
    """Each valid horizon retains its own frozen execution range."""
    later_fill = _fill_event(
        _CLIENT_ORDER,
        _KRAKEN_SHARD,
        sequence_id=2,
    )
    later_fill.timestamp = _LATER
    async with repository.session() as s:
        s.add_all(
            [
                _instrument(),
                _order(),
                _execution("kraken", 1, _EARLIER),
                _execution("kraken", 2, _LATER),
                _fill_event(_CLIENT_ORDER, _KRAKEN_SHARD),
                later_fill,
            ]
        )
        await s.commit()

    bundle = await repository.get_pnl_timeline_execution_prefix_bundle(
        _WALLET,
        "live",
        _LATER,
        _AS_OF,
    )

    assert bundle["request"]["watermarks"] == {"kraken": 2}
    assert bundle["activation"]["watermarks"] == {"kraken": 1}
    assert [row["scope_sequence"] for row in bundle["request"]["executions"]] == [1, 2]
    assert [row["scope_sequence"] for row in bundle["activation"]["executions"]] == [1]


async def test_bundle_reuses_the_proven_snapshot_for_equal_cuts(
    repository: SQLAlchemyRepository,
) -> None:
    """Equal horizons need one proof and expose the same immutable snapshot."""
    async with repository.session() as s:
        s.add_all(
            [
                _instrument(),
                _order(),
                _execution("kraken", 1),
                _fill_event(_CLIENT_ORDER, _KRAKEN_SHARD),
            ]
        )
        await s.commit()

    bundle = await repository.get_pnl_timeline_execution_prefix_bundle(
        _WALLET,
        "live",
        _AS_OF,
        _AS_OF,
    )

    assert bundle["activation"] is bundle["request"]
    assert bundle["request"]["watermarks"] == {"kraken": 1}


async def test_bundle_sets_repeatable_read_only_as_the_first_postgresql_statement(
    repository: SQLAlchemyRepository,
) -> None:
    """PostgreSQL establishes the certified snapshot before either cut loads."""
    transaction_context = AsyncMock()
    session = AsyncMock(begin=MagicMock(return_value=transaction_context))
    session.execute = AsyncMock(return_value=MagicMock())
    request = PnlTimelineExecutionPrefix(watermarks={"kraken": 2}, executions=[], annulments=[])
    activation = PnlTimelineExecutionPrefix(watermarks={"kraken": 1}, executions=[], annulments=[])
    loader = AsyncMock(side_effect=[request, activation])
    with (
        patch.object(
            SQLAlchemyRepository,
            "dialect_name",
            new_callable=PropertyMock,
            return_value="postgresql",
        ),
        patch.object(repository, "session") as session_context,
        patch.object(
            repository,
            "_load_pnl_timeline_execution_prefix_snapshot",
            new=loader,
        ),
    ):
        session_context.return_value.__aenter__.return_value = session
        session_context.return_value.__aexit__.return_value = None
        bundle = await repository.get_pnl_timeline_execution_prefix_bundle(
            _WALLET,
            "live",
            _LATER,
            _AS_OF,
        )

    statement = session.execute.await_args_list[0].args[0]
    assert str(statement) == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
    assert session.execute.await_count == 1
    session.begin.assert_called_once_with()
    assert loader.await_args_list[0].args == (session, _WALLET, "live", _LATER)
    assert loader.await_args_list[1].args == (session, _WALLET, "live", _AS_OF)
    assert bundle == {"request": request, "activation": activation}


@pytest.mark.skipif(
    _configured_postgresql_url() is None,
    reason="live PostgreSQL snapshot proof requires PNL_TEST_POSTGRES_URL",
)
async def test_postgresql_bundle_keeps_both_cuts_in_one_repeatable_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A committed append between cut loaders remains outside both PG views."""
    database_url = _configured_postgresql_url()
    assert database_url is not None
    repository = SQLAlchemyRepository(database_url)
    table_name = f"pnl_bundle_snapshot_{uuid7().hex}"
    quoted_table = f'"{table_name}"'
    observed_counts: list[int] = []

    async def load_interleaved_cut(
        session: AsyncSession,
        wallet_public_id: str,
        mode: str,
        as_of: datetime,
    ) -> PnlTimelineExecutionPrefix:
        """Read one real relation and commit an append after the first view."""
        del wallet_public_id, mode, as_of
        count = await session.scalar(text(f"SELECT count(*) FROM {quoted_table}"))
        assert count is not None
        observed_counts.append(int(count))
        if len(observed_counts) == 1:
            async with repository.engine.begin() as writer:
                await writer.execute(text(f"INSERT INTO {quoted_table} (id) VALUES (2)"))
        return PnlTimelineExecutionPrefix(
            watermarks={"probe": int(count)},
            executions=[],
            annulments=[],
        )

    try:
        async with repository.engine.begin() as connection:
            await connection.execute(text(f"CREATE TABLE {quoted_table} (id integer NOT NULL)"))
            await connection.execute(text(f"INSERT INTO {quoted_table} (id) VALUES (1)"))
        monkeypatch.setattr(
            repository,
            "_load_pnl_timeline_execution_prefix_snapshot",
            load_interleaved_cut,
        )

        bundle = await repository.get_pnl_timeline_execution_prefix_bundle(
            _WALLET,
            "live",
            _LATER,
            _AS_OF,
        )
        async with repository.engine.connect() as connection:
            committed_count = await connection.scalar(text(f"SELECT count(*) FROM {quoted_table}"))

        assert observed_counts == [1, 1]
        assert bundle["request"]["watermarks"] == {"probe": 1}
        assert bundle["activation"]["watermarks"] == {"probe": 1}
        assert committed_count == 2
    finally:
        async with repository.engine.begin() as connection:
            await connection.execute(text(f"DROP TABLE IF EXISTS {quoted_table}"))
        await repository.engine.dispose()


async def test_visible_suffix_after_capture_stays_outside_the_frozen_range(
    repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A same-session flushed suffix is visible but remains above frozen W."""
    async with repository.session() as s:
        s.add_all(
            [
                _instrument(),
                _order(),
                _execution("kraken", 1),
                _fill_event(_CLIENT_ORDER, _KRAKEN_SHARD),
            ]
        )
        await s.commit()
    original_read = repository._read_pnl_timeline_execution_prefix

    async def append_then_read(
        s: AsyncSession,
        wallet_public_id: str,
        mode: str,
        watermarks: dict[str, int],
    ) -> list[tuple[Execution, Order | None, Instrument | None]]:
        """Expose a newly flushed suffix immediately before the bounded read."""
        assert watermarks == {"kraken": 1}
        s.add(_execution("kraken", 2))
        await s.flush()
        return await original_read(s, wallet_public_id, mode, watermarks)

    monkeypatch.setattr(repository, "_read_pnl_timeline_execution_prefix", append_then_read)

    prefix = await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)

    assert prefix["watermarks"] == {"kraken": 1}
    assert [row["scope_sequence"] for row in prefix["executions"]] == [1]


async def test_prefix_rejects_inactive_execution_rows(
    repository: SQLAlchemyRepository,
) -> None:
    """A closed in-range execution remains visible and fails validation."""
    async with repository.session() as s:
        s.add_all([_instrument(), _order(), _execution("kraken", 1, known_to=_AS_OF)])
        await s.commit()

    with pytest.raises(ExecutionChainError, match="superseded_execution_row"):
        await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)


async def test_prefix_rejects_missing_scope_sequences(
    repository: SQLAlchemyRepository,
) -> None:
    """A watermark of three cannot certify a stored range missing sequence two."""
    async with repository.session() as s:
        s.add_all(
            [
                _instrument(),
                _order(),
                _execution("kraken", 1),
                _execution("kraken", 3),
                _fill_event(_CLIENT_ORDER, _KRAKEN_SHARD),
                _fill_event(
                    _CLIENT_ORDER,
                    _KRAKEN_SHARD,
                    sequence_id=2,
                    execution_scope_sequence=3,
                ),
            ]
        )
        await s.commit()

    with pytest.raises(ExecutionChainError, match="non_contiguous_execution_prefix"):
        await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)


async def test_prefix_rejects_duplicate_active_order_lineage(
    repository: SQLAlchemyRepository,
) -> None:
    """Two active Order versions must not multiply one execution silently."""
    async with repository.session() as s:
        await s.execute(text("DROP INDEX ix_orders_public_id"))
        await s.commit()
    async with repository.session() as s:
        first = _order()
        second = _order(client_order_id="client-duplicate-lineage")
        second.sequence_id = 2
        s.add_all(
            [
                _instrument(),
                first,
                second,
                _execution("kraken", 1),
                _fill_event(_CLIENT_ORDER, _KRAKEN_SHARD),
                _fill_event(
                    "client-duplicate-lineage",
                    _KRAKEN_SHARD,
                    sequence_id=2,
                    execution_scope_sequence=1,
                ),
            ]
        )
        await s.commit()

    with pytest.raises(ExecutionChainError, match="duplicate_execution_prefix_row"):
        await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)


async def test_prefix_rejects_dangling_order_lineage(
    repository: SQLAlchemyRepository,
) -> None:
    """An execution without one active Order lineage cannot be projected."""
    async with repository.session() as s:
        s.add(_execution("kraken", 1))
        await s.commit()

    with pytest.raises(ExecutionChainError, match="dangling_execution_order_lineage"):
        await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)


async def test_prefix_rejects_crossed_instrument_exchange_lineage(
    repository: SQLAlchemyRepository,
) -> None:
    """Execution exchange cannot disagree with its active Instrument lineage."""
    async with repository.session() as s:
        s.add_all([_instrument("zonda"), _order(), _execution("kraken", 1)])
        await s.commit()

    with pytest.raises(ExecutionChainError, match="crossed_execution_instrument_lineage"):
        await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)


async def test_prefix_rejects_crossed_order_scope_lineage(
    repository: SQLAlchemyRepository,
) -> None:
    """An active Order from another wallet cannot certify the execution."""
    async with repository.session() as s:
        s.add_all(
            [
                _instrument(),
                _order(wallet_public_id=_FOREIGN_WALLET),
                _execution("kraken", 1),
            ]
        )
        await s.commit()

    with pytest.raises(ExecutionChainError, match="crossed_execution_order_lineage"):
        await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)


async def test_prefix_rejects_dangling_instrument_lineage(
    repository: SQLAlchemyRepository,
) -> None:
    """An active Order without an active Instrument cannot seed an opening pool."""
    async with repository.session() as s:
        s.add_all([_order(), _execution("kraken", 1)])
        await s.commit()

    with pytest.raises(ExecutionChainError, match="dangling_execution_instrument_lineage"):
        await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)


async def test_prefix_rejects_missing_client_order_identity(
    repository: SQLAlchemyRepository,
) -> None:
    """Shard proof is impossible when active Order lineage has no client id."""
    async with repository.session() as s:
        s.add_all(
            [
                _instrument(),
                _order(client_order_id=""),
                _execution("kraken", 1),
            ]
        )
        await s.commit()

    with pytest.raises(ExecutionChainError, match="missing_execution_client_order_lineage"):
        await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)


async def test_prefix_rejects_missing_fill_shard_lineage(
    repository: SQLAlchemyRepository,
) -> None:
    """An execution with no durable fill event cannot enter the opening prefix."""
    async with repository.session() as s:
        s.add_all([_instrument(), _order(), _execution("kraken", 1)])
        await s.commit()

    with pytest.raises(ExecutionChainError, match="missing_execution_shard_lineage"):
        await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)


@pytest.mark.parametrize(
    "shard_keys",
    [
        pytest.param(
            [
                _KRAKEN_SHARD,
                f"kraken.BTC-USD.live.w{compute_wallet_short(_WALLET)}",
            ],
            id="disagreeing",
        ),
        pytest.param([""], id="empty"),
    ],
)
async def test_prefix_rejects_ambiguous_fill_shard_lineage(
    repository: SQLAlchemyRepository,
    shard_keys: list[str],
) -> None:
    """Disagreeing or empty durable shard evidence refuses the whole prefix."""
    async with repository.session() as s:
        rows: list[Instrument | Order | Execution | VenueEvent] = [
            _instrument(),
            _order(),
            _execution("kraken", 1),
        ]
        rows.extend(
            _fill_event(
                _CLIENT_ORDER,
                shard_key,
                sequence_id=index,
                execution_scope_sequence=1,
            )
            for index, shard_key in enumerate(shard_keys, start=1)
        )
        s.add_all(rows)
        await s.commit()

    with pytest.raises(ExecutionChainError, match="ambiguous_execution_shard_lineage"):
        await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)


@pytest.mark.parametrize(
    ("wallet_public_id", "exchange", "fill_mode", "expected_error"),
    [
        pytest.param(
            _FOREIGN_WALLET,
            "kraken",
            "live",
            "crossed_execution_shard_lineage",
            id="wallet",
        ),
        pytest.param(
            _WALLET,
            "zonda",
            "live",
            "crossed_execution_fill_scope_lineage",
            id="exchange",
        ),
        pytest.param(
            _WALLET,
            "kraken",
            "paper",
            "crossed_execution_fill_scope_lineage",
            id="mode",
        ),
    ],
)
async def test_prefix_rejects_crossed_fill_scope_lineage(
    repository: SQLAlchemyRepository,
    wallet_public_id: str,
    exchange: str,
    fill_mode: str,
    expected_error: str,
) -> None:
    """Fill evidence outside any immutable execution scope coordinate refuses."""
    async with repository.session() as s:
        s.add_all(
            [
                _instrument(),
                _order(),
                _execution("kraken", 1),
                _fill_event(
                    _CLIENT_ORDER,
                    _KRAKEN_SHARD,
                    wallet_public_id=wallet_public_id,
                    exchange=exchange,
                    mode=fill_mode,
                ),
            ]
        )
        await s.commit()

    with pytest.raises(ExecutionChainError, match=expected_error):
        await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)


async def test_prefix_ignores_same_client_id_evidence_from_other_scopes(
    repository: SQLAlchemyRepository,
) -> None:
    """Global client-id collisions cannot override one exact-scope fill proof."""
    async with repository.session() as s:
        s.add_all(
            [
                _instrument(),
                _order(),
                _execution("kraken", 1),
                _fill_event(_CLIENT_ORDER, _KRAKEN_SHARD),
                _fill_event(
                    _CLIENT_ORDER,
                    "kraken.BTC-USD.live.foreign",
                    wallet_public_id=_FOREIGN_WALLET,
                    sequence_id=2,
                    execution_scope_sequence=1,
                ),
                _fill_event(
                    _CLIENT_ORDER,
                    "zonda.BTC-USD.live",
                    exchange="zonda",
                    sequence_id=3,
                    execution_scope_sequence=1,
                ),
                _fill_event(
                    _CLIENT_ORDER,
                    "kraken.BTC-USD.paper",
                    mode="paper",
                    sequence_id=4,
                    execution_scope_sequence=1,
                ),
            ]
        )
        await s.commit()

    prefix = await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)

    assert [row["shard_key"] for row in prefix["executions"]] == [_KRAKEN_SHARD]


async def test_prefix_accepts_canonical_wallet_aware_shard_lineage(
    repository: SQLAlchemyRepository,
) -> None:
    """A canonical full-wallet-derived shard suffix remains durable evidence."""
    shard_key = f"kraken.BTC-USD.live.w{compute_wallet_short(_WALLET)}"
    async with repository.session() as s:
        s.add_all(
            [
                _instrument(),
                _order(),
                _execution("kraken", 1),
                _fill_event(_CLIENT_ORDER, shard_key),
            ]
        )
        await s.commit()

    prefix = await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)

    assert [row["shard_key"] for row in prefix["executions"]] == [shard_key]


@pytest.mark.parametrize(
    ("shard_key", "expected_error"),
    [
        pytest.param(
            "zonda.BTC-USD.live",
            "crossed_execution_shard_key_lineage",
            id="exchange",
        ),
        pytest.param(
            "kraken.ETH-USD.live",
            "crossed_execution_shard_key_lineage",
            id="instrument",
        ),
        pytest.param(
            "kraken.BTC-USD.paper",
            "crossed_execution_shard_key_lineage",
            id="mode",
        ),
        pytest.param(
            "kraken.BTC-USD.live.w000000000bad",
            "crossed_execution_shard_wallet_lineage",
            id="wallet_suffix",
        ),
        pytest.param(
            " kraken.BTC-USD.live",
            "ambiguous_execution_shard_lineage",
            id="leading_whitespace",
        ),
        pytest.param(
            "kraken.BTC-USD.l ive",
            "ambiguous_execution_shard_lineage",
            id="embedded_whitespace",
        ),
        pytest.param(
            "not-a-shard",
            "ambiguous_execution_shard_lineage",
            id="unparseable",
        ),
        pytest.param(
            "kraken.BTC-USD.live.strategy.extra",
            "ambiguous_execution_shard_lineage",
            id="extra_segment",
        ),
        pytest.param(
            "kraken.BTC-USD.live.strategy",
            "crossed_execution_shard_key_lineage",
            id="live_strategy_tag",
        ),
    ],
)
async def test_prefix_rejects_shard_strings_that_contradict_fill_evidence(
    repository: SQLAlchemyRepository,
    shard_key: str,
    expected_error: str,
) -> None:
    """A fill's free-text shard cannot contradict its normalized identity columns."""
    async with repository.session() as s:
        s.add_all(
            [
                _instrument(),
                _order(),
                _execution("kraken", 1),
                _fill_event(_CLIENT_ORDER, shard_key),
            ]
        )
        await s.commit()

    with pytest.raises(ExecutionChainError, match=expected_error):
        await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)


async def test_prefix_rejects_fill_identity_mismatch(
    repository: SQLAlchemyRepository,
) -> None:
    """A same-CID fill from another execution is not a shard proof fallback."""
    fill = _fill_event(_CLIENT_ORDER, _KRAKEN_SHARD)
    fill.trade_id = "trade-kraken-other"
    async with repository.session() as s:
        s.add_all([_instrument(), _order(), _execution("kraken", 1), fill])
        await s.commit()

    with pytest.raises(ExecutionChainError, match="missing_execution_fill_identity_lineage"):
        await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)


async def test_prefix_accepts_legacy_idless_fill_lineage(
    repository: SQLAlchemyRepository,
) -> None:
    """Scoped client and historical instrument lineage certify a legal id-less fill."""
    execution = _execution("kraken", 1)
    execution.exec_id = None
    execution.trade_id = None
    fill = _fill_event(_CLIENT_ORDER, _KRAKEN_SHARD)
    fill.exec_id = None
    fill.trade_id = None
    async with repository.session() as s:
        s.add_all(
            [
                _instrument(),
                _order(),
                execution,
                fill,
            ]
        )
        await s.commit()

    prefix = await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)

    assert [row["shard_key"] for row in prefix["executions"]] == [_KRAKEN_SHARD]


async def test_prefix_rejects_cid_only_lineage_reused_across_stable_instruments(
    repository: SQLAlchemyRepository,
) -> None:
    """A historical symbol spelling cannot let another Order donate its shard."""
    execution = _execution("kraken", 1)
    execution.exec_id = None
    execution.trade_id = None
    colliding_fill = _fill_event(
        _CLIENT_ORDER,
        "kraken.XBT-USD.live",
        instrument="XBT-USD",
    )
    colliding_fill.exec_id = None
    colliding_fill.trade_id = None
    async with repository.session() as s:
        s.add_all(
            [
                _instrument(),
                _order(),
                execution,
                colliding_fill,
                *_symbol_reuse_collision_lineage(),
            ]
        )
        await s.commit()

    with pytest.raises(
        ExecutionChainError,
        match="ambiguous_execution_order_instrument_lineage",
    ):
        await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)


async def test_prefix_cid_fallback_counts_order_with_missing_instrument_lineage(
    repository: SQLAlchemyRepository,
) -> None:
    """A dangling historical instrument cannot hide reuse of the full-wallet CID."""
    execution = _execution("kraken", 1)
    execution.exec_id = None
    execution.trade_id = None
    ambiguous_fill = _fill_event(_CLIENT_ORDER, _KRAKEN_SHARD)
    ambiguous_fill.exec_id = None
    ambiguous_fill.trade_id = None
    historical_collision = _order(
        _COLLIDING_ORDER,
        instrument_public_id=_COLLIDING_INSTRUMENT,
        client_order_id=_CLIENT_ORDER,
    )
    historical_collision.known_to = _AS_OF
    async with repository.session() as s:
        s.add_all(
            [
                _instrument(),
                _order(),
                historical_collision,
                execution,
                ambiguous_fill,
            ]
        )
        await s.commit()

    with pytest.raises(
        ExecutionChainError,
        match="ambiguous_execution_order_instrument_lineage",
    ):
        await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)


async def test_prefix_exact_fill_identity_wins_over_cid_instrument_collision(
    repository: SQLAlchemyRepository,
) -> None:
    """Exact execution ids retain the correct shard despite a colliding CID."""
    async with repository.session() as s:
        s.add_all(
            [
                _instrument(),
                _order(),
                _execution("kraken", 1),
                _fill_event(_CLIENT_ORDER, _KRAKEN_SHARD),
                _fill_event(
                    _CLIENT_ORDER,
                    "kraken.XBT-USD.live",
                    sequence_id=2,
                    execution_scope_sequence=2,
                    instrument="XBT-USD",
                ),
                *_symbol_reuse_collision_lineage(),
            ]
        )
        await s.commit()

    prefix = await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)

    assert [row["shard_key"] for row in prefix["executions"]] == [_KRAKEN_SHARD]


async def test_prefix_exact_index_discards_same_id_rows_for_another_native_symbol(
    repository: SQLAlchemyRepository,
) -> None:
    """A direct exact-id bucket still enforces stable native-symbol lineage."""
    wrong_native = _fill_event(
        _CLIENT_ORDER,
        "kraken.ETH-USD.live",
        sequence_id=2,
        execution_scope_sequence=1,
        instrument="ETH-USD",
    )
    async with repository.session() as s:
        s.add_all(
            [
                _instrument(),
                _order(),
                _execution("kraken", 1),
                _fill_event(_CLIENT_ORDER, _KRAKEN_SHARD),
                wrong_native,
            ]
        )
        await s.commit()

    prefix = await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)

    assert [row["shard_key"] for row in prefix["executions"]] == [_KRAKEN_SHARD]


async def test_prefix_accepts_idless_fill_through_exchange_order_identity(
    repository: SQLAlchemyRepository,
) -> None:
    """Venue order identity certifies an id-less fill before CID fallback."""
    execution = _execution("kraken", 1)
    execution.exec_id = None
    execution.trade_id = None
    fill = _fill_event(
        _CLIENT_ORDER,
        _KRAKEN_SHARD,
        exchange_order_id="venue-order-1",
    )
    fill.exec_id = None
    fill.trade_id = None
    async with repository.session() as s:
        s.add_all(
            [
                _instrument(),
                _order(exchange_order_id="venue-order-1"),
                execution,
                fill,
            ]
        )
        await s.commit()

    prefix = await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)

    assert [row["shard_key"] for row in prefix["executions"]] == [_KRAKEN_SHARD]


async def test_prefix_exchange_order_identity_wins_over_cid_instrument_collision(
    repository: SQLAlchemyRepository,
) -> None:
    """Exact venue-order identity remains sufficient when the CID is reused."""
    execution = _execution("kraken", 1)
    execution.exec_id = None
    execution.trade_id = None
    exact_fill = _fill_event(
        _CLIENT_ORDER,
        _KRAKEN_SHARD,
        exchange_order_id="venue-order-1",
    )
    exact_fill.exec_id = None
    exact_fill.trade_id = None
    colliding_fill = _fill_event(
        _CLIENT_ORDER,
        "kraken.XBT-USD.live",
        sequence_id=2,
        execution_scope_sequence=2,
        instrument="XBT-USD",
        exchange_order_id="venue-order-2",
    )
    colliding_fill.exec_id = None
    colliding_fill.trade_id = None
    async with repository.session() as s:
        s.add_all(
            [
                _instrument(),
                _order(exchange_order_id="venue-order-1"),
                execution,
                exact_fill,
                colliding_fill,
                *_symbol_reuse_collision_lineage(exchange_order_id="venue-order-2"),
            ]
        )
        await s.commit()

    prefix = await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)

    assert [row["shard_key"] for row in prefix["executions"]] == [_KRAKEN_SHARD]


async def test_prefix_accepts_pre_ack_idless_fill_with_known_order_identity(
    repository: SQLAlchemyRepository,
) -> None:
    """An id-less pre-ACK event may use scoped CID and instrument fallback."""
    execution = _execution("kraken", 1)
    execution.exec_id = None
    execution.trade_id = None
    fill = _fill_event(_CLIENT_ORDER, _KRAKEN_SHARD)
    fill.exec_id = None
    fill.trade_id = None
    async with repository.session() as s:
        s.add_all(
            [
                _instrument(),
                _order(exchange_order_id="venue-order-1"),
                execution,
                fill,
            ]
        )
        await s.commit()

    prefix = await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)

    assert [row["shard_key"] for row in prefix["executions"]] == [_KRAKEN_SHARD]


async def test_prefix_pre_ack_order_identity_still_refuses_reused_cid_instrument(
    repository: SQLAlchemyRepository,
) -> None:
    """A blank pre-ACK venue id cannot bypass ambiguous stable CID lineage."""
    execution = _execution("kraken", 1)
    execution.exec_id = None
    execution.trade_id = None
    fill = _fill_event(_CLIENT_ORDER, _KRAKEN_SHARD)
    fill.exec_id = None
    fill.trade_id = None
    async with repository.session() as s:
        s.add_all(
            [
                _instrument(),
                _order(exchange_order_id="venue-order-1"),
                execution,
                fill,
                *_symbol_reuse_collision_lineage(),
            ]
        )
        await s.commit()

    with pytest.raises(
        ExecutionChainError,
        match="ambiguous_execution_order_instrument_lineage",
    ):
        await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)


async def test_prefix_rejects_reusing_one_idless_fill_for_two_executions(
    repository: SQLAlchemyRepository,
) -> None:
    """One legacy witness cannot certify two separately committed executions."""
    first = _execution("kraken", 1)
    second = _execution("kraken", 2)
    fill = _fill_event(_CLIENT_ORDER, _KRAKEN_SHARD)
    for execution in (first, second):
        execution.exec_id = None
        execution.trade_id = None
    fill.exec_id = None
    fill.trade_id = None
    async with repository.session() as s:
        s.add_all([_instrument(), _order(), first, second, fill])
        await s.commit()

    with pytest.raises(ExecutionChainError, match="reused_execution_fill_lineage"):
        await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)


def test_prefix_rejects_reusing_one_exact_fill_for_two_executions() -> None:
    """Exact identifiers still consume one account-scoped witness only once."""
    first = _execution(
        "kraken",
        1,
        exec_id="shared-exec",
        trade_id="shared-trade",
    )
    second = _execution(
        "kraken",
        2,
        exec_id="shared-exec",
        trade_id="shared-trade",
    )
    fill = _fill_event(_CLIENT_ORDER, _KRAKEN_SHARD)
    fill.exec_id = "shared-exec"
    fill.trade_id = "shared-trade"

    with pytest.raises(ExecutionChainError, match="reused_execution_fill_lineage"):
        _certified_prefix(
            {"kraken": 2},
            [
                (first, _order(), _instrument()),
                (second, _order(), _instrument()),
            ],
            [fill],
            {_SYMBOL: {"BTC-USD"}},
            {_CLIENT_ORDER: {_INSTRUMENT}},
        )


def test_prefix_reuses_one_cached_exact_lookup_for_duplicate_execution_ids() -> None:
    """A repeated complete lookup returns its already materialized witness set."""
    execution = _execution(
        "kraken",
        1,
        exec_id="cached-exec",
        trade_id="cached-trade",
    )
    candidate = _PnlTimelineExecutionCandidate(
        execution=execution,
        order=_order(),
        client_order_id=_CLIENT_ORDER,
        symbol_public_id=_SYMBOL,
        allowed_native_symbols=frozenset({"BTC-USD"}),
    )
    scope_key = (_CLIENT_ORDER, _WALLET, "kraken", "live")
    cached: dict[tuple[str, str, str, str], list[VenueEvent]] = {}
    state = _PnlTimelineFillAssignmentState(
        fill_index=SQLAlchemyRepository._index_pnl_timeline_fill_rows([]),
        order_instrument_ids_by_scope={},
        consumed_identities=set(),
        fallback_identities=set(),
        exact_witnesses={
            (
                scope_key,
                _SYMBOL,
                "cached-exec",
                "cached-trade",
            ): cached
        },
        fallback_pools={},
        scope_lineage_evidence={},
        fallback_evidence_by_execution_public_id={},
        order_partitions_by_owner={},
        fallback_pools_by_identity={},
    )

    assert (
        SQLAlchemyRepository._exact_pnl_timeline_witnesses(
            candidate,
            state,
            scope_key,
        )
        is cached
    )


def test_prefix_exact_precompute_rejects_conflicting_same_symbol_lineages() -> None:
    """One stable symbol cannot carry two alias sets inside an exact group."""
    first_execution = _execution(
        "kraken",
        1,
        exec_id="shared-exec",
        trade_id="shared-trade",
    )
    second_execution = _execution(
        "kraken",
        2,
        exec_id="shared-exec",
        trade_id="shared-trade",
    )
    first_fill = _fill_event(
        _CLIENT_ORDER,
        "kraken.FIRST-USD.live",
        instrument="FIRST-USD",
    )
    second_fill = _fill_event(
        _CLIENT_ORDER,
        "kraken.SECOND-USD.live",
        sequence_id=2,
        execution_scope_sequence=1,
        instrument="SECOND-USD",
    )
    first_fill.public_id = "19000000-0000-7000-8000-000000000001"
    second_fill.public_id = "19000000-0000-7000-8000-000000000002"
    for fill in (first_fill, second_fill):
        fill.exec_id = "shared-exec"
        fill.trade_id = "shared-trade"
    order = _order()

    with pytest.raises(
        ExecutionChainError,
        match="ambiguous_execution_fill_instrument_lineage",
    ):
        SQLAlchemyRepository._resolve_pnl_timeline_execution_shards(
            [
                _PnlTimelineExecutionCandidate(
                    execution=first_execution,
                    order=order,
                    client_order_id=_CLIENT_ORDER,
                    symbol_public_id=_SYMBOL,
                    allowed_native_symbols=frozenset({"FIRST-USD"}),
                ),
                _PnlTimelineExecutionCandidate(
                    execution=second_execution,
                    order=order,
                    client_order_id=_CLIENT_ORDER,
                    symbol_public_id=_SYMBOL,
                    allowed_native_symbols=frozenset({"SECOND-USD"}),
                ),
            ],
            [first_fill, second_fill],
            {_CLIENT_ORDER: {_INSTRUMENT}},
        )


async def test_prefix_rejects_unconsumed_idless_fill_witness(
    repository: SQLAlchemyRepository,
) -> None:
    """A sealed fallback fill multiset cannot exceed its execution multiset."""
    execution = _execution("kraken", 1)
    execution.exec_id = None
    execution.trade_id = None
    first = _fill_event(_CLIENT_ORDER, _KRAKEN_SHARD)
    second = _fill_event(
        _CLIENT_ORDER,
        _KRAKEN_SHARD,
        sequence_id=2,
        execution_scope_sequence=2,
    )
    for fill in (first, second):
        fill.exec_id = None
        fill.trade_id = None
    async with repository.session() as s:
        s.add_all([_instrument(), _order(), execution, first, second])
        await s.commit()

    with pytest.raises(ExecutionChainError, match="unconsumed_execution_fill_lineage"):
        await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)


@pytest.mark.parametrize("execution_size", [0.5, 2.0])
async def test_prefix_rejects_under_or_over_consumed_fill_quantity(
    repository: SQLAlchemyRepository,
    execution_size: float,
) -> None:
    """Execution and exact fill witnesses must certify equal finite quantities."""
    execution = _execution("kraken", 1)
    execution.size = execution_size
    async with repository.session() as s:
        s.add_all(
            [
                _instrument(),
                _order(),
                execution,
                _fill_event(_CLIENT_ORDER, _KRAKEN_SHARD),
            ]
        )
        await s.commit()

    with pytest.raises(ExecutionChainError, match="execution_fill_quantity_mismatch"):
        await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)


async def test_prefix_rejects_idless_quantity_mismatch(
    repository: SQLAlchemyRepository,
) -> None:
    """Legacy fallback also requires a quantity-equal witness multiset."""
    execution = _execution("kraken", 1)
    execution.exec_id = None
    execution.trade_id = None
    execution.size = 0.5
    fill = _fill_event(_CLIENT_ORDER, _KRAKEN_SHARD)
    fill.exec_id = None
    fill.trade_id = None
    async with repository.session() as s:
        s.add_all([_instrument(), _order(), execution, fill])
        await s.commit()

    with pytest.raises(ExecutionChainError, match="execution_fill_quantity_mismatch"):
        await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)


async def test_prefix_rejects_conflicting_redelivery_quantities(
    repository: SQLAlchemyRepository,
) -> None:
    """One fill identity cannot select the largest conflicting redelivery."""
    execution = _execution("kraken", 1)
    execution.size = 2.0
    first = _fill_event(_CLIENT_ORDER, _KRAKEN_SHARD)
    second = _fill_event(
        _CLIENT_ORDER,
        _KRAKEN_SHARD,
        sequence_id=2,
        execution_scope_sequence=1,
    )
    second.fill_size = 2.0
    async with repository.session() as s:
        s.add_all([_instrument(), _order(), execution, first, second])
        await s.commit()

    with pytest.raises(ExecutionChainError, match="conflicting_execution_fill_quantity"):
        await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)


async def test_prefix_accepts_redelivery_quantity_within_tolerance(
    repository: SQLAlchemyRepository,
) -> None:
    """Near-identical redelivery quantities share the explicit fill tolerance."""
    first = _fill_event(_CLIENT_ORDER, _KRAKEN_SHARD)
    second = _fill_event(
        _CLIENT_ORDER,
        _KRAKEN_SHARD,
        sequence_id=2,
        execution_scope_sequence=1,
    )
    second.fill_size = 1.0 + 5e-10
    async with repository.session() as s:
        s.add_all([_instrument(), _order(), _execution("kraken", 1), first, second])
        await s.commit()

    prefix = await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)

    assert [row["size"] for row in prefix["executions"]] == [1.0]


async def test_prefix_rejects_cross_shard_idless_quantity_ambiguity(
    repository: SQLAlchemyRepository,
) -> None:
    """Equal id-less quantities on two paper shards cannot select one pool."""
    execution = _execution("kraken", 1, mode="paper")
    execution.exec_id = None
    execution.trade_id = None
    first = _fill_event(
        _CLIENT_ORDER,
        "kraken.BTC-USD.paper.alpha",
        mode="paper",
    )
    second = _fill_event(
        _CLIENT_ORDER,
        "kraken.BTC-USD.paper.beta",
        mode="paper",
        sequence_id=2,
    )
    for fill in (first, second):
        fill.exec_id = None
        fill.trade_id = None
    async with repository.session() as s:
        s.add_all(
            [
                _instrument(),
                _order(mode="paper"),
                execution,
                first,
                second,
            ]
        )
        await s.commit()

    with pytest.raises(ExecutionChainError, match="ambiguous_execution_shard_lineage"):
        await repository.get_pnl_timeline_execution_prefix(_WALLET, "paper", _AS_OF)


async def test_prefix_rejects_cross_node_tolerance_matches_on_distinct_shards(
    repository: SQLAlchemyRepository,
) -> None:
    """Separate quantity nodes cannot conceal two matching durable shards."""
    execution = _execution("kraken", 1)
    execution.exec_id = None
    execution.trade_id = None
    first = _fill_event(_CLIENT_ORDER, _KRAKEN_SHARD)
    second = _fill_event(
        _CLIENT_ORDER,
        f"kraken.BTC-USD.live.w{compute_wallet_short(_WALLET)}",
        sequence_id=2,
    )
    for fill in (first, second):
        fill.exec_id = None
        fill.trade_id = None
    second.fill_size = 1.0 - 5e-10
    async with repository.session() as s:
        s.add_all([_instrument(), _order(), execution, first, second])
        await s.commit()

    with pytest.raises(ExecutionChainError, match="ambiguous_execution_shard_lineage"):
        await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)


async def test_prefix_consumes_overlapping_tolerance_nodes_in_canonical_identity_order(
    repository: SQLAlchemyRepository,
) -> None:
    """Overlapping size windows consume each same-shard identity injectively."""
    executions = [_execution("kraken", scope_sequence) for scope_sequence in (1, 2, 3)]
    fills = [
        _fill_event(
            _CLIENT_ORDER,
            _KRAKEN_SHARD,
            sequence_id=scope_sequence,
        )
        for scope_sequence in (1, 2, 3)
    ]
    quantities = (1.0 - 5e-10, 1.0, 1.0 + 5e-10)
    public_ids = (
        "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa",
        "eeeeeeee-eeee-7eee-8eee-eeeeeeeeeeee",
        "ffffffff-ffff-7fff-8fff-ffffffffffff",
    )
    for execution in executions:
        execution.exec_id = None
        execution.trade_id = None
        execution.size = 1.0
    for fill, quantity, public_id in zip(fills, quantities, public_ids, strict=True):
        fill.exec_id = None
        fill.trade_id = None
        fill.fill_size = quantity
        fill.public_id = public_id
    async with repository.session() as s:
        s.add_all([_instrument(), _order(), *executions, *fills])
        await s.commit()

    prefix = await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)

    assert [row["scope_sequence"] for row in prefix["executions"]] == [1, 2, 3]
    assert [row["shard_key"] for row in prefix["executions"]] == [
        _KRAKEN_SHARD,
        _KRAKEN_SHARD,
        _KRAKEN_SHARD,
    ]


async def test_prefix_rejects_one_trade_id_spanning_multiple_fill_identities(
    repository: SQLAlchemyRepository,
) -> None:
    """One trade-only execution cannot consume two distinct exec witnesses."""
    execution = _execution("kraken", 1, trade_id="shared-trade")
    execution.exec_id = None
    first = _fill_event(_CLIENT_ORDER, _KRAKEN_SHARD)
    second = _fill_event(
        _CLIENT_ORDER,
        _KRAKEN_SHARD,
        sequence_id=2,
        execution_scope_sequence=2,
    )
    first.exec_id = "first-exec"
    first.trade_id = "shared-trade"
    second.exec_id = "second-exec"
    second.trade_id = "shared-trade"
    async with repository.session() as s:
        s.add_all([_instrument(), _order(), execution, first, second])
        await s.commit()

    with pytest.raises(ExecutionChainError, match="ambiguous_execution_fill_lineage"):
        await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)


@pytest.mark.parametrize("invalid_size", [0.0, math.inf])
async def test_prefix_rejects_invalid_execution_quantity(
    repository: SQLAlchemyRepository,
    invalid_size: float,
) -> None:
    """A non-positive or non-finite execution cannot consume a fill witness."""
    execution = _execution("kraken", 1)
    execution.size = invalid_size
    async with repository.session() as s:
        s.add_all(
            [
                _instrument(),
                _order(),
                execution,
                _fill_event(_CLIENT_ORDER, _KRAKEN_SHARD),
            ]
        )
        await s.commit()

    with pytest.raises(ExecutionChainError, match="invalid_execution_fill_quantity"):
        await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)


@pytest.mark.parametrize("invalid_size", [None, 0.0, math.inf])
async def test_prefix_rejects_invalid_witness_quantity(
    repository: SQLAlchemyRepository,
    invalid_size: float | None,
) -> None:
    """A null, non-positive, or non-finite fill cannot certify consumption."""
    fill = _fill_event(_CLIENT_ORDER, _KRAKEN_SHARD)
    fill.fill_size = invalid_size
    async with repository.session() as s:
        s.add_all([_instrument(), _order(), _execution("kraken", 1), fill])
        await s.commit()

    with pytest.raises(ExecutionChainError, match="invalid_execution_fill_quantity"):
        await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)


async def test_prefix_rejects_conflicting_exchange_order_identity(
    repository: SQLAlchemyRepository,
) -> None:
    """A conflicting venue order id cannot fall back to scoped client identity."""
    execution = _execution("kraken", 1)
    execution.exec_id = None
    execution.trade_id = None
    fill = _fill_event(
        _CLIENT_ORDER,
        _KRAKEN_SHARD,
        exchange_order_id="venue-order-other",
    )
    fill.exec_id = None
    fill.trade_id = None
    async with repository.session() as s:
        s.add_all(
            [
                _instrument(),
                _order(exchange_order_id="venue-order-1"),
                execution,
                fill,
            ]
        )
        await s.commit()

    with pytest.raises(ExecutionChainError, match="missing_execution_order_identity_lineage"):
        await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)


@pytest.mark.parametrize("missing_identity", ["exec_id", "trade_id"])
async def test_prefix_accepts_each_single_exact_fill_identity(
    repository: SQLAlchemyRepository,
    missing_identity: Literal["exec_id", "trade_id"],
) -> None:
    """Either venue id certifies a fill when the other identity is unavailable."""
    execution = _execution("kraken", 1)
    fill = _fill_event(_CLIENT_ORDER, _KRAKEN_SHARD)
    if missing_identity == "exec_id":
        execution.exec_id = None
        fill.exec_id = None
    else:
        execution.trade_id = None
        fill.trade_id = None
    async with repository.session() as s:
        s.add_all(
            [
                _instrument(),
                _order(),
                execution,
                fill,
            ]
        )
        await s.commit()

    prefix = await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)

    assert [row["shard_key"] for row in prefix["executions"]] == [_KRAKEN_SHARD]


async def test_prefix_rejects_superseded_fill_evidence(
    repository: SQLAlchemyRepository,
) -> None:
    """Closed venue evidence violates append-only fill lineage certification."""
    async with repository.session() as s:
        s.add_all(
            [
                _instrument(),
                _order(),
                _execution("kraken", 1),
                _fill_event(
                    _CLIENT_ORDER,
                    _KRAKEN_SHARD,
                    known_to=_AS_OF,
                ),
            ]
        )
        await s.commit()

    with pytest.raises(ExecutionChainError, match="superseded_execution_fill_lineage"):
        await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)


async def test_prefix_uses_stable_instrument_id_not_versioned_native_symbol(
    repository: SQLAlchemyRepository,
) -> None:
    """Venue symbol renames do not replace stable Order-to-Instrument lineage."""
    async with repository.session() as s:
        s.add_all(
            [
                _symbol(
                    "XBT-USD",
                    timestamp=_EARLIER - timedelta(days=1),
                    known_to=_EARLIER,
                    sequence_id=0,
                ),
                _instrument(),
                _order(),
                _execution("kraken", 1),
                _fill_event(
                    _CLIENT_ORDER,
                    "kraken.XBT-USD.live",
                    instrument="XBT-USD",
                ),
            ]
        )
        await s.commit()

    prefix = await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)

    assert [row["instrument_public_id"] for row in prefix["executions"]] == [_INSTRUMENT]


async def test_prefix_rejects_wrong_instrument_fill_lineage(
    repository: SQLAlchemyRepository,
) -> None:
    """A same-CID fill for another instrument cannot donate its shard."""
    async with repository.session() as s:
        s.add_all(
            [
                _instrument(),
                _order(),
                _execution("kraken", 1),
                _fill_event(
                    _CLIENT_ORDER,
                    "kraken.ETH-USD.live",
                    instrument="ETH-USD",
                ),
            ]
        )
        await s.commit()

    with pytest.raises(ExecutionChainError, match="crossed_execution_fill_instrument_lineage"):
        await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)


async def test_prefix_rejects_missing_native_symbol_lineage(
    repository: SQLAlchemyRepository,
) -> None:
    """An Instrument with no stable Symbol history cannot certify a venue name."""
    missing_symbol = "00000000-0000-7000-8000-000000000a02"
    async with repository.session() as s:
        s.add_all(
            [
                _instrument(symbol_public_id=missing_symbol),
                _order(),
                _execution("kraken", 1),
                _fill_event(_CLIENT_ORDER, _KRAKEN_SHARD),
            ]
        )
        await s.commit()

    with pytest.raises(ExecutionChainError, match="missing_execution_native_symbol_lineage"):
        await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)


async def test_prefix_preserves_distinct_tagged_shards_for_one_paper_instrument(
    repository: SQLAlchemyRepository,
) -> None:
    """Same-instrument paper strategies retain separate durable pool identities."""
    second_order = "00000000-0000-7000-8000-000000000c03"
    second_client_order = "client-paper-b"
    first_shard = "paper.BTC-USD.paper.strategy-a"
    second_shard = "paper.BTC-USD.paper.strategy-b"
    async with repository.session() as s:
        s.add_all(
            [
                _instrument("paper"),
                _order(mode="paper", client_order_id=_CLIENT_ORDER),
                _order(
                    second_order,
                    mode="paper",
                    client_order_id=second_client_order,
                ),
                _execution("paper", 1, mode="paper"),
                _execution("paper", 2, order_public_id=second_order, mode="paper"),
                _fill_event(
                    _CLIENT_ORDER,
                    first_shard,
                    exchange="paper",
                    mode="paper",
                ),
                _fill_event(
                    second_client_order,
                    second_shard,
                    exchange="paper",
                    mode="paper",
                    sequence_id=2,
                ),
            ]
        )
        await s.commit()

    prefix = await repository.get_pnl_timeline_execution_prefix(_WALLET, "paper", _AS_OF)

    assert prefix["watermarks"] == {"paper": 2}
    assert [row["instrument_public_id"] for row in prefix["executions"]] == [
        _INSTRUMENT,
        _INSTRUMENT,
    ]
    assert [row["shard_key"] for row in prefix["executions"]] == [first_shard, second_shard]


def _shared_trade_adversarial_rows(
    execution_count: int,
) -> tuple[
    list[tuple[Execution, Order | None, Instrument | None]],
    list[VenueEvent],
    dict[str, set[str]],
    set[str],
]:
    """Build one same-CID trade bucket partitioned by stable instruments."""
    source_rows: list[tuple[Execution, Order | None, Instrument | None]] = []
    fill_rows: list[VenueEvent] = []
    native_symbols_by_symbol_public_id: dict[str, set[str]] = {}
    instrument_public_ids: set[str] = set()
    for scope_sequence in range(1, execution_count + 1):
        suffix = f"{scope_sequence:012x}"
        symbol_public_id = f"10000000-0000-7000-8000-{suffix}"
        instrument_public_id = f"20000000-0000-7000-8000-{suffix}"
        order_public_id = f"30000000-0000-7000-8000-{suffix}"
        native_symbol = f"ASSET-{scope_sequence}-USD"
        instrument = _instrument(
            public_id=instrument_public_id,
            symbol_public_id=symbol_public_id,
        )
        order = _order(
            order_public_id,
            instrument_public_id=instrument_public_id,
        )
        execution = _execution(
            "kraken",
            scope_sequence,
            order_public_id=order_public_id,
            trade_id="shared-trade",
        )
        execution.public_id = f"40000000-0000-7000-8000-{suffix}"
        execution.exec_id = None
        fill = _fill_event(
            _CLIENT_ORDER,
            f"kraken.{native_symbol}.live",
            sequence_id=scope_sequence,
            execution_scope_sequence=scope_sequence,
            instrument=native_symbol,
        )
        fill.public_id = f"50000000-0000-7000-8000-{suffix}"
        fill.trade_id = "shared-trade"
        source_rows.append((execution, order, instrument))
        fill_rows.append(fill)
        native_symbols_by_symbol_public_id[symbol_public_id] = {native_symbol}
        instrument_public_ids.add(instrument_public_id)
    return (
        source_rows,
        fill_rows,
        native_symbols_by_symbol_public_id,
        instrument_public_ids,
    )


def _shared_alias_exact_rows(
    execution_count: int,
    alias_count: int,
) -> tuple[
    list[tuple[Execution, Order | None, Instrument | None]],
    list[VenueEvent],
    set[str],
]:
    """Build distinct exact pairs sharing one wide stable symbol lineage."""
    native_symbols = {f"EXACT-{alias_index:03d}-USD" for alias_index in range(1, alias_count + 1)}
    selected_native_symbols = sorted(native_symbols)[:execution_count]
    instrument = _instrument()
    source_rows: list[tuple[Execution, Order | None, Instrument | None]] = []
    fill_rows: list[VenueEvent] = []
    for scope_sequence, native_symbol in enumerate(selected_native_symbols, start=1):
        suffix = f"{scope_sequence:012x}"
        order_public_id = f"16000000-0000-7000-8000-{suffix}"
        execution = _execution(
            "kraken",
            scope_sequence,
            order_public_id=order_public_id,
        )
        execution.public_id = f"17000000-0000-7000-8000-{suffix}"
        fill = _fill_event(
            _CLIENT_ORDER,
            f"kraken.{native_symbol}.live",
            sequence_id=scope_sequence,
            instrument=native_symbol,
        )
        fill.public_id = f"18000000-0000-7000-8000-{suffix}"
        source_rows.append(
            (
                execution,
                _order(order_public_id),
                instrument,
            )
        )
        fill_rows.append(fill)
    return source_rows, fill_rows, native_symbols


def _distinct_order_fallback_rows(
    execution_count: int,
) -> tuple[
    list[tuple[Execution, Order | None, Instrument | None]],
    list[VenueEvent],
]:
    """Build one same-CID native family with distinct venue-order identities."""
    instrument = _instrument()
    source_rows: list[tuple[Execution, Order | None, Instrument | None]] = []
    fill_rows: list[VenueEvent] = []
    for scope_sequence in range(1, execution_count + 1):
        suffix = f"{scope_sequence:012x}"
        order_public_id = f"60000000-0000-7000-8000-{suffix}"
        exchange_order_id = f"venue-order-{scope_sequence}"
        order = _order(
            order_public_id,
            exchange_order_id=exchange_order_id,
        )
        execution = _execution(
            "kraken",
            scope_sequence,
            order_public_id=order_public_id,
        )
        execution.public_id = f"70000000-0000-7000-8000-{suffix}"
        execution.exec_id = None
        execution.trade_id = None
        fill = _fill_event(
            _CLIENT_ORDER,
            _KRAKEN_SHARD,
            sequence_id=scope_sequence,
            exchange_order_id=exchange_order_id,
        )
        fill.public_id = f"80000000-0000-7000-8000-{suffix}"
        fill.exec_id = None
        fill.trade_id = None
        source_rows.append((execution, order, instrument))
        fill_rows.append(fill)
    return source_rows, fill_rows


def _shared_order_conflict_rows(
    execution_count: int,
) -> tuple[
    list[tuple[Execution, Order | None, Instrument | None]],
    list[VenueEvent],
    set[str],
]:
    """Build distinct stable instruments claiming one atomic order partition."""
    source_rows: list[tuple[Execution, Order | None, Instrument | None]] = []
    fill_rows: list[VenueEvent] = []
    instrument_public_ids: set[str] = set()
    for scope_sequence in range(1, execution_count + 1):
        suffix = f"{scope_sequence:012x}"
        instrument_public_id = f"90000000-0000-7000-8000-{suffix}"
        order_public_id = f"a0000000-0000-7000-8000-{suffix}"
        instrument = _instrument(public_id=instrument_public_id)
        order = _order(
            order_public_id,
            instrument_public_id=instrument_public_id,
            exchange_order_id="shared-venue-order",
        )
        execution = _execution(
            "kraken",
            scope_sequence,
            order_public_id=order_public_id,
        )
        execution.public_id = f"b0000000-0000-7000-8000-{suffix}"
        execution.exec_id = None
        execution.trade_id = None
        fill = _fill_event(
            _CLIENT_ORDER,
            _KRAKEN_SHARD,
            sequence_id=scope_sequence,
            exchange_order_id="shared-venue-order",
        )
        fill.public_id = f"c0000000-0000-7000-8000-{suffix}"
        fill.exec_id = None
        fill.trade_id = None
        source_rows.append((execution, order, instrument))
        fill_rows.append(fill)
        instrument_public_ids.add(instrument_public_id)
    return source_rows, fill_rows, instrument_public_ids


def _shared_alias_order_fallback_rows(
    execution_count: int,
    alias_count: int,
) -> tuple[
    list[tuple[Execution, Order | None, Instrument | None]],
    list[VenueEvent],
    set[str],
]:
    """Build one shared lineage and order spanning many atomic alias buckets."""
    native_symbols = {f"ASSET-{alias_index:03d}-USD" for alias_index in range(1, alias_count + 1)}
    selected_native_symbols = sorted(native_symbols)[:execution_count]
    instrument = _instrument()
    source_rows: list[tuple[Execution, Order | None, Instrument | None]] = []
    fill_rows: list[VenueEvent] = []
    for scope_sequence, native_symbol in enumerate(selected_native_symbols, start=1):
        suffix = f"{scope_sequence:012x}"
        order_public_id = f"d0000000-0000-7000-8000-{suffix}"
        order = _order(
            order_public_id,
            exchange_order_id="shared-alias-order",
        )
        execution = _execution(
            "kraken",
            scope_sequence,
            order_public_id=order_public_id,
        )
        execution.public_id = f"e0000000-0000-7000-8000-{suffix}"
        execution.exec_id = None
        execution.trade_id = None
        execution.size = float(scope_sequence)
        fill = _fill_event(
            _CLIENT_ORDER,
            f"kraken.{native_symbol}.live",
            sequence_id=scope_sequence,
            exchange_order_id="shared-alias-order",
            instrument=native_symbol,
        )
        fill.public_id = f"f0000000-0000-7000-8000-{suffix}"
        fill.exec_id = None
        fill.trade_id = None
        fill.fill_size = float(scope_sequence)
        fill.cum_fill_size = float(scope_sequence)
        source_rows.append((execution, order, instrument))
        fill_rows.append(fill)
    return source_rows, fill_rows, native_symbols


def _disjoint_lineage_order_fallback_rows(
    execution_count: int,
) -> tuple[
    list[tuple[Execution, Order | None, Instrument | None]],
    list[VenueEvent],
    dict[str, set[str]],
    set[str],
]:
    """Build disjoint stable lineages sharing one venue-order identifier."""
    source_rows: list[tuple[Execution, Order | None, Instrument | None]] = []
    fill_rows: list[VenueEvent] = []
    native_symbols_by_symbol_public_id: dict[str, set[str]] = {}
    instrument_public_ids: set[str] = set()
    for scope_sequence in range(1, execution_count + 1):
        suffix = f"{scope_sequence:012x}"
        symbol_public_id = f"11000000-0000-7000-8000-{suffix}"
        instrument_public_id = f"12000000-0000-7000-8000-{suffix}"
        order_public_id = f"13000000-0000-7000-8000-{suffix}"
        native_symbol = f"DISJOINT-{scope_sequence:03d}-USD"
        instrument = _instrument(
            public_id=instrument_public_id,
            symbol_public_id=symbol_public_id,
        )
        order = _order(
            order_public_id,
            instrument_public_id=instrument_public_id,
            exchange_order_id="shared-disjoint-order",
        )
        execution = _execution(
            "kraken",
            scope_sequence,
            order_public_id=order_public_id,
        )
        execution.public_id = f"14000000-0000-7000-8000-{suffix}"
        execution.exec_id = None
        execution.trade_id = None
        execution.size = float(scope_sequence)
        fill = _fill_event(
            _CLIENT_ORDER,
            f"kraken.{native_symbol}.live",
            sequence_id=scope_sequence,
            exchange_order_id="shared-disjoint-order",
            instrument=native_symbol,
        )
        fill.public_id = f"15000000-0000-7000-8000-{suffix}"
        fill.exec_id = None
        fill.trade_id = None
        fill.fill_size = float(scope_sequence)
        fill.cum_fill_size = float(scope_sequence)
        source_rows.append((execution, order, instrument))
        fill_rows.append(fill)
        native_symbols_by_symbol_public_id[symbol_public_id] = {native_symbol}
        instrument_public_ids.add(instrument_public_id)
    return (
        source_rows,
        fill_rows,
        native_symbols_by_symbol_public_id,
        instrument_public_ids,
    )


def _wrap_counted_exact_index(
    values: dict[_ExactIndexKey, _ExactPartitionMap],
    counters: _ExactResolverCounters,
) -> dict[_ExactIndexKey, _ExactPartitionMap]:
    """Wrap exact reverse-index groups, partitions, and source rows."""
    wrapped_groups: dict[_ExactIndexKey, _ExactPartitionMap] = {}
    for exact_key, partitions in values.items():
        wrapped_partitions: _ExactPartitionMap = {}
        for native_scope_key, indexed_fills in partitions.items():
            wrapped_partitions[native_scope_key] = _CountingIndexedFillList(
                indexed_fills,
                counters.indexed_row_visits,
            )
        wrapped_groups[exact_key] = cast(
            _ExactPartitionMap,
            _CountingExactPartitionMapping(
                wrapped_partitions,
                counters.exact_partition_visits,
            ),
        )
    return cast(
        dict[_ExactIndexKey, _ExactPartitionMap],
        _CountingExactIndexMapping(
            wrapped_groups,
            counters.exact_key_probes,
        ),
    )


def _instrument_exact_resolver(
    monkeypatch: pytest.MonkeyPatch,
) -> _ExactResolverCounters:
    """Install direct-work counters across every exact reverse index."""
    counters = _ExactResolverCounters()
    original_freeze = SQLAlchemyRepository._freeze_pnl_timeline_native_symbol_lineage
    original_presence = SQLAlchemyRepository._pnl_timeline_native_scope_presence
    original_index = SQLAlchemyRepository._index_pnl_timeline_fill_rows
    original_owners = SQLAlchemyRepository._pnl_timeline_exact_native_owners
    original_precompute = SQLAlchemyRepository._precompute_pnl_timeline_exact_evidence

    def counted_freeze(aliases: set[str]) -> frozenset[str]:
        """Count each immutable lineage materialization and copied alias."""
        counters.alias_freeze_calls += 1
        counters.alias_copy_visits += len(aliases)
        return original_freeze(aliases)

    def counted_presence(
        native_scope_key: _PnlTimelineFillNativeScopeKey,
        fill_index: _PnlTimelineFillIndex,
    ) -> tuple[bool, bool]:
        """Count each direct native-presence probe."""
        counters.native_presence_probes += 1
        return original_presence(native_scope_key, fill_index)

    def counted_index(rows: list[VenueEvent]) -> _PnlTimelineFillIndex:
        """Wrap the three complete-key reverse indices after one build."""
        fill_index = original_index(rows)
        fill_index.fills_by_exec_id = cast(
            dict[
                _PnlTimelineScopeExactFillKey,
                _ExactPartitionMap,
            ],
            _wrap_counted_exact_index(
                cast(
                    dict[_ExactIndexKey, _ExactPartitionMap],
                    fill_index.fills_by_exec_id,
                ),
                counters,
            ),
        )
        fill_index.fills_by_exec_trade_id = cast(
            dict[
                _PnlTimelineScopeExecTradeFillKey,
                _ExactPartitionMap,
            ],
            _wrap_counted_exact_index(
                cast(
                    dict[_ExactIndexKey, _ExactPartitionMap],
                    fill_index.fills_by_exec_trade_id,
                ),
                counters,
            ),
        )
        fill_index.fills_by_trade_id = cast(
            dict[
                _PnlTimelineScopeExactFillKey,
                _ExactPartitionMap,
            ],
            _wrap_counted_exact_index(
                cast(
                    dict[_ExactIndexKey, _ExactPartitionMap],
                    fill_index.fills_by_trade_id,
                ),
                counters,
            ),
        )
        return fill_index

    def counted_owners(
        lineages: dict[str, frozenset[str]],
    ) -> dict[str, str | None]:
        """Count every native alias edge fed to exact ownership."""
        counters.native_owner_alias_visits += sum(
            len(native_symbols) for native_symbols in lineages.values()
        )
        return original_owners(lineages)

    def counted_precompute(
        candidates: list[_PnlTimelineExecutionCandidate],
        state: _PnlTimelineFillAssignmentState,
    ) -> None:
        """Count exact witness-cache entries actually materialized."""
        before = len(state.exact_witnesses)
        original_precompute(candidates, state)
        counters.exact_cache_entries += len(state.exact_witnesses) - before

    monkeypatch.setattr(
        SQLAlchemyRepository,
        "_freeze_pnl_timeline_native_symbol_lineage",
        staticmethod(counted_freeze),
    )
    monkeypatch.setattr(
        SQLAlchemyRepository,
        "_pnl_timeline_native_scope_presence",
        staticmethod(counted_presence),
    )
    monkeypatch.setattr(
        SQLAlchemyRepository,
        "_index_pnl_timeline_fill_rows",
        staticmethod(counted_index),
    )
    monkeypatch.setattr(
        SQLAlchemyRepository,
        "_pnl_timeline_exact_native_owners",
        staticmethod(counted_owners),
    )
    monkeypatch.setattr(
        SQLAlchemyRepository,
        "_precompute_pnl_timeline_exact_evidence",
        staticmethod(counted_precompute),
    )
    return counters


def test_prefix_exact_pair_reverse_index_scales_with_shared_alias_lineage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Distinct exact pairs cost aliases plus actual native partitions."""
    execution_count = 40
    alias_count = 50
    source_rows, fill_rows, native_symbols = _shared_alias_exact_rows(
        execution_count,
        alias_count,
    )
    counters = _instrument_exact_resolver(monkeypatch)

    resolved = _certified_prefix(
        {"kraken": execution_count},
        source_rows,
        fill_rows,
        {_SYMBOL: native_symbols},
        {_CLIENT_ORDER: {_INSTRUMENT}},
    )
    permuted = _certified_prefix(
        {"kraken": execution_count},
        source_rows,
        list(reversed(fill_rows)),
        {_SYMBOL: native_symbols},
        {_CLIENT_ORDER: {_INSTRUMENT}},
    )

    assert resolved == permuted
    assert [row["scope_sequence"] for row in resolved] == list(range(1, execution_count + 1))
    assert len({row["shard_key"] for row in resolved}) == execution_count
    assert counters.alias_freeze_calls == 2
    assert counters.alias_copy_visits == alias_count * 2
    assert counters.native_presence_probes == alias_count * 2
    assert counters.native_owner_alias_visits == 0
    assert counters.exact_key_probes[0] == execution_count * 2
    assert counters.exact_partition_visits[0] == execution_count * 2
    assert counters.indexed_row_visits[0] == execution_count * 2
    assert counters.exact_cache_entries == execution_count * 2


@pytest.mark.parametrize("mismatched_component", ["exec_id", "trade_id"])
def test_prefix_exact_pair_requires_both_identity_components(
    mismatched_component: Literal["exec_id", "trade_id"],
) -> None:
    """A combined exact key cannot degrade to either single identifier."""
    source_rows, fill_rows, native_symbols = _shared_alias_exact_rows(1, 1)
    if mismatched_component == "exec_id":
        fill_rows[0].exec_id = "wrong-exec"
    else:
        fill_rows[0].trade_id = "wrong-trade"

    with pytest.raises(
        ExecutionChainError,
        match="missing_execution_fill_identity_lineage",
    ):
        _certified_prefix(
            {"kraken": 1},
            source_rows,
            fill_rows,
            {_SYMBOL: native_symbols},
            {_CLIENT_ORDER: {_INSTRUMENT}},
        )


def test_prefix_exact_index_visits_shared_trade_distinct_instruments_linearly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One shared trade key assigns disjoint native partitions in one pass."""
    execution_count = 40
    (
        source_rows,
        fill_rows,
        native_symbols_by_symbol_public_id,
        instrument_public_ids,
    ) = _shared_trade_adversarial_rows(execution_count)
    counters = _instrument_exact_resolver(monkeypatch)

    resolved = _certified_prefix(
        {"kraken": execution_count},
        source_rows,
        fill_rows,
        native_symbols_by_symbol_public_id,
        {_CLIENT_ORDER: instrument_public_ids},
    )
    permuted = _certified_prefix(
        {"kraken": execution_count},
        source_rows,
        list(reversed(fill_rows)),
        native_symbols_by_symbol_public_id,
        {_CLIENT_ORDER: instrument_public_ids},
    )

    assert resolved == permuted
    assert [row["scope_sequence"] for row in resolved] == list(range(1, execution_count + 1))
    assert len({row["shard_key"] for row in resolved}) == execution_count
    assert counters.alias_freeze_calls == execution_count * 2
    assert counters.alias_copy_visits == execution_count * 2
    assert counters.native_presence_probes == execution_count * 2
    assert counters.native_owner_alias_visits == execution_count * 2
    assert counters.exact_key_probes[0] == 2
    assert counters.exact_partition_visits[0] == execution_count * 2
    assert counters.indexed_row_visits[0] == execution_count * 2
    assert counters.exact_cache_entries == execution_count * 2


def test_prefix_exact_shared_trade_allows_unused_historical_alias_overlap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unused historical overlap does not reject disjoint actual partitions."""
    execution_count = 2
    (
        source_rows,
        fill_rows,
        native_symbols_by_symbol_public_id,
        instrument_public_ids,
    ) = _shared_trade_adversarial_rows(execution_count)
    for native_symbols in native_symbols_by_symbol_public_id.values():
        native_symbols.add("SHARED-HISTORICAL-USD")
    unrelated_fill = _fill_event(
        _CLIENT_ORDER,
        "kraken.UNRELATED-EXACT-USD.live",
        sequence_id=3,
        instrument="UNRELATED-EXACT-USD",
    )
    unrelated_fill.trade_id = "shared-trade"
    fill_rows.append(unrelated_fill)
    counters = _instrument_exact_resolver(monkeypatch)

    resolved = _certified_prefix(
        {"kraken": execution_count},
        source_rows,
        fill_rows,
        native_symbols_by_symbol_public_id,
        {_CLIENT_ORDER: instrument_public_ids},
    )
    permuted = _certified_prefix(
        {"kraken": execution_count},
        source_rows,
        list(reversed(fill_rows)),
        native_symbols_by_symbol_public_id,
        {_CLIENT_ORDER: instrument_public_ids},
    )

    assert resolved == permuted
    assert len({row["shard_key"] for row in resolved}) == execution_count
    assert counters.native_presence_probes == execution_count * 2 * 2
    assert counters.native_owner_alias_visits == execution_count * 2 * 2
    assert counters.exact_key_probes[0] == 2
    assert counters.exact_partition_visits[0] == (execution_count + 1) * 2
    assert counters.indexed_row_visits[0] == execution_count * 2
    assert counters.exact_cache_entries == execution_count * 2


def test_prefix_exact_shared_trade_rejects_actual_alias_overlap_before_row_fanout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An actually used multiply-owned native partition fails before row visits."""
    execution_count = 2
    (
        source_rows,
        fill_rows,
        native_symbols_by_symbol_public_id,
        instrument_public_ids,
    ) = _shared_trade_adversarial_rows(execution_count)
    for native_symbols in native_symbols_by_symbol_public_id.values():
        native_symbols.add("SHARED-ACTUAL-USD")
    fill_rows[0].instrument = "SHARED-ACTUAL-USD"
    fill_rows[0].shard_key = "kraken.SHARED-ACTUAL-USD.live"
    counters = _instrument_exact_resolver(monkeypatch)

    for rows in (fill_rows, list(reversed(fill_rows))):
        with pytest.raises(
            ExecutionChainError,
            match="ambiguous_execution_fill_instrument_lineage",
        ):
            _certified_prefix(
                {"kraken": execution_count},
                source_rows,
                rows,
                native_symbols_by_symbol_public_id,
                {_CLIENT_ORDER: instrument_public_ids},
            )

    assert counters.native_presence_probes == execution_count * 2 * 2
    assert counters.native_owner_alias_visits == execution_count * 2 * 2
    assert counters.exact_key_probes[0] == 2
    assert counters.exact_partition_visits[0] == execution_count * 2
    assert counters.indexed_row_visits[0] == 0
    assert counters.exact_cache_entries == 0


def test_prefix_fallback_rejects_shared_order_partition_before_row_scan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One atomic order partition cannot belong to distinct stable instruments."""
    execution_count = 20
    source_rows, fill_rows, instrument_public_ids = _shared_order_conflict_rows(execution_count)
    identity_calls = 0
    indexed_row_visits = [0]
    original_identity = SQLAlchemyRepository._pnl_timeline_fill_identity_from_ids
    original_index = SQLAlchemyRepository._index_pnl_timeline_fill_rows

    def counted_identity(
        fill_row: VenueEvent,
        exec_id: str | None,
        trade_id: str | None,
    ) -> tuple[str, str, str, str]:
        """Count the one-pass normalization of every adversarial fill."""
        nonlocal identity_calls
        identity_calls += 1
        return original_identity(fill_row, exec_id, trade_id)

    def counted_index(rows: list[VenueEvent]) -> _PnlTimelineFillIndex:
        """Wrap the shared atomic bucket to detect any resolver row scan."""
        fill_index = original_index(rows)
        for key, values in fill_index.fills_by_exchange_order_id.items():
            fill_index.fills_by_exchange_order_id[key] = _CountingIndexedFillList(
                values,
                indexed_row_visits,
            )
        return fill_index

    monkeypatch.setattr(
        SQLAlchemyRepository,
        "_pnl_timeline_fill_identity_from_ids",
        staticmethod(counted_identity),
    )
    monkeypatch.setattr(
        SQLAlchemyRepository,
        "_index_pnl_timeline_fill_rows",
        staticmethod(counted_index),
    )

    with pytest.raises(
        ExecutionChainError,
        match="ambiguous_execution_order_instrument_lineage",
    ):
        _certified_prefix(
            {"kraken": execution_count},
            source_rows,
            fill_rows,
            {_SYMBOL: {"BTC-USD"}},
            {_CLIENT_ORDER: instrument_public_ids},
        )

    assert identity_calls == execution_count
    assert indexed_row_visits[0] == 0


def test_prefix_fallback_rejects_actual_overlap_between_symbol_lineages() -> None:
    """Distinct lineages cannot claim one atomic bucket through a shared alias."""
    first_execution = _execution("kraken", 1, order_public_id=_ORDER)
    second_execution = _execution("kraken", 2, order_public_id=_COLLIDING_ORDER)
    first_execution.public_id = "91000000-0000-7000-8000-000000000001"
    second_execution.public_id = "91000000-0000-7000-8000-000000000002"
    for execution in (first_execution, second_execution):
        execution.exec_id = None
        execution.trade_id = None
    fill = _fill_event(
        _CLIENT_ORDER,
        "kraken.XBT-USD.live",
        exchange_order_id="overlap-order",
        instrument="XBT-USD",
    )
    fill.public_id = "92000000-0000-7000-8000-000000000001"
    fill.exec_id = None
    fill.trade_id = None

    with pytest.raises(
        ExecutionChainError,
        match="ambiguous_execution_order_instrument_lineage",
    ):
        _certified_prefix(
            {"kraken": 2},
            [
                (
                    first_execution,
                    _order(exchange_order_id="overlap-order"),
                    _instrument(),
                ),
                (
                    second_execution,
                    _order(
                        _COLLIDING_ORDER,
                        instrument_public_id=_COLLIDING_INSTRUMENT,
                        exchange_order_id="overlap-order",
                    ),
                    _instrument(
                        public_id=_COLLIDING_INSTRUMENT,
                        symbol_public_id=_COLLIDING_SYMBOL,
                    ),
                ),
            ],
            [fill],
            {
                _SYMBOL: {"BTC-USD", "XBT-USD"},
                _COLLIDING_SYMBOL: {"ETH-USD", "XBT-USD"},
            },
            {_CLIENT_ORDER: {_INSTRUMENT, _COLLIDING_INSTRUMENT}},
        )


def _instrument_shared_alias_resolver(
    monkeypatch: pytest.MonkeyPatch,
) -> _SharedAliasResolverCounters:
    """Install exact work counters for the shared-alias complexity oracle."""
    counters = _SharedAliasResolverCounters()
    original_freeze = SQLAlchemyRepository._freeze_pnl_timeline_native_symbol_lineage
    original_presence = SQLAlchemyRepository._pnl_timeline_native_scope_presence
    original_order_pool = SQLAlchemyRepository._order_scoped_pnl_timeline_indexed_fills
    original_index = SQLAlchemyRepository._index_pnl_timeline_fill_rows
    original_quantity_path = SQLAlchemyRepository._pnl_timeline_quantity_path
    original_quantity_range = SQLAlchemyRepository._pnl_timeline_quantity_range
    original_resolve = SQLAlchemyRepository._resolve_pnl_timeline_execution_shards

    def counted_freeze(aliases: set[str]) -> frozenset[str]:
        """Count every alias copied into an interned immutable lineage."""
        counters.alias_freeze_calls += 1
        counters.alias_copy_visits += len(aliases)
        return original_freeze(aliases)

    def counted_presence(
        native_scope_key: _PnlTimelineFillNativeScopeKey,
        fill_index: _PnlTimelineFillIndex,
    ) -> tuple[bool, bool]:
        """Count actual native-scope presence probes across shared aliases."""
        counters.native_presence_probes += 1
        return original_presence(native_scope_key, fill_index)

    def counted_order_pool(
        partition_keys: frozenset[_PnlTimelineNativeExactFillKey],
        state: _PnlTimelineFillAssignmentState,
    ) -> list[_PnlTimelineIndexedFill]:
        """Count one materialization of the frozen shared owner partition."""
        counters.order_pool_build_calls += 1
        counters.prepared_partition_counts.append(len(partition_keys))
        return original_order_pool(partition_keys, state)

    def counted_index(rows: list[VenueEvent]) -> _PnlTimelineFillIndex:
        """Wrap atomic order buckets to count every actual indexed-row visit."""
        fill_index = original_index(rows)
        counting_partitions = _CountingOrderPartitionDict(
            fill_index.fills_by_exchange_order_id,
            counters.order_partition_membership_probes,
        )
        for key, values in counting_partitions.items():
            counting_partitions[key] = _CountingIndexedFillList(
                values,
                counters.indexed_row_visits,
            )
        fill_index.fills_by_exchange_order_id = counting_partitions
        return fill_index

    def counted_quantity_path(quantity: float) -> list[tuple[int, int]]:
        """Count fixed-depth quantity-index construction and consumption."""
        return _CountingQuantityNodeList(
            original_quantity_path(quantity),
            counters.quantity_path_node_visits,
        )

    def counted_quantity_range(lower: float, upper: float) -> list[tuple[int, int]]:
        """Count bounded matching-range nodes for each execution."""
        return _CountingQuantityNodeList(
            original_quantity_range(lower, upper),
            counters.quantity_range_node_visits,
        )

    def counted_resolve(
        candidates: list[_PnlTimelineExecutionCandidate],
        rows: list[VenueEvent],
        order_instrument_ids_by_scope: dict[str, set[str]],
    ) -> dict[str, str]:
        """Capture the exact shared immutable alias object used by candidates."""
        alias_reference_ids = {id(candidate.allowed_native_symbols) for candidate in candidates}
        counters.shared_alias_reference_ids.update(alias_reference_ids)
        counters.alias_reference_counts.append(len(alias_reference_ids))
        return original_resolve(
            candidates,
            rows,
            order_instrument_ids_by_scope,
        )

    monkeypatch.setattr(
        SQLAlchemyRepository,
        "_freeze_pnl_timeline_native_symbol_lineage",
        staticmethod(counted_freeze),
    )
    monkeypatch.setattr(
        SQLAlchemyRepository,
        "_pnl_timeline_native_scope_presence",
        staticmethod(counted_presence),
    )
    monkeypatch.setattr(
        SQLAlchemyRepository,
        "_order_scoped_pnl_timeline_indexed_fills",
        staticmethod(counted_order_pool),
    )
    monkeypatch.setattr(
        SQLAlchemyRepository,
        "_index_pnl_timeline_fill_rows",
        staticmethod(counted_index),
    )
    monkeypatch.setattr(
        SQLAlchemyRepository,
        "_pnl_timeline_quantity_path",
        staticmethod(counted_quantity_path),
    )
    monkeypatch.setattr(
        SQLAlchemyRepository,
        "_pnl_timeline_quantity_range",
        staticmethod(counted_quantity_range),
    )
    monkeypatch.setattr(
        SQLAlchemyRepository,
        "_resolve_pnl_timeline_execution_shards",
        staticmethod(counted_resolve),
    )
    return counters


def test_prefix_fallback_interns_shared_alias_evidence_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Shared alias and order evidence costs scale with aliases plus rows."""
    execution_count = 40
    alias_count = 50
    source_rows, fill_rows, native_symbols = _shared_alias_order_fallback_rows(
        execution_count,
        alias_count,
    )
    counters = _instrument_shared_alias_resolver(monkeypatch)

    resolved = _certified_prefix(
        {"kraken": execution_count},
        source_rows,
        fill_rows,
        {_SYMBOL: native_symbols},
        {_CLIENT_ORDER: {_INSTRUMENT}},
    )

    assert [row["scope_sequence"] for row in resolved] == list(range(1, execution_count + 1))
    assert len({row["shard_key"] for row in resolved}) == execution_count
    assert counters.alias_freeze_calls == 1
    assert counters.alias_copy_visits == alias_count
    assert len(counters.shared_alias_reference_ids) == 1
    assert counters.native_presence_probes == alias_count
    assert counters.order_partition_membership_probes[0] == alias_count
    assert counters.order_pool_build_calls == 1
    assert counters.prepared_partition_counts == [execution_count]
    assert counters.indexed_row_visits[0] == execution_count
    assert counters.quantity_path_node_visits[0] == execution_count * 2 * 65
    assert execution_count <= counters.quantity_range_node_visits[0] <= execution_count * 128


def test_prefix_fallback_directly_probes_disjoint_order_partitions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Disjoint lineages use direct atomic lookups instead of scanning peers."""
    execution_count = 40
    (
        source_rows,
        fill_rows,
        native_symbols_by_symbol_public_id,
        instrument_public_ids,
    ) = _disjoint_lineage_order_fallback_rows(execution_count)
    counters = _instrument_shared_alias_resolver(monkeypatch)

    resolved = _certified_prefix(
        {"kraken": execution_count},
        source_rows,
        fill_rows,
        native_symbols_by_symbol_public_id,
        {_CLIENT_ORDER: instrument_public_ids},
    )

    assert counters.alias_freeze_calls == execution_count
    assert counters.alias_copy_visits == execution_count
    assert counters.alias_reference_counts == [execution_count]
    assert counters.native_presence_probes == execution_count
    assert counters.order_partition_membership_probes[0] == execution_count
    assert counters.order_pool_build_calls == execution_count
    assert counters.prepared_partition_counts == [1] * execution_count
    assert counters.indexed_row_visits[0] == execution_count

    permuted = _certified_prefix(
        {"kraken": execution_count},
        source_rows,
        list(reversed(fill_rows)),
        native_symbols_by_symbol_public_id,
        {_CLIENT_ORDER: instrument_public_ids},
    )

    assert resolved == permuted
    assert len({row["shard_key"] for row in resolved}) == execution_count
    assert counters.alias_freeze_calls == execution_count * 2
    assert counters.alias_copy_visits == execution_count * 2
    assert counters.alias_reference_counts == [execution_count, execution_count]
    assert counters.native_presence_probes == execution_count * 2
    assert counters.order_partition_membership_probes[0] == execution_count * 2
    assert counters.order_pool_build_calls == execution_count * 2
    assert counters.prepared_partition_counts == [1] * execution_count * 2
    assert counters.indexed_row_visits[0] == execution_count * 2
    assert counters.quantity_path_node_visits[0] == execution_count * 2 * 2 * 65
    assert execution_count * 2 <= counters.quantity_range_node_visits[0]
    assert counters.quantity_range_node_visits[0] <= execution_count * 2 * 128


def test_prefix_fallback_index_visits_distinct_order_buckets_linearly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Complete venue-order keys bound actual fill and quantity-node visits."""
    execution_count = 200
    source_rows, fill_rows = _distinct_order_fallback_rows(execution_count)
    identity_calls = 0
    indexed_row_visits = [0]
    quantity_path_node_visits = [0]
    quantity_range_node_visits = [0]
    consumed_identities: list[tuple[str, str, str, str]] = []
    original_identity = SQLAlchemyRepository._pnl_timeline_fill_identity_from_ids
    original_index = SQLAlchemyRepository._index_pnl_timeline_fill_rows
    original_quantity_path = SQLAlchemyRepository._pnl_timeline_quantity_path
    original_quantity_range = SQLAlchemyRepository._pnl_timeline_quantity_range
    original_consume = SQLAlchemyRepository._consume_pnl_timeline_fill_identity

    def counted_identity(
        fill_row: VenueEvent,
        exec_id: str | None,
        trade_id: str | None,
    ) -> tuple[str, str, str, str]:
        """Count the single normalization/index pass over every fill."""
        nonlocal identity_calls
        identity_calls += 1
        return original_identity(fill_row, exec_id, trade_id)

    def counted_index(rows: list[VenueEvent]) -> _PnlTimelineFillIndex:
        """Wrap venue-order buckets to count each actual row iteration."""
        fill_index = original_index(rows)
        for key, values in fill_index.fills_by_exchange_order_id.items():
            fill_index.fills_by_exchange_order_id[key] = _CountingIndexedFillList(
                values,
                indexed_row_visits,
            )
        return fill_index

    def counted_quantity_path(quantity: float) -> list[tuple[int, int]]:
        """Wrap every fixed-depth path so iteration counts individual nodes."""
        return _CountingQuantityNodeList(
            original_quantity_path(quantity),
            quantity_path_node_visits,
        )

    def counted_quantity_range(lower: float, upper: float) -> list[tuple[int, int]]:
        """Wrap every tolerance range so iteration counts individual nodes."""
        return _CountingQuantityNodeList(
            original_quantity_range(lower, upper),
            quantity_range_node_visits,
        )

    def counted_consume(
        identity: tuple[str, str, str, str],
        state: _PnlTimelineFillAssignmentState,
    ) -> None:
        """Record deterministic witness order before applying consumption."""
        consumed_identities.append(identity)
        original_consume(identity, state)

    monkeypatch.setattr(
        SQLAlchemyRepository,
        "_pnl_timeline_fill_identity_from_ids",
        staticmethod(counted_identity),
    )
    monkeypatch.setattr(
        SQLAlchemyRepository,
        "_index_pnl_timeline_fill_rows",
        staticmethod(counted_index),
    )
    monkeypatch.setattr(
        SQLAlchemyRepository,
        "_pnl_timeline_quantity_path",
        staticmethod(counted_quantity_path),
    )
    monkeypatch.setattr(
        SQLAlchemyRepository,
        "_pnl_timeline_quantity_range",
        staticmethod(counted_quantity_range),
    )
    monkeypatch.setattr(
        SQLAlchemyRepository,
        "_consume_pnl_timeline_fill_identity",
        staticmethod(counted_consume),
    )

    resolved = _certified_prefix(
        {"kraken": execution_count},
        source_rows,
        fill_rows,
        {_SYMBOL: {"BTC-USD"}},
        {_CLIENT_ORDER: {_INSTRUMENT}},
    )
    first_consumption_order = list(consumed_identities)
    consumed_identities.clear()
    permuted = _certified_prefix(
        {"kraken": execution_count},
        source_rows,
        list(reversed(fill_rows)),
        {_SYMBOL: {"BTC-USD"}},
        {_CLIENT_ORDER: {_INSTRUMENT}},
    )

    assert resolved == permuted
    assert consumed_identities == first_consumption_order
    assert len(set(first_consumption_order)) == execution_count
    assert identity_calls == execution_count * 2
    assert indexed_row_visits[0] == execution_count * 2
    assert quantity_path_node_visits[0] == execution_count * 2 * 2 * 65
    assert execution_count * 2 <= quantity_range_node_visits[0] <= execution_count * 2 * 128


def test_prefix_validator_rejects_out_of_range_and_empty_resolved_shard() -> None:
    """Defensive validation refuses impossible query rows and empty resolver output."""
    source_rows = [(_execution("kraken", 1), _order(), _instrument())]
    ignored_fill = _fill_event(_CLIENT_ORDER, _KRAKEN_SHARD, sequence_id=2)
    ignored_fill.client_order_id = None
    with pytest.raises(ExecutionChainError, match="out_of_range_execution_prefix_row"):
        _certified_prefix(
            {"kraken": 0},
            source_rows,
            [],
            {_SYMBOL: {"BTC-USD"}},
            {_CLIENT_ORDER: {_INSTRUMENT}},
        )

    invalid_wallet_execution = _execution("kraken", 1)
    invalid_wallet_execution.wallet_public_id = "not-a-wallet-uuid"
    with pytest.raises(ExecutionChainError, match="invalid_execution_fill_identity"):
        _certified_prefix(
            {"kraken": 1},
            [(invalid_wallet_execution, _order(), _instrument())],
            [_fill_event(_CLIENT_ORDER, _KRAKEN_SHARD)],
            {_SYMBOL: {"BTC-USD"}},
            {_CLIENT_ORDER: {_INSTRUMENT}},
        )

    with pytest.raises(ExecutionChainError, match="ambiguous_execution_shard_lineage"):
        _certified_prefix(
            {"kraken": 1},
            source_rows,
            [ignored_fill, _fill_event(_CLIENT_ORDER, "")],
            {_SYMBOL: {"BTC-USD"}},
            {_CLIENT_ORDER: {_INSTRUMENT}},
        )

    assert SQLAlchemyRepository._pnl_timeline_quantity_range(0.0, 0.0) == [(64, 0)]


def test_fill_stable_order_rejects_missing_fixed_width_identity() -> None:
    """A transient row with no PK, UUID, or sequence cannot enter the radix."""
    fill = _fill_event(_CLIENT_ORDER, _KRAKEN_SHARD)
    fill.public_id = "not-a-public-uuid"
    fill.sequence_id = -1

    with pytest.raises(ExecutionChainError, match="invalid_execution_fill_identity"):
        SQLAlchemyRepository._pnl_timeline_fill_stable_order(fill)


async def test_empty_scope_returns_an_empty_frozen_bundle(
    repository: SQLAlchemyRepository,
) -> None:
    """A scope with no qualifying execution has no watermarks, rows, or corrections."""
    prefix = await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)

    assert prefix == {"watermarks": {}, "executions": [], "annulments": []}
