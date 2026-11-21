"""Pointwise event FX reads only the required minutes with dense-read parity.

The pointwise series consumer reads FX rows only at its required pair minutes, so
its event phase selects those exact minutes inside the original event bounds
instead of every minute the bounds span. These tests compare the exact read
with the original interval or dense read through the real SQLite repository:
series points, replay metadata, rate sources, FX shadow evaluations and built
shadow artifacts must agree, while event rows stop growing with chunk position.
"""

import os
import random
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Mapping
from collections.abc import Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import cast
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy import update
from sqlalchemy.engine import make_url

from snapper.application.portfolio import pnl_timeline_service
from snapper.application.portfolio.basket_valuation import ValuationEvidence
from snapper.application.portfolio.basket_valuation import value_currency
from snapper.application.portfolio.fx_conversion_shadow import FxShadowEvaluation
from snapper.application.portfolio.fx_conversion_shadow import FxShadowPinContext
from snapper.application.portfolio.fx_conversion_shadow import _build_artifact
from snapper.application.portfolio.fx_conversion_shadow import activate_fx_shadow_context
from snapper.application.portfolio.fx_rates import FxPairKey
from snapper.application.portfolio.fx_rates import currency_pair_key
from snapper.application.portfolio.pnl_timeline_service import PNL_SAMPLE_CALC_VERSION
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReadPolicy
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReplayOptions
from snapper.application.portfolio.pnl_timeline_service import PnlWalletSeriesResult
from snapper.application.portfolio.pnl_timeline_service import _discover_event_fx_candidates
from snapper.application.portfolio.pnl_timeline_service import _event_fx_selection
from snapper.application.portfolio.pnl_timeline_service import _FxEventSelection
from snapper.application.portfolio.pnl_timeline_service import _FxMinuteSets
from snapper.application.portfolio.pnl_timeline_service import _FxRequest
from snapper.application.portfolio.pnl_timeline_service import _load_event_fx_rows
from snapper.application.portfolio.pnl_timeline_service import build_wallet_pnl_series
from snapper.application.portfolio.pnl_timeline_service import load_basket_fiat_evidence_by_minute
from snapper.data.models import Candle
from snapper.data.models import Instrument
from snapper.data.models import Symbol
from snapper.data.repository import Repository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import InstrumentSymbolRefRow
from snapper.data.repository_types import PnlFxRatePlane
from snapper.data.repository_types import PnlFxRateRow
from snapper.data.repository_types import PnlTimelineCandleRow
from snapper.data.repository_types import PnlTimelineOpeningExecutionRow
from tests.application.portfolio.test_pnl_timeline_service import _I1
from tests.application.portfolio.test_pnl_timeline_service import _I2
from tests.application.portfolio.test_pnl_timeline_service import _W1
from tests.application.portfolio.test_pnl_timeline_service import FakeRepo
from tests.application.portfolio.test_pnl_timeline_service import _candle
from tests.application.portfolio.test_pnl_timeline_service import _exec_row
from tests.application.portfolio.test_pnl_timeline_service import _m
from tests.application.portfolio.test_pnl_timeline_service import _ref
from tests.data.test_pnl_fx_rate_repository import _candle as _fx_candle
from tests.data.test_pnl_fx_rate_repository import _instrument
from tests.data.test_pnl_fx_rate_repository import _symbol

_ORIGIN = -2000
_EUR_USD = currency_pair_key("EUR", "USD")
_PLN_USD = currency_pair_key("PLN", "USD")
_GENERATED_KEYS = frozenset({"public_id", "session_id", "election_public_id"})
_SYMBOLS: tuple[tuple[str, str, str, str], ...] = (
    ("sym-eurusd", "EUR-USD", "EUR", "USD"),
    ("sym-usdeur", "USD-EUR", "USD", "EUR"),
    ("sym-usdpln", "USD-PLN", "USD", "PLN"),
    ("sym-eurusd-alt", "EURUSD-ALT", "EUR", "USD"),
)
_INSTRUMENTS: tuple[tuple[str, str, str], ...] = (
    ("ins-eurusd-bitstamp", "sym-eurusd", "bitstamp"),
    ("ins-eurusd-kraken", "sym-eurusd", "kraken"),
    ("ins-eurusd-walutomat", "sym-eurusd", "walutomat"),
    ("ins-usdeur-kraken", "sym-usdeur", "kraken"),
    ("ins-usdpln-walutomat", "sym-usdpln", "walutomat"),
    ("ins-usdpln-kraken", "sym-usdpln", "kraken"),
    ("ins-eurusd-kraken-alt", "sym-eurusd-alt", "kraken"),
)
_EVENT_LAYOUTS: dict[str, tuple[tuple[int, ...], tuple[int, ...]]] = {
    "sparse": ((-130, -70, -10), (-70,)),
    "clustered": (tuple(range(-12, -2)), (-8, -4)),
    "fragmented": (tuple(range(-450, 0, 25)), tuple(range(-450, 0, 75))),
}
_WALUTOMAT_EUR_USD: PnlFxRatePlane = ("EUR", "USD", "walutomat")


def _kraken_close(minute: int) -> float | None:
    """Return the primary Kraken EUR/USD close, gapped on every third minute."""
    return None if minute % 3 == 0 else 1.12 + (minute % 7) / 1000


def _kraken_alt_close(minute: int) -> float | None:
    """Return a second Kraken EUR/USD listing: equal, conflicting or absent."""
    primary = _kraken_close(minute)
    if primary is None or minute % 4 not in (1, 2):
        return None
    return primary if minute % 4 == 1 else primary + 0.5


_CLOSES: dict[str, Callable[[int], float | None]] = {
    "ins-eurusd-bitstamp": lambda minute: 1.10 if minute % 2 == 0 else None,
    "ins-eurusd-kraken": _kraken_close,
    "ins-eurusd-walutomat": lambda minute: 1.15,
    "ins-usdeur-kraken": lambda minute: 0.9,
    "ins-usdpln-walutomat": lambda minute: None if minute % 5 == 0 else 4.0,
    "ins-usdpln-kraken": lambda minute: 4.1,
    "ins-eurusd-kraken-alt": _kraken_alt_close,
}


def _symbols() -> list[Symbol]:
    """Build fresh ORM symbols for the four EUR/USD and two PLN/USD planes."""
    return [
        _symbol(public_id, native, base, quote, timestamp=_m(_ORIGIN))
        for public_id, native, base, quote in _SYMBOLS
    ]


def _instruments() -> list[Instrument]:
    """Build fresh ORM instruments, including a duplicate Kraken EUR/USD listing."""
    return [
        _instrument(public_id, symbol_public_id, exchange, timestamp=_m(_ORIGIN))
        for public_id, symbol_public_id, exchange in _INSTRUMENTS
    ]


def _standard_candles(minutes: Iterable[int]) -> list[Candle]:
    """Build every plane's candle closing at each grid minute, honouring gaps."""
    return [
        _fx_candle(instrument, _m(minute - 1), close)
        for minute in sorted(set(minutes))
        for instrument, closes in _CLOSES.items()
        if (close := closes(minute)) is not None
    ]


async def _seed(
    repository: SQLAlchemyRepository,
    candles: Sequence[Candle],
    symbols: Sequence[Symbol],
    instruments: Sequence[Instrument],
) -> None:
    """Insert the standard planes, any extra listings and the given candles.

    Args:
        repository: Store whose FX tables already exist.
        candles: Candles to store, inserted in the given order.
        symbols: Additional symbols beyond the standard FX set.
        instruments: Additional instruments beyond the standard FX set.
    """
    async with repository.session() as session:
        session.add_all([*_symbols(), *symbols, *_instruments(), *instruments])
        await session.flush()
        session.add_all(candles)
        await session.commit()


class _Databases:
    """Create isolated real SQLite FX stores and dispose every engine at teardown."""

    def __init__(self, root: Path) -> None:
        """Remember where the per-test database files are created."""
        self._root = root
        self._created: list[SQLAlchemyRepository] = []

    async def create(
        self,
        candles: Sequence[Candle],
        symbols: Sequence[Symbol] = (),
        instruments: Sequence[Instrument] = (),
    ) -> SQLAlchemyRepository:
        """Create one store holding the standard planes plus any extra listings.

        Args:
            candles: Candles to store, inserted in the given order.
            symbols: Additional symbols beyond the standard FX set.
            instruments: Additional instruments beyond the standard FX set.

        Returns:
            A repository over the populated SQLite file.
        """
        path = self._root / f"exact-event-fx-{len(self._created)}.db"
        schema = create_engine(f"sqlite:///{path}")
        Symbol.__table__.create(schema)
        Instrument.__table__.create(schema)
        Candle.__table__.create(schema)
        schema.dispose()
        repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{path}")
        self._created.append(repository)
        await _seed(repository, candles, symbols, instruments)
        return repository

    async def dispose(self) -> None:
        """Dispose every engine this helper created."""
        for repository in self._created:
            await repository.engine.dispose()


@pytest.fixture
async def databases(tmp_path: Path) -> AsyncIterator[_Databases]:
    """Provide the SQLite store factory and release its engines afterwards."""
    factory = _Databases(tmp_path)
    try:
        yield factory
    finally:
        await factory.dispose()


class _PostgresDatabases:
    """Create isolated PostgreSQL FX databases and drop every one at teardown."""

    def __init__(self, configured: str) -> None:
        """Remember the maintenance URL used to create and drop databases."""
        self._configured = configured
        self._names: list[str] = []
        self._created: list[SQLAlchemyRepository] = []

    async def _admin(self, statement: str) -> None:
        """Run one database-level statement outside a transaction.

        Args:
            statement: ``CREATE DATABASE`` or ``DROP DATABASE`` text.
        """
        maintenance = SQLAlchemyRepository(self._configured)
        try:
            async with maintenance.engine.connect() as connection:
                admin = await connection.execution_options(isolation_level="AUTOCOMMIT")
                await admin.exec_driver_sql(statement)
        finally:
            await maintenance.engine.dispose()

    async def create(self, candles: Sequence[Candle]) -> SQLAlchemyRepository:
        """Create one database holding the standard planes and these candles.

        Args:
            candles: Candles to store, inserted in the given order.

        Returns:
            A repository over the populated PostgreSQL database.
        """
        name = f"pnl_exact_fx_{uuid4().hex}"
        await self._admin(f'CREATE DATABASE "{name}"')
        self._names.append(name)
        target = make_url(self._configured).set(database=name)
        repository = SQLAlchemyRepository(target.render_as_string(hide_password=False))
        self._created.append(repository)
        async with repository.engine.begin() as connection:
            for table in (Symbol.__table__, Instrument.__table__, Candle.__table__):
                await connection.run_sync(table.create)
        await _seed(repository, candles, (), ())
        return repository

    async def dispose(self) -> None:
        """Dispose every engine and drop every database this helper created."""
        for repository in self._created:
            await repository.engine.dispose()
        for name in self._names:
            await self._admin(f'DROP DATABASE IF EXISTS "{name}"')


@pytest.fixture
async def postgres_databases() -> AsyncIterator[_PostgresDatabases]:
    """Provide isolated PostgreSQL stores when an opt-in server URL is configured."""
    configured = os.environ.get("PNL_TEST_POSTGRES_URL")
    if configured is None:
        pytest.skip("requires isolated PNL_TEST_POSTGRES_URL")
    assert make_url(configured).get_backend_name() == "postgresql"
    factory = _PostgresDatabases(configured)
    try:
        yield factory
    finally:
        await factory.dispose()


@dataclass(frozen=True, slots=True)
class _Scenario:
    """Canned non-FX evidence for one wallet replay."""

    executions: tuple[PnlTimelineOpeningExecutionRow, ...]
    refs: tuple[InstrumentSymbolRefRow, ...]
    candles: tuple[PnlTimelineCandleRow, ...]


def _scenario(eur_fills: Iterable[int], pln_fills: Iterable[int], window: range) -> _Scenario:
    """Hold BTC-EUR and BTC-PLN positions bought at foreign-currency fill minutes.

    Args:
        eur_fills: Minutes of EUR-priced BTC-EUR buys with EUR fees.
        pln_fills: Minutes of PLN-priced BTC-PLN buys with PLN fees.
        window: Chart minutes needing a mark for each instrument.

    Returns:
        Executions numbered in time order, symbol references and mark candles.
    """
    fills = sorted(
        [(minute, _I1, 100.0, "EUR") for minute in eur_fills]
        + [(minute, _I2, 400.0, "PLN") for minute in pln_fills]
    )
    executions = tuple(
        _exec_row(instrument, scope, minute, "buy", 1.0, price, price / 1000, currency)
        for scope, (minute, instrument, price, currency) in enumerate(fills, start=1)
    )
    refs = (
        _ref(_I1, "BTC-EUR", "EUR", valid_from=_m(_ORIGIN)),
        _ref(_I2, "BTC-PLN", "PLN", valid_from=_m(_ORIGIN)),
    )
    candles = tuple(
        _candle(_m(minute - 1), close, instrument)
        for minute in window
        for instrument, close in ((_I1, 110.0), (_I2, 450.0))
    )
    return _Scenario(executions, refs, candles)


class _RealFxRepo(FakeRepo):
    """Serve every FX read from real SQLite while recording each phase's rows."""

    def __init__(
        self,
        database: SQLAlchemyRepository,
        scenario: _Scenario,
        correction: Callable[[], Awaitable[None]] | None = None,
        *,
        correct_after: int = 1,
    ) -> None:
        """Wrap the canned scenario and delegate FX reads to the real store.

        Args:
            database: Real SQLite repository answering every FX read.
            scenario: Executions, references and mark candles of the wallet.
            correction: Optional write committed right after one range load.
            correct_after: Which range load, counted from one, the write follows.
        """
        super().__init__(
            executions=scenario.executions,
            refs=scenario.refs,
            candles=scenario.candles,
            fx_repository=database,
        )
        self.database = database
        self.exact_rows: list[int] = []
        self.range_rows: list[int] = []
        self._correction = correction
        self._correct_after = correct_after

    async def get_pnl_fx_rate_candles_at(
        self,
        planes: Sequence[PnlFxRatePlane],
        open_ats: Sequence[datetime],
        as_of: datetime,
    ) -> list[PnlFxRateRow]:
        """Record and run one exact-minute candle load against real SQLite."""
        self.fx_exact_minute_calls.append((list(planes), list(open_ats)))
        rows = await self.database.get_pnl_fx_rate_candles_at(planes, open_ats, as_of)
        self.exact_rows.append(len(rows))
        return rows

    async def get_pnl_fx_rate_candles(
        self,
        planes: Sequence[PnlFxRatePlane],
        start: datetime,
        end: datetime,
        as_of: datetime,
        *,
        open_intervals: Sequence[tuple[datetime, datetime]] = (),
    ) -> list[PnlFxRateRow]:
        """Record one real range load and publish the optional correction after it."""
        rows = await super().get_pnl_fx_rate_candles(
            planes, start, end, as_of, open_intervals=open_intervals
        )
        self.range_rows.append(len(rows))
        if self._correction is not None and len(self.range_rows) == self._correct_after:
            await self._correction()
        return rows


@dataclass(frozen=True, slots=True)
class _Run:
    """One pointwise replay's financial output, shadow evidence and read record."""

    result: PnlWalletSeriesResult
    evaluations: list[FxShadowEvaluation]
    repo: _RealFxRepo


async def _replay(repo: _RealFxRepo, window: range, *, shadow: bool) -> _Run:
    """Replay one chunk as the snapshotter does, optionally collecting FX shadow.

    Args:
        repo: Repository double serving FX from real SQLite.
        window: Inclusive chart minutes of the chunk.
        shadow: Whether FX shadow pinning is active, as in production.

    Returns:
        The series, its collected shadow evaluations and the recording repository.
    """
    context = (
        FxShadowPinContext(calculation_version=PNL_SAMPLE_CALC_VERSION, evaluations=[])
        if shadow
        else None
    )
    with activate_fx_shadow_context(context):
        result = await build_wallet_pnl_series(
            cast(Repository, repo),
            _W1,
            "live",
            _m(window[0]),
            _m(window[-1]),
            "1m",
            _m(window[-1] + 2),
            policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
            options=PnlSeriesReplayOptions(pointwise_exact_fx=True, baseline_watermarks={}),
        )
    return _Run(result, [] if context is None else list(context.evaluations), repo)


def _force_dense_event_reads(monkeypatch: pytest.MonkeyPatch) -> None:
    """Route the pointwise consumer through today's interval or dense event read."""
    original = pnl_timeline_service._read_fx_candidate_evidence

    async def dense(
        repo: Repository,
        request: _FxRequest,
        minute_sets: _FxMinuteSets,
        *,
        has_general_consumers: bool,
        exact_event_minutes: bool,
    ) -> tuple[list[PnlFxRateRow], dict[FxPairKey, set[PnlFxRatePlane]]]:
        """Ignore the exact flag and run the original event-phase read."""
        assert exact_event_minutes
        return await original(
            repo,
            request,
            minute_sets,
            has_general_consumers=has_general_consumers,
            exact_event_minutes=False,
        )

    monkeypatch.setattr(pnl_timeline_service, "_read_fx_candidate_evidence", dense)


async def _exact_and_dense(
    make_repo: Callable[[], Awaitable[_RealFxRepo]],
    window: range,
    monkeypatch: pytest.MonkeyPatch,
    *,
    shadow: bool,
) -> tuple[_Run, _Run]:
    """Replay the same evidence with the exact event read and with the dense oracle.

    Args:
        make_repo: Factory returning a fresh recording repository per replay.
        window: Inclusive chart minutes of the chunk.
        monkeypatch: Scoped patcher used only around the dense oracle replay.
        shadow: Whether FX shadow pinning is active.

    Returns:
        The exact replay followed by the dense oracle replay.
    """
    exact = await _replay(await make_repo(), window, shadow=shadow)
    with monkeypatch.context() as patch:
        _force_dense_event_reads(patch)
        dense = await _replay(await make_repo(), window, shadow=shadow)
    return exact, dense


def _without_generated(row: Mapping[str, object]) -> dict[str, object]:
    """Drop the generated UUID columns of one artifact row."""
    return {key: value for key, value in row.items() if key not in _GENERATED_KEYS}


def _artifacts(
    evaluations: Sequence[FxShadowEvaluation],
) -> list[tuple[dict[str, object], list[dict[str, object]]]]:
    """Build every shadow artifact with its generated identities normalised."""
    built: list[tuple[dict[str, object], list[dict[str, object]]]] = []
    for evaluation in evaluations:
        election, proofs = _build_artifact(
            evaluation, evaluation.requested_knowledge_at, PNL_SAMPLE_CALC_VERSION
        )
        built.append((_without_generated(election), [_without_generated(row) for row in proofs]))
    return built


def _without_candle_uuid(row: PnlFxRateRow) -> PnlFxRateRow:
    """Blank the candle UUID a separately seeded store generates at insert."""
    return {**row, "candle_public_id": ""}


def _row_identity(row: PnlFxRateRow) -> tuple[datetime, str, str, str, int]:
    """Order rows by the repository sort key, then by durable candle identity."""
    return (row["open_at"], row["base"], row["quote"], row["exchange"], row["candle_id"])


def _comparable(
    evaluations: Sequence[FxShadowEvaluation], *, fresh_stores: bool, tie_insensitive: bool
) -> list[FxShadowEvaluation]:
    """Normalise what may legitimately differ between two reads of one store.

    Args:
        evaluations: Shadow evaluations collected by one replay.
        fresh_stores: Whether replays seeded separate stores, so candle UUIDs differ.
        tie_insensitive: Whether rows tying on the repository sort key may arrive
            in a different order, as PostgreSQL leaves them unordered.

    Returns:
        Evaluations with only those differences removed.
    """
    comparable: list[FxShadowEvaluation] = []
    for evaluation in evaluations:
        rows = evaluation.rows
        if fresh_stores:
            rows = tuple(_without_candle_uuid(row) for row in rows)
        if tie_insensitive:
            rows = tuple(sorted(rows, key=_row_identity))
        comparable.append(replace(evaluation, rows=rows))
    return comparable


def _assert_parity(
    exact: _Run, dense: _Run, *, fresh_stores: bool = False, tie_insensitive: bool = False
) -> None:
    """Assert identical series output, shadow evaluations and built artifacts.

    Args:
        exact: Replay using the exact event read.
        dense: Replay using the original interval or dense event read.
        fresh_stores: Whether each replay seeded its own store, so candle UUIDs differ.
        tie_insensitive: Whether evidence rows tying on the sort key may be reordered.
    """
    assert exact.result.points == dense.result.points
    assert exact.result.replay_metadata == dense.result.replay_metadata
    assert exact.result.replay_metadata is not None
    assert exact.result.rate_sources == dense.result.rate_sources
    assert exact.result == dense.result
    exact_evaluations = _comparable(
        exact.evaluations, fresh_stores=fresh_stores, tie_insensitive=tie_insensitive
    )
    dense_evaluations = _comparable(
        dense.evaluations, fresh_stores=fresh_stores, tie_insensitive=tie_insensitive
    )
    assert exact_evaluations == dense_evaluations
    assert _artifacts(exact_evaluations) == _artifacts(dense_evaluations)
    assert exact.repo.fx_exact_minute_calls
    assert dense.repo.fx_exact_minute_calls == []
    assert len(exact.repo.range_rows) == 1
    assert len(dense.repo.range_rows) == 2


@contextmanager
def _statements(database: SQLAlchemyRepository) -> Iterator[list[str]]:
    """Record every SQL statement the store executes inside the block."""
    statements: list[str] = []

    def capture(*args: object) -> None:
        """Append the executed statement text without altering the result."""
        statement = args[2]
        assert isinstance(statement, str)
        statements.append(statement)

    event.listen(database.engine.sync_engine, "before_cursor_execute", capture)
    try:
        yield statements
    finally:
        event.remove(database.engine.sync_engine, "before_cursor_execute", capture)


@pytest.mark.parametrize("shadow", [False, True])
@pytest.mark.parametrize("fx_layout", ["every_minute", "near_requirements"])
@pytest.mark.parametrize("event_layout", ["sparse", "clustered", "fragmented"])
async def test_exact_event_read_matches_dense_series_through_sqlite(
    databases: _Databases,
    monkeypatch: pytest.MonkeyPatch,
    event_layout: str,
    fx_layout: str,
    shadow: bool,
) -> None:
    """The exact event read reproduces today's pointwise series byte for byte.

    Given: Four EUR/USD and two PLN/USD planes with gaps, equal and conflicting
    duplicates, and sparse, clustered or more than sixteen disjoint event windows.
    When: One pointwise chunk replays with the exact event read and the dense oracle.
    Then: Points, replay metadata, rate sources and shadow artifacts are identical.
    """
    eur_fills, pln_fills = _EVENT_LAYOUTS[event_layout]
    window = range(0, 10)
    required = [*eur_fills, *pln_fills, *window]
    if fx_layout == "every_minute":
        fx_minutes: Iterable[int] = range(min(required) - 25, window[-1] + 2)
    else:
        fx_minutes = {minute - offset for minute in required for offset in range(3)}
    database = await databases.create(_standard_candles(fx_minutes))
    scenario = _scenario(eur_fills, pln_fills, window)

    async def make_repo() -> _RealFxRepo:
        """Serve the shared read-only store through a fresh recorder."""
        return _RealFxRepo(database, scenario)

    exact, dense = await _exact_and_dense(make_repo, window, monkeypatch, shadow=shadow)

    _assert_parity(exact, dense)
    assert any(point.valuation_status == "complete" for point in exact.result.points)
    assert {(source.base_currency, source.quote_currency) for source in exact.result.rate_sources}
    assert sum(exact.repo.exact_rows) <= dense.repo.range_rows[1]
    assert bool(exact.evaluations) is shadow


@pytest.mark.parametrize("bulk", [64, 65])
async def test_shadow_duplicates_and_row_cap_survive_the_exact_event_read(
    databases: _Databases, monkeypatch: pytest.MonkeyPatch, bulk: int
) -> None:
    """Shadow evidence keeps duplicates, conflicts and the 64 plus 1 row cap.

    Given: An event minute carrying 64 or 65 EUR/USD rows with equal and
    conflicting duplicates, and chart minutes with a duplicate Kraken listing.
    When: The chunk replays under FX shadow with the exact read and the dense oracle.
    Then: Evaluations and artifacts match; 64 rows are pinned and 65 are skipped.
    """
    event_minute = -40
    exchanges = ("bitstamp", "kraken", "walutomat")
    bulk_symbols = [
        _symbol(
            f"sym-bulk-{index:02d}", f"EURUSD-B{index:02d}", "EUR", "USD", timestamp=_m(_ORIGIN)
        )
        for index in range(bulk)
    ]
    bulk_instruments = [
        _instrument(
            f"ins-bulk-{index:02d}",
            f"sym-bulk-{index:02d}",
            exchanges[index % 3],
            timestamp=_m(_ORIGIN),
        )
        for index in range(bulk)
    ]
    bulk_candles = [
        _fx_candle(f"ins-bulk-{index:02d}", _m(event_minute - 1), 1.3 if index == 4 else 1.2)
        for index in range(bulk)
    ]
    window = range(0, 5)
    database = await databases.create(
        [*_standard_candles(range(-25, 7)), *bulk_candles], bulk_symbols, bulk_instruments
    )
    scenario = _scenario((event_minute,), (), window)

    async def make_repo() -> _RealFxRepo:
        """Serve the shared read-only store through a fresh recorder."""
        return _RealFxRepo(database, scenario)

    exact, dense = await _exact_and_dense(make_repo, window, monkeypatch, shadow=True)

    _assert_parity(exact, dense)
    by_minute = {
        evaluation.required_minutes: evaluation
        for evaluation in exact.evaluations
        if evaluation.pair == _EUR_USD
    }
    pinned = by_minute.get(frozenset({_m(event_minute)}))
    if bulk == 64:
        assert pinned is not None
        assert len(pinned.rows) == 64
        assert pinned.selected_plane == ("EUR", "USD", "bitstamp")
    else:
        assert pinned is None

    def kraken_closes(minute: int) -> list[float]:
        """Return the direct Kraken EUR/USD closes in one minute's evidence."""
        return [
            row["close"]
            for row in by_minute[frozenset({_m(minute)})].rows
            if (row["base"], row["exchange"]) == ("EUR", "kraken")
        ]

    equal = kraken_closes(1)
    conflicting = kraken_closes(2)
    assert len(equal) == 2
    assert len(set(equal)) == 1
    assert len(conflicting) == 2
    assert len(set(conflicting)) == 2
    assert by_minute[frozenset({_m(2)})].selected_plane != ("EUR", "USD", "kraken")


async def _publish_backdated_correction(database: SQLAlchemyRepository) -> None:
    """Replace the close of the mark-only minute -50 with a backdated version."""
    async with database.session() as session:
        await session.execute(
            update(Candle)
            .where(
                Candle.instrument_public_id == "ins-eurusd-walutomat",
                Candle.open_at == _m(-51),
            )
            .values(known_to=_m(-30))
        )
        replacement = _fx_candle("ins-eurusd-walutomat", _m(-51), 1.25)
        replacement.timestamp = _m(-30)
        session.add(replacement)
        await session.commit()


@pytest.mark.parametrize("shadow", [False, True])
async def test_event_read_sees_inside_mark_conflicts_and_skips_outside_marks(
    databases: _Databases, monkeypatch: pytest.MonkeyPatch, shadow: bool
) -> None:
    """Mark-only minutes inside the event bounds stay in the event read.

    Given: Fills at -100 and -40, a chart from -60 to 0, and a backdated correction
    at mark-only minute -50 committed between the mark and the event reads.
    When: The chunk replays with the exact event read and with the dense oracle.
    Then: Both withhold minute -50, and the exact read fetches marks only up to -39.
    """
    window = range(-60, 1)
    scenario = _scenario((-100, -40), (), window)

    async def make_repo() -> _RealFxRepo:
        """Seed a fresh store so each replay observes its own late correction."""
        database = await databases.create(
            [_fx_candle("ins-eurusd-walutomat", _m(minute - 1), 1.15) for minute in range(-125, 2)]
        )

        async def correct() -> None:
            """Commit the backdated correction into this replay's store."""
            await _publish_backdated_correction(database)

        return _RealFxRepo(database, scenario, correct)

    exact, dense = await _exact_and_dense(make_repo, window, monkeypatch, shadow=shadow)

    _assert_parity(exact, dense, fresh_stores=True)
    points = {point.point_time: point for point in exact.result.points}
    assert points[_m(-50)].valuation_status == "incomplete"
    assert [reason.reason for reason in points[_m(-50)].incompleteness_reasons] == [
        "fx_conversion_unproven"
    ]
    assert points[_m(-49)].valuation_status == "complete"
    expected_opens = [_m(-101), *(_m(minute) for minute in range(-61, -39))]
    assert exact.repo.fx_exact_candidate_calls == [expected_opens]
    assert exact.repo.fx_exact_minute_calls == [([_WALUTOMAT_EUR_USD], expected_opens)]
    assert max(expected_opens) == _m(-40)


async def _publish_late_required_row(database: SQLAlchemyRepository) -> None:
    """Commit a Bitstamp close at the fill minute, stamped before the read horizon."""
    async with database.session() as session:
        late = _fx_candle("ins-eurusd-bitstamp", _m(-101), 1.3)
        late.timestamp = _m(1)
        session.add(late)
        await session.commit()


@pytest.mark.parametrize("shadow", [False, True])
async def test_mid_read_commit_is_read_as_if_it_landed_after_the_load(
    databases: _Databases, monkeypatch: pytest.MonkeyPatch, shadow: bool
) -> None:
    """A required row committed during the read is seen as a commit after the load.

    Given: A fill at -100 with Walutomat closes at every minute and one Bitstamp
    close at the unconsumed minute -110, so only dense discovery finds Bitstamp.
    When: A Bitstamp close at the fill minute, stamped inside the horizon, commits
    between event discovery and the event load.
    Then: The dense load reads it and the exact load does not, and the exact result
    equals the dense result for the same commit landing after the event load.
    """
    window = range(-5, 1)
    scenario = _scenario((-100,), (), window)

    def recorder(correct_after: int) -> Callable[[], Awaitable[_RealFxRepo]]:
        """Seed a fresh store whose late commit follows the given range load."""

        async def make_repo() -> _RealFxRepo:
            """Seed the store and arm its late required-minute commit."""
            database = await databases.create(
                [
                    *(
                        _fx_candle("ins-eurusd-walutomat", _m(minute - 1), 1.15)
                        for minute in range(-125, 2)
                    ),
                    _fx_candle("ins-eurusd-bitstamp", _m(-111), 1.1),
                ]
            )

            async def publish() -> None:
                """Commit the late required row into this replay's store."""
                await _publish_late_required_row(database)

            return _RealFxRepo(database, scenario, publish, correct_after=correct_after)

        return make_repo

    exact = await _replay(await recorder(1)(), window, shadow=shadow)
    with monkeypatch.context() as patch:
        _force_dense_event_reads(patch)
        dense_mid_read = await _replay(await recorder(1)(), window, shadow=shadow)
        dense_after_load = await _replay(await recorder(2)(), window, shadow=shadow)

    _assert_parity(exact, dense_after_load, fresh_stores=True)
    bitstamp: PnlFxRatePlane = ("EUR", "USD", "bitstamp")
    assert all(bitstamp not in planes for planes, _ in exact.repo.fx_exact_minute_calls)
    assert bitstamp in dense_mid_read.repo.fx_plane_calls[1]
    assert dense_mid_read.repo.range_rows[1] == dense_after_load.repo.range_rows[1] + 1
    if shadow:
        fill = frozenset({_m(-100)})

        def fill_candidates(run: _Run) -> frozenset[PnlFxRatePlane]:
            """Return the candidate planes the shared fill-minute decision saw."""
            return next(
                evaluation.candidate_planes
                for evaluation in run.evaluations
                if evaluation.pair == _EUR_USD and evaluation.required_minutes == fill
            )

        assert bitstamp not in fill_candidates(exact)
        assert bitstamp in fill_candidates(dense_mid_read)


async def test_event_rows_stay_flat_as_the_chunk_moves_forward(
    databases: _Databases, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exact event rows no longer grow with the distance to an old fill.

    Given: Every FX minute stored, an old fill at -300 and a fill at each chunk start.
    When: Shadow-pinned chunks at minutes 0, 120 and 240 replay both ways.
    Then: Exact event rows stay at three while dense event rows grow with position.
    """
    candles = [
        _fx_candle("ins-eurusd-walutomat", _m(minute - 1), 1.15) for minute in range(-325, 251)
    ]
    database = await databases.create(candles)
    exact_rows: list[list[int]] = []
    dense_rows: list[int] = []
    for position in (0, 120, 240):
        window = range(position, position + 5)
        scenario = _scenario((-300, position), (), window)

        async def make_repo(scenario: _Scenario = scenario) -> _RealFxRepo:
            """Serve the shared read-only store through a fresh recorder."""
            return _RealFxRepo(database, scenario)

        exact, dense = await _exact_and_dense(make_repo, window, monkeypatch, shadow=True)
        _assert_parity(exact, dense)
        exact_rows.append(exact.repo.exact_rows)
        dense_rows.append(dense.repo.range_rows[1])
    assert exact_rows == [[3], [3], [3]]
    assert dense_rows == [322, 442, 562]


async def test_empty_exact_selections_run_no_query(databases: _Databases) -> None:
    """An empty exact selection is empty, never the dense range.

    Given: A real store holding EUR/USD candles and a recording repository double.
    When: Exact discovery and loading receive no minutes, no pairs or no planes.
    Then: Each returns an empty list without executing a statement or repository call.
    """
    database = await databases.create(_standard_candles(range(-5, 5)))
    horizon = _m(10)
    with _statements(database) as statements:
        assert await database.get_pnl_fx_rate_exchanges_at([("EUR", "USD")], [], horizon) == []
        assert await database.get_pnl_fx_rate_exchanges_at([], [_m(0)], horizon) == []
        assert await database.get_pnl_fx_rate_candles_at([_WALUTOMAT_EUR_USD], [], horizon) == []
    assert statements == []
    fake = FakeRepo(fx_repository=database)
    repo = cast(Repository, fake)
    requirements = {_EUR_USD: {_m(1)}}
    empty = _FxEventSelection(opens=())
    assert await _discover_event_fx_candidates(repo, requirements, horizon, empty) == []
    assert (
        await _discover_event_fx_candidates(repo, {}, horizon, _FxEventSelection(opens=(_m(0),)))
        == []
    )
    planes = {_EUR_USD: {_WALUTOMAT_EUR_USD}}
    assert await _load_event_fx_rows(repo, requirements, planes, horizon, empty) == []
    assert (
        await _load_event_fx_rows(
            repo, requirements, {}, horizon, _FxEventSelection(opens=(_m(0),))
        )
        == []
    )
    assert fake.fx_candidate_pair_calls == []
    assert fake.fx_exact_minute_calls == []
    assert fake.fx_plane_calls == []


def test_interval_and_exact_selectors_cannot_be_combined() -> None:
    """Supplying both selectors is ambiguous and rejected.

    Given: One interval selector and one exact selector, empty or not.
    When: An event selection is constructed from both.
    Then: Construction raises instead of silently preferring either read.
    """
    interval = ((_m(-21), _m(0)),)
    with pytest.raises(ValueError, match="intervals or exact opens"):
        _FxEventSelection(intervals=interval, opens=(_m(-1),))
    with pytest.raises(ValueError, match="intervals or exact opens"):
        _FxEventSelection(intervals=interval, opens=())


def test_exact_selection_keeps_the_original_event_bounds() -> None:
    """Exact opens are every requirement minute inside the unchanged event bounds.

    Given: Events at -100 and -40 and marks on both sides of each bound on two pairs.
    When: The exact and the interval event selections are planned.
    Then: Exact opens span -121 to -40 after the shift; no events select nothing.
    """
    events = {_EUR_USD: {_m(-100), _m(-40)}}
    requirements = {
        _EUR_USD: {_m(-121), _m(-120), _m(-100), _m(-40), _m(-39), _m(-38)},
        _PLN_USD: {_m(-60), _m(5)},
    }
    exact = _event_fx_selection(events, requirements, exact_minutes=True)
    assert exact == _FxEventSelection(
        opens=(_m(-121), _m(-101), _m(-61), _m(-41), _m(-40)),
    )
    assert _event_fx_selection({}, requirements, exact_minutes=True) == _FxEventSelection(opens=())
    legacy = _event_fx_selection(events, requirements, exact_minutes=False)
    assert legacy.opens is None


async def test_exact_reads_match_dense_rows_order_and_duplicates(databases: _Databases) -> None:
    """The exact repository reads equal the dense reads restricted to their minutes.

    Given: Shuffled inserts of every plane with gaps and equal or conflicting duplicates.
    When: Exact discovery and loading run beside the range, interval and per-minute reads.
    Then: Rows match in multiplicity and order, and discovered planes match too.
    """
    candles = _standard_candles(range(-30, 11))
    random.Random(7).shuffle(candles)
    database = await databases.create(candles)
    horizon = _m(12)
    planes = sorted(
        {
            (base, quote, exchange)
            for _, symbol, exchange in _INSTRUMENTS
            for public_id, _, base, quote in _SYMBOLS
            if public_id == symbol
        }
    )
    pairs = [("EUR", "USD"), ("USD", "EUR"), ("PLN", "USD"), ("USD", "PLN")]
    minutes = (-20, -7, 1, 2, 5, 6, 9)
    opens = [_m(minute - 1) for minute in minutes]
    dense = await database.get_pnl_fx_rate_candles(planes, _m(-31), _m(10), horizon)
    intervals = await database.get_pnl_fx_rate_candles(
        planes, _m(-31), _m(10), horizon, open_intervals=((_m(-21), _m(-8)), (_m(0), _m(8)))
    )
    exact = await database.get_pnl_fx_rate_candles_at(planes, list(reversed(opens)), horizon)
    wanted = set(opens)
    assert exact == [row for row in dense if row["open_at"] in wanted]
    assert exact == [row for row in intervals if row["open_at"] in wanted]
    duplicates = [row for row in exact if row["exchange"] == "kraken" and row["base"] == "EUR"]
    assert len(duplicates) > len({row["open_at"] for row in duplicates})
    per_minute: set[PnlFxRatePlane] = set()
    for open_at in opens:
        per_minute.update(
            await database.get_pnl_fx_rate_exchanges(pairs, open_at, open_at, horizon)
        )
    discovered = await database.get_pnl_fx_rate_exchanges_at(pairs, opens, horizon)
    assert discovered == sorted(per_minute)
    assert discovered == sorted({(row["base"], row["quote"], row["exchange"]) for row in exact})


async def test_exact_discovery_batches_minutes_into_one_logical_read(
    databases: _Databases,
) -> None:
    """A long exact selection is split into bounded statements and merged once.

    Given: More exact opens than one bound-parameter batch carries.
    When: Exact discovery runs.
    Then: Three statements run and each plane appears once in lexical order.
    """
    database = await databases.create(_standard_candles(range(-5, 5)))
    opens = [_m(offset - 450) for offset in range(900)]
    with _statements(database) as statements:
        planes = await database.get_pnl_fx_rate_exchanges_at(
            [("EUR", "USD"), ("USD", "PLN")], opens, _m(10)
        )
    assert len(statements) == 3
    assert planes == [
        ("EUR", "USD", "bitstamp"),
        ("EUR", "USD", "kraken"),
        ("EUR", "USD", "walutomat"),
        ("USD", "PLN", "kraken"),
        ("USD", "PLN", "walutomat"),
    ]


async def test_basket_keeps_its_lookback_pin_and_missing_rate_reason(
    databases: _Databases,
) -> None:
    """The separate basket path still pins an earlier plane and refuses the rate.

    Given: No exact USD/PLN close at each valuation minute, and one candle 1 to 19
    minutes earlier on Walutomat.
    When: Basket fiat evidence is loaded for that single minute.
    Then: The Walutomat plane is pinned, the leg reports a missing fiat rate, and
    only the unchanged range reads run.
    """
    offsets = range(1, 20)
    candles = [_fx_candle("ins-usdpln-walutomat", _m(100 * offset - 1), 4.0) for offset in offsets]
    database = await databases.create(candles)
    plane = ("USD", "PLN", "walutomat")
    for offset in offsets:
        point_time = _m(100 * offset + offset)
        fake = FakeRepo(fx_repository=database)
        rates, venues, versions = await load_basket_fiat_evidence_by_minute(
            cast(Repository, fake), frozenset({"PLN"}), point_time, point_time, point_time
        )
        leg = value_currency(
            "walutomat",
            "PLN",
            400.0,
            point_time,
            ValuationEvidence(rates, {}, versions, {}, venues),
        )
        assert venues == {point_time: {_PLN_USD: plane}}
        assert leg.usd_value is None
        assert leg.reason == "missing_fiat_rate"
        assert fake.fx_exact_candidate_calls == []
        assert fake.fx_exact_minute_calls == []
        assert fake.fx_range_calls


async def test_postgres_exact_reads_match_dense_rows_and_planes(
    postgres_databases: _PostgresDatabases,
) -> None:
    """PostgreSQL exact reads return the dense rows and planes at their minutes.

    Given: Shuffled PostgreSQL inserts of every plane with gaps and duplicates.
    When: Exact discovery and loading run beside the range and per-minute reads.
    Then: Exact rows keep the sort key and equal the dense rows at those minutes
    as a multiset, and the discovered planes equal the per-minute union.
    """
    candles = _standard_candles(range(-30, 11))
    random.Random(11).shuffle(candles)
    database = await postgres_databases.create(candles)
    horizon = _m(12)
    planes = sorted(
        {
            (base, quote, exchange)
            for _, symbol, exchange in _INSTRUMENTS
            for public_id, _, base, quote in _SYMBOLS
            if public_id == symbol
        }
    )
    pairs = [("EUR", "USD"), ("USD", "EUR"), ("PLN", "USD"), ("USD", "PLN")]
    opens = [_m(minute - 1) for minute in (-20, -7, 1, 2, 5, 6, 9)]
    dense = await database.get_pnl_fx_rate_candles(planes, _m(-31), _m(10), horizon)
    exact = await database.get_pnl_fx_rate_candles_at(planes, list(reversed(opens)), horizon)
    wanted = set(opens)
    restricted = [row for row in dense if row["open_at"] in wanted]
    assert [_row_identity(row)[:4] for row in exact] == [
        _row_identity(row)[:4] for row in restricted
    ]
    assert sorted(exact, key=_row_identity) == sorted(restricted, key=_row_identity)
    duplicates = [row for row in exact if row["exchange"] == "kraken" and row["base"] == "EUR"]
    assert len(duplicates) > len({row["open_at"] for row in duplicates})
    per_minute: set[PnlFxRatePlane] = set()
    for open_at in opens:
        per_minute.update(
            await database.get_pnl_fx_rate_exchanges(pairs, open_at, open_at, horizon)
        )
    discovered = await database.get_pnl_fx_rate_exchanges_at(pairs, opens, horizon)
    assert discovered == sorted(per_minute)


@pytest.mark.parametrize("event_layout", ["sparse", "fragmented"])
async def test_postgres_exact_event_read_matches_dense_series(
    postgres_databases: _PostgresDatabases,
    monkeypatch: pytest.MonkeyPatch,
    event_layout: str,
) -> None:
    """PostgreSQL pointwise series and shadow artifacts match the dense oracle.

    Given: Every FX minute stored in PostgreSQL with equal and conflicting duplicates.
    When: One shadow-pinned chunk replays with the exact read and the dense oracle.
    Then: The series and the built artifacts are identical as returned, and the
    evaluations are identical once rows tying on the sort key are ordered.
    """
    eur_fills, pln_fills = _EVENT_LAYOUTS[event_layout]
    window = range(0, 10)
    required = [*eur_fills, *pln_fills, *window]
    database = await postgres_databases.create(
        _standard_candles(range(min(required) - 25, window[-1] + 2))
    )
    scenario = _scenario(eur_fills, pln_fills, window)

    async def make_repo() -> _RealFxRepo:
        """Serve the shared read-only store through a fresh recorder."""
        return _RealFxRepo(database, scenario)

    exact, dense = await _exact_and_dense(make_repo, window, monkeypatch, shadow=True)

    _assert_parity(exact, dense, tie_insensitive=True)
    assert exact.evaluations
    assert _artifacts(exact.evaluations) == _artifacts(dense.evaluations)
    assert sum(exact.repo.exact_rows) < dense.repo.range_rows[1]
