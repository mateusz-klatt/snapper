"""Tests for the temporal venue-order-minimum repository read.

The suite pins author-time identity and capability joins, half-open window
clipping, as-of knowledge, base/quote projection, explicit missing-spec rows,
identity collisions and non-null minimum instability. Rows remain
undeduplicated so the pure layer, never SQL row order, decides P3-P7.
"""

from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any
from typing import cast

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Row

from snapper.application.portfolio.basket_realizability import VenueOrderMinimumVersion
from snapper.application.portfolio.basket_realizability import resolve_venue_order_minimums
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Instrument
from snapper.data.models import InstrumentSpec
from snapper.data.models import Symbol
from snapper.data.models import SymbolExchangeCapability
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import _pnl_order_minimum_capability_segments
from snapper.data.repository import _pnl_order_minimum_current_symbol_intervals
from snapper.data.repository import _pnl_order_minimum_missing_symbol_rows
from snapper.data.repository import _PnlOrderMinimumWindow

_WINDOW_START = datetime(2026, 7, 20, 10, 0, tzinfo=UTC)
_WINDOW_END = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)
_AS_OF = datetime(2026, 7, 20, 13, 0, tzinfo=UTC)
_EARLY = _WINDOW_START - timedelta(hours=1)
_LATE = _AS_OF + timedelta(hours=1)
_SESSION = "00000000-0000-7000-8000-000000000902"


@dataclass(frozen=True)
class _Ver:
    """One SCD2 author-time interval."""

    timestamp: datetime = _EARLY
    known_to: datetime = KNOWN_TO_MAX


@dataclass(frozen=True)
class _Pair:
    """Compact seed description for one instrument identity."""

    name: str
    base: str
    quote: str | None
    exchange: str = "kraken"
    asset_type: str = "crypto"
    minimum: float | None = 3.0
    status: str | None = "active"
    instrument_kind: str | None = "spot"
    quantity_unit: str | None = "base_asset"
    certified: bool = True
    can_trade: bool = True
    with_spec: bool = True
    ver: _Ver = _Ver()


def _symbol(pair: _Pair) -> Symbol:
    """Build one temporal symbol version."""
    return Symbol(
        public_id=f"sym-{pair.name}",
        native_symbol=pair.name,
        base=pair.base,
        quote=pair.quote,
        asset_type=pair.asset_type,
        created_at=pair.ver.timestamp,
        timestamp=pair.ver.timestamp,
        known_to=pair.ver.known_to,
        session_id=_SESSION,
        sequence_id=1,
    )


def _instrument(pair: _Pair) -> Instrument:
    """Build one temporal instrument version."""
    return Instrument(
        public_id=f"ins-{pair.name}",
        symbol_public_id=f"sym-{pair.name}",
        exchange=pair.exchange,
        requires_ai_review=False,
        timestamp=pair.ver.timestamp,
        known_to=pair.ver.known_to,
        session_id=_SESSION,
        sequence_id=1,
    )


def _spec(pair: _Pair) -> InstrumentSpec:
    """Build one temporal instrument-spec version."""
    source = f"{pair.exchange}:venue_metadata" if pair.certified else None
    return InstrumentSpec(
        public_id=f"spec-{pair.name}",
        instrument_public_id=f"ins-{pair.name}",
        min_order_size=pair.minimum,
        status=pair.status,
        instrument_kind=pair.instrument_kind,
        quantity_unit=pair.quantity_unit,
        spec_source=source,
        spec_version="venue-v1" if pair.certified else None,
        spec_observed_at=pair.ver.timestamp if pair.certified else None,
        timestamp=pair.ver.timestamp,
        known_to=pair.ver.known_to,
        session_id=_SESSION,
        sequence_id=1,
    )


def _capability(pair: _Pair) -> SymbolExchangeCapability:
    """Build the author-time exchange capability for one pair."""
    return SymbolExchangeCapability(
        public_id=f"cap-{pair.name}",
        symbol_public_id=f"sym-{pair.name}",
        exchange=pair.exchange,
        can_market_data=True,
        can_trade=pair.can_trade,
        source="test",
        created_at=pair.ver.timestamp,
        timestamp=pair.ver.timestamp,
        known_to=pair.ver.known_to,
        session_id=_SESSION,
        sequence_id=1,
    )


async def _seed_pair(repository: SQLAlchemyRepository, pair: _Pair) -> None:
    """Persist one pair and its optional spec."""
    rows: list[object] = [_symbol(pair), _instrument(pair), _capability(pair)]
    if pair.with_spec:
        rows.append(_spec(pair))
    async with repository.session() as session:
        session.add_all(rows)
        await session.commit()


def test_interval_projectors_ignore_disjoint_evidence() -> None:
    """Defensive projectors do not manufacture overlap outside the read window.

    Given: Capability, Symbol, and Instrument evidence whose intervals are
        disjoint from their target interval,
    When: The three temporal projectors clip that evidence,
    Then: Capability becomes UNKNOWN and no identity or missing-Symbol marker
        is emitted.
    """
    window = _PnlOrderMinimumWindow(
        exchanges=("kraken",),
        currencies=frozenset({"BTC"}),
        start=_WINDOW_START,
        end=_WINDOW_END,
        as_of=_AS_OF,
    )
    capability_segments = _pnl_order_minimum_capability_segments(
        _WINDOW_START,
        _WINDOW_END,
        [(_EARLY, _WINDOW_START, True)],
    )
    identity_intervals = _pnl_order_minimum_current_symbol_intervals(
        cast(
            list[Row[Any]],
            [
                (
                    "kraken",
                    "BTC",
                    "USD",
                    "crypto",
                    "ins-BTC-USD",
                    "sym-BTC-USD",
                    "BTC-USD",
                    False,
                    _WINDOW_START,
                    _WINDOW_START + timedelta(minutes=30),
                    _WINDOW_START + timedelta(minutes=30),
                    _WINDOW_END,
                    1,
                )
            ],
        ),
        window,
    )
    missing_symbol_rows = _pnl_order_minimum_missing_symbol_rows(
        cast(
            list[Row[Any]],
            [
                (
                    "kraken",
                    "ins-BTC-USD",
                    "sym-BTC-USD",
                    _EARLY,
                    _WINDOW_START,
                    1,
                )
            ],
        ),
        window,
        {},
    )

    assert capability_segments == [(_WINDOW_START, _WINDOW_END, None)]
    assert identity_intervals == {}
    assert missing_symbol_rows == []


@pytest.fixture()
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create an isolated schema containing only the four read-side tables."""
    db_path = tmp_path / "pnl-order-minimum.db"
    schema_engine = create_engine(f"sqlite:///{db_path}")
    Symbol.__table__.create(schema_engine)
    Instrument.__table__.create(schema_engine)
    InstrumentSpec.__table__.create(schema_engine)
    SymbolExchangeCapability.__table__.create(schema_engine)
    schema_engine.dispose()
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    try:
        yield repo
    finally:
        await repo.engine.dispose()


class TestPnlSpotOrderMinimumRepository:
    """Pin every evidence and temporal guarantee of the S1 read."""

    async def test_empty_or_invalid_request_short_circuits(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """Empty dimensions and an empty window return without querying."""
        assert (
            await repository.get_pnl_spot_order_minimum_window(
                [],
                ["BTC"],
                _WINDOW_START,
                _WINDOW_END,
                _AS_OF,
            )
            == []
        )
        assert (
            await repository.get_pnl_spot_order_minimum_window(
                ["kraken"],
                [],
                _WINDOW_START,
                _WINDOW_END,
                _AS_OF,
            )
            == []
        )
        assert (
            await repository.get_pnl_spot_order_minimum_window(
                ["kraken"],
                ["BTC"],
                _WINDOW_END,
                _WINDOW_START,
                _AS_OF,
            )
            == []
        )

    async def test_single_certified_pair_projects_complete_base_row(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """One pair exposes every identity, provenance and threshold field."""
        await _seed_pair(repository, _Pair("TRUMP-USD", "TRUMP", "USD"))
        rows = await repository.get_pnl_spot_order_minimum_window(
            ["kraken"],
            ["TRUMP"],
            _WINDOW_START,
            _WINDOW_END,
            _AS_OF,
        )
        assert len(rows) == 1
        row = rows[0]
        assert row == {
            "exchange": "kraken",
            "currency": "TRUMP",
            "role": "base",
            "asset_type": "crypto",
            "instrument_public_id": "ins-TRUMP-USD",
            "symbol_public_id": "sym-TRUMP-USD",
            "native_symbol": "TRUMP-USD",
            "counter_currency": "USD",
            "instrument_kind": "spot",
            "quantity_unit": "base_asset",
            "status": "active",
            "spec_public_id": "spec-TRUMP-USD",
            "spec_source": "kraken:venue_metadata",
            "spec_version": "venue-v1",
            "spec_observed_at": _EARLY,
            "min_order_size": 3.0,
            "can_trade": True,
            "identity_conflicted": False,
            "minimum_unstable": False,
            "valid_from": _WINDOW_START,
            "valid_to": _WINDOW_END,
        }

    async def test_multiple_pairs_remain_undeduplicated_and_sorted(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """No SQL grouping erases the pair set used for min-of-minima."""
        await _seed_pair(repository, _Pair("TRUMP-USD", "TRUMP", "USD", minimum=3.0))
        await _seed_pair(repository, _Pair("TRUMP-EUR", "TRUMP", "EUR", minimum=1.0))
        rows = await repository.get_pnl_spot_order_minimum_window(
            ["kraken"],
            ["TRUMP"],
            _WINDOW_START,
            _WINDOW_END,
            _AS_OF,
        )
        assert [(row["native_symbol"], row["min_order_size"]) for row in rows] == [
            ("TRUMP-EUR", 1.0),
            ("TRUMP-USD", 3.0),
        ]

    async def test_late_spec_emits_pre_spec_unknown_interval(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """A tradable sibling stays visible before its first spec version."""
        seam = _WINDOW_START + timedelta(hours=1)
        await _seed_pair(
            repository,
            _Pair("GOOD-USD", "GOOD", "USD", minimum=2.0),
        )
        late_identity = _Pair(
            "LATE-EUR",
            "GOOD",
            "EUR",
            with_spec=False,
            ver=_Ver(timestamp=_EARLY),
        )
        late_spec = _Pair(
            "LATE-EUR",
            "GOOD",
            "EUR",
            minimum=3.0,
            ver=_Ver(timestamp=seam),
        )
        async with repository.session() as session:
            session.add_all(
                [
                    _symbol(late_identity),
                    _instrument(late_identity),
                    _capability(late_identity),
                    _spec(late_spec),
                ]
            )
            await session.commit()
        rows = await repository.get_pnl_spot_order_minimum_window(
            ["kraken"],
            ["GOOD"],
            _WINDOW_START,
            _WINDOW_END,
            _AS_OF,
        )
        late_rows = [row for row in rows if row["instrument_public_id"] == "ins-LATE-EUR"]
        assert [
            (row["spec_public_id"], row["valid_from"], row["valid_to"]) for row in late_rows
        ] == [
            (None, _WINDOW_START, seam),
            ("spec-LATE-EUR", seam, _WINDOW_END),
        ]
        versions = [VenueOrderMinimumVersion(**row) for row in rows]
        before = resolve_venue_order_minimums(
            versions,
            _WINDOW_START + timedelta(minutes=30),
        )[("kraken", "GOOD")]
        after = resolve_venue_order_minimums(
            versions,
            seam + timedelta(minutes=30),
        )[("kraken", "GOOD")]
        assert before.threshold is None
        assert before.refusal == "uncertified_tradable_pair"
        assert after.threshold == 2.0
        assert after.refusal is None

    async def test_no_spec_complement_covers_before_gaps_and_after(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """Missing-spec rows exactly fill every uncovered identity interval."""
        first_start = _WINDOW_START + timedelta(minutes=10)
        first_end = _WINDOW_START + timedelta(minutes=30)
        second_start = _WINDOW_START + timedelta(minutes=50)
        second_end = _WINDOW_START + timedelta(minutes=70)
        identity = _Pair(
            "GAPPED-USD",
            "GAPPED",
            "USD",
            with_spec=False,
            ver=_Ver(timestamp=_EARLY),
        )
        first = _Pair(
            "GAPPED-USD",
            "GAPPED",
            "USD",
            minimum=2.0,
            ver=_Ver(timestamp=first_start, known_to=first_end),
        )
        second = _Pair(
            "GAPPED-USD",
            "GAPPED",
            "USD",
            minimum=3.0,
            ver=_Ver(timestamp=second_start, known_to=second_end),
        )
        async with repository.session() as session:
            session.add_all(
                [
                    _symbol(identity),
                    _instrument(identity),
                    _capability(identity),
                    _spec(first),
                    _spec(second),
                ]
            )
            await session.commit()
        rows = await repository.get_pnl_spot_order_minimum_window(
            ["kraken"],
            ["GAPPED"],
            _WINDOW_START,
            _WINDOW_END,
            _AS_OF,
        )
        assert [
            (
                row["spec_public_id"],
                row["min_order_size"],
                row["valid_from"],
                row["valid_to"],
            )
            for row in rows
        ] == [
            (None, None, _WINDOW_START, first_start),
            ("spec-GAPPED-USD", 2.0, first_start, first_end),
            (None, None, first_end, second_start),
            ("spec-GAPPED-USD", 3.0, second_start, second_end),
            (None, None, second_end, _WINDOW_END),
        ]

    async def test_version_straddling_window_is_clipped(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """Returned validity is the overlap with the half-open request window."""
        pair = _Pair(
            "BTC-USD",
            "BTC",
            "USD",
            ver=_Ver(
                timestamp=_WINDOW_START + timedelta(minutes=15),
                known_to=_WINDOW_END - timedelta(minutes=15),
            ),
        )
        await _seed_pair(repository, pair)
        row = (
            await repository.get_pnl_spot_order_minimum_window(
                ["kraken"],
                ["BTC"],
                _WINDOW_START,
                _WINDOW_END,
                _AS_OF,
            )
        )[0]
        assert row["valid_from"] == _WINDOW_START + timedelta(minutes=15)
        assert row["valid_to"] == _WINDOW_END - timedelta(minutes=15)

    async def test_spec_keeps_its_author_time_identity_after_instrument_correction(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """A later instrument version cannot detach an older in-force spec."""
        correction = _WINDOW_START - timedelta(minutes=30)
        identity = _Pair(
            "AUTHOR-USD",
            "AUTHOR",
            "USD",
            with_spec=False,
            ver=_Ver(timestamp=_EARLY),
        )
        old_instrument = _Pair(
            "AUTHOR-USD",
            "AUTHOR",
            "USD",
            with_spec=False,
            ver=_Ver(timestamp=_EARLY, known_to=correction),
        )
        new_instrument = _Pair(
            "AUTHOR-USD",
            "AUTHOR",
            "USD",
            with_spec=False,
            ver=_Ver(timestamp=correction),
        )
        spec_pair = _Pair(
            "AUTHOR-USD",
            "AUTHOR",
            "USD",
            ver=_Ver(timestamp=_EARLY, known_to=_WINDOW_END),
        )
        async with repository.session() as session:
            session.add_all(
                [
                    _symbol(identity),
                    _instrument(old_instrument),
                    _instrument(new_instrument),
                    _capability(identity),
                    _spec(spec_pair),
                ]
            )
            await session.commit()
        rows = await repository.get_pnl_spot_order_minimum_window(
            ["kraken"],
            ["AUTHOR"],
            _WINDOW_START,
            _WINDOW_END,
            _AS_OF,
        )
        assert len(rows) == 1
        assert rows[0]["min_order_size"] == 3.0
        assert rows[0]["valid_from"] == _WINDOW_START
        assert rows[0]["valid_to"] == _WINDOW_END

    async def test_current_symbol_correction_surfaces_new_base_no_spec_sibling(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """A corrected base identity cannot hide an uncertified sibling."""
        seam = _WINDOW_START + timedelta(hours=1)
        await _seed_pair(repository, _Pair("NEW-USD", "NEW", "USD"))
        old_identity = _Pair(
            "RENAMED-EUR",
            "OLD",
            "EUR",
            with_spec=False,
            ver=_Ver(timestamp=_EARLY, known_to=seam),
        )
        new_identity = _Pair(
            "RENAMED-EUR",
            "NEW",
            "EUR",
            with_spec=False,
            ver=_Ver(timestamp=seam),
        )
        instrument = _Pair(
            "RENAMED-EUR",
            "OLD",
            "EUR",
            with_spec=False,
            ver=_Ver(timestamp=_EARLY),
        )
        async with repository.session() as session:
            session.add_all(
                [
                    _symbol(old_identity),
                    _symbol(new_identity),
                    _instrument(instrument),
                    _capability(instrument),
                ]
            )
            await session.commit()
        rows = await repository.get_pnl_spot_order_minimum_window(
            ["kraken"],
            ["NEW"],
            _WINDOW_START,
            _WINDOW_END,
            _AS_OF,
        )
        renamed = [row for row in rows if row["instrument_public_id"] == "ins-RENAMED-EUR"]
        assert len(renamed) == 1
        assert renamed[0]["currency"] == "NEW"
        assert renamed[0]["spec_public_id"] is None
        assert renamed[0]["valid_from"] == seam
        versions = [VenueOrderMinimumVersion(**row) for row in rows]
        before = resolve_venue_order_minimums(
            versions,
            seam - timedelta(minutes=1),
        )[("kraken", "NEW")]
        after = resolve_venue_order_minimums(
            versions,
            seam,
        )[("kraken", "NEW")]
        assert before.threshold == 3.0
        assert after.refusal == "uncertified_tradable_pair"

    async def test_missing_symbol_interval_emits_identity_unknown_marker(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """An instrument remains fail-closed while its Symbol fact is absent."""
        gap_start = _WINDOW_START + timedelta(minutes=40)
        gap_end = _WINDOW_START + timedelta(minutes=80)
        await _seed_pair(repository, _Pair("GOOD-USD", "GOOD", "USD"))
        before_gap = _Pair(
            "GAP-EUR",
            "GOOD",
            "EUR",
            with_spec=False,
            ver=_Ver(timestamp=_EARLY, known_to=gap_start),
        )
        after_gap = _Pair(
            "GAP-EUR",
            "GOOD",
            "EUR",
            with_spec=False,
            ver=_Ver(timestamp=gap_end, known_to=_WINDOW_END),
        )
        instrument = _Pair(
            "GAP-EUR",
            "GOOD",
            "EUR",
            with_spec=False,
            ver=_Ver(timestamp=_EARLY),
        )
        async with repository.session() as session:
            session.add_all(
                [
                    _symbol(before_gap),
                    _symbol(after_gap),
                    _instrument(instrument),
                    _capability(instrument),
                ]
            )
            await session.commit()
        rows = await repository.get_pnl_spot_order_minimum_window(
            ["kraken"],
            ["GOOD"],
            _WINDOW_START,
            _WINDOW_END,
            _AS_OF,
        )
        gap_rows = [
            row
            for row in rows
            if row["instrument_public_id"] == "ins-GAP-EUR"
            and row["valid_from"] == gap_start
            and row["valid_to"] == gap_end
        ]
        assert len(gap_rows) == 1
        assert gap_rows[0]["can_trade"] is None
        assert gap_rows[0]["identity_conflicted"] is True
        resolution = resolve_venue_order_minimums(
            [VenueOrderMinimumVersion(**row) for row in rows],
            gap_start + timedelta(minutes=1),
        )[("kraken", "GOOD")]
        assert resolution.refusal == "identity_ambiguous"

    async def test_overlapping_symbol_versions_project_identity_conflict(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """Competing in-force bases for one symbol are never silently elected."""
        overlap_start = _WINDOW_START + timedelta(hours=1)
        overlap_end = overlap_start + timedelta(minutes=20)
        old_identity = _Pair(
            "OVERLAP-USD",
            "ALPHA",
            "USD",
            with_spec=False,
            ver=_Ver(timestamp=_EARLY, known_to=overlap_end),
        )
        new_identity = _Pair(
            "OVERLAP-USD",
            "BETA",
            "USD",
            with_spec=False,
            ver=_Ver(timestamp=overlap_start, known_to=_WINDOW_END),
        )
        instrument = _Pair(
            "OVERLAP-USD",
            "ALPHA",
            "USD",
            with_spec=False,
            ver=_Ver(timestamp=_EARLY),
        )
        async with repository.session() as session:
            session.add_all(
                [
                    _symbol(old_identity),
                    _symbol(new_identity),
                    _instrument(instrument),
                    _capability(instrument),
                ]
            )
            await session.commit()
        rows = await repository.get_pnl_spot_order_minimum_window(
            ["kraken"],
            ["BETA"],
            _WINDOW_START,
            _WINDOW_END,
            _AS_OF,
        )
        overlapping = [row for row in rows if row["valid_from"] <= overlap_start < row["valid_to"]]
        assert overlapping
        assert all(row["identity_conflicted"] for row in overlapping)

    async def test_later_symbol_overlap_marks_author_time_spec(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """A long spec cannot hide a later overlap of its logical Symbol."""
        overlap_start = _WINDOW_START + timedelta(hours=1)
        overlap_seam = overlap_start + timedelta(minutes=10)
        overlap_end = overlap_start + timedelta(minutes=30)
        author_symbol = _Pair(
            "HISTOV-USD",
            "HISTOV",
            "USD",
            with_spec=False,
            ver=_Ver(
                timestamp=_EARLY,
                known_to=_WINDOW_START - timedelta(minutes=10),
            ),
        )
        later_left = _Pair(
            "HISTOV-USD",
            "HISTOV",
            "USD",
            with_spec=False,
            ver=_Ver(timestamp=overlap_start, known_to=overlap_end),
        )
        later_right = _Pair(
            "HISTOV-USD",
            "HISTOV",
            "USD",
            with_spec=False,
            ver=_Ver(timestamp=overlap_seam),
        )
        instrument = _Pair(
            "HISTOV-USD",
            "HISTOV",
            "USD",
            with_spec=False,
            ver=_Ver(timestamp=_EARLY),
        )
        async with repository.session() as session:
            session.add_all(
                [
                    _symbol(author_symbol),
                    _symbol(later_left),
                    _symbol(later_right),
                    _instrument(instrument),
                    _spec(instrument),
                    _capability(instrument),
                ]
            )
            await session.commit()
        rows = await repository.get_pnl_spot_order_minimum_window(
            ["kraken"],
            ["HISTOV"],
            _WINDOW_START,
            _WINDOW_END,
            _AS_OF,
        )
        spec_row = next(row for row in rows if row["spec_public_id"] is not None)
        assert spec_row["identity_conflicted"] is True
        resolution = resolve_venue_order_minimums(
            [VenueOrderMinimumVersion(**row) for row in rows],
            overlap_seam,
        )[("kraken", "HISTOV")]
        assert resolution.refusal == "identity_ambiguous"

    async def test_sequential_instrument_rekey_exposes_new_quote_utility(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """An old spec cannot cover a later logical instrument's new symbol."""
        seam = _WINDOW_START + timedelta(hours=1)
        await _seed_pair(repository, _Pair("GOOD-USD", "GOOD", "USD"))
        old_symbol = _Pair(
            "Q-GOOD",
            "Q",
            "GOOD",
            with_spec=False,
            ver=_Ver(timestamp=_EARLY),
        )
        new_symbol = _Pair(
            "R-GOOD",
            "R",
            "GOOD",
            with_spec=False,
            ver=_Ver(timestamp=_EARLY),
        )
        old_instrument = _Pair(
            "Q-GOOD",
            "Q",
            "GOOD",
            with_spec=False,
            ver=_Ver(timestamp=_EARLY, known_to=seam),
        )
        new_instrument = _instrument(
            _Pair(
                "R-GOOD",
                "R",
                "GOOD",
                with_spec=False,
                ver=_Ver(timestamp=seam),
            )
        )
        new_instrument.public_id = "ins-Q-GOOD"
        old_capability = _Pair(
            "Q-GOOD",
            "Q",
            "GOOD",
            can_trade=False,
            with_spec=False,
            ver=_Ver(timestamp=_EARLY),
        )
        async with repository.session() as session:
            session.add_all(
                [
                    _symbol(old_symbol),
                    _symbol(new_symbol),
                    _instrument(old_instrument),
                    new_instrument,
                    _spec(old_symbol),
                    _capability(old_capability),
                    _capability(new_symbol),
                ]
            )
            await session.commit()
        rows = await repository.get_pnl_spot_order_minimum_window(
            ["kraken"],
            ["GOOD"],
            _WINDOW_START,
            _WINDOW_END,
            _AS_OF,
        )
        rekeyed = [
            row
            for row in rows
            if row["instrument_public_id"] == "ins-Q-GOOD"
            and row["symbol_public_id"] == "sym-R-GOOD"
        ]
        assert len(rekeyed) == 1
        assert rekeyed[0]["spec_public_id"] is None
        assert rekeyed[0]["can_trade"] is True
        assert rekeyed[0]["valid_from"] == seam
        resolution = resolve_venue_order_minimums(
            [VenueOrderMinimumVersion(**row) for row in rows],
            seam,
        )[("kraken", "GOOD")]
        assert resolution.refusal == "is_quote_currency"

    async def test_later_instrument_overlap_marks_author_time_quote_spec(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """A later logical-instrument overlap remains visible through old specs."""
        overlap_start = _WINDOW_START + timedelta(hours=1)
        overlap_seam = overlap_start + timedelta(minutes=10)
        overlap_end = overlap_start + timedelta(minutes=30)
        await _seed_pair(repository, _Pair("GOOD-USD", "GOOD", "USD"))
        old_symbol = _Pair(
            "Q-GOOD",
            "Q",
            "GOOD",
            with_spec=False,
            can_trade=False,
            ver=_Ver(timestamp=_EARLY),
        )
        new_symbol = _Pair(
            "R-GOOD",
            "R",
            "GOOD",
            with_spec=False,
            ver=_Ver(timestamp=_EARLY),
        )
        author_instrument = _Pair(
            "Q-GOOD",
            "Q",
            "GOOD",
            with_spec=False,
            ver=_Ver(
                timestamp=_EARLY,
                known_to=_WINDOW_START - timedelta(minutes=10),
            ),
        )
        later_left = _Pair(
            "Q-GOOD",
            "Q",
            "GOOD",
            with_spec=False,
            ver=_Ver(timestamp=overlap_start, known_to=overlap_end),
        )
        later_right = _instrument(
            _Pair(
                "R-GOOD",
                "R",
                "GOOD",
                with_spec=False,
                ver=_Ver(timestamp=overlap_seam),
            )
        )
        later_right.public_id = "ins-Q-GOOD"
        async with repository.session() as session:
            session.add_all(
                [
                    _symbol(old_symbol),
                    _symbol(new_symbol),
                    _instrument(author_instrument),
                    _instrument(later_left),
                    later_right,
                    _spec(old_symbol),
                    _capability(old_symbol),
                    _capability(new_symbol),
                ]
            )
            await session.commit()
        rows = await repository.get_pnl_spot_order_minimum_window(
            ["kraken"],
            ["GOOD"],
            _WINDOW_START,
            _WINDOW_END,
            _AS_OF,
        )
        quote_spec = next(
            row
            for row in rows
            if row["instrument_public_id"] == "ins-Q-GOOD" and row["spec_public_id"] is not None
        )
        assert quote_spec["can_trade"] is False
        assert quote_spec["identity_conflicted"] is True
        resolution = resolve_venue_order_minimums(
            [VenueOrderMinimumVersion(**row) for row in rows],
            overlap_seam,
        )[("kraken", "GOOD")]
        assert resolution.refusal == "is_quote_currency"

    async def test_other_instrument_version_does_not_mask_symbol_gap(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """Current Symbol coverage is scoped to one concrete Instrument row."""
        gap_start = _WINDOW_START + timedelta(minutes=40)
        await _seed_pair(repository, _Pair("MASK-USD", "MASK", "USD"))
        requested_symbol = _Pair(
            "MASK-EUR",
            "MASK",
            "EUR",
            with_spec=False,
            ver=_Ver(timestamp=_EARLY, known_to=gap_start),
        )
        other_symbol = _Pair(
            "OTHER-EUR",
            "OTHER",
            "EUR",
            with_spec=False,
            ver=_Ver(timestamp=_EARLY),
        )
        requested_instrument = _Pair(
            "MASK-EUR",
            "MASK",
            "EUR",
            with_spec=False,
            ver=_Ver(timestamp=_EARLY, known_to=_WINDOW_END),
        )
        other_instrument = _instrument(
            _Pair(
                "OTHER-EUR",
                "OTHER",
                "EUR",
                with_spec=False,
                ver=_Ver(timestamp=gap_start - timedelta(minutes=10)),
            )
        )
        other_instrument.public_id = "ins-MASK-EUR"
        async with repository.session() as session:
            session.add_all(
                [
                    _symbol(requested_symbol),
                    _symbol(other_symbol),
                    _instrument(requested_instrument),
                    other_instrument,
                    _capability(requested_instrument),
                    _capability(other_symbol),
                ]
            )
            await session.commit()
        rows = await repository.get_pnl_spot_order_minimum_window(
            ["kraken"],
            ["MASK"],
            _WINDOW_START,
            _WINDOW_END,
            _AS_OF,
        )
        marker = next(
            row
            for row in rows
            if row["instrument_public_id"] == "ins-MASK-EUR" and row["valid_from"] == gap_start
        )
        assert marker["valid_to"] == _WINDOW_END
        assert marker["identity_conflicted"] is True
        assert marker["can_trade"] is None
        resolution = resolve_venue_order_minimums(
            [VenueOrderMinimumVersion(**row) for row in rows],
            gap_start + timedelta(minutes=1),
        )[("kraken", "MASK")]
        assert resolution.refusal == "identity_ambiguous"

    async def test_spec_known_after_as_of_is_not_projected(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """A future-known spec cannot retroactively supply a threshold."""
        pair = _Pair(
            "LATE-USD",
            "LATE",
            "USD",
            ver=_Ver(timestamp=_LATE),
        )
        symbol_pair = _Pair(
            "LATE-USD",
            "LATE",
            "USD",
            with_spec=False,
            ver=_Ver(timestamp=_EARLY),
        )
        async with repository.session() as session:
            session.add_all(
                [
                    _symbol(symbol_pair),
                    _instrument(symbol_pair),
                    _capability(symbol_pair),
                    _spec(pair),
                ]
            )
            await session.commit()
        rows = await repository.get_pnl_spot_order_minimum_window(
            ["kraken"],
            ["LATE"],
            _WINDOW_START,
            _WINDOW_END,
            _AS_OF,
        )
        assert len(rows) == 1
        assert rows[0]["spec_public_id"] is None
        assert rows[0]["min_order_size"] is None

    async def test_spec_authored_in_symbol_gap_emits_venue_wide_unknown(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """A spec without one author-time Symbol identity cannot disappear."""
        await _seed_pair(repository, _Pair("C-USD", "C", "USD"))
        before_gap = _Pair(
            "ORPHAN-EUR",
            "C",
            "EUR",
            with_spec=False,
            ver=_Ver(
                timestamp=_EARLY - timedelta(hours=1),
                known_to=_EARLY - timedelta(minutes=30),
            ),
        )
        after_gap = _Pair(
            "ORPHAN-EUR",
            "D",
            "EUR",
            with_spec=False,
            ver=_Ver(timestamp=_EARLY + timedelta(minutes=30)),
        )
        instrument = _Pair(
            "ORPHAN-EUR",
            "C",
            "EUR",
            with_spec=False,
            ver=_Ver(timestamp=_EARLY - timedelta(hours=1)),
        )
        spec = _Pair(
            "ORPHAN-EUR",
            "C",
            "EUR",
            ver=_Ver(timestamp=_EARLY),
        )
        async with repository.session() as session:
            session.add_all(
                [
                    _symbol(before_gap),
                    _symbol(after_gap),
                    _instrument(instrument),
                    _spec(spec),
                    _capability(instrument),
                ]
            )
            await session.commit()
        rows = await repository.get_pnl_spot_order_minimum_window(
            ["kraken"],
            ["C"],
            _WINDOW_START,
            _WINDOW_END,
            _AS_OF,
        )
        marker = next(row for row in rows if row["spec_public_id"] == "spec-ORPHAN-EUR")
        assert marker["currency"] == "C"
        assert marker["asset_type"] == "unknown"
        assert marker["can_trade"] is None
        assert marker["identity_conflicted"] is True
        resolution = resolve_venue_order_minimums(
            [VenueOrderMinimumVersion(**row) for row in rows],
            _WINDOW_START,
        )[("kraken", "C")]
        assert resolution.refusal == "identity_ambiguous"

    async def test_instrument_without_any_symbol_history_blocks_every_requested_currency(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """An unconstrained symbol reference cannot hide an unknown sibling."""
        await _seed_pair(repository, _Pair("C-USD", "C", "USD"))
        orphan = _Pair(
            "NOHISTORY",
            "UNSEEN",
            "USD",
            with_spec=False,
            ver=_Ver(timestamp=_EARLY),
        )
        async with repository.session() as session:
            session.add(_instrument(orphan))
            await session.commit()
        rows = await repository.get_pnl_spot_order_minimum_window(
            ["kraken"],
            ["C"],
            _WINDOW_START,
            _WINDOW_END,
            _AS_OF,
        )
        marker = next(row for row in rows if row["instrument_public_id"] == "ins-NOHISTORY")
        assert marker["currency"] == "C"
        assert marker["symbol_public_id"] == "sym-NOHISTORY"
        assert marker["valid_from"] == _WINDOW_START
        assert marker["valid_to"] == _WINDOW_END
        assert marker["identity_conflicted"] is True
        resolution = resolve_venue_order_minimums(
            [VenueOrderMinimumVersion(**row) for row in rows],
            _WINDOW_START,
        )[("kraken", "C")]
        assert resolution.refusal == "identity_ambiguous"

    async def test_identity_collision_is_projected_on_every_pair(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """The PEP crypto/equity collision sets the P3 conflict flag."""
        await _seed_pair(
            repository,
            _Pair("PEP-EUR", "PEP", "EUR", asset_type="crypto"),
        )
        await _seed_pair(
            repository,
            _Pair("PEP", "PEP", None, asset_type="equity", with_spec=False),
        )
        rows = await repository.get_pnl_spot_order_minimum_window(
            ["kraken"],
            ["PEP"],
            _WINDOW_START,
            _WINDOW_END,
            _AS_OF,
        )
        assert {row["instrument_public_id"] for row in rows} == {
            "ins-PEP",
            "ins-PEP-EUR",
        }
        assert all(row["identity_conflicted"] for row in rows)
        assert {row["asset_type"] for row in rows} == {"crypto", "equity"}

    async def test_two_distinct_non_null_minima_are_unstable(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """Every non-null version sees the other differing value under P7."""
        seam = _WINDOW_START + timedelta(hours=1)
        identity = _Pair(
            "CHANGED-USD",
            "CHANGED",
            "USD",
            with_spec=False,
            ver=_Ver(timestamp=_EARLY),
        )
        old_pair = _Pair(
            "CHANGED-USD",
            "CHANGED",
            "USD",
            minimum=2.0,
            ver=_Ver(timestamp=_EARLY, known_to=seam),
        )
        new_pair = _Pair(
            "CHANGED-USD",
            "CHANGED",
            "USD",
            minimum=3.0,
            ver=_Ver(timestamp=seam),
        )
        old_spec = _spec(old_pair)
        new_spec = _spec(new_pair)
        new_spec.public_id = old_spec.public_id
        async with repository.session() as session:
            session.add_all(
                [
                    _symbol(identity),
                    _instrument(identity),
                    _capability(identity),
                    old_spec,
                    new_spec,
                ]
            )
            await session.commit()
        rows = await repository.get_pnl_spot_order_minimum_window(
            ["kraken"],
            ["CHANGED"],
            _WINDOW_START,
            _WINDOW_END,
            _AS_OF,
        )
        assert [row["min_order_size"] for row in rows] == [2.0, 3.0]
        assert all(row["minimum_unstable"] for row in rows)

    async def test_null_to_value_transition_is_not_unstable(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """A provenance backfill ignores the historical null in P7."""
        seam = _WINDOW_START + timedelta(hours=1)
        identity = _Pair(
            "BACKFILL-USD",
            "BACKFILL",
            "USD",
            with_spec=False,
            ver=_Ver(timestamp=_EARLY),
        )
        null_pair = _Pair(
            "BACKFILL-USD",
            "BACKFILL",
            "USD",
            minimum=None,
            ver=_Ver(timestamp=_EARLY, known_to=seam),
        )
        value_pair = _Pair(
            "BACKFILL-USD",
            "BACKFILL",
            "USD",
            minimum=3.0,
            ver=_Ver(timestamp=seam),
        )
        old_spec = _spec(null_pair)
        new_spec = _spec(value_pair)
        new_spec.public_id = old_spec.public_id
        async with repository.session() as session:
            session.add_all(
                [
                    _symbol(identity),
                    _instrument(identity),
                    _capability(identity),
                    old_spec,
                    new_spec,
                ]
            )
            await session.commit()
        rows = await repository.get_pnl_spot_order_minimum_window(
            ["kraken"],
            ["BACKFILL"],
            _WINDOW_START,
            _WINDOW_END,
            _AS_OF,
        )
        assert [row["min_order_size"] for row in rows] == [None, 3.0]
        assert not any(row["minimum_unstable"] for row in rows)

    async def test_instrument_without_spec_still_surfaces(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """P3 and P6 can see a tradable sibling with no spec."""
        await _seed_pair(
            repository,
            _Pair("NOSPEC-USD", "NOSPEC", "USD", with_spec=False),
        )
        row = (
            await repository.get_pnl_spot_order_minimum_window(
                ["kraken"],
                ["NOSPEC"],
                _WINDOW_START,
                _WINDOW_END,
                _AS_OF,
            )
        )[0]
        assert row["instrument_public_id"] == "ins-NOSPEC-USD"
        assert row["spec_public_id"] is None
        assert row["spec_source"] is None
        assert row["min_order_size"] is None
        assert row["can_trade"] is True

    async def test_missing_capability_projects_unknown_and_blocks_sibling_subset(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """Capability absence remains UNKNOWN and cannot license a subset min."""
        await _seed_pair(repository, _Pair("NOCAP-USD", "NOCAP", "USD"))
        pair = _Pair("NOCAP-EUR", "NOCAP", "EUR")
        async with repository.session() as session:
            session.add_all([_symbol(pair), _instrument(pair), _spec(pair)])
            await session.commit()
        rows = await repository.get_pnl_spot_order_minimum_window(
            ["kraken"],
            ["NOCAP"],
            _WINDOW_START,
            _WINDOW_END,
            _AS_OF,
        )
        missing = next(row for row in rows if row["instrument_public_id"] == "ins-NOCAP-EUR")
        assert missing["can_trade"] is None
        resolution = resolve_venue_order_minimums(
            [VenueOrderMinimumVersion(**row) for row in rows],
            _WINDOW_START,
        )[("kraken", "NOCAP")]
        assert resolution.refusal == "uncertified_tradable_pair"

    async def test_capability_is_selected_at_the_valued_minute(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """A capability correction splits the spec proof at its own seam."""
        seam = _WINDOW_START + timedelta(hours=1)
        identity = _Pair(
            "CAP-USD",
            "CAP",
            "USD",
            with_spec=False,
            ver=_Ver(timestamp=_EARLY),
        )
        old_capability = _Pair(
            "CAP-USD",
            "CAP",
            "USD",
            can_trade=True,
            with_spec=False,
            ver=_Ver(timestamp=_EARLY, known_to=seam),
        )
        new_capability = _Pair(
            "CAP-USD",
            "CAP",
            "USD",
            can_trade=False,
            with_spec=False,
            ver=_Ver(timestamp=seam),
        )
        async with repository.session() as session:
            session.add_all(
                [
                    _symbol(identity),
                    _instrument(identity),
                    _spec(identity),
                    _capability(old_capability),
                    _capability(new_capability),
                ]
            )
            await session.commit()
        rows = await repository.get_pnl_spot_order_minimum_window(
            ["kraken"],
            ["CAP"],
            _WINDOW_START,
            _WINDOW_END,
            _AS_OF,
        )
        assert [(row["can_trade"], row["valid_from"], row["valid_to"]) for row in rows] == [
            (True, _WINDOW_START, seam),
            (False, seam, _WINDOW_END),
        ]
        versions = [VenueOrderMinimumVersion(**row) for row in rows]
        before = resolve_venue_order_minimums(
            versions,
            seam - timedelta(minutes=1),
        )[("kraken", "CAP")]
        after = resolve_venue_order_minimums(versions, seam)[("kraken", "CAP")]
        assert before.threshold == 3.0
        assert after.refusal == "no_certified_pair"

    async def test_overlapping_capabilities_project_unknown(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """Competing capability versions are UNKNOWN only while they overlap."""
        overlap_start = _WINDOW_START + timedelta(minutes=30)
        overlap_end = _WINDOW_START + timedelta(minutes=90)
        identity = _Pair(
            "CAPOVER-USD",
            "CAPOVER",
            "USD",
            with_spec=False,
            ver=_Ver(timestamp=_EARLY),
        )
        old_capability = _Pair(
            "CAPOVER-USD",
            "CAPOVER",
            "USD",
            can_trade=True,
            with_spec=False,
            ver=_Ver(timestamp=_EARLY, known_to=overlap_end),
        )
        new_capability = _Pair(
            "CAPOVER-USD",
            "CAPOVER",
            "USD",
            can_trade=False,
            with_spec=False,
            ver=_Ver(timestamp=overlap_start, known_to=_WINDOW_END),
        )
        async with repository.session() as session:
            session.add_all(
                [
                    _symbol(identity),
                    _instrument(identity),
                    _spec(identity),
                    _capability(old_capability),
                    _capability(new_capability),
                ]
            )
            await session.commit()
        rows = await repository.get_pnl_spot_order_minimum_window(
            ["kraken"],
            ["CAPOVER"],
            _WINDOW_START,
            _WINDOW_END,
            _AS_OF,
        )
        assert [(row["can_trade"], row["valid_from"], row["valid_to"]) for row in rows] == [
            (True, _WINDOW_START, overlap_start),
            (None, overlap_start, overlap_end),
            (False, overlap_end, _WINDOW_END),
        ]

    async def test_paper_exchange_is_excluded(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """A PAPER listing never supplies live venue-order evidence."""
        await _seed_pair(
            repository,
            _Pair("PAPER-USD", "PAPER", "USD", exchange="paper"),
        )
        rows = await repository.get_pnl_spot_order_minimum_window(
            ["paper"],
            ["PAPER"],
            _WINDOW_START,
            _WINDOW_END,
            _AS_OF,
        )
        assert rows == []

    async def test_quote_role_rows_are_returned(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """Requested quote currencies expose P4 spot-order utility."""
        await _seed_pair(
            repository,
            _Pair("JITOSOL-SOL", "JITOSOL", "SOL", minimum=0.05),
        )
        rows = await repository.get_pnl_spot_order_minimum_window(
            ["kraken"],
            ["SOL"],
            _WINDOW_START,
            _WINDOW_END,
            _AS_OF,
        )
        assert len(rows) == 1
        assert rows[0]["currency"] == "SOL"
        assert rows[0]["role"] == "quote"
        assert rows[0]["counter_currency"] == "JITOSOL"

    async def test_unrequested_exchange_and_currency_return_empty(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """Both request dimensions are exact filters."""
        await _seed_pair(repository, _Pair("BTC-USD", "BTC", "USD"))
        assert (
            await repository.get_pnl_spot_order_minimum_window(
                ["binance"],
                ["BTC"],
                _WINDOW_START,
                _WINDOW_END,
                _AS_OF,
            )
            == []
        )
        assert (
            await repository.get_pnl_spot_order_minimum_window(
                ["kraken"],
                ["ETH"],
                _WINDOW_START,
                _WINDOW_END,
                _AS_OF,
            )
            == []
        )
