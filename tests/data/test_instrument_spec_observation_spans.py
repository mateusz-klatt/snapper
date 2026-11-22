"""Tests for value-versioned instrument specs, observation spans, and updater locks.

Covers pure span-coalescing decisions, exact set-equality property tests over
monotonic and out-of-order observation streams, sync/async repository revision
paths, valuation and reconciliation bundle span loading, the catalog-wide updater
lock, concurrent updater races on SQLite, and opt-in PostgreSQL witness checks.
"""

import asyncio
import os
import random
import threading
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from decimal import Decimal
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session as SyncSession

from snapper.application.portfolio.spot_precision_certification import KRAKEN_INSTRUMENT_SPEC_SOURCE
from snapper.application.portfolio.spot_precision_certification import (
    is_spot_instrument_precision_certified,
)
from snapper.application.portfolio.spot_precision_certification import (
    kraken_instrument_precision_version,
)
from snapper.application.updaters.symbols.base import SymbolUpdaterService
from snapper.application.updaters.symbols.kraken import KrakenSymbolUpdaterService
from snapper.application.updaters.symbols.kraken_equities import KrakenEquitiesSymbolUpdaterService
from snapper.application.updaters.symbols.kraken_futures import KrakenFuturesSymbolUpdaterService
from snapper.application.updaters.symbols.polygon import PolygonSymbolUpdaterService
from snapper.application.updaters.symbols.types import InstrumentMetadataInput
from snapper.application.updaters.symbols.walutomat import WalutomatSymbolUpdaterService
from snapper.application.updaters.underlying_updater import UnderlyingUpdater
from snapper.application.updaters.underlying_updater import _MatchResult
from snapper.config.settings import AppSettings
from snapper.core.types import AssetTypeEnum
from snapper.core.types import ExchangeEnum
from snapper.core.types import InstrumentKindEnum
from snapper.core.types import RelationshipTypeEnum
from snapper.data.execution_unit_admission_store import _derivative_catalog_fresh
from snapper.data.execution_unit_catalog import UnitCatalogLookup
from snapper.data.execution_unit_catalog import UnitCatalogObservation
from snapper.data.execution_unit_catalog import load_unit_catalog
from snapper.data.instrument_spec_observation_spans import SPEC_OBSERVATION_FRESHNESS
from snapper.data.instrument_spec_observation_spans import SpecObservationSpanTarget
from snapper.data.instrument_spec_observation_spans import SpecVersionIdentity
from snapper.data.instrument_spec_observation_spans import decide_observation_span_write
from snapper.data.instrument_spec_observation_spans import is_spec_observation_fresh
from snapper.data.instrument_spec_observation_spans import (
    latest_instrument_spec_observation_at_sync,
)
from snapper.data.instrument_spec_observation_spans import latest_spec_observation_at
from snapper.data.instrument_spec_observation_spans import load_instrument_spec_observation_spans
from snapper.data.instrument_spec_observation_spans import matching_observation_spans
from snapper.data.instrument_spec_observation_spans import select_vouching_observation_span
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Instrument
from snapper.data.models import InstrumentSpec
from snapper.data.models import InstrumentSpecObservationSpan
from snapper.data.models import Position
from snapper.data.models import Setting
from snapper.data.models import Symbol
from snapper.data.repository import SYMBOL_UPDATER_LOCK_IDENTITY
from snapper.data.repository import DatabaseRepository
from snapper.data.repository import InstrumentSpecInput
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import is_effective_unit_certified
from snapper.data.repository_types import InstrumentSpecObservationSpanRow
from snapper.data.repository_types import InstrumentSpecRow
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.exchanges.futures_spec_provenance import FUTURES_SPEC_SOURCE

_T0 = datetime(2026, 11, 21, 0, 0, tzinfo=UTC)
_H12 = SPEC_OBSERVATION_FRESHNESS

type _ConcreteUpdater = (
    KrakenSymbolUpdaterService
    | KrakenFuturesSymbolUpdaterService
    | KrakenEquitiesSymbolUpdaterService
    | WalutomatSymbolUpdaterService
    | PolygonSymbolUpdaterService
)


def _at(hours: float = 0.0, microseconds: int = 0) -> datetime:
    """Build a UTC timestamp offset from ``_T0``."""
    return _T0 + timedelta(hours=hours, microseconds=microseconds)


def _span(
    first: datetime,
    last: datetime,
    *,
    span_id: int = 1,
    identity: SpecVersionIdentity | None = None,
) -> InstrumentSpecObservationSpanRow:
    """Build one synthetic observation-span snapshot."""
    version = identity or SpecVersionIdentity("inst-1", "spec-1", _T0)
    return InstrumentSpecObservationSpanRow(
        id=span_id,
        instrument_public_id=version.instrument_public_id,
        spec_public_id=version.spec_public_id or "spec-1",
        spec_timestamp=version.spec_timestamp or _T0,
        spec_version="v1",
        first_observed_at=first,
        last_observed_at=last,
        session_id="session-1",
        sequence_id=1,
    )


def _spec_row(
    observed_at: datetime | None,
    *,
    spans: tuple[InstrumentSpecObservationSpanRow, ...] = (),
    spec_public_id: str = "spec-1",
    spec_timestamp: datetime = _T0,
    instrument_public_id: str = "inst-1",
) -> InstrumentSpecRow:
    """Build one synthetic InstrumentSpecRow with optional observation spans."""
    return InstrumentSpecRow(
        instrument_public_id=instrument_public_id,
        tick_size=0.01,
        lot_size=0.001,
        min_order_size=0.01,
        max_order_size=100.0,
        cost_decimals=2,
        qty_decimals=3,
        margin_initial=None,
        position_limit_long=None,
        position_limit_short=None,
        status="active",
        contract_size=Decimal(1),
        quantity_unit="contract_count",
        native_order_quantity_unit="contract_count",
        contract_payoff_model="linear",
        contract_value_currency="BTC",
        settlement_currency="USD",
        spec_source=FUTURES_SPEC_SOURCE,
        spec_version="s2a-v4:test",
        spec_observed_at=observed_at,
        unit_certified=True,
        expiry_at=None,
        instrument_kind="perpetual",
        funding_type="perpetual_funding",
        funding_frequency_hours=1,
        rollover_rate_long=None,
        rollover_rate_short=None,
        max_funding_rate=0.0025,
        spec_public_id=spec_public_id,
        spec_timestamp=spec_timestamp,
        observation_spans=spans,
    )


def _spec_input(
    observed_at: datetime | None,
    *,
    tick_size: float = 0.01,
    spec_version: str | None = "s2a-v4:digest-1",
    spec_source: str | None = FUTURES_SPEC_SOURCE,
    unit_certified: bool | None = None,
) -> InstrumentSpecInput:
    """Build a complete InstrumentSpecInput for sync or async revisions."""
    certified = observed_at is not None if unit_certified is None else unit_certified
    source = None if observed_at is None else spec_source
    version = None if observed_at is None else spec_version
    return InstrumentSpecInput(
        tick_size=tick_size,
        lot_size=0.001,
        min_order_size=0.01,
        max_order_size=100.0,
        cost_decimals=2,
        qty_decimals=3,
        status="active",
        contract_size=Decimal(1),
        quantity_unit="contract_count",
        native_order_quantity_unit="contract_count",
        contract_payoff_model="linear",
        contract_value_currency="BTC",
        settlement_currency="USD",
        spec_source=source,
        spec_version=version,
        spec_observed_at=observed_at,
        unit_certified=certified,
        instrument_kind="perpetual",
    )


class TestPureObservationSpanLogic:
    """Pure span write decisions, filtering, freshness, and set-equality proofs."""

    def test_span_target_validates_and_normalizes_utc(self) -> None:
        """Convert aware non-UTC datetimes to UTC and reject naive timestamps."""
        plus_two = timezone(timedelta(hours=2))
        target = SpecObservationSpanTarget(
            instrument_public_id="inst-1",
            spec_public_id="spec-1",
            spec_timestamp=datetime(2026, 11, 21, 2, 0, tzinfo=plus_two),
            spec_version="v1",
            observed_at=datetime(2026, 11, 21, 3, 0, tzinfo=plus_two),
            session_id="sess-1",
            sequence_id=1,
        )
        assert target.spec_timestamp == _at(0)
        assert target.observed_at == _at(1)
        with pytest.raises(ValueError, match="spec_timestamp"):
            SpecObservationSpanTarget(
                instrument_public_id="inst-1",
                spec_public_id="spec-1",
                spec_timestamp=datetime(2026, 11, 21, 0, 0),
                spec_version="v1",
                observed_at=_at(1),
                session_id="sess-1",
                sequence_id=1,
            )
        with pytest.raises(ValueError, match="observed_at"):
            SpecObservationSpanTarget(
                instrument_public_id="inst-1",
                spec_public_id="spec-1",
                spec_timestamp=_at(0),
                spec_version="v1",
                observed_at=datetime(2026, 11, 21, 1, 0),
                session_id="sess-1",
                sequence_id=1,
            )

    def test_decide_write_boundaries_interior_and_out_of_order(self) -> None:
        """Apply exact 12-hour boundary, interior no-op, and out-of-order rules."""
        assert decide_observation_span_write(None, _at(0)) == "insert"
        latest = _span(_at(2), _at(8))
        assert decide_observation_span_write(latest, _at(2)) == "noop"
        assert decide_observation_span_write(latest, _at(5)) == "noop"
        assert decide_observation_span_write(latest, _at(8)) == "noop"
        assert decide_observation_span_write(latest, _at(20, -1)) == "extend"
        assert decide_observation_span_write(latest, _at(20)) == "insert"
        assert decide_observation_span_write(latest, _at(25)) == "insert"
        assert decide_observation_span_write(latest, _at(1)) == "insert"

        with pytest.raises(ValueError, match="observed_at"):
            decide_observation_span_write(latest, datetime(2026, 11, 21, 9, 0))
        naive_first = _span(datetime(2026, 11, 21, 2, 0), _at(8))
        with pytest.raises(ValueError, match="span bounds"):
            decide_observation_span_write(naive_first, _at(9))
        naive_last = _span(_at(2), datetime(2026, 11, 21, 8, 0))
        with pytest.raises(ValueError, match="span bounds"):
            decide_observation_span_write(naive_last, _at(9))

    def test_matching_and_vouching_span_selection(self) -> None:
        """Match exact version identity and break ties deterministically."""
        own_1 = _span(_at(0), _at(4), span_id=1)
        own_2 = _span(_at(1), _at(4), span_id=2)
        other_inst = _span(
            _at(0), _at(10), span_id=3, identity=SpecVersionIdentity("inst-2", "spec-1", _T0)
        )
        other_pub = _span(
            _at(0), _at(10), span_id=4, identity=SpecVersionIdentity("inst-1", "spec-2", _T0)
        )
        other_ts = _span(
            _at(0), _at(10), span_id=5, identity=SpecVersionIdentity("inst-1", "spec-1", _at(1))
        )
        naive_span = _span(datetime(2026, 11, 21, 0, 0), _at(10), span_id=6)
        matched = matching_observation_spans(
            (own_1, own_2, other_inst, other_pub, other_ts),
            SpecVersionIdentity("inst-1", "spec-1", _T0),
        )
        assert matched == (own_1, own_2)
        assert select_vouching_observation_span(matched, datetime(2026, 11, 21, 5, 0)) is None
        assert select_vouching_observation_span((naive_span,), _at(5)) is None
        assert select_vouching_observation_span(matched, _at(5)) == own_2
        assert select_vouching_observation_span(matched, _at(16, -1)) == own_2
        assert select_vouching_observation_span(matched, _at(16)) is None
        assert select_vouching_observation_span(matched, _at(0, -1)) is None
        assert (
            matching_observation_spans(
                (own_1, own_2),
                SpecVersionIdentity("inst-1", None, _T0),
            )
            == ()
        )
        assert (
            matching_observation_spans(
                (own_1, own_2),
                SpecVersionIdentity("inst-1", "spec-1", None),
            )
            == ()
        )

    def test_freshness_and_latest_observation_with_spans_and_fallback(self) -> None:
        """Evaluate freshness from matching spans, falling back only when no spans exist."""
        old_span = _span(_at(0), _at(10), span_id=1)
        new_span = _span(_at(30), _at(40), span_id=2)
        spec = _spec_row(_at(0), spans=(old_span, new_span))
        assert is_spec_observation_fresh(spec, _at(0)) is True
        assert is_spec_observation_fresh(spec, _at(22, -1)) is True
        assert is_spec_observation_fresh(spec, _at(22)) is False
        assert is_spec_observation_fresh(spec, _at(29, -1)) is False
        assert is_spec_observation_fresh(spec, _at(30)) is True
        assert is_spec_observation_fresh(spec, _at(52, -1)) is True
        assert is_spec_observation_fresh(spec, _at(52)) is False
        assert is_spec_observation_fresh(spec, datetime(2026, 11, 21, 5, 0)) is False
        assert latest_spec_observation_at(spec) == _at(40)

        under_identified = _spec_row(_at(0), spans=(old_span,))
        under_identified.pop("spec_public_id", None)
        assert is_spec_observation_fresh(under_identified, _at(1)) is False
        assert latest_spec_observation_at(under_identified) == _at(0)

        missing_ts = _spec_row(_at(0), spans=(old_span,))
        missing_ts.pop("spec_timestamp", None)
        assert is_spec_observation_fresh(missing_ts, _at(1)) is False

        foreign_version_span = _span(
            _at(0),
            _at(40),
            span_id=9,
            identity=SpecVersionIdentity("inst-1", "spec-1", _at(-5)),
        )
        spec_with_foreign = _spec_row(_at(0), spans=(foreign_version_span,))
        assert is_spec_observation_fresh(spec_with_foreign, _at(1)) is False
        assert latest_spec_observation_at(spec_with_foreign) == _at(0)

        fallback_spec = _spec_row(_at(0), spans=())
        assert is_spec_observation_fresh(fallback_spec, _at(12, -1)) is True
        assert is_spec_observation_fresh(fallback_spec, _at(12)) is False
        assert is_spec_observation_fresh(fallback_spec, _at(0, -1)) is False
        assert latest_spec_observation_at(fallback_spec) == _at(0)

        null_obs_spec = _spec_row(None, spans=())
        assert is_spec_observation_fresh(null_obs_spec, _at(1)) is False
        assert latest_spec_observation_at(null_obs_spec) is None

    def test_derivative_admission_freshness_uses_observation_spans(self) -> None:
        """Derivative submission admission keeps unchanged specs fresh via matching spans."""
        spec_orm = InstrumentSpec(
            instrument_public_id="inst-1",
            public_id="spec-1",
            timestamp=_at(0),
            spec_observed_at=_at(0),
            spec_source=FUTURES_SPEC_SOURCE,
            spec_version="s2a-v4:test",
            unit_certified=True,
            session_id="sess-1",
            sequence_id=1,
        )
        symbol_orm = Symbol(
            public_id="sym-1",
            native_symbol="BTC-USD-PERP",
            base="BTC",
            quote="USD",
            asset_type=AssetTypeEnum.CRYPTO,
            created_at=_at(0),
            timestamp=_at(0),
            session_id="sess-1",
            sequence_id=1,
        )
        instrument_orm = Instrument(
            public_id="inst-1",
            symbol_public_id="sym-1",
            exchange=ExchangeEnum.KRAKEN_FUTURES,
            timestamp=_at(0),
            session_id="sess-1",
            sequence_id=1,
        )
        fresh_obs = UnitCatalogObservation(
            instrument_orm,
            symbol_orm,
            spec_orm,
            (_span(_at(0), _at(23)),),
        )
        assert _derivative_catalog_fresh(fresh_obs, _at(0), _at(24)) is True

        expired_obs = UnitCatalogObservation(
            instrument_orm,
            symbol_orm,
            spec_orm,
            (_span(_at(0), _at(12)),),
        )
        assert _derivative_catalog_fresh(expired_obs, _at(0), _at(24)) is False

        fallback_obs = UnitCatalogObservation(instrument_orm, symbol_orm, spec_orm, ())
        assert _derivative_catalog_fresh(fallback_obs, _at(10), _at(21)) is True
        assert _derivative_catalog_fresh(fallback_obs, _at(10), _at(22)) is False
        assert _derivative_catalog_fresh(fallback_obs, _at(10), _at(9)) is False

        foreign_obs = UnitCatalogObservation(
            instrument_orm,
            symbol_orm,
            spec_orm,
            (_span(_at(0), _at(23), identity=SpecVersionIdentity("inst-1", "spec-1", _at(-1))),),
        )
        assert _derivative_catalog_fresh(foreign_obs, _at(0), _at(24)) is False

        fallback_obs = UnitCatalogObservation(instrument_orm, symbol_orm, spec_orm, ())
        assert _derivative_catalog_fresh(fallback_obs, _at(0), _at(12, -1)) is True
        assert _derivative_catalog_fresh(fallback_obs, _at(0), _at(12)) is False

    @pytest.mark.parametrize("shuffle", [False, True])
    def test_property_span_union_equals_brute_force_observation_model(
        self,
        shuffle: bool,
    ) -> None:
        """Match brute-force ``(as_of - 12h, as_of]`` over monotonic and out-of-order streams.

        Given: Two hundred random observation streams with gaps both under and over 12 hours.
        When: Spans are maintained via ``decide_observation_span_write`` ordered by
            ``(last_observed_at DESC, id DESC)``.
        Then: Span freshness equals brute-force freshness at every observation, boundary,
            and midpoint instant.
        """
        rng = random.Random(20261121 + int(shuffle))
        for _ in range(200):
            times = [_at(rng.uniform(0, 72)) for _ in range(rng.randint(1, 14))]
            if not shuffle:
                times.sort()
            spans: list[InstrumentSpecObservationSpanRow] = []
            next_id = 1
            for obs in times:
                latest = (
                    max(spans, key=lambda row: (row["last_observed_at"], row["id"]))
                    if spans
                    else None
                )
                decision = decide_observation_span_write(latest, obs)
                if decision == "extend" and latest is not None:
                    spans.remove(latest)
                    spans.append(
                        _span(
                            latest["first_observed_at"],
                            obs,
                            span_id=latest["id"],
                        )
                    )
                elif decision == "insert":
                    spans.append(_span(obs, obs, span_id=next_id))
                    next_id += 1
            probes: list[datetime] = []
            for obs in times:
                probes.extend(
                    [
                        obs - timedelta(microseconds=1),
                        obs,
                        obs + _H12 - timedelta(microseconds=1),
                        obs + _H12,
                    ]
                )
            for earlier, later in zip(sorted(times), sorted(times)[1:], strict=False):
                probes.append(earlier + (later - earlier) / 2)
            for probe in probes:
                expected = any(timedelta(0) <= probe - obs < _H12 for obs in times)
                actual = select_vouching_observation_span(spans, probe) is not None
                assert actual is expected


class TestPersistedSpansAndRevisions:
    """SQLite end-to-end tests for value-versioned revisions and all repository loaders."""

    def test_sync_revision_lifecycle_boundaries_out_of_order_and_flip_back(
        self,
        tmp_path: Path,
    ) -> None:
        """Verify sync revision value-versioning, span extension, gap split, and flip-back."""
        db_url = f"sqlite:///{tmp_path / 'sync-spans.db'}"
        repo = DatabaseRepository(db_url)
        repo.create_all()
        inst_id = str(uuid4())
        sess_id = str(uuid4())
        try:
            with repo.get_session() as session:
                assert (
                    repo.revise_instrument_spec_sync(
                        session, inst_id, sess_id, 1, _at(0), _spec_input(_at(0))
                    )
                    == "created"
                )
                assert (
                    repo.revise_instrument_spec_sync(
                        session, inst_id, sess_id, 2, _at(6), _spec_input(_at(6))
                    )
                    == "unchanged"
                )
                assert (
                    repo.revise_instrument_spec_sync(
                        session, inst_id, sess_id, 3, _at(7), _spec_input(_at(4))
                    )
                    == "unchanged"
                )
                assert (
                    repo.revise_instrument_spec_sync(
                        session, inst_id, sess_id, 4, _at(18), _spec_input(_at(18, -1))
                    )
                    == "unchanged"
                )
                assert (
                    repo.revise_instrument_spec_sync(
                        session, inst_id, sess_id, 5, _at(30), _spec_input(_at(30, -1))
                    )
                    == "unchanged"
                )
                assert (
                    repo.revise_instrument_spec_sync(
                        session, inst_id, sess_id, 6, _at(31), _spec_input(_at(29))
                    )
                    == "unchanged"
                )
                assert (
                    repo.revise_instrument_spec_sync(
                        session, inst_id, sess_id, 7, _at(32), _spec_input(_at(31), tick_size=0.05)
                    )
                    == "updated"
                )
                assert (
                    repo.revise_instrument_spec_sync(
                        session, inst_id, sess_id, 8, _at(33), _spec_input(_at(33), tick_size=0.01)
                    )
                    == "updated"
                )
                null_inst = str(uuid4())
                assert (
                    repo.revise_instrument_spec_sync(
                        session, null_inst, sess_id, 9, _at(0), _spec_input(None)
                    )
                    == "created"
                )
                assert (
                    repo.revise_instrument_spec_sync(
                        session, null_inst, sess_id, 10, _at(1), _spec_input(None)
                    )
                    == "unchanged"
                )
                session.commit()

                specs = (
                    session.execute(
                        select(InstrumentSpec)
                        .where(InstrumentSpec.instrument_public_id == inst_id)
                        .order_by(InstrumentSpec.timestamp)
                    )
                    .scalars()
                    .all()
                )
                spans = (
                    session.execute(
                        select(InstrumentSpecObservationSpan)
                        .where(InstrumentSpecObservationSpan.instrument_public_id == inst_id)
                        .order_by(InstrumentSpecObservationSpan.id)
                    )
                    .scalars()
                    .all()
                )
                null_spans = (
                    session.execute(
                        select(InstrumentSpecObservationSpan).where(
                            InstrumentSpecObservationSpan.instrument_public_id == null_inst
                        )
                    )
                    .scalars()
                    .all()
                )
            assert len(specs) == 3
            assert [row.tick_size for row in specs] == [0.01, 0.05, 0.01]
            assert [row.spec_observed_at for row in specs] == [_at(0), _at(31), _at(33)]
            assert specs[0].public_id == specs[1].public_id == specs[2].public_id
            v1_spans = [row for row in spans if row.spec_timestamp == specs[0].timestamp]
            v2_spans = [row for row in spans if row.spec_timestamp == specs[1].timestamp]
            v3_spans = [row for row in spans if row.spec_timestamp == specs[2].timestamp]
            assert [(s.first_observed_at, s.last_observed_at) for s in v1_spans] == [
                (_at(0), _at(18, -1)),
                (_at(30, -1), _at(30, -1)),
                (_at(29), _at(29)),
            ]
            assert [(s.first_observed_at, s.last_observed_at) for s in v2_spans] == [
                (_at(31), _at(31))
            ]
            assert [(s.first_observed_at, s.last_observed_at) for s in v3_spans] == [
                (_at(33), _at(33))
            ]
            assert null_spans == []
        finally:
            repo.engine.dispose()

    @staticmethod
    async def _seed_loader_entities(
        repo: SQLAlchemyRepository,
        sess_id: str,
        wallet_id: str,
    ) -> tuple[str, str]:
        """Insert futures and spot symbols, instruments, and one futures position."""
        fut_sym_id = str(uuid4())
        fut_inst_id = str(uuid4())
        spot_sym_id = str(uuid4())
        spot_inst_id = str(uuid4())
        async with repo.session() as session:
            session.add_all(
                [
                    Symbol(
                        public_id=fut_sym_id,
                        native_symbol="BTC-USD-PERP",
                        base="BTC",
                        quote="USD",
                        asset_type=AssetTypeEnum.CRYPTO,
                        created_at=_at(0),
                        timestamp=_at(0),
                        known_to=KNOWN_TO_MAX,
                        session_id=sess_id,
                        sequence_id=1,
                    ),
                    Instrument(
                        public_id=fut_inst_id,
                        symbol_public_id=fut_sym_id,
                        exchange=ExchangeEnum.KRAKEN_FUTURES,
                        timestamp=_at(0),
                        known_to=KNOWN_TO_MAX,
                        session_id=sess_id,
                        sequence_id=2,
                    ),
                    Position(
                        public_id=str(uuid4()),
                        wallet_public_id=wallet_id,
                        instrument_public_id=fut_inst_id,
                        mode="live",
                        quantity=1.0,
                        average_price=50000.0,
                        unrealized_pnl=0.0,
                        realized_pnl=0.0,
                        timestamp=_at(0),
                        known_to=KNOWN_TO_MAX,
                        session_id=sess_id,
                        sequence_id=3,
                    ),
                    Symbol(
                        public_id=spot_sym_id,
                        native_symbol="ETH-USD",
                        base="ETH",
                        quote="USD",
                        asset_type=AssetTypeEnum.CRYPTO,
                        created_at=_at(0),
                        timestamp=_at(0),
                        known_to=KNOWN_TO_MAX,
                        session_id=sess_id,
                        sequence_id=4,
                    ),
                    Instrument(
                        public_id=spot_inst_id,
                        symbol_public_id=spot_sym_id,
                        exchange=ExchangeEnum.KRAKEN,
                        timestamp=_at(0),
                        known_to=KNOWN_TO_MAX,
                        session_id=sess_id,
                        sequence_id=5,
                    ),
                ]
            )
            await session.commit()
        return fut_inst_id, spot_inst_id

    async def test_async_revision_and_all_repository_spec_loaders_attach_spans(
        self,
        tmp_path: Path,
    ) -> None:
        """Async revise, valuation metadata, and spot/futures loaders preserve span freshness."""
        db_url = f"sqlite+aiosqlite:///{tmp_path / 'async-spans.db'}"
        repo = SQLAlchemyRepository(db_url)
        await repo.create_all()
        sess_id = str(uuid4())
        wallet_id = str(uuid4())
        try:
            async with repo.session() as session:
                assert await load_instrument_spec_observation_spans(session, []) == {}

            fut_inst_id, spot_inst_id = await self._seed_loader_entities(repo, sess_id, wallet_id)
            spot_version = kraken_instrument_precision_version(
                tick_size=0.01,
                lot_size=0.001,
                min_order_size=0.01,
                max_order_size=100.0,
                cost_decimals=2,
                qty_decimals=3,
                status="active",
                quantity_unit="base_asset",
            )

            first_id = await repo.revise_instrument_spec(
                fut_inst_id, sess_id, 10, _at(0), _spec_input(_at(0))
            )
            assert (
                await repo.revise_instrument_spec(
                    fut_inst_id, sess_id, 11, _at(6), _spec_input(_at(6))
                )
                == first_id
            )
            assert (
                await repo.revise_instrument_spec(
                    fut_inst_id, sess_id, 12, _at(7), _spec_input(_at(3))
                )
                == first_id
            )
            assert (
                await repo.revise_instrument_spec(
                    fut_inst_id, sess_id, 13, _at(20), _spec_input(_at(20))
                )
                == first_id
            )

            spot_input = InstrumentSpecInput(
                tick_size=0.01,
                lot_size=0.001,
                min_order_size=0.01,
                max_order_size=100.0,
                cost_decimals=2,
                qty_decimals=3,
                status="active",
                quantity_unit="base_asset",
                spec_source=KRAKEN_INSTRUMENT_SPEC_SOURCE,
                spec_version=spot_version,
                spec_observed_at=_at(0),
                unit_certified=False,
                instrument_kind="spot",
            )
            await repo.revise_instrument_spec(spot_inst_id, sess_id, 14, _at(0), spot_input)
            await repo.revise_instrument_spec(
                spot_inst_id,
                sess_id,
                15,
                _at(10),
                InstrumentSpecInput(**{**spot_input.__dict__, "spec_observed_at": _at(10)}),
            )

            null_inst = str(uuid4())
            null_id = await repo.revise_instrument_spec(
                null_inst, sess_id, 16, _at(0), _spec_input(None)
            )
            assert (
                await repo.revise_instrument_spec(null_inst, sess_id, 17, _at(1), _spec_input(None))
                == null_id
            )

            eval_at = _at(21)
            loaded = await repo.get_instrument_spec(fut_inst_id, eval_at)
            assert loaded is not None
            assert loaded["spec_observed_at"] == _at(0)
            assert len(loaded.get("observation_spans", ())) == 2
            assert is_effective_unit_certified(loaded, eval_at) is True
            assert is_effective_unit_certified(loaded, _at(32)) is False

            valuation = await repo.get_active_instrument_valuation_metadata(fut_inst_id)
            assert valuation is not None
            assert valuation["spec"] is not None
            assert is_effective_unit_certified(valuation["spec"], eval_at) is True

            futures_bundle = await repo.get_futures_reconciliation_bundle(
                wallet_id, ExchangeEnum.KRAKEN_FUTURES, "live", eval_at, {"BTC-USD-PERP"}
            )
            fut_spec = futures_bundle.specs_by_instrument_public_id[fut_inst_id]
            assert fut_spec is not None
            assert is_effective_unit_certified(fut_spec, eval_at) is True

            async with repo.session() as session:
                persisted_spot, spot_spans = await repo._load_spot_reconciliation_spec_records(
                    session, (spot_inst_id,), _at(15)
                )
            assert len(persisted_spot) == 1
            spot_orm = persisted_spot[0]
            spot_loaded = repo._instrument_spec_row(
                spot_orm,
                spot_spans.get(
                    (spot_orm.instrument_public_id, spot_orm.public_id, spot_orm.timestamp),
                    (),
                ),
            )
            assert (
                is_spot_instrument_precision_certified(
                    ExchangeEnum.KRAKEN,
                    spot_loaded,
                    _at(15),
                )
                is True
            )
        finally:
            await repo.engine.dispose()

    async def test_underlying_updater_yaml_fallback_carries_latest_span_observation(
        self, tmp_path: Path
    ) -> None:
        """YAML fallback carries forward the latest span observation so R2 stays fresh.

        Given: Version R1 first observed at ``now - 13h`` with its span extended
            to ``now - 1h`` and ``instrument_kind=None``.
        When: ``UnderlyingUpdater`` applies a YAML fallback minting version R2.
        Then: R2 records ``spec_observed_at == now - 1h`` and is fresh at ``now``.
        """
        db_url = f"sqlite+aiosqlite:///{tmp_path / 'yaml-carry.db'}"
        repo = SQLAlchemyRepository(db_url)
        await repo.create_all()
        now = _at(13)
        first_obs = _at(0)
        latest_obs = _at(12)
        sess_id = str(uuid4())
        try:
            yaml_inst = str(uuid4())
            r1_input = InstrumentSpecInput(
                **{
                    **_spec_input(
                        first_obs,
                        spec_source="kraken:catalog",
                        spec_version="v1:yaml",
                    ).__dict__,
                    "instrument_kind": None,
                }
            )
            r1_id = await repo.revise_instrument_spec(yaml_inst, sess_id, 1, first_obs, r1_input)
            r1_ext_id = await repo.revise_instrument_spec(
                yaml_inst,
                sess_id,
                2,
                latest_obs,
                InstrumentSpecInput(**{**r1_input.__dict__, "spec_observed_at": latest_obs}),
            )
            assert r1_ext_id == r1_id

            yaml_updater = UnderlyingUpdater(db_url=db_url)
            yaml_updater._repo = repo
            desired = {
                yaml_inst: _MatchResult(
                    underlying_ticker="BTC",
                    relationship_type=RelationshipTypeEnum.EXACT,
                    contract_family=None,
                    instrument_type=InstrumentKindEnum.PERPETUAL,
                    expiry_override=None,
                )
            }
            assert await yaml_updater._apply_yaml_spec_fallbacks(desired, sess_id, now) == 1
            r2 = await repo.get_instrument_spec(yaml_inst, now)
            assert r2 is not None
            assert r2["instrument_kind"] == "perpetual"
            assert r2["spec_observed_at"] == latest_obs
            assert latest_spec_observation_at(r2) == latest_obs
            assert is_spec_observation_fresh(r2, now) is True
            assert is_effective_unit_certified(r2, now) is True
        finally:
            await repo.engine.dispose()

    async def test_underlying_updater_yaml_fallback_reloads_spans_under_lock_on_heartbeat_race(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """YAML fallback reloads spans under the row lock instead of reusing stale pre-lock spans.

        Given: Version R1 with ``instrument_kind=None`` pre-read with latest observation ``t1``,
            followed by a committed API unchanged heartbeat extending R1's span to ``t2`` before
            YAML acquires R1's lock.
        When: ``UnderlyingUpdater`` applies the YAML fallback minting successor R2.
        Then: R2 carries forward ``t2`` rather than stale ``t1`` and stays fresh at ``now``.
        """
        db_url = f"sqlite+aiosqlite:///{tmp_path / 'yaml-heartbeat-race.db'}"
        repo = SQLAlchemyRepository(db_url)
        await repo.create_all()
        t0, t1, t2, now = _at(0), _at(1), _at(12), _at(13)
        seed_sess, api_sess, yaml_sess = str(uuid4()), str(uuid4()), str(uuid4())
        try:
            yaml_inst = str(uuid4())
            r1_input = InstrumentSpecInput(
                **{
                    **_spec_input(
                        t0,
                        spec_source="kraken:catalog",
                        spec_version="v1:yaml",
                    ).__dict__,
                    "instrument_kind": None,
                    "native_order_quantity_unit": "contract_count",
                    "contract_payoff_model": "inverse",
                    "contract_value_currency": "USD",
                    "settlement_currency": "BTC",
                }
            )
            r1_id = await repo.revise_instrument_spec(yaml_inst, seed_sess, 1, t0, r1_input)
            assert (
                await repo.revise_instrument_spec(
                    yaml_inst,
                    seed_sess,
                    2,
                    t1,
                    InstrumentSpecInput(**{**r1_input.__dict__, "spec_observed_at": t1}),
                )
                == r1_id
            )

            real_get_spec = repo.get_instrument_spec
            interleaved = False

            async def _get_spec_then_commit_heartbeat(
                instrument_public_id: str,
                as_of: datetime,
            ) -> InstrumentSpecRow | None:
                nonlocal interleaved
                pre = await real_get_spec(instrument_public_id, as_of)
                if not interleaved and instrument_public_id == yaml_inst:
                    interleaved = True
                    heartbeat_id = await repo.revise_instrument_spec(
                        yaml_inst,
                        api_sess,
                        1,
                        t2,
                        InstrumentSpecInput(**{**r1_input.__dict__, "spec_observed_at": t2}),
                    )
                    assert heartbeat_id == r1_id
                return pre

            monkeypatch.setattr(repo, "get_instrument_spec", _get_spec_then_commit_heartbeat)

            yaml_updater = UnderlyingUpdater(db_url=db_url)
            yaml_updater._repo = repo
            desired = {
                yaml_inst: _MatchResult(
                    underlying_ticker="BTC",
                    relationship_type=RelationshipTypeEnum.EXACT,
                    contract_family=None,
                    instrument_type=InstrumentKindEnum.PERPETUAL,
                    expiry_override=None,
                )
            }
            assert await yaml_updater._apply_yaml_spec_fallbacks(desired, yaml_sess, now) == 1
            assert interleaved is True
            r2 = await real_get_spec(yaml_inst, now)
            assert r2 is not None
            assert r2["instrument_kind"] == "perpetual"
            assert r2["spec_observed_at"] == t2
            assert latest_spec_observation_at(r2) == t2
            assert is_spec_observation_fresh(r2, now) is True
            assert r2.get("native_order_quantity_unit") == "contract_count"
            assert r2.get("contract_payoff_model") == "inverse"
            assert r2.get("contract_value_currency") == "USD"
            assert r2.get("settlement_currency") == "BTC"
        finally:
            await repo.engine.dispose()

    async def test_symbol_updater_preserved_metadata_carries_latest_span_observation(
        self, tmp_path: Path
    ) -> None:
        """Preserved-metadata sync revisions carry forward the latest span observation.

        Given: Version R1 first observed at ``now - 13h`` with its span extended
            to ``now - 1h``, plus null-observation and spanless rows.
        When: ``SymbolUpdaterService._revise_instrument_spec`` updates funding
            fields with default ``metadata=PRESERVE_EXISTING``.
        Then: Minted R2 carries ``now - 1h`` and stays fresh at ``now``, while
            null and spanless rows resolve cleanly via ``latest_instrument_spec_observation_at_sync``.
        """
        db_file = tmp_path / "sync-carry.db"
        async_repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_file}")
        sync_repo = DatabaseRepository(f"sqlite:///{db_file}")
        await async_repo.create_all()
        now = _at(13)
        first_obs = _at(0)
        latest_obs = _at(12)
        sess_id = str(uuid4())
        try:
            sync_inst = str(uuid4())
            with sync_repo.get_session() as session:
                assert (
                    DatabaseRepository.revise_instrument_spec_sync(
                        session, sync_inst, sess_id, 10, first_obs, _spec_input(first_obs)
                    )
                    == "created"
                )
                assert (
                    DatabaseRepository.revise_instrument_spec_sync(
                        session, sync_inst, sess_id, 11, latest_obs, _spec_input(latest_obs)
                    )
                    == "unchanged"
                )
                SymbolUpdaterService._revise_instrument_spec(
                    session,
                    sync_inst,
                    now,
                    session_id=sess_id,
                    sequence_id=12,
                    instrument_kind="perpetual",
                    funding_type="perpetual_funding",
                    funding_frequency_hours=1,
                )
                session.commit()

                null_inst = str(uuid4())
                DatabaseRepository.revise_instrument_spec_sync(
                    session, null_inst, sess_id, 13, first_obs, _spec_input(None)
                )
                null_orm = DatabaseRepository.get_instrument_spec_sync(session, null_inst, now)
                assert null_orm is not None
                assert latest_instrument_spec_observation_at_sync(session, null_orm) is None

                spanless_inst = str(uuid4())
                spanless_orm = InstrumentSpec(
                    instrument_public_id=spanless_inst,
                    timestamp=first_obs,
                    known_to=KNOWN_TO_MAX,
                    spec_source=FUTURES_SPEC_SOURCE,
                    spec_version="s2a-v4:spanless",
                    spec_observed_at=first_obs,
                    unit_certified=False,
                    session_id=sess_id,
                    sequence_id=14,
                )
                session.add(spanless_orm)
                session.flush()
                assert (
                    latest_instrument_spec_observation_at_sync(session, spanless_orm) == first_obs
                )

            r2_sync = await async_repo.get_instrument_spec(sync_inst, now)
            assert r2_sync is not None
            assert r2_sync["funding_type"] == "perpetual_funding"
            assert r2_sync["spec_observed_at"] == latest_obs
            assert latest_spec_observation_at(r2_sync) == latest_obs
            assert is_spec_observation_fresh(r2_sync, now) is True
            assert is_effective_unit_certified(r2_sync, now) is True
        finally:
            await async_repo.engine.dispose()
            sync_repo.engine.dispose()

    async def test_load_unit_catalog_past_as_of_uses_historical_version_spans(
        self, tmp_path: Path
    ) -> None:
        """Past-``as_of`` catalog load binds spans of the historical version active at ``as_of``."""
        db_url = f"sqlite+aiosqlite:///{tmp_path / 'past-asof.db'}"
        repo = SQLAlchemyRepository(db_url)
        await repo.create_all()
        sess_id = str(uuid4())
        try:
            async with repo.session() as session:
                sym = Symbol(
                    native_symbol="BTC-USD-PERP",
                    base="BTC",
                    quote="USD",
                    asset_type=AssetTypeEnum.CRYPTO,
                    created_at=_at(0),
                    timestamp=_at(0),
                    known_to=KNOWN_TO_MAX,
                    session_id=sess_id,
                    sequence_id=1,
                )
                session.add(sym)
                await session.flush()
                inst = Instrument(
                    symbol_public_id=sym.public_id,
                    exchange=ExchangeEnum.KRAKEN_FUTURES,
                    timestamp=_at(0),
                    known_to=KNOWN_TO_MAX,
                    session_id=sess_id,
                    sequence_id=2,
                )
                session.add(inst)
                await session.flush()
                inst_id = inst.public_id
                await session.commit()

            await repo.revise_instrument_spec(inst_id, sess_id, 3, _at(0), _spec_input(_at(0)))
            await repo.revise_instrument_spec(inst_id, sess_id, 4, _at(10), _spec_input(_at(10)))
            await repo.revise_instrument_spec(inst_id, sess_id, 5, _at(20), _spec_input(_at(20)))
            await repo.revise_instrument_spec(
                inst_id,
                sess_id,
                6,
                _at(35),
                _spec_input(_at(35), tick_size=0.05, spec_version="s2a-v4:digest-2"),
            )

            lookup = UnitCatalogLookup(
                exchange=ExchangeEnum.KRAKEN_FUTURES,
                native_symbol="BTC-USD-PERP",
            )
            async with repo.session() as session:
                past_fresh = await load_unit_catalog(session, lookup, _at(25))
                past_stale = await load_unit_catalog(session, lookup, _at(33))
            assert past_fresh.spec.tick_size == 0.01
            assert _derivative_catalog_fresh(past_fresh, _at(0), _at(25)) is True
            assert past_stale.spec.tick_size == 0.01
            assert _derivative_catalog_fresh(past_stale, _at(0), _at(33)) is False
        finally:
            await repo.engine.dispose()


@dataclass(frozen=True)
class _RaceConfig:
    """Parameters configuring one synthetic concurrency-test updater instance."""

    db_url: str
    instrument_public_id: str
    observed_at: datetime
    force: bool = False
    on_locked: threading.Event | None = None
    release_lock: threading.Event | None = None


class _RaceUpdater(SymbolUpdaterService[ExchangeClientBase]):
    """Concrete updater used to verify catalog-wide lock serialization and re-check."""

    def __init__(self, config: _RaceConfig) -> None:
        super().__init__(update_threshold_hours=6, force=config.force)
        self.config = config
        self.writes_performed = 0
        self.broadcast_count = 0

    def _create_exchange_client(self) -> ExchangeClientBase:
        """Return a minimal disconnected async stub client."""
        client = MagicMock(spec=ExchangeClientBase)
        client.connect = AsyncMock()
        client.disconnect = AsyncMock()
        return client

    def _get_setting_key(self) -> str:
        """Return a dedicated setting key for concurrency tests."""
        return "kraken_symbols_last_update"

    async def _fetch_symbols(self, client: ExchangeClientBase) -> list[dict[str, object]]:
        """Return one synthetic symbol payload."""
        return [{"symbol": "BTC-USD"}]

    async def broadcast_cache_invalidation(self) -> None:
        """Count cache invalidation broadcasts without ZMQ sockets."""
        self.broadcast_count += 1

    async def _update_database(self, symbols: list[dict[str, object]]) -> None:
        """Acquire lock C, hold optional barrier, revise spec, and commit timestamp."""
        assert self.repository is not None
        with self.repository.get_session() as session:
            now = datetime.now(UTC)
            if not self._enter_locked_update(session, now):
                return
            if self.config.on_locked is not None:
                self.config.on_locked.set()
            if self.config.release_lock is not None:
                assert self.config.release_lock.wait(timeout=5.0)
            self._revise_instrument_spec(
                session,
                self.config.instrument_public_id,
                now,
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence("specs"),
                metadata=InstrumentMetadataInput(
                    tick_size=0.01,
                    lot_size=0.001,
                    min_order_size=0.01,
                    max_order_size=100.0,
                    cost_decimals=2,
                    qty_decimals=3,
                    margin_initial=None,
                    position_limit_long=None,
                    position_limit_short=None,
                    status="active",
                    contract_size=None,
                    quantity_unit="base_asset",
                    spec_source="kraken:ccxt.load_markets",
                    spec_version="s2a-v1:race",
                    spec_observed_at=self.config.observed_at,
                    unit_certified=False,
                ),
            )
            self.writes_performed += 1
            self._finalize_locked_update(session, now)
            session.commit()


def _race_settings(db_url: str) -> AppSettings:
    """Build minimal AppSettings-compatible stub pointing at one SQLite file."""
    stub = MagicMock()
    stub.db_url = db_url
    stub.zmq_broker_xsub = "tcp://127.0.0.1:5555"
    return cast(AppSettings, stub)


def _run_race_pair(
    run_a: _RaceUpdater,
    run_b: _RaceUpdater,
    locked_event: threading.Event,
    release_event: threading.Event,
) -> list[Exception]:
    """Run two updaters concurrently under shared settings patches and return any errors."""
    errors: list[Exception] = []
    settings_stub = _race_settings(run_a.config.db_url)
    run_a.settings = settings_stub
    run_b.settings = settings_stub

    def _target(updater: _RaceUpdater) -> None:
        try:
            asyncio.run(updater.start())
        except Exception as exc:
            errors.append(exc)

    with (
        patch(
            "snapper.application.updaters.symbols.base.get_settings_service",
            new=AsyncMock(return_value=MagicMock()),
        ),
        patch(
            "snapper.application.updaters.symbols.base.get_settings_with_service",
            return_value=settings_stub,
        ),
        patch.object(SymbolUpdaterService, "_setup_zmq"),
        patch.object(SymbolUpdaterService, "_cleanup_zmq"),
    ):
        thread_a = threading.Thread(target=_target, args=(run_a,))
        thread_b = threading.Thread(target=_target, args=(run_b,))
        thread_a.start()
        assert locked_event.wait(timeout=5.0)
        thread_b.start()
        threading.Event().wait(timeout=0.15)
        release_event.set()
        thread_a.join(timeout=10.0)
        thread_b.join(timeout=10.0)
    return errors


class TestSymbolUpdaterLocksAndConcurrency:
    """Catalog-wide lock dialect contracts, early skip paths, and SQLite concurrency."""

    def test_all_updaters_share_single_advisory_lock_identity_on_postgres(self) -> None:
        """Verify every registered symbol updater acquires the same shared catalog lock.

        Given: All five concrete symbol updaters configured with mocked PostgreSQL SyncSessions,
        When: Each updater enters its locked update section,
        Then: Every updater executes pg_advisory_xact_lock with the same catalog-wide identity.
        """
        updaters: tuple[_ConcreteUpdater, ...] = (
            KrakenSymbolUpdaterService(),
            KrakenFuturesSymbolUpdaterService(),
            KrakenEquitiesSymbolUpdaterService(),
            WalutomatSymbolUpdaterService(update_threshold_hours=6),
            PolygonSymbolUpdaterService(),
        )
        observed_params: list[dict[str, str]] = []
        now = _at(0)
        for updater in updaters:
            updater.repository = MagicMock()
            pg_session = MagicMock(spec=SyncSession)
            pg_session.get_bind.return_value.dialect.name = "postgresql"
            assert updater._enter_locked_update(pg_session, now) is True
            sql = str(pg_session.execute.call_args.args[0])
            params = pg_session.execute.call_args.args[1]
            assert "pg_advisory_xact_lock(hashtext(:identity))" in sql
            observed_params.append(params)
        assert SYMBOL_UPDATER_LOCK_IDENTITY == "symbol_updater\x1fcatalog"
        assert all(
            params == {"identity": "symbol_updater\x1fcatalog"} for params in observed_params
        )

    async def test_two_exchanges_map_to_same_postgres_advisory_identity_before_catalog_access(
        self,
    ) -> None:
        """Verify distinct exchange updaters serialize on one shared PostgreSQL lock key.

        Given: Kraken and Polygon symbol updaters with mocked PostgreSQL SyncSessions,
        When: Both run _update_database,
        Then: Both enter READ COMMITTED then acquire pg_advisory_xact_lock before catalog access
            with the same bound identity parameter.
        """
        kraken_updater = KrakenSymbolUpdaterService()
        polygon_updater = PolygonSymbolUpdaterService()
        bound_identities: list[dict[str, str]] = []
        for updater in (kraken_updater, polygon_updater):
            pg_session = MagicMock(spec=SyncSession)
            pg_session.info = {}
            pg_session.get_bind.return_value.dialect.name = "postgresql"
            mock_repo = MagicMock()
            mock_repo.get_session.return_value.__enter__ = MagicMock(return_value=pg_session)
            mock_repo.get_session.return_value.__exit__ = MagicMock(return_value=False)
            updater.repository = mock_repo
            await updater._update_database([])
            iso_call, lock_call = pg_session.execute.call_args_list[:2]
            assert str(iso_call.args[0]) == "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
            assert "pg_advisory_xact_lock(hashtext(:identity))" in str(lock_call.args[0])
            bound_identities.append(lock_call.args[1])
        assert bound_identities[0] == bound_identities[1]
        assert bound_identities[0] == {"identity": "symbol_updater\x1fcatalog"}

    def test_acquire_symbol_updater_lock_sync_dialects_and_guards(self, tmp_path: Path) -> None:
        """Exercise PostgreSQL SQL, SQLite BEGIN IMMEDIATE, TypeError, and unsupported dialect.

        Given: Mocked PostgreSQL, invalid SQLite, unsupported dialect, and real SQLite sessions,
        When: acquire_symbol_updater_lock_sync and _enter_locked_update are invoked,
        Then: PostgreSQL binds the shared catalog identity, SQLite begins immediate, and invalid inputs fail.
        """
        pg_session = MagicMock(spec=SyncSession)
        pg_session.get_bind.return_value.dialect.name = "postgresql"
        DatabaseRepository.acquire_symbol_updater_lock_sync(pg_session)
        sql = str(pg_session.execute.call_args.args[0])
        params = pg_session.execute.call_args.args[1]
        assert "pg_advisory_xact_lock(hashtext(:identity))" in sql
        assert params == {"identity": "symbol_updater\x1fcatalog"}

        bad_sqlite = MagicMock(spec=SyncSession)
        bad_sqlite.get_bind.return_value.dialect.name = "sqlite"
        bad_sqlite.connection.return_value.connection.driver_connection = object()
        with pytest.raises(TypeError, match="sqlite3 connection"):
            DatabaseRepository.acquire_symbol_updater_lock_sync(bad_sqlite)

        unsupported = MagicMock(spec=SyncSession)
        unsupported.get_bind.return_value.dialect.name = "mysql"
        with pytest.raises(NotImplementedError, match="mysql"):
            DatabaseRepository.acquire_symbol_updater_lock_sync(unsupported)

        repo = DatabaseRepository(f"sqlite:///{tmp_path / 'lock.db'}")
        repo.create_all()
        try:
            with repo.get_session() as session:
                DatabaseRepository.acquire_symbol_updater_lock_sync(session)
                DatabaseRepository.acquire_symbol_updater_lock_sync(session)
                session.commit()
        finally:
            repo.engine.dispose()

        updater = KrakenSymbolUpdaterService()
        updater.repository = MagicMock()
        with pytest.raises(TypeError, match="SyncSession"):
            updater._enter_locked_update(MagicMock(), _at(0))

    async def test_all_five_updaters_skip_when_lock_recheck_declines(self, tmp_path: Path) -> None:
        """Each concrete updater exits early without committing when locked re-check returns False."""
        db_url = f"sqlite:///{tmp_path / 'skip-all.db'}"
        repo = DatabaseRepository(db_url)
        repo.create_all()
        now = datetime.now(UTC)
        updaters: tuple[_ConcreteUpdater, ...] = (
            KrakenSymbolUpdaterService(update_threshold_hours=6, force=False),
            KrakenFuturesSymbolUpdaterService(update_threshold_hours=6, force=False),
            KrakenEquitiesSymbolUpdaterService(update_threshold_hours=6, force=False),
            WalutomatSymbolUpdaterService(update_threshold_hours=6, force=False),
            PolygonSymbolUpdaterService(update_threshold_hours=6, force=False),
        )
        try:
            with repo.get_session() as session:
                for idx, updater in enumerate(updaters, start=1):
                    session.add(
                        Setting(
                            key=updater._get_setting_key(),
                            value=now.isoformat(),
                            category="system",
                            description="recent",
                            timestamp=now,
                            known_to=KNOWN_TO_MAX,
                            session_id=str(uuid4()),
                            sequence_id=idx,
                        )
                    )
                session.commit()
            for updater in updaters:
                updater.repository = repo
                updater._in_managed_run = True
                updater._last_update_skipped = False
                await updater._update_database([])
                assert updater._last_update_skipped is True
        finally:
            repo.engine.dispose()

    def test_sqlite_concurrent_managed_updaters_serialize_and_recheck(self, tmp_path: Path) -> None:
        """A non-forced concurrent waiter skips while a forced waiter serializes cleanly."""
        db_url = f"sqlite:///{tmp_path / 'race.db'}"
        setup_repo = DatabaseRepository(db_url)
        setup_repo.create_all()
        inst_id = str(uuid4())
        setup_repo.engine.dispose()

        locked_event = threading.Event()
        release_event = threading.Event()
        t_first = datetime.now(UTC)
        t_second = t_first + timedelta(minutes=5)
        t_third = t_first + timedelta(minutes=10)

        run_a = _RaceUpdater(
            _RaceConfig(
                db_url=db_url,
                instrument_public_id=inst_id,
                observed_at=t_first,
                force=False,
                on_locked=locked_event,
                release_lock=release_event,
            )
        )
        run_b = _RaceUpdater(
            _RaceConfig(db_url=db_url, instrument_public_id=inst_id, observed_at=t_second)
        )
        assert _run_race_pair(run_a, run_b, locked_event, release_event) == []
        assert run_a.writes_performed == 1
        assert run_a.broadcast_count == 1
        assert run_b.writes_performed == 0
        assert run_b.broadcast_count == 0
        assert run_b._last_update_skipped is True

        run_c = _RaceUpdater(
            _RaceConfig(
                db_url=db_url, instrument_public_id=inst_id, observed_at=t_third, force=True
            )
        )
        run_c_settings = _race_settings(db_url)
        run_c.settings = run_c_settings
        with (
            patch(
                "snapper.application.updaters.symbols.base.get_settings_service",
                new=AsyncMock(return_value=MagicMock()),
            ),
            patch(
                "snapper.application.updaters.symbols.base.get_settings_with_service",
                return_value=run_c_settings,
            ),
            patch.object(SymbolUpdaterService, "_setup_zmq"),
            patch.object(SymbolUpdaterService, "_cleanup_zmq"),
        ):
            asyncio.run(run_c.start())
        assert run_c.writes_performed == 1
        assert run_c.broadcast_count == 1

        verify_repo = DatabaseRepository(db_url)
        try:
            with verify_repo.get_session() as session:
                specs = (
                    session.execute(
                        select(InstrumentSpec).where(InstrumentSpec.instrument_public_id == inst_id)
                    )
                    .scalars()
                    .all()
                )
                spans = (
                    session.execute(
                        select(InstrumentSpecObservationSpan).where(
                            InstrumentSpecObservationSpan.instrument_public_id == inst_id
                        )
                    )
                    .scalars()
                    .all()
                )
            assert len(specs) == 1
            assert specs[0].spec_observed_at == t_first
            assert len(spans) == 1
            assert spans[0].first_observed_at == t_first
            assert spans[0].last_observed_at == t_third
        finally:
            verify_repo.engine.dispose()


def _pg_urls() -> tuple[str, str]:
    """Resolve optional PostgreSQL async and sync URLs from ``PNL_TEST_POSTGRES_URL``."""
    raw = os.environ.get("PNL_TEST_POSTGRES_URL", "").strip()
    if not raw:
        pytest.skip("PNL_TEST_POSTGRES_URL is not set; skipping native PostgreSQL witness test")
    parsed = make_url(raw)
    async_url = parsed.set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)
    sync_url = parsed.set(drivername="postgresql+psycopg2").render_as_string(hide_password=False)
    return async_url, sync_url


@pytest.fixture
async def pg_repos() -> AsyncIterator[tuple[SQLAlchemyRepository, DatabaseRepository]]:
    """Provide initialized async and sync repositories against the opt-in PostgreSQL witness."""
    async_url, sync_url = _pg_urls()
    async_repo = SQLAlchemyRepository(async_url)
    sync_repo = DatabaseRepository(sync_url)
    await async_repo.create_all()
    try:
        yield async_repo, sync_repo
    finally:
        await async_repo.engine.dispose()
        sync_repo.engine.dispose()


def _run_pg_lock_race(
    sync_repo: DatabaseRepository,
    race_inst: str,
    times: tuple[datetime, datetime],
) -> tuple[dict[str, str], list[Exception]]:
    """Run two PostgreSQL threads competing on ``pg_advisory_xact_lock``."""
    locked_event = threading.Event()
    release_event = threading.Event()
    t1, t2 = times
    results: dict[str, str] = {}
    errors: list[Exception] = []

    def _holder() -> None:
        try:
            with sync_repo.get_session() as session:
                DatabaseRepository.acquire_symbol_updater_lock_sync(session)
                locked_event.set()
                if not release_event.wait(timeout=5.0):
                    raise TimeoutError("Timed out waiting for release_event in _holder")
                results["holder"] = DatabaseRepository.revise_instrument_spec_sync(
                    session, race_inst, str(uuid4()), 1, t1, _spec_input(t1)
                )
                session.commit()
        except Exception as exc:
            errors.append(exc)

    def _waiter() -> None:
        try:
            with sync_repo.get_session() as session:
                DatabaseRepository.acquire_symbol_updater_lock_sync(session)
                results["waiter"] = DatabaseRepository.revise_instrument_spec_sync(
                    session, race_inst, str(uuid4()), 2, t2, _spec_input(t2)
                )
                session.commit()
        except Exception as exc:
            errors.append(exc)

    holder_thread = threading.Thread(target=_holder)
    waiter_thread = threading.Thread(target=_waiter)
    holder_thread.start()
    assert locked_event.wait(timeout=5.0)
    waiter_thread.start()
    threading.Event().wait(timeout=0.15)
    release_event.set()
    holder_thread.join(timeout=10.0)
    waiter_thread.join(timeout=10.0)
    return results, errors


class TestPostgresObservationSpansAndConcurrency:
    """Opt-in PostgreSQL witness tests for span writes and advisory-lock serialization."""

    async def test_pg_value_versioned_revision_and_concurrent_updater_race(
        self,
        pg_repos: tuple[SQLAlchemyRepository, DatabaseRepository],
    ) -> None:
        """Verify PostgreSQL span writes, UUID types, and ``pg_advisory_xact_lock`` race."""
        async_repo, sync_repo = pg_repos
        inst_id = str(uuid4())
        sess_id = str(uuid4())
        first_id = await async_repo.revise_instrument_spec(
            inst_id, sess_id, 1, _at(0), _spec_input(_at(0))
        )
        assert (
            await async_repo.revise_instrument_spec(
                inst_id, sess_id, 2, _at(5), _spec_input(_at(5))
            )
            == first_id
        )
        loaded = await async_repo.get_instrument_spec(inst_id, _at(13))
        assert loaded is not None
        assert loaded["spec_observed_at"] == _at(0)
        assert is_effective_unit_certified(loaded, _at(13)) is True
        assert latest_spec_observation_at(loaded) == _at(5)

        race_inst = str(uuid4())
        results, errors = _run_pg_lock_race(
            sync_repo,
            race_inst,
            (_at(1), _at(2)),
        )
        assert errors == []
        assert results == {"holder": "created", "waiter": "unchanged"}

        race_spec = await async_repo.get_instrument_spec(race_inst, _at(10))
        assert race_spec is not None
        assert race_spec["spec_observed_at"] == _at(1)
        assert latest_spec_observation_at(race_spec) == _at(2)
