"""Tests for the async P&L timeline orchestrator + mark builder (Phase 5A).

Covers the seam between the repository reads and the pure builder: zero-safe
flow mapping, canonical-source PAPER marks, batched candle loading, total-work
budgeting, durable fill-gap withholding, and end-to-end orchestration.
"""

import math
from collections.abc import AsyncIterator
from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine

from snapper.application.portfolio.fx_rates import convert_amount
from snapper.application.portfolio.fx_rates import currency_pair_key
from snapper.application.portfolio.pnl_timeline_service import PNL_TIMELINE_CALC_VERSION
from snapper.application.portfolio.pnl_timeline_service import PNL_TIMELINE_MARK_SOURCE
from snapper.application.portfolio.pnl_timeline_service import PNL_TIMELINE_MARKER_LIMIT
from snapper.application.portfolio.pnl_timeline_service import PNL_TIMELINE_MAX_WORK_UNITS
from snapper.application.portfolio.pnl_timeline_service import PnlAiDecisionMarker
from snapper.application.portfolio.pnl_timeline_service import PnlFillMarker
from snapper.application.portfolio.pnl_timeline_service import PnlSignalMarker
from snapper.application.portfolio.pnl_timeline_service import PnlTimelineWorkBudgetError
from snapper.application.portfolio.pnl_timeline_service import _build_execution_lineage
from snapper.application.portfolio.pnl_timeline_service import _to_timeline_accrual
from snapper.application.portfolio.pnl_timeline_service import _to_timeline_execution
from snapper.application.portfolio.pnl_timeline_service import build_fx_rates
from snapper.application.portfolio.pnl_timeline_service import build_marks
from snapper.application.portfolio.pnl_timeline_service import build_wallet_pnl_series
from snapper.application.portfolio.pnl_timeline_service import build_wallet_pnl_timeline
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Candle
from snapper.data.models import Instrument
from snapper.data.models import Symbol
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import InstrumentSymbolRefRow
from snapper.data.repository_types import PnlFxRatePlane
from snapper.data.repository_types import PnlFxRateRow
from snapper.data.repository_types import PnlTimelineAccrualRow
from snapper.data.repository_types import PnlTimelineAiDecisionMarkerRow
from snapper.data.repository_types import PnlTimelineCandleRow
from snapper.data.repository_types import PnlTimelineExecutionLineageRow
from snapper.data.repository_types import PnlTimelineExecutionRow
from snapper.data.repository_types import PnlTimelineSignalMarkerRow

_T0 = datetime(2026, 7, 20, 10, 0, tzinfo=UTC)
_I1 = "00000000-0000-7000-8000-000000000b01"
_I2 = "00000000-0000-7000-8000-000000000b02"
_FX_SESSION = "00000000-0000-7000-8000-000000000b03"


def _m(minute: int) -> datetime:
    """Return the grid minute ``_T0 + minute``."""
    return _T0 + timedelta(minutes=minute)


@pytest.fixture()
async def conflicting_fx_repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create one pre-revision GBP-PLN rate whose later quote conflicts."""
    db_path = tmp_path / "pnl-service-fx.db"
    schema_engine = create_engine(f"sqlite:///{db_path}")
    Symbol.__table__.create(schema_engine)
    Instrument.__table__.create(schema_engine)
    Candle.__table__.create(schema_engine)
    schema_engine.dispose()
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    revision_at = _m(1)
    async with repository.session() as session:
        session.add_all(
            [
                Symbol(
                    public_id="sym-gbpx-service",
                    native_symbol="GBP-X",
                    base="GBP",
                    quote="PLN",
                    asset_type="forex",
                    created_at=_m(-2),
                    timestamp=_m(-2),
                    known_to=revision_at,
                    session_id=_FX_SESSION,
                    sequence_id=1,
                ),
                Symbol(
                    public_id="sym-gbpx-service",
                    native_symbol="GBP-X",
                    base="GBP",
                    quote="USD",
                    asset_type="forex",
                    created_at=_m(-2),
                    timestamp=revision_at,
                    known_to=KNOWN_TO_MAX,
                    session_id=_FX_SESSION,
                    sequence_id=2,
                ),
                Instrument(
                    public_id="ins-gbpx-service",
                    symbol_public_id="sym-gbpx-service",
                    exchange="walutomat",
                    requires_ai_review=False,
                    timestamp=_m(-2),
                    known_to=KNOWN_TO_MAX,
                    session_id=_FX_SESSION,
                    sequence_id=1,
                ),
                Candle(
                    instrument_public_id="ins-gbpx-service",
                    open_at=_m(-1),
                    timeframe="1m",
                    open=5.0,
                    high=5.0,
                    low=5.0,
                    close=5.0,
                    volume=1.0,
                    source="native",
                    complete=True,
                    timestamp=_m(-1),
                    known_to=KNOWN_TO_MAX,
                    session_id=_FX_SESSION,
                    sequence_id=1,
                ),
            ]
        )
        await session.commit()
    try:
        yield repository
    finally:
        await repository.engine.dispose()


def _exec_row(
    instrument: str,
    scope: int,
    minute: int,
    side: str,
    size: float,
    price: float,
    fee: float,
    fee_asset: str,
    exchange: str = "kraken",
) -> PnlTimelineExecutionRow:
    """Build one execution row as ``get_pnl_timeline_executions`` returns it."""
    return {
        "public_id": f"execution-{instrument}-{scope}",
        "instrument_public_id": instrument,
        "exchange": exchange,
        "scope_sequence": scope,
        "order_public_id": f"order-{instrument}-{scope}",
        "side": side,
        "status": "filled",
        "size": size,
        "price": price,
        "fee": fee,
        "fee_asset": fee_asset,
        "executed_at": None,
        "timestamp": _m(minute),
        "exec_id": f"exec-{scope}",
        "trade_id": f"trade-{scope}",
    }


def _lineage_row(
    order_public_id: str,
    source_surface: str | None,
    plan_public_id: str | None = None,
    signal_public_id: str | None = None,
    origin: str | None = "live",
    strategy_name: str | None = None,
) -> PnlTimelineExecutionLineageRow:
    """Build one row as the exact execution-lineage read returns it."""
    return {
        "order_public_id": order_public_id,
        "source_surface": source_surface,
        "plan_public_id": plan_public_id,
        "signal_public_id": signal_public_id,
        "origin": origin,
        "strategy_name": strategy_name,
    }


def _signal_row(
    public_id: str,
    marker_time: datetime,
    has_execution: bool,
    instrument: str = _I1,
    price: float | None = 101.0,
) -> PnlTimelineSignalMarkerRow:
    """Build one independently sourced signal-marker row."""
    return {
        "public_id": public_id,
        "instrument_public_id": instrument,
        "fired_at": marker_time,
        "side": "buy",
        "strategy_name": "momentum",
        "strength": 0.8,
        "reason": "breakout",
        "price": price,
        "has_execution": has_execution,
    }


def _ai_decision_row(
    public_id: str,
    marker_time: datetime,
    decision: str | int | None,
    new_status: str,
    has_execution: bool,
    rationale: str | int | None = "reviewed",
) -> PnlTimelineAiDecisionMarkerRow:
    """Build one append-only AI decision-event marker row."""
    return {
        "event_public_id": public_id,
        "review_public_id": f"review-{public_id}",
        "instrument_public_id": _I1,
        "strategy_public_id": "strategy-1",
        "occurred_at": marker_time,
        "new_status": new_status,
        "payload": {"decision": decision, "rationale": rationale},
        "has_execution": has_execution,
    }


def _accrual_row(
    instrument: str,
    minute: int,
    amount: float,
    amount_asset: str,
) -> PnlTimelineAccrualRow:
    """Build one accrual row as ``get_accruals_for_pnl`` returns it."""
    return {
        "instrument_public_id": instrument,
        "exchange": "kraken",
        "mode": "live",
        "accrual_type": "funding",
        "accrued_at": _m(minute),
        "amount": amount,
        "amount_asset": amount_asset,
    }


def _ref(
    instrument: str,
    native_symbol: str,
    quote: str | None,
    exchange: str = "kraken",
    instrument_exchange: str | None = None,
    valid_from: datetime = _T0 - timedelta(days=1),
    valid_to: datetime = datetime.max.replace(tzinfo=UTC),
    base: str | None = None,
) -> InstrumentSymbolRefRow:
    """Build one symbol reference row as ``get_instrument_symbol_refs`` returns it."""
    return {
        "instrument_public_id": instrument,
        "native_symbol": native_symbol,
        "exchange": exchange,
        "instrument_exchange": (exchange if instrument_exchange is None else instrument_exchange),
        "base_currency": native_symbol.split("-", maxsplit=1)[0] if base is None else base,
        "quote_currency": quote,
        "valid_from": valid_from,
        "valid_to": valid_to,
    }


def _candle(
    open_at: datetime,
    close: float | None,
    instrument: str = _I1,
) -> PnlTimelineCandleRow:
    """Build one row as the batched timeline candle read returns it."""
    return {
        "instrument_public_id": instrument,
        "open_at": open_at,
        "close": close,
    }


def _fx_row(
    base: str, quote: str, minute: int, close: float, exchange: str = "kraken"
) -> PnlFxRateRow:
    """Build one FX candle row whose bar CLOSES at grid minute ``minute``."""
    return {
        "base": base,
        "quote": quote,
        "exchange": exchange,
        "open_at": _m(minute - 1),
        "close": close,
    }


class FakeRepo:
    """Minimal repository double exposing only the reads the service uses."""

    def __init__(
        self,
        executions: Sequence[PnlTimelineExecutionRow] = (),
        accruals: Sequence[PnlTimelineAccrualRow] = (),
        refs: Sequence[InstrumentSymbolRefRow] = (),
        candles: Sequence[PnlTimelineCandleRow] = (),
        signals: Sequence[PnlTimelineSignalMarkerRow] = (),
        ai_decisions: Sequence[PnlTimelineAiDecisionMarkerRow] = (),
        lineage: Sequence[PnlTimelineExecutionLineageRow] = (),
        fill_shard_keys: Sequence[str] = (),
        gapped_shards: set[str] | None = None,
        fx_rows: Sequence[PnlFxRateRow] | None = None,
        fx_repository: SQLAlchemyRepository | None = None,
    ) -> None:
        """Store the canned read results and record the calls made."""
        self._executions = list(executions)
        self._accruals = list(accruals)
        self._refs = list(refs)
        self._candles = list(candles)
        self._signals = list(signals)
        self._ai_decisions = list(ai_decisions)
        self._lineage = list(lineage)
        self._fill_shard_keys = list(fill_shard_keys)
        self._gapped_shards = gapped_shards or set()
        self._fx_rows: list[PnlFxRateRow] = list(fx_rows or [])
        self._fx_repository = fx_repository
        self.fx_pair_calls: list[list[tuple[str, str]]] = []
        self.fx_plane_calls: list[list[PnlFxRatePlane]] = []
        self.fx_candidate_pair_calls: list[list[tuple[str, str]]] = []
        self.fx_candidate_range_calls: list[tuple[datetime, datetime]] = []
        self.fx_range_calls: list[tuple[datetime, datetime]] = []
        self.symbol_ref_calls: list[list[str]] = []
        self.candle_calls: list[
            tuple[list[InstrumentSymbolRefRow], datetime, datetime, datetime]
        ] = []
        self.fill_scope_calls: list[tuple[str, str, datetime]] = []
        self.fill_gap_calls: list[tuple[str, str, str, datetime]] = []
        self.execution_calls: list[tuple[str, str, datetime]] = []
        self.lineage_calls: list[tuple[list[str], datetime]] = []
        self.accrual_calls: list[tuple[str, str, datetime]] = []
        self.symbol_ref_as_of_calls: list[datetime] = []
        self.signal_calls: list[tuple[str, str, datetime, datetime, datetime, int]] = []
        self.ai_decision_calls: list[tuple[str, str, datetime, datetime, datetime, int]] = []

    async def get_fill_shard_keys_for_scope(
        self,
        wallet_public_id: str,
        mode: str,
        as_of: datetime,
    ) -> list[str]:
        """Record the exact scope and return its fill-bearing shards."""
        self.fill_scope_calls.append((wallet_public_id, mode, as_of))
        return list(self._fill_shard_keys)

    async def pnl_timeline_shard_has_fill_gap(
        self,
        shard_key: str,
        wallet_public_id: str,
        mode: str,
        as_of: datetime,
    ) -> bool:
        """Record the evidence lookup and return its canned gap status."""
        self.fill_gap_calls.append((shard_key, wallet_public_id, mode, as_of))
        return shard_key in self._gapped_shards

    async def get_pnl_timeline_executions(
        self, wallet_public_id: str, mode: str, as_of: datetime
    ) -> list[PnlTimelineExecutionRow]:
        """Return the canned execution rows."""
        self.execution_calls.append((wallet_public_id, mode, as_of))
        return list(self._executions)

    async def get_pnl_timeline_execution_lineage(
        self,
        order_public_ids: Sequence[str],
        as_of: datetime,
    ) -> list[PnlTimelineExecutionLineageRow]:
        """Record the exact order scope and return its canned lineage rows."""
        requested = set(order_public_ids)
        self.lineage_calls.append((list(order_public_ids), as_of))
        return [row for row in self._lineage if row["order_public_id"] in requested]

    async def get_pnl_timeline_signals(
        self,
        wallet_public_id: str,
        mode: str,
        from_time: datetime,
        to_time: datetime,
        as_of: datetime,
        limit: int,
    ) -> list[PnlTimelineSignalMarkerRow]:
        """Record one bounded signal read and return its newest rows."""
        self.signal_calls.append((wallet_public_id, mode, from_time, to_time, as_of, limit))
        return list(self._signals[:limit])

    async def get_pnl_timeline_ai_decisions(
        self,
        wallet_public_id: str,
        mode: str,
        from_time: datetime,
        to_time: datetime,
        as_of: datetime,
        limit: int,
    ) -> list[PnlTimelineAiDecisionMarkerRow]:
        """Record one bounded AI-event read and return its newest rows."""
        self.ai_decision_calls.append((wallet_public_id, mode, from_time, to_time, as_of, limit))
        return list(self._ai_decisions[:limit])

    async def get_accruals_for_pnl(
        self, wallet_public_id: str, mode: str, as_of: datetime
    ) -> list[PnlTimelineAccrualRow]:
        """Return the canned accrual rows."""
        self.accrual_calls.append((wallet_public_id, mode, as_of))
        return list(self._accruals)

    async def get_instrument_symbol_refs(
        self, instrument_public_ids: Sequence[str], as_of: datetime
    ) -> list[InstrumentSymbolRefRow]:
        """Record the requested ids and return the matching refs."""
        self.symbol_ref_calls.append(list(instrument_public_ids))
        self.symbol_ref_as_of_calls.append(as_of)
        requested = set(instrument_public_ids)
        return [ref for ref in self._refs if ref["instrument_public_id"] in requested]

    async def get_pnl_timeline_candles(
        self,
        refs: Sequence[InstrumentSymbolRefRow],
        start: datetime,
        end: datetime,
        as_of: datetime,
    ) -> list[PnlTimelineCandleRow]:
        """Record one batched read and return candles for the requested ids."""
        ref_list = list(refs)
        self.candle_calls.append((ref_list, start, end, as_of))
        requested = {ref["instrument_public_id"] for ref in refs}
        return [candle for candle in self._candles if candle["instrument_public_id"] in requested]

    async def get_pnl_fx_rate_exchanges(
        self,
        pairs: Sequence[tuple[str, str]],
        start: datetime,
        end: datetime,
        as_of: datetime,
    ) -> list[PnlFxRatePlane]:
        """Discover venue-qualified FX planes with bounded canned evidence."""
        self.fx_candidate_pair_calls.append(list(pairs))
        self.fx_candidate_range_calls.append((start, end))
        if self._fx_repository is not None:
            return await self._fx_repository.get_pnl_fx_rate_exchanges(
                pairs,
                start,
                end,
                as_of,
            )
        requested = set(pairs)
        return sorted(
            {
                (row["base"], row["quote"], row["exchange"])
                for row in self._fx_rows
                if (row["base"], row["quote"]) in requested and start <= row["open_at"] <= end
            }
        )

    async def get_pnl_fx_rate_candles(
        self,
        planes: Sequence[PnlFxRatePlane],
        start: datetime,
        end: datetime,
        as_of: datetime,
    ) -> list[PnlFxRateRow]:
        """Record exact venue planes and return their bounded canned candles."""
        plane_list = list(planes)
        self.fx_plane_calls.append(plane_list)
        self.fx_pair_calls.append([(base, quote) for base, quote, _ in plane_list])
        self.fx_range_calls.append((start, end))
        if self._fx_repository is not None:
            return await self._fx_repository.get_pnl_fx_rate_candles(
                planes,
                start,
                end,
                as_of,
            )
        requested = set(planes)
        return [
            row
            for row in self._fx_rows
            if (row["base"], row["quote"], row["exchange"]) in requested
            and start <= row["open_at"] <= end
        ]


class TestToTimelineExecution:
    """Cover the execution row mapping and fee currency discipline."""

    def test_maps_fields_and_uses_timestamp_as_event_time(self) -> None:
        """Row fields map through and ``event_time`` is the timestamp axis."""
        row = _exec_row(_I1, 1, 3, "buy", 2.0, 100.0, 0.5, "USD")
        mapped = _to_timeline_execution(row, "USD", {})
        assert mapped.instrument_public_id == _I1
        assert mapped.order_public_id == f"order-{_I1}-1"
        assert mapped.event_time == _m(3)
        assert mapped.size == 2.0
        assert mapped.price == 100.0
        assert mapped.fee == 0.5
        assert mapped.fee_asset == "USD"

    def test_non_valuation_currency_fee_is_unknown_not_zero(self) -> None:
        """A fee in another asset is UNKNOWN (NaN), never a silent zero.

        Substituting zero would understate a real cost and fabricate a favourable
        P&L; NaN routes through the builder's finiteness guard so the point is
        withheld instead (the deferred-FX stance).
        """
        row = _exec_row(_I2, 1, 3, "buy", 2.0, 100.0, 0.9, "EUR")
        mapped = _to_timeline_execution(row, "USD", {})
        assert math.isnan(mapped.fee)
        assert mapped.fee_asset == "EUR"

    def test_exact_zero_fee_is_currency_invariant_with_empty_asset(self) -> None:
        """Production's fee-free empty asset maps to a real zero, not NaN."""
        row = _exec_row(_I1, 1, 3, "buy", 2.0, 100.0, 0.0, "")
        mapped = _to_timeline_execution(row, "USD", {})
        assert mapped.fee == 0.0
        assert mapped.fee_asset == ""


class TestBuildExecutionLineage:
    """Cover fail-closed projection of repository lineage candidates."""

    def test_maps_each_unique_order_candidate(self) -> None:
        """A unique row preserves every raw discriminator for the pure engine."""
        manual_order = f"order-{_I1}-1"
        system_order = f"order-{_I1}-2"
        mapped = _build_execution_lineage(
            [
                _lineage_row(manual_order, "rest", plan_public_id="manual-plan"),
                _lineage_row(
                    system_order,
                    "strategy",
                    signal_public_id="signal-1",
                    strategy_name="momentum",
                ),
            ]
        )
        assert set(mapped) == {manual_order, system_order}
        assert mapped[manual_order].source_surface == "rest"
        assert mapped[manual_order].plan_public_id == "manual-plan"
        assert mapped[manual_order].signal_public_id is None
        assert mapped[manual_order].origin == "live"
        assert mapped[manual_order].strategy_name is None
        assert mapped[system_order].source_surface == "strategy"
        assert mapped[system_order].signal_public_id == "signal-1"
        assert mapped[system_order].strategy_name == "momentum"

    def test_omits_every_repeated_order_candidate(self) -> None:
        """Duplicate rows never acquire lineage through first-row selection."""
        ambiguous_order = f"order-{_I1}-1"
        mapped = _build_execution_lineage(
            [
                _lineage_row(ambiguous_order, "rest", plan_public_id="plan-1"),
                _lineage_row(
                    ambiguous_order,
                    "strategy",
                    signal_public_id="signal-1",
                    strategy_name="momentum",
                ),
                _lineage_row(ambiguous_order, "rest", plan_public_id="plan-1"),
            ]
        )
        assert mapped == {}


class TestToTimelineAccrual:
    """Cover the accrual mapping and currency-invariant exact zero."""

    def test_valuation_currency_amount_maps_through(self) -> None:
        """A nonzero valuation-currency accrual remains finite."""
        mapped = _to_timeline_accrual(_accrual_row(_I1, 1, 3.0, "USD"), "USD", {})
        assert mapped.instrument_public_id == _I1
        assert mapped.accrued_at == _m(1)
        assert mapped.amount_usd == 3.0

    def test_exact_zero_is_currency_invariant(self) -> None:
        """A zero accrual needs no FX conversion even with a foreign asset."""
        mapped = _to_timeline_accrual(_accrual_row(_I1, 1, 0.0, "EUR"), "USD", {})
        assert mapped.amount_usd == 0.0

    def test_nonzero_foreign_amount_is_unknown(self) -> None:
        """A nonzero foreign accrual maps to NaN for builder withholding."""
        mapped = _to_timeline_accrual(_accrual_row(_I1, 1, 3.0, "EUR"), "USD", {})
        assert math.isnan(mapped.amount_usd)


class TestBuildMarks:
    """Cover the mark builder's quote gate and open_at keying."""

    async def test_only_marks_matching_quote_currency(self) -> None:
        """Only a USD-quote instrument produces marks; EUR and null are skipped."""
        refs = [
            _ref(_I1, "BTC-USD", "USD"),
            _ref(_I2, "BTC-EUR", "EUR"),
            _ref("i3", "SPX", None),
        ]
        repo = FakeRepo(candles=[_candle(_m(-1), 105.0)])
        marks = await build_marks(repo, refs, _T0, _m(2), _T0, "USD")
        assert marks == {(_I1, _T0): 105.0}
        assert len(repo.candle_calls) == 1
        assert repo.candle_calls[0][0] == [refs[0]]

    async def test_no_matching_quote_skips_the_batch_read(self) -> None:
        """A wholly foreign ref set yields no marks without querying candles."""
        refs = [_ref(_I1, "BTC-EUR", "EUR"), _ref(_I2, "SPX", None)]
        repo = FakeRepo()
        marks = await build_marks(repo, refs, _T0, _m(2), _T0, "USD")
        assert marks == {}
        assert repo.candle_calls == []

    async def test_keys_mark_one_minute_after_open_at(self) -> None:
        """A bar covering [M-1m, M) is keyed at minute M (no look-ahead)."""
        refs = [_ref(_I1, "BTC-USD", "USD")]
        candles = [_candle(_m(-1), 100.0), _candle(_m(0), 110.0), _candle(_m(1), 120.0)]
        repo = FakeRepo(candles=candles)
        marks = await build_marks(repo, refs, _T0, _m(2), _T0, "USD")
        assert marks == {(_I1, _m(0)): 100.0, (_I1, _m(1)): 110.0, (_I1, _m(2)): 120.0}

    @pytest.mark.parametrize(
        "bad_close",
        [0.0, -5.0, None, float("nan"), float("inf")],
    )
    async def test_non_positive_or_non_finite_close_is_omitted(
        self, bad_close: float | None
    ) -> None:
        """An invalid candle close leaves no mark-map key behind."""
        refs = [_ref(_I1, "BTC-USD", "USD")]
        repo = FakeRepo(candles=[_candle(_m(-1), bad_close)])
        marks = await build_marks(repo, refs, _T0, _T0, _T0, "USD")
        assert marks == {}

    async def test_candle_read_window_floors_from_and_backs_one_minute(self) -> None:
        """The candle range starts one minute before the floored grid start."""
        refs = [_ref(_I1, "BTC-USD", "USD")]
        repo = FakeRepo()
        await build_marks(repo, refs, _T0 + timedelta(seconds=30), _m(2), _T0, "USD")
        call = repo.candle_calls[0]
        assert call[1] == _m(-1)
        assert call[2] == _m(2)
        assert call[3] == _T0


class TestBuildWalletPnlSeries:
    """Cover the end-to-end orchestration."""

    async def test_end_to_end_series_with_marks(self) -> None:
        """A single USD instrument produces complete points with the expected net."""
        executions = [_exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.5, "USD")]
        lineage = [
            _lineage_row(
                executions[0]["order_public_id"],
                "strategy",
                signal_public_id="signal-1",
                strategy_name="momentum",
            )
        ]
        refs = [_ref(_I1, "BTC-USD", "USD")]
        candles = [_candle(_m(-1), 105.0), _candle(_m(0), 110.0), _candle(_m(1), 120.0)]
        repo = FakeRepo(
            executions=executions,
            lineage=lineage,
            refs=refs,
            candles=candles,
            fill_shard_keys=["clean-shard"],
        )
        as_of = _m(3)
        result = await build_wallet_pnl_series(repo, "w1", "live", _T0, _m(2), "1m", as_of)
        assert result.granularity == "1m"
        assert result.valuation_ccy == "USD"
        assert [p.valuation_status for p in result.points] == ["complete"] * 3
        assert result.points[0].realized_pnl == 0.0
        assert result.points[0].fee_pnl == -0.5
        assert result.points[0].unrealized_pnl == 5.0
        assert result.points[0].net_pnl == 4.5
        assert result.points[2].unrealized_pnl == 20.0
        assert result.points[2].net_pnl == 19.5
        assert len(result.points[0].attribution) == 1
        attribution = result.points[0].attribution[0]
        assert attribution.origin == "system"
        assert attribution.strategy_name == "momentum"
        assert attribution.fee_pnl == -0.5
        assert attribution.unrealized_pnl == 5.0
        assert repo.fill_scope_calls == [("w1", "live", as_of)]
        assert repo.fill_gap_calls == [("clean-shard", "w1", "live", as_of)]
        assert repo.execution_calls == [("w1", "live", as_of)]
        assert repo.lineage_calls == [([executions[0]["order_public_id"]], as_of)]
        assert repo.accrual_calls == [("w1", "live", as_of)]
        assert repo.symbol_ref_as_of_calls == [as_of]
        assert repo.candle_calls[0][3] == as_of

    async def test_empty_asset_zero_fee_keeps_the_series_complete(self) -> None:
        """A fee-free production fill cannot poison an otherwise complete series."""
        executions = [_exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.0, "")]
        refs = [_ref(_I1, "BTC-USD", "USD")]
        candles = [_candle(_m(-1), 100.0), _candle(_m(0), 100.0)]
        repo = FakeRepo(executions=executions, refs=refs, candles=candles)
        result = await build_wallet_pnl_series(repo, "w1", "live", _T0, _m(1), "1m", _T0)
        assert [point.valuation_status for point in result.points] == ["complete", "complete"]
        assert [point.fee_pnl for point in result.points] == [0.0, 0.0]
        assert [point.net_pnl for point in result.points] == [0.0, 0.0]

    @pytest.mark.parametrize("bad_close", [0.0, -5.0])
    async def test_non_positive_mark_close_is_mark_incomplete(self, bad_close: float) -> None:
        """A non-positive mark withholds stock values but keeps cumulatives."""
        executions = [_exec_row(_I1, 1, 0, "buy", 10.0, 100.0, 0.0, "USD")]
        repo = FakeRepo(
            executions=executions,
            refs=[_ref(_I1, "BTC-USD", "USD")],
            candles=[_candle(_m(-1), bad_close)],
        )
        result = await build_wallet_pnl_series(repo, "w1", "live", _T0, _T0, "1m", _T0)
        point = result.points[0]
        assert point.valuation_status == "incomplete"
        assert (
            point.realized_pnl,
            point.fee_pnl,
            point.accrual_pnl,
            point.unrealized_pnl,
            point.net_pnl,
        ) == (0.0, 0.0, 0.0, None, None)

    async def test_nonzero_foreign_fee_withholds_the_series(self) -> None:
        """A nonzero foreign fee remains unknown and cannot become a zero cost."""
        executions = [_exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.5, "EUR")]
        refs = [_ref(_I1, "BTC-USD", "USD")]
        candles = [_candle(_m(-1), 100.0), _candle(_m(0), 100.0)]
        repo = FakeRepo(executions=executions, refs=refs, candles=candles)
        result = await build_wallet_pnl_series(repo, "w1", "live", _T0, _m(1), "1m", _T0)
        assert [point.valuation_status for point in result.points] == [
            "incomplete",
            "incomplete",
        ]
        assert all(point.fee_pnl is None for point in result.points)
        assert all(point.net_pnl is None for point in result.points)

    @pytest.mark.parametrize("direct_close", [0.0, -1.25])
    async def test_non_positive_direct_fx_fee_withholds_series(self, direct_close: float) -> None:
        """A zero or negative direct FX close cannot erase or reverse a real fee."""
        executions = [_exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.04, "EUR")]
        repo = FakeRepo(
            executions=executions,
            refs=[_ref(_I1, "BTC-USD", "USD")],
            candles=[_candle(_m(-1), 100.0)],
            fx_rows=[_fx_row("EUR", "USD", 0, direct_close)],
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _T0,
            "1m",
            _T0,
        )
        point = result.points[0]
        assert point.valuation_status == "incomplete"
        assert point.fee_pnl is None
        assert point.net_pnl is None

    async def test_valuation_currency_accrual_becomes_accrual_pnl(self) -> None:
        """A USD accrual becomes accrual P&L with the holder-pays sign."""
        executions = [_exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.0, "USD")]
        accruals = [_accrual_row(_I1, 1, 3.0, "USD")]
        refs = [_ref(_I1, "BTC-USD", "USD")]
        candles = [_candle(_m(-1), 100.0), _candle(_m(0), 100.0), _candle(_m(1), 100.0)]
        repo = FakeRepo(
            executions=executions,
            accruals=accruals,
            refs=refs,
            candles=candles,
        )
        result = await build_wallet_pnl_series(repo, "w1", "live", _T0, _m(2), "1m", _T0)
        assert result.points[0].accrual_pnl == 0.0
        assert result.points[1].accrual_pnl == -3.0
        assert result.points[2].accrual_pnl == -3.0

    async def test_zero_foreign_accrual_keeps_the_series_complete(self) -> None:
        """A zero foreign accrual is real zero and does not poison cumulatives."""
        executions = [_exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.0, "USD")]
        accruals = [_accrual_row(_I1, 1, 0.0, "EUR")]
        refs = [_ref(_I1, "BTC-USD", "USD")]
        candles = [_candle(_m(-1), 100.0), _candle(_m(0), 100.0), _candle(_m(1), 100.0)]
        repo = FakeRepo(executions=executions, accruals=accruals, refs=refs, candles=candles)
        result = await build_wallet_pnl_series(repo, "w1", "live", _T0, _m(2), "1m", _T0)
        assert [point.valuation_status for point in result.points] == ["complete"] * 3
        assert [point.accrual_pnl for point in result.points] == [0.0, 0.0, 0.0]

    async def test_non_valuation_currency_accrual_withholds_the_point(self) -> None:
        """A EUR accrual is UNKNOWN (NaN), so the point is withheld, never dropped.

        Silently dropping it would understate the funding cost and overstate P&L;
        the builder's finiteness guard withholds every component instead.
        """
        executions = [_exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.0, "USD")]
        accruals = [_accrual_row(_I1, 1, 99.0, "EUR")]
        refs = [_ref(_I1, "BTC-USD", "USD")]
        candles = [_candle(_m(-1), 100.0), _candle(_m(0), 100.0), _candle(_m(1), 100.0)]
        repo = FakeRepo(
            executions=executions,
            accruals=accruals,
            refs=refs,
            candles=candles,
        )
        result = await build_wallet_pnl_series(repo, "w1", "live", _T0, _m(2), "1m", _T0)
        assert result.points[0].accrual_pnl == 0.0
        assert result.points[1].valuation_status == "incomplete"
        assert result.points[1].accrual_pnl is None
        assert result.points[1].net_pnl is None

    async def test_paper_instrument_uses_source_venue_marks_with_paper_key(self) -> None:
        """A PAPER instrument resolves Kraken candles and remains the mark key."""
        executions = [
            _exec_row(
                _I1,
                1,
                0,
                "buy",
                1.0,
                100.0,
                0.0,
                "USD",
                exchange="paper",
            )
        ]
        refs = [
            _ref(
                _I1,
                "BTC-USD",
                "USD",
                exchange="kraken",
                instrument_exchange="paper",
            )
        ]
        candles = [_candle(_m(-1), 105.0), _candle(_m(0), 110.0)]
        repo = FakeRepo(executions=executions, refs=refs, candles=candles)
        result = await build_wallet_pnl_series(repo, "w1", "paper", _T0, _m(1), "1m", _T0)
        assert [point.valuation_status for point in result.points] == ["complete", "complete"]
        assert result.points[0].unrealized_pnl == 5.0
        assert result.points[1].unrealized_pnl == 10.0
        assert repo.candle_calls[0][0] == refs
        assert repo.candle_calls[0][0][0]["exchange"] == "kraken"
        assert result.points[0].per_instrument[0].instrument_public_id == _I1

    async def test_proven_fill_gap_withholds_every_monetary_field(self) -> None:
        """Any scoped shard gap makes every aggregate and contribution untrusted."""
        executions = [_exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.5, "USD")]
        refs = [_ref(_I1, "BTC-USD", "USD")]
        candles = [_candle(_m(-1), 105.0), _candle(_m(0), 110.0)]
        repo = FakeRepo(
            executions=executions,
            lineage=[
                _lineage_row(
                    executions[0]["order_public_id"],
                    "rest",
                    plan_public_id="manual-once-plan",
                )
            ],
            refs=refs,
            candles=candles,
            fill_shard_keys=["clean-shard", "gapped-shard", "unreached-shard"],
            gapped_shards={"gapped-shard", "unreached-shard"},
        )
        result = await build_wallet_pnl_series(repo, "w1", "live", _T0, _m(1), "1m", _T0)
        assert result.granularity == "1m"
        assert result.valuation_ccy == "USD"
        assert repo.fill_gap_calls == [
            ("clean-shard", "w1", "live", _T0),
            ("gapped-shard", "w1", "live", _T0),
        ]
        for point in result.points:
            assert point.valuation_status == "incomplete"
            assert point.realized_pnl is None
            assert point.fee_pnl is None
            assert point.accrual_pnl is None
            assert point.unrealized_pnl is None
            assert point.net_pnl is None
            assert len(point.per_instrument) == 1
            contribution = point.per_instrument[0]
            assert contribution.instrument_public_id == _I1
            assert contribution.realized_pnl is None
            assert contribution.fee_pnl is None
            assert contribution.accrual_pnl is None
            assert contribution.unrealized_pnl is None
            assert len(point.attribution) == 1
            attribution = point.attribution[0]
            assert attribution.origin == "manual"
            assert attribution.strategy_name is None
            assert attribution.realized_pnl is None
            assert attribution.fee_pnl is None
            assert attribution.accrual_pnl is None
            assert attribution.unrealized_pnl is None

    async def test_ambiguous_lineage_falls_back_to_unattributed(self) -> None:
        """Conflicting command candidates are omitted instead of choosing one."""
        executions = [_exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.5, "USD")]
        order_public_id = executions[0]["order_public_id"]
        repo = FakeRepo(
            executions=executions,
            lineage=[
                _lineage_row(order_public_id, "rest", plan_public_id="manual-plan"),
                _lineage_row(
                    order_public_id,
                    "strategy",
                    signal_public_id="signal-1",
                    strategy_name="momentum",
                ),
            ],
            refs=[_ref(_I1, "BTC-USD", "USD")],
            candles=[_candle(_m(-1), 100.0)],
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _T0,
            "1m",
            _m(1),
        )
        assert len(result.points[0].attribution) == 1
        attribution = result.points[0].attribution[0]
        assert attribution.origin == "unattributed"
        assert attribution.strategy_name is None
        assert attribution.fee_pnl == -0.5
        assert repo.lineage_calls == [([order_public_id], _m(1))]

    async def test_lineage_read_uses_distinct_execution_orders_and_as_of(self) -> None:
        """The read receives only deduplicated order ids from the replay prefix."""
        first = _exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.0, "USD")
        second = _exec_row(_I1, 2, 0, "buy", 1.0, 100.0, 0.0, "USD")
        third = _exec_row(_I1, 3, 0, "buy", 1.0, 100.0, 0.0, "USD")
        second["order_public_id"] = first["order_public_id"]
        as_of = _m(4)
        repo = FakeRepo(executions=[first, second, third])
        await build_wallet_pnl_series(repo, "w1", "live", _T0, _T0, "1m", as_of)
        assert repo.lineage_calls == [([first["order_public_id"], third["order_public_id"]], as_of)]

    async def test_total_work_budget_counts_execution_and_accrual_instruments(self) -> None:
        """Two instruments halve the permitted raw minute span and fail actionably."""
        executions = [_exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.0, "USD")]
        accruals = [_accrual_row(_I2, 1, 0.0, "USD")]
        repo = FakeRepo(executions=executions, accruals=accruals)
        oversized_end = _m(PNL_TIMELINE_MAX_WORK_UNITS // 2)
        with pytest.raises(PnlTimelineWorkBudgetError) as exc_info:
            await build_wallet_pnl_series(
                repo,
                "w1",
                "live",
                _T0,
                oversized_end,
                "1d",
                _T0,
            )
        assert "131,042 minute-instrument work units" in str(exc_info.value)
        assert "Shorten the window" in str(exc_info.value)
        assert repo.symbol_ref_calls == []
        assert repo.candle_calls == []

    async def test_resolves_distinct_instruments_once(self) -> None:
        """Repeated instrument ids collapse to a single deduped ref lookup."""
        executions = [
            _exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.0, "USD"),
            _exec_row(_I1, 2, 1, "sell", 1.0, 110.0, 0.0, "USD"),
            _exec_row(_I2, 1, 0, "buy", 1.0, 50.0, 0.0, "USD"),
        ]
        refs = [_ref(_I1, "BTC-USD", "USD"), _ref(_I2, "ETH-USD", "USD")]
        repo = FakeRepo(executions=executions, refs=refs)
        await build_wallet_pnl_series(repo, "w1", "live", _T0, _m(2), "1m", _T0)
        assert repo.symbol_ref_calls == [[_I1, _I2]]

    async def test_empty_scope_yields_only_incomplete_or_flat_points(self) -> None:
        """With no executions the series still spans the grid at the granularity."""
        repo = FakeRepo()
        result = await build_wallet_pnl_series(repo, "w1", "live", _T0, _m(2), "1m", _T0)
        assert len(result.points) == 3
        assert repo.symbol_ref_calls == [[]]


class TestExecutionQuoteCurrencyProof:
    """Cover fail-closed proof of replayed execution-price denomination."""

    async def test_foreign_quote_round_trip_is_fully_withheld(self) -> None:
        """A USD gain can never be published as complete EUR P&L."""
        executions = [
            _exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.0, "USD"),
            _exec_row(_I1, 2, 0, "sell", 1.0, 110.0, 0.0, "USD"),
        ]
        repo = FakeRepo(executions=executions, refs=[_ref(_I1, "BTC-USD", "USD")])
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _T0,
            "1m",
            _T0,
            valuation_ccy="EUR",
        )
        point = result.points[0]
        assert point.valuation_status == "incomplete"
        assert (
            point.realized_pnl,
            point.fee_pnl,
            point.accrual_pnl,
            point.unrealized_pnl,
            point.net_pnl,
        ) == (None, None, None, None, None)
        assert len(point.per_instrument) == 1
        contribution = point.per_instrument[0]
        assert contribution.instrument_public_id == _I1
        assert (
            contribution.realized_pnl,
            contribution.fee_pnl,
            contribution.accrual_pnl,
            contribution.unrealized_pnl,
        ) == (None, None, None, None)
        assert len(point.attribution) == 1
        attribution = point.attribution[0]
        assert (
            attribution.realized_pnl,
            attribution.fee_pnl,
            attribution.accrual_pnl,
            attribution.unrealized_pnl,
        ) == (None, None, None, None)

    async def test_same_currency_round_trip_remains_complete(self) -> None:
        """One unique EUR quote proves a EUR execution-price realization."""
        executions = [
            _exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.0, "EUR"),
            _exec_row(_I1, 2, 0, "sell", 1.0, 110.0, 0.0, "EUR"),
        ]
        repo = FakeRepo(executions=executions, refs=[_ref(_I1, "BTC-EUR", "EUR")])
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _T0,
            "1m",
            _T0,
            valuation_ccy="EUR",
        )
        point = result.points[0]
        assert point.valuation_status == "complete"
        assert (
            point.realized_pnl,
            point.fee_pnl,
            point.accrual_pnl,
            point.unrealized_pnl,
            point.net_pnl,
        ) == (10.0, 0.0, 0.0, 0.0, 10.0)

    async def test_response_time_successor_cannot_relabel_closed_round_trip(self) -> None:
        """A USD successor beginning after PLN fills cannot certify their prices."""
        executions = [
            _exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.0, "USD"),
            _exec_row(_I1, 2, 0, "sell", 1.0, 110.0, 0.0, "USD"),
        ]
        refs = [
            _ref(
                _I1,
                "BTC-X",
                "USD",
                valid_from=_m(1),
            )
        ]
        result = await build_wallet_pnl_series(
            FakeRepo(executions=executions, refs=refs),
            "w1",
            "live",
            _T0,
            _T0,
            "1m",
            _m(2),
            valuation_ccy="USD",
        )
        point = result.points[0]
        assert point.valuation_status == "incomplete"
        assert point.realized_pnl is None
        assert point.net_pnl is None

    async def test_symbol_and_exchange_revision_inside_fill_span_withholds(self) -> None:
        """No single identity can certify fills that straddle a venue re-key."""
        executions = [
            _exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.0, "USD"),
            _exec_row(
                _I1,
                2,
                2,
                "sell",
                1.0,
                110.0,
                0.0,
                "USD",
                exchange="coinbase",
            ),
        ]
        refs = [
            _ref(_I1, "BTC-USD", "USD", valid_to=_m(1)),
            _ref(
                _I1,
                "XBT-USD",
                "USD",
                exchange="coinbase",
                valid_from=_m(1),
            ),
        ]
        result = await build_wallet_pnl_series(
            FakeRepo(executions=executions, refs=refs),
            "w1",
            "live",
            _T0,
            _m(2),
            "1m",
            _m(3),
        )
        assert all(point.valuation_status == "incomplete" for point in result.points)
        assert all(point.realized_pnl is None for point in result.points)
        assert all(point.net_pnl is None for point in result.points)

    async def test_quote_revision_after_closed_span_withholds_prior_prices(self) -> None:
        """A later quote correction invalidates the earlier denomination claim."""
        executions = [
            _exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.0, "EUR"),
            _exec_row(_I1, 2, 0, "sell", 1.0, 110.0, 0.0, "EUR"),
        ]
        refs = [
            _ref(_I1, "BTC-X", "EUR", valid_to=_m(1)),
            _ref(_I1, "BTC-X", "USD", valid_from=_m(1)),
        ]
        result = await build_wallet_pnl_series(
            FakeRepo(executions=executions, refs=refs),
            "w1",
            "live",
            _T0,
            _T0,
            "1m",
            _m(2),
            valuation_ccy="EUR",
        )
        point = result.points[0]
        assert point.valuation_status == "incomplete"
        assert (
            point.realized_pnl,
            point.fee_pnl,
            point.accrual_pnl,
            point.unrealized_pnl,
            point.net_pnl,
        ) == (None, None, None, None, None)

    async def test_same_projection_revisions_are_merged_before_price_proof(self) -> None:
        """Metadata-only version churn does not blank a covered round trip."""
        executions = [
            _exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.0, "USD"),
            _exec_row(_I1, 2, 2, "sell", 1.0, 110.0, 0.0, "USD"),
        ]
        refs = [
            _ref(_I1, "BTC-USD", "USD", valid_to=_m(1)),
            _ref(
                _I1,
                "BTC-USD",
                "USD",
                valid_from=_T0,
                valid_to=_m(2),
            ),
            _ref(_I1, "BTC-USD", "USD", valid_from=_m(2)),
        ]
        repo = FakeRepo(executions=executions, refs=refs)
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _m(2),
            _m(2),
            "1m",
            _m(3),
        )
        point = result.points[0]
        assert point.valuation_status == "complete"
        assert point.realized_pnl == 10.0
        assert point.net_pnl == 10.0
        merged_refs = repo.candle_calls[0][0]
        assert len(merged_refs) == 1
        assert merged_refs[0]["valid_from"] == refs[0]["valid_from"]
        assert merged_refs[0]["valid_to"] == refs[2]["valid_to"]

    async def test_same_projection_gap_remains_untrusted(self) -> None:
        """Equal denomination projections cannot bridge a knowledge gap."""
        executions = [
            _exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.0, "USD"),
            _exec_row(_I1, 2, 2, "sell", 1.0, 110.0, 0.0, "USD"),
        ]
        refs = [
            _ref(_I1, "BTC-USD", "USD", valid_to=_m(1)),
            _ref(
                _I1,
                "BTC-USD",
                "USD",
                valid_from=_m(1) + timedelta(seconds=1),
            ),
        ]
        result = await build_wallet_pnl_series(
            FakeRepo(executions=executions, refs=refs),
            "w1",
            "live",
            _T0,
            _m(2),
            "1m",
            _m(3),
        )
        assert all(point.valuation_status == "incomplete" for point in result.points)
        assert all(point.realized_pnl is None for point in result.points)

    async def test_execution_venue_must_match_historical_instrument_venue(self) -> None:
        """A unique same-quote ref cannot certify fills from another venue."""
        executions = [
            _exec_row(
                _I1,
                1,
                0,
                "buy",
                1.0,
                100.0,
                0.0,
                "USD",
                exchange="coinbase",
            ),
            _exec_row(
                _I1,
                2,
                0,
                "sell",
                1.0,
                110.0,
                0.0,
                "USD",
                exchange="coinbase",
            ),
        ]
        result = await build_wallet_pnl_series(
            FakeRepo(
                executions=executions,
                refs=[_ref(_I1, "BTC-USD", "USD", instrument_exchange="kraken")],
            ),
            "w1",
            "live",
            _T0,
            _T0,
            "1m",
            _m(1),
        )
        point = result.points[0]
        assert point.valuation_status == "incomplete"
        assert point.realized_pnl is None
        assert point.net_pnl is None

    async def test_missing_symbol_reference_withholds_round_trip(self) -> None:
        """An absent reference cannot prove the execution-price currency."""
        executions = [
            _exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.0, "EUR"),
            _exec_row(_I1, 2, 0, "sell", 1.0, 110.0, 0.0, "EUR"),
        ]
        result = await build_wallet_pnl_series(
            FakeRepo(executions=executions),
            "w1",
            "live",
            _T0,
            _T0,
            "1m",
            _T0,
            valuation_ccy="EUR",
        )
        point = result.points[0]
        assert point.valuation_status == "incomplete"
        assert (
            point.realized_pnl,
            point.fee_pnl,
            point.accrual_pnl,
            point.unrealized_pnl,
            point.net_pnl,
        ) == (None, None, None, None, None)

    async def test_multiple_symbol_references_withhold_round_trip(self) -> None:
        """One matching candidate cannot override a second ambiguous reference."""
        executions = [
            _exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.0, "EUR"),
            _exec_row(_I1, 2, 0, "sell", 1.0, 110.0, 0.0, "EUR"),
        ]
        refs = [
            _ref(_I1, "BTC-EUR", "EUR"),
            _ref(_I1, "BTC-USD", "USD", exchange="other"),
        ]
        result = await build_wallet_pnl_series(
            FakeRepo(executions=executions, refs=refs),
            "w1",
            "live",
            _T0,
            _T0,
            "1m",
            _T0,
            valuation_ccy="EUR",
        )
        point = result.points[0]
        assert point.valuation_status == "incomplete"
        assert (
            point.realized_pnl,
            point.fee_pnl,
            point.accrual_pnl,
            point.unrealized_pnl,
            point.net_pnl,
        ) == (None, None, None, None, None)

    async def test_untrusted_instrument_preserves_proven_peer_and_withholds_total(self) -> None:
        """One untrusted instrument cannot blank a separately proven contribution."""
        executions = [
            _exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.0, "EUR"),
            _exec_row(_I1, 2, 0, "sell", 1.0, 110.0, 0.0, "EUR"),
            _exec_row(_I2, 3, 1, "buy", 1.0, 200.0, 0.25, "EUR"),
            _exec_row(_I2, 4, 1, "sell", 1.0, 220.0, 0.25, "EUR"),
        ]
        lineage = [
            _lineage_row(executions[0]["order_public_id"], "rest"),
            _lineage_row(executions[1]["order_public_id"], "rest"),
            _lineage_row(
                executions[2]["order_public_id"],
                "strategy",
                signal_public_id="signal-1",
                strategy_name="momentum",
            ),
            _lineage_row(
                executions[3]["order_public_id"],
                "strategy",
                signal_public_id="signal-2",
                strategy_name="momentum",
            ),
        ]
        refs = [
            _ref(_I1, "BTC-EUR", "EUR"),
            _ref(_I2, "ETH-USD", "USD"),
        ]
        result = await build_wallet_pnl_series(
            FakeRepo(
                executions=executions,
                accruals=[_accrual_row(_I2, 1, 3.0, "EUR")],
                refs=refs,
                lineage=lineage,
            ),
            "w1",
            "live",
            _T0,
            _m(1),
            "1m",
            _m(1),
            valuation_ccy="EUR",
        )
        proven_point, untrusted_point = result.points
        assert proven_point.valuation_status == "complete"
        assert proven_point.realized_pnl == 10.0
        assert proven_point.net_pnl == 10.0
        assert [item.instrument_public_id for item in proven_point.per_instrument] == [_I1]
        assert proven_point.per_instrument[0].realized_pnl == 10.0
        assert len(proven_point.attribution) == 1
        assert proven_point.attribution[0].origin == "manual"
        assert proven_point.attribution[0].realized_pnl == 10.0
        assert untrusted_point.valuation_status == "incomplete"
        assert (
            untrusted_point.realized_pnl,
            untrusted_point.fee_pnl,
            untrusted_point.accrual_pnl,
            untrusted_point.unrealized_pnl,
            untrusted_point.net_pnl,
        ) == (None, None, None, None, None)
        assert [item.instrument_public_id for item in untrusted_point.per_instrument] == [
            _I1,
            _I2,
        ]
        proven_contribution, untrusted_contribution = untrusted_point.per_instrument
        assert (
            proven_contribution.realized_pnl,
            proven_contribution.fee_pnl,
            proven_contribution.accrual_pnl,
            proven_contribution.unrealized_pnl,
        ) == (10.0, 0.0, 0.0, 0.0)
        assert (
            untrusted_contribution.realized_pnl,
            untrusted_contribution.fee_pnl,
            untrusted_contribution.accrual_pnl,
            untrusted_contribution.unrealized_pnl,
        ) == (None, None, None, None)
        assert {(item.origin, item.strategy_name) for item in untrusted_point.attribution} == {
            ("manual", None),
            ("system", "momentum"),
        }
        assert all(
            contribution.realized_pnl is None
            and contribution.fee_pnl is None
            and contribution.accrual_pnl is None
            and contribution.unrealized_pnl is None
            for contribution in untrusted_point.attribution
        )


class TestCrossCurrencyPrices:
    """Cover exact-minute conversion of execution prices and historical marks."""

    def test_same_minute_rates_from_two_venues_remain_distinct(self) -> None:
        """The selected venue answers while its same-minute rival is ignored."""
        rows = [
            _fx_row("EUR", "PLN", 0, 2.0, exchange="polygon"),
            _fx_row("EUR", "PLN", 0, 4.0, exchange="walutomat"),
        ]
        rates = build_fx_rates(rows)
        pair = currency_pair_key("EUR", "PLN")
        assert rates[("EUR", "PLN", "polygon", _T0)] == 2.0
        assert rates[("EUR", "PLN", "walutomat", _T0)] == 4.0
        assert (
            convert_amount(
                1.0,
                "EUR",
                "PLN",
                _T0,
                rates,
                {pair: ("EUR", "PLN", "walutomat")},
            )
            == 4.0
        )

    def test_conflicting_rows_on_one_full_plane_are_refused(self) -> None:
        """Two closes for one full identity cannot let row order choose money."""
        rows = [
            _fx_row("EUR", "PLN", 0, 4.0, exchange="walutomat"),
            _fx_row("EUR", "PLN", 0, 4.1, exchange="walutomat"),
        ]
        rates = build_fx_rates(rows)
        pair = currency_pair_key("EUR", "PLN")
        assert math.isnan(rates[("EUR", "PLN", "walutomat", _T0)])
        assert (
            convert_amount(
                1.0,
                "EUR",
                "PLN",
                _T0,
                rates,
                {pair: ("EUR", "PLN", "walutomat")},
            )
            is None
        )

    async def test_eur_pln_identity_forces_walutomat_despite_polygon_collision(
        self,
    ) -> None:
        """A held EUR-PLN instrument values flat in EUR on its own venue."""
        repo = FakeRepo(
            executions=[
                _exec_row(
                    _I1,
                    1,
                    0,
                    "buy",
                    1.0,
                    4.0,
                    0.0,
                    "",
                    exchange="walutomat",
                )
            ],
            refs=[_ref(_I1, "EUR-PLN", "PLN", exchange="walutomat")],
            candles=[_candle(_m(-1), 4.0), _candle(_m(0), 5.0)],
            fx_rows=[
                _fx_row("EUR", "PLN", 0, 2.0, exchange="polygon"),
                _fx_row("EUR", "PLN", 1, 10.0, exchange="polygon"),
                _fx_row("EUR", "PLN", 0, 4.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 1, 5.0, exchange="walutomat"),
            ],
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _m(1),
            "1m",
            _m(2),
            valuation_ccy="EUR",
        )
        assert [point.valuation_status for point in result.points] == ["complete", "complete"]
        point = result.points[1]
        assert point.realized_pnl == 0.0
        assert point.unrealized_pnl == 0.0
        assert point.net_pnl == 0.0
        assert point.per_instrument[0].realized_pnl == 0.0
        assert point.per_instrument[0].unrealized_pnl == 0.0
        assert [
            (
                source.source_currency,
                source.valuation_currency,
                source.base_currency,
                source.quote_currency,
                source.exchange,
            )
            for source in result.rate_sources
        ] == [("PLN", "EUR", "EUR", "PLN", "walutomat")]
        assert repo.fx_candidate_pair_calls == [
            [("EUR", "PLN"), ("PLN", "EUR")],
            [("EUR", "PLN"), ("PLN", "EUR")],
        ]
        assert repo.fx_plane_calls == [
            [
                ("EUR", "PLN", "polygon"),
                ("EUR", "PLN", "walutomat"),
            ],
            [
                ("EUR", "PLN", "polygon"),
                ("EUR", "PLN", "walutomat"),
            ],
        ]

    async def test_identity_plane_uses_held_orientation_when_reverse_is_also_complete(
        self,
    ) -> None:
        """The held symbol's own series cancels its mark despite a reverse rival."""
        repo = FakeRepo(
            executions=[
                _exec_row(
                    _I1,
                    1,
                    0,
                    "buy",
                    1.0,
                    4.0,
                    0.0,
                    "",
                    exchange="walutomat",
                )
            ],
            refs=[_ref(_I1, "EUR-PLN", "PLN", exchange="walutomat")],
            candles=[_candle(_m(-1), 4.0), _candle(_m(0), 5.0)],
            fx_rows=[
                _fx_row("EUR", "PLN", 0, 4.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 1, 5.0, exchange="walutomat"),
                _fx_row("PLN", "EUR", 0, 0.24, exchange="walutomat"),
                _fx_row("PLN", "EUR", 1, 0.19, exchange="walutomat"),
            ],
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _m(1),
            "1m",
            _m(2),
            valuation_ccy="EUR",
        )
        assert [point.valuation_status for point in result.points] == ["complete", "complete"]
        assert result.points[1].unrealized_pnl == 0.0
        assert result.points[1].net_pnl == 0.0
        assert [
            (source.base_currency, source.quote_currency, source.exchange)
            for source in result.rate_sources
        ] == [("EUR", "PLN", "walutomat")]

    async def test_identity_plane_gap_does_not_switch_to_reverse_orientation(
        self,
    ) -> None:
        """A reverse symbol cannot fill a held symbol's missing mark-rate minute."""
        repo = FakeRepo(
            executions=[
                _exec_row(
                    _I1,
                    1,
                    0,
                    "buy",
                    1.0,
                    4.0,
                    0.0,
                    "",
                    exchange="walutomat",
                )
            ],
            refs=[_ref(_I1, "EUR-PLN", "PLN", exchange="walutomat")],
            candles=[_candle(_m(-1), 4.0), _candle(_m(0), 5.0)],
            fx_rows=[
                _fx_row("EUR", "PLN", 0, 4.0, exchange="walutomat"),
                _fx_row("PLN", "EUR", 1, 0.19, exchange="walutomat"),
            ],
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _m(1),
            "1m",
            _m(2),
            valuation_ccy="EUR",
        )
        assert result.points[0].valuation_status == "complete"
        assert result.points[0].unrealized_pnl == 0.0
        point = result.points[1]
        assert point.valuation_status == "incomplete"
        assert point.realized_pnl == 0.0
        assert point.unrealized_pnl is None
        assert point.net_pnl is None

    async def test_pinned_fill_venue_does_not_switch_to_rival_for_mark(self) -> None:
        """A missing canonical-venue mark rate withholds instead of venue-hopping."""
        repo = FakeRepo(
            executions=[
                _exec_row(
                    _I1,
                    1,
                    0,
                    "buy",
                    1.0,
                    4.0,
                    0.0,
                    "",
                    exchange="walutomat",
                )
            ],
            refs=[_ref(_I1, "EUR-PLN", "PLN", exchange="walutomat")],
            candles=[_candle(_m(-1), 4.0), _candle(_m(0), 5.0)],
            fx_rows=[
                _fx_row("EUR", "PLN", 0, 4.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 0, 2.0, exchange="polygon"),
                _fx_row("EUR", "PLN", 1, 5.0, exchange="polygon"),
            ],
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _m(1),
            "1m",
            _m(2),
            valuation_ccy="EUR",
        )
        assert result.points[0].valuation_status == "complete"
        assert result.points[0].unrealized_pnl == 0.0
        point = result.points[1]
        assert point.valuation_status == "incomplete"
        assert (
            point.realized_pnl,
            point.fee_pnl,
            point.accrual_pnl,
            point.unrealized_pnl,
            point.net_pnl,
        ) == (0.0, 0.0, 0.0, None, None)
        contribution = point.per_instrument[0]
        assert (
            contribution.realized_pnl,
            contribution.fee_pnl,
            contribution.accrual_pnl,
            contribution.unrealized_pnl,
        ) == (0.0, 0.0, 0.0, None)

    async def test_identity_gap_withholds_only_itself_without_cross_plane_borrowing(
        self,
    ) -> None:
        """A peer uses Polygon while the held pair keeps its gapped Walutomat plane."""
        repo = FakeRepo(
            executions=[
                _exec_row(_I1, 1, -5, "buy", 1.0, 10.0, 0.0, ""),
                _exec_row(
                    _I2,
                    2,
                    0,
                    "buy",
                    1.0,
                    4.0,
                    0.0,
                    "",
                    exchange="walutomat",
                ),
            ],
            refs=[
                _ref(_I1, "XRP-PLN", "PLN"),
                _ref(_I2, "EUR-PLN", "PLN", exchange="walutomat"),
            ],
            candles=[
                _candle(_m(-1), 12.0),
                _candle(_m(0), 15.0),
                _candle(_m(-1), 4.0, instrument=_I2),
                _candle(_m(0), 5.0, instrument=_I2),
            ],
            fx_rows=[
                _fx_row("EUR", "PLN", 0, 4.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", -5, 2.0, exchange="polygon"),
                _fx_row("EUR", "PLN", 0, 2.0, exchange="polygon"),
                _fx_row("EUR", "PLN", 1, 2.0, exchange="polygon"),
            ],
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _m(1),
            "1m",
            _m(2),
            valuation_ccy="EUR",
        )
        first_point, gapped_point = result.points
        assert first_point.valuation_status == "complete"
        assert first_point.unrealized_pnl == 1.0
        first_peer, first_identity = first_point.per_instrument
        assert first_peer.instrument_public_id == _I1
        assert first_peer.unrealized_pnl == 1.0
        assert first_identity.instrument_public_id == _I2
        assert first_identity.unrealized_pnl == 0.0
        assert gapped_point.valuation_status == "incomplete"
        assert gapped_point.unrealized_pnl is None
        assert gapped_point.net_pnl is None
        peer, identity = gapped_point.per_instrument
        assert peer.instrument_public_id == _I1
        assert peer.unrealized_pnl == 2.5
        assert identity.instrument_public_id == _I2
        assert identity.realized_pnl == 0.0
        assert identity.unrealized_pnl is None
        assert [
            (source.base_currency, source.quote_currency, source.exchange)
            for source in result.rate_sources
        ] == [
            ("EUR", "PLN", "polygon"),
            ("EUR", "PLN", "walutomat"),
        ]
        assert repo.fx_candidate_pair_calls == [
            [("EUR", "PLN"), ("PLN", "EUR")],
            [("EUR", "PLN"), ("PLN", "EUR")],
        ]

    async def test_zero_coverage_identity_does_not_displace_peer_plane(self) -> None:
        """An empty canonical plane withholds its claimant but not a covered peer."""
        repo = FakeRepo(
            executions=[
                _exec_row(_I1, 1, -5, "buy", 1.0, 10.0, 0.0, ""),
                _exec_row(
                    _I2,
                    2,
                    0,
                    "buy",
                    1.0,
                    4.0,
                    0.0,
                    "",
                    exchange="walutomat",
                ),
            ],
            refs=[
                _ref(_I1, "XRP-PLN", "PLN"),
                _ref(_I2, "EUR-PLN", "PLN", exchange="walutomat"),
            ],
            candles=[
                _candle(_m(-1), 12.0),
                _candle(_m(-1), 4.0, instrument=_I2),
            ],
            fx_rows=[
                _fx_row("EUR", "PLN", -5, 2.0, exchange="polygon"),
                _fx_row("EUR", "PLN", 0, 2.0, exchange="polygon"),
            ],
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _T0,
            "1m",
            _m(1),
            valuation_ccy="EUR",
        )
        point = result.points[0]
        assert point.valuation_status == "incomplete"
        assert point.unrealized_pnl is None
        assert point.net_pnl is None
        peer, identity = point.per_instrument
        assert peer.instrument_public_id == _I1
        assert peer.unrealized_pnl == 1.0
        assert identity.instrument_public_id == _I2
        assert identity.unrealized_pnl is None
        assert [
            (source.base_currency, source.quote_currency, source.exchange)
            for source in result.rate_sources
        ] == [("EUR", "PLN", "polygon")]

    async def test_complete_identity_and_peer_publish_mixed_plane_total(self) -> None:
        """Individually proven Walutomat and Polygon legs form one honest total."""
        repo = FakeRepo(
            executions=[
                _exec_row(_I1, 1, -5, "buy", 1.0, 10.0, 0.0, ""),
                _exec_row(
                    _I2,
                    2,
                    0,
                    "buy",
                    1.0,
                    4.0,
                    0.0,
                    "",
                    exchange="walutomat",
                ),
            ],
            refs=[
                _ref(_I1, "XRP-PLN", "PLN"),
                _ref(_I2, "EUR-PLN", "PLN", exchange="walutomat"),
            ],
            candles=[
                _candle(_m(-1), 12.0),
                _candle(_m(0), 15.0),
                _candle(_m(-1), 4.0, instrument=_I2),
                _candle(_m(0), 5.0, instrument=_I2),
            ],
            fx_rows=[
                _fx_row("EUR", "PLN", 0, 4.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 1, 5.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", -5, 2.0, exchange="polygon"),
                _fx_row("EUR", "PLN", 0, 2.0, exchange="polygon"),
                _fx_row("EUR", "PLN", 1, 2.0, exchange="polygon"),
            ],
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _m(1),
            "1m",
            _m(2),
            valuation_ccy="EUR",
        )
        assert [point.valuation_status for point in result.points] == ["complete", "complete"]
        assert [point.unrealized_pnl for point in result.points] == [1.0, 2.5]
        assert [point.net_pnl for point in result.points] == [1.0, 2.5]
        assert [
            [contribution.unrealized_pnl for contribution in point.per_instrument]
            for point in result.points
        ] == [[1.0, 0.0], [2.5, 0.0]]
        assert [
            (source.base_currency, source.quote_currency, source.exchange)
            for source in result.rate_sources
        ] == [
            ("EUR", "PLN", "polygon"),
            ("EUR", "PLN", "walutomat"),
        ]

    async def test_identity_only_minutes_do_not_outvote_peer_plane(self) -> None:
        """Shared-plane coverage ranks only minutes required by general consumers."""
        repo = FakeRepo(
            executions=[
                _exec_row(_I1, 1, -5, "buy", 1.0, 10.0, 0.0, ""),
                _exec_row(_I1, 2, -5, "sell", 1.0, 12.0, 0.0, ""),
                _exec_row(
                    _I2,
                    3,
                    0,
                    "buy",
                    1.0,
                    4.0,
                    0.0,
                    "",
                    exchange="walutomat",
                ),
            ],
            refs=[
                _ref(_I1, "XRP-PLN", "PLN"),
                _ref(_I2, "EUR-PLN", "PLN", exchange="walutomat"),
            ],
            candles=[
                _candle(_m(-1), 20.0),
                _candle(_m(0), 21.0),
                _candle(_m(1), 22.0),
                _candle(_m(2), 23.0),
                _candle(_m(-1), 4.0, instrument=_I2),
                _candle(_m(0), 5.0, instrument=_I2),
                _candle(_m(1), 6.0, instrument=_I2),
                _candle(_m(2), 7.0, instrument=_I2),
            ],
            fx_rows=[
                _fx_row("EUR", "PLN", -5, 2.0, exchange="polygon"),
                _fx_row("EUR", "PLN", 0, 4.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 1, 5.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 2, 6.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 3, 7.0, exchange="walutomat"),
            ],
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _m(3),
            "1m",
            _m(4),
            valuation_ccy="EUR",
        )
        assert [point.valuation_status for point in result.points] == [
            "complete",
            "complete",
            "complete",
            "complete",
        ]
        assert [point.realized_pnl for point in result.points] == [1.0, 1.0, 1.0, 1.0]
        assert [point.unrealized_pnl for point in result.points] == [0.0, 0.0, 0.0, 0.0]
        assert [point.net_pnl for point in result.points] == [1.0, 1.0, 1.0, 1.0]
        assert [
            [
                (
                    contribution.instrument_public_id,
                    contribution.realized_pnl,
                    contribution.unrealized_pnl,
                )
                for contribution in point.per_instrument
            ]
            for point in result.points
        ] == [
            [(_I1, 1.0, 0.0), (_I2, 0.0, 0.0)],
            [(_I1, 1.0, 0.0), (_I2, 0.0, 0.0)],
            [(_I1, 1.0, 0.0), (_I2, 0.0, 0.0)],
            [(_I1, 1.0, 0.0), (_I2, 0.0, 0.0)],
        ]
        assert [
            (source.base_currency, source.quote_currency, source.exchange)
            for source in result.rate_sources
        ] == [
            ("EUR", "PLN", "polygon"),
            ("EUR", "PLN", "walutomat"),
        ]

    async def test_regression_shadow_marks_do_not_steer_shared_fx_plane(self) -> None:
        """Marks inside a scope-order regression shadow cannot outvote a valid peer."""
        repo = FakeRepo(
            executions=[
                _exec_row(_I2, 1, 0, "buy", 1.0, 20.0, 0.0, ""),
                _exec_row(_I2, 2, 10, "buy", 1.0, 30.0, 0.0, ""),
                _exec_row(_I2, 3, 1, "sell", 2.0, 25.0, 0.0, ""),
                _exec_row(_I1, 4, 11, "buy", 1.0, 10.0, 0.0, ""),
            ],
            refs=[
                _ref(_I1, "XRP-EUR", "EUR"),
                _ref(_I2, "BTC-EUR", "EUR"),
            ],
            candles=[
                _candle(_m(10), 10.0),
                _candle(_m(11), 12.0),
                _candle(_m(0), 20.0, instrument=_I2),
                _candle(_m(1), 20.0, instrument=_I2),
                _candle(_m(2), 20.0, instrument=_I2),
                _candle(_m(3), 20.0, instrument=_I2),
                _candle(_m(4), 20.0, instrument=_I2),
                _candle(_m(5), 20.0, instrument=_I2),
                _candle(_m(6), 20.0, instrument=_I2),
                _candle(_m(7), 20.0, instrument=_I2),
                _candle(_m(8), 20.0, instrument=_I2),
            ],
            fx_rows=[
                _fx_row("EUR", "PLN", 0, 2.0, exchange="polygon"),
                _fx_row("EUR", "PLN", 1, 2.0, exchange="polygon"),
                _fx_row("EUR", "PLN", 10, 2.0, exchange="polygon"),
                _fx_row("EUR", "PLN", 11, 2.0, exchange="polygon"),
                _fx_row("EUR", "PLN", 12, 2.0, exchange="polygon"),
                _fx_row("EUR", "PLN", 2, 4.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 3, 4.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 4, 4.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 5, 4.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 6, 4.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 7, 4.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 8, 4.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 9, 4.0, exchange="walutomat"),
            ],
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _m(12),
            "1m",
            _m(13),
            valuation_ccy="PLN",
        )
        peer_point = result.points[12]
        assert peer_point.valuation_status == "complete"
        assert peer_point.realized_pnl == 0.0
        assert peer_point.unrealized_pnl == 4.0
        assert peer_point.net_pnl == 4.0
        peer_contribution = next(
            contribution
            for contribution in peer_point.per_instrument
            if contribution.instrument_public_id == _I1
        )
        assert peer_contribution.realized_pnl == 0.0
        assert peer_contribution.unrealized_pnl == 4.0
        assert [
            (source.base_currency, source.quote_currency, source.exchange)
            for source in result.rate_sources
        ] == [("EUR", "PLN", "polygon")]

    async def test_untrusted_instrument_regression_shadow_does_not_steer_peer_plane(
        self,
    ) -> None:
        """A global shadow remains binding when its instrument identity is untrusted."""
        repo = FakeRepo(
            executions=[
                _exec_row(_I2, 1, 0, "buy", 1.0, 20.0, 0.0, ""),
                _exec_row(_I2, 2, 10, "buy", 1.0, 30.0, 0.0, ""),
                _exec_row(_I2, 3, 1, "sell", 2.0, 25.0, 0.0, ""),
                _exec_row(_I1, 4, 0, "buy", 1.0, 10.0, 0.0, ""),
            ],
            refs=[_ref(_I1, "XRP-EUR", "EUR")],
            candles=[
                _candle(_m(minute - 1), 10.0 if minute == 0 else 12.0) for minute in range(12)
            ],
            fx_rows=[
                _fx_row("EUR", "PLN", 0, 2.0, exchange="polygon"),
                _fx_row("EUR", "PLN", 10, 2.0, exchange="polygon"),
                _fx_row("EUR", "PLN", 11, 2.0, exchange="polygon"),
                *[
                    _fx_row("EUR", "PLN", minute, 4.0, exchange="walutomat")
                    for minute in range(1, 10)
                ],
            ],
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _m(11),
            "1m",
            _m(12),
            valuation_ccy="PLN",
        )
        peer_unrealized = [
            next(
                contribution.unrealized_pnl
                for contribution in result.points[index].per_instrument
                if contribution.instrument_public_id == _I1
            )
            for index in (0, 10, 11)
        ]
        assert peer_unrealized == [0.0, 4.0, 4.0]
        assert [
            (source.base_currency, source.quote_currency, source.exchange)
            for source in result.rate_sources
        ] == [("EUR", "PLN", "polygon")]

    async def test_future_predecessor_regression_shadow_does_not_steer_peer_plane(
        self,
    ) -> None:
        """Scope order retains a future predecessor that shadows the current chart."""
        repo = FakeRepo(
            executions=[
                _exec_row(_I2, 1, 0, "buy", 1.0, 20.0, 0.0, ""),
                _exec_row(_I2, 2, 20, "buy", 1.0, 30.0, 0.0, ""),
                _exec_row(_I2, 3, 1, "sell", 2.0, 25.0, 0.0, ""),
                _exec_row(_I1, 4, 0, "buy", 1.0, 10.0, 0.0, ""),
            ],
            refs=[_ref(_I1, "XRP-EUR", "EUR")],
            candles=[
                _candle(_m(minute - 1), 10.0 if minute == 0 else 12.0) for minute in range(12)
            ],
            fx_rows=[
                _fx_row("EUR", "PLN", 0, 2.0, exchange="polygon"),
                *[
                    _fx_row("EUR", "PLN", minute, 4.0, exchange="walutomat")
                    for minute in range(1, 12)
                ],
            ],
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _m(11),
            "1m",
            _m(21),
            valuation_ccy="PLN",
        )
        peer_contribution = next(
            contribution
            for contribution in result.points[0].per_instrument
            if contribution.instrument_public_id == _I1
        )
        assert peer_contribution.realized_pnl == 0.0
        assert peer_contribution.unrealized_pnl == 0.0
        assert [
            (source.base_currency, source.quote_currency, source.exchange)
            for source in result.rate_sources
        ] == [("EUR", "PLN", "polygon")]

    @pytest.mark.parametrize(
        "bad_size",
        [
            pytest.param(-1.0, id="negative"),
            pytest.param(math.nan, id="nan"),
            pytest.param(math.inf, id="infinite"),
        ],
    )
    async def test_invalid_size_instrument_does_not_steer_shared_fx_plane(
        self,
        bad_size: float,
    ) -> None:
        """An already-invalid consumer cannot outvote a covered valid peer."""
        repo = FakeRepo(
            executions=[
                _exec_row(_I1, 1, 0, "buy", 1.0, 10.0, 0.0, ""),
                _exec_row(_I1, 2, 1, "sell", 1.0, 12.0, 0.0, ""),
                _exec_row(_I2, 3, 2, "buy", bad_size, 20.0, 0.0, ""),
                _exec_row(_I2, 4, 3, "buy", 1.0, 21.0, 0.0, ""),
                _exec_row(_I2, 5, 4, "buy", 1.0, 22.0, 0.0, ""),
                _exec_row(_I2, 6, 5, "buy", 1.0, 23.0, 0.0, ""),
            ],
            refs=[
                _ref(_I1, "XRP-EUR", "EUR"),
                _ref(_I2, "BTC-EUR", "EUR"),
            ],
            candles=[
                _candle(_m(-1), 10.0),
                _candle(_m(0), 12.0),
                _candle(_m(1), 20.0, instrument=_I2),
                _candle(_m(2), 21.0, instrument=_I2),
                _candle(_m(3), 22.0, instrument=_I2),
                _candle(_m(4), 23.0, instrument=_I2),
            ],
            fx_rows=[
                _fx_row("EUR", "PLN", 0, 2.0, exchange="polygon"),
                _fx_row("EUR", "PLN", 1, 2.0, exchange="polygon"),
                _fx_row("EUR", "PLN", 2, 4.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 3, 4.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 4, 4.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 5, 4.0, exchange="walutomat"),
            ],
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _m(5),
            "1m",
            _m(6),
            valuation_ccy="PLN",
        )
        valid_point = result.points[1]
        assert valid_point.valuation_status == "complete"
        assert valid_point.realized_pnl == 4.0
        assert valid_point.unrealized_pnl == 0.0
        assert valid_point.net_pnl == 4.0
        assert [
            (
                contribution.instrument_public_id,
                contribution.realized_pnl,
                contribution.unrealized_pnl,
            )
            for contribution in valid_point.per_instrument
        ] == [(_I1, 4.0, 0.0)]
        invalid_point = result.points[2]
        invalid_contribution = next(
            contribution
            for contribution in invalid_point.per_instrument
            if contribution.instrument_public_id == _I2
        )
        assert (
            invalid_contribution.realized_pnl,
            invalid_contribution.fee_pnl,
            invalid_contribution.accrual_pnl,
            invalid_contribution.unrealized_pnl,
        ) == (None, None, None, None)
        final_invalid_contribution = next(
            contribution
            for contribution in result.points[-1].per_instrument
            if contribution.instrument_public_id == _I2
        )
        assert (
            final_invalid_contribution.realized_pnl,
            final_invalid_contribution.fee_pnl,
            final_invalid_contribution.accrual_pnl,
            final_invalid_contribution.unrealized_pnl,
        ) == (None, None, None, None)
        assert [
            (source.base_currency, source.quote_currency, source.exchange)
            for source in result.rate_sources
        ] == [("EUR", "PLN", "polygon")]

    @pytest.mark.parametrize(
        "invalid_price",
        [
            pytest.param(0.0, id="zero"),
            pytest.param(-1.0, id="negative"),
            pytest.param(math.nan, id="nan"),
            pytest.param(math.inf, id="infinite"),
        ],
    )
    async def test_invalid_price_instrument_does_not_steer_shared_fx_plane(
        self,
        invalid_price: float,
    ) -> None:
        """A known-invalid raw price cannot make later marks outvote a valid peer."""
        repo = FakeRepo(
            executions=[
                _exec_row(_I1, 1, 0, "buy", 1.0, 10.0, 0.0, ""),
                _exec_row(_I1, 2, 1, "sell", 1.0, 12.0, 0.0, ""),
                _exec_row(_I2, 3, 2, "buy", 1.0, invalid_price, 0.0, ""),
                _exec_row(_I2, 4, 3, "buy", 1.0, 21.0, 0.0, ""),
                _exec_row(_I2, 5, 4, "buy", 1.0, 22.0, 0.0, ""),
                _exec_row(_I2, 6, 5, "buy", 1.0, 23.0, 0.0, ""),
            ],
            refs=[
                _ref(_I1, "XRP-EUR", "EUR"),
                _ref(_I2, "BTC-EUR", "EUR"),
            ],
            candles=[
                _candle(_m(-1), 10.0),
                _candle(_m(0), 12.0),
                _candle(_m(1), 20.0, instrument=_I2),
                _candle(_m(2), 21.0, instrument=_I2),
                _candle(_m(3), 22.0, instrument=_I2),
                _candle(_m(4), 23.0, instrument=_I2),
            ],
            fx_rows=[
                _fx_row("EUR", "PLN", 0, 2.0, exchange="polygon"),
                _fx_row("EUR", "PLN", 1, 2.0, exchange="polygon"),
                _fx_row("EUR", "PLN", 2, 4.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 3, 4.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 4, 4.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 5, 4.0, exchange="walutomat"),
            ],
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _m(5),
            "1m",
            _m(6),
            valuation_ccy="PLN",
        )
        valid_point = result.points[1]
        assert valid_point.valuation_status == "complete"
        assert valid_point.realized_pnl == 4.0
        assert valid_point.unrealized_pnl == 0.0
        assert valid_point.net_pnl == 4.0
        assert [
            (
                contribution.instrument_public_id,
                contribution.realized_pnl,
                contribution.unrealized_pnl,
            )
            for contribution in valid_point.per_instrument
        ] == [(_I1, 4.0, 0.0)]
        invalid_point = result.points[2]
        invalid_contribution = next(
            contribution
            for contribution in invalid_point.per_instrument
            if contribution.instrument_public_id == _I2
        )
        assert invalid_contribution.unrealized_pnl is None
        final_invalid_contribution = next(
            contribution
            for contribution in result.points[-1].per_instrument
            if contribution.instrument_public_id == _I2
        )
        assert final_invalid_contribution.realized_pnl == 0.0
        assert final_invalid_contribution.unrealized_pnl is None
        assert [
            (source.base_currency, source.quote_currency, source.exchange)
            for source in result.rate_sources
        ] == [("EUR", "PLN", "polygon")]

    async def test_neither_venue_complete_pins_best_coverage_and_withholds_gap(
        self,
    ) -> None:
        """A higher-coverage venue stays pinned when a rival alone has one mark."""
        repo = FakeRepo(
            executions=[_exec_row(_I1, 1, 0, "buy", 1.0, 10.0, 0.0, "")],
            refs=[_ref(_I1, "XRP-EUR", "EUR")],
            candles=[
                _candle(_m(-1), 10.0),
                _candle(_m(0), 12.0),
                _candle(_m(1), 13.0),
            ],
            fx_rows=[
                _fx_row("EUR", "PLN", 0, 4.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 1, 5.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 2, 6.0, exchange="polygon"),
            ],
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _m(2),
            "1m",
            _m(3),
            valuation_ccy="PLN",
        )
        assert result.points[0].valuation_status == "complete"
        assert result.points[1].valuation_status == "complete"
        point = result.points[2]
        assert point.valuation_status == "incomplete"
        assert point.realized_pnl == 0.0
        assert point.unrealized_pnl is None
        assert point.net_pnl is None

    async def test_pinned_orientation_gap_does_not_switch_to_reverse_series(self) -> None:
        """A reverse-oriented candle cannot fill the selected series' later gap."""
        repo = FakeRepo(
            executions=[_exec_row(_I1, 1, 0, "buy", 1.0, 10.0, 0.0, "")],
            refs=[_ref(_I1, "XRP-EUR", "EUR")],
            candles=[
                _candle(_m(-1), 10.0),
                _candle(_m(0), 10.0),
                _candle(_m(1), 10.0),
            ],
            fx_rows=[
                _fx_row("EUR", "PLN", 0, 4.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 1, 4.0, exchange="walutomat"),
                _fx_row("PLN", "EUR", 2, 0.27, exchange="walutomat"),
            ],
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _m(2),
            "1m",
            _m(3),
            valuation_ccy="PLN",
        )
        assert [point.valuation_status for point in result.points] == [
            "complete",
            "complete",
            "incomplete",
        ]
        assert result.points[0].unrealized_pnl == 0.0
        assert result.points[1].unrealized_pnl == 0.0
        assert result.points[2].unrealized_pnl is None
        assert result.points[2].net_pnl is None
        assert [
            (source.base_currency, source.quote_currency, source.exchange)
            for source in result.rate_sources
        ] == [("EUR", "PLN", "walutomat")]

    async def test_complete_nonidentity_pln_tie_prefers_walutomat(self) -> None:
        """Complete PLN planes use Walutomat despite a lexical Polygon rival."""
        repo = FakeRepo(
            executions=[_exec_row(_I1, 1, 0, "buy", 1.0, 10.0, 0.0, "")],
            refs=[_ref(_I1, "XRP-EUR", "EUR")],
            candles=[_candle(_m(-1), 10.0), _candle(_m(1), 12.0)],
            fx_rows=[
                _fx_row("EUR", "PLN", 0, 2.0, exchange="polygon"),
                _fx_row("EUR", "PLN", 2, 2.0, exchange="polygon"),
                _fx_row("EUR", "PLN", 0, 4.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 1, 4.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 2, 4.0, exchange="walutomat"),
            ],
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _m(2),
            "1m",
            _m(3),
            valuation_ccy="PLN",
        )
        assert result.points[2].valuation_status == "complete"
        assert result.points[2].unrealized_pnl == 8.0
        assert result.points[2].net_pnl == 8.0

    async def test_complete_orientation_tie_pins_requested_direction_once(self) -> None:
        """Equal orientations elect source-to-valuation once for the whole request."""
        repo = FakeRepo(
            executions=[_exec_row(_I1, 1, 0, "buy", 1.0, 10.0, 0.0, "")],
            refs=[_ref(_I1, "XRP-EUR", "EUR")],
            candles=[_candle(_m(-1), 10.0), _candle(_m(0), 12.0)],
            fx_rows=[
                _fx_row("EUR", "PLN", 0, 4.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 1, 5.0, exchange="walutomat"),
                _fx_row("PLN", "EUR", 0, 0.24, exchange="walutomat"),
                _fx_row("PLN", "EUR", 1, 0.19, exchange="walutomat"),
            ],
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _m(1),
            "1m",
            _m(2),
            valuation_ccy="PLN",
        )
        assert result.points[1].valuation_status == "complete"
        assert result.points[1].unrealized_pnl == 20.0
        assert [
            (source.base_currency, source.quote_currency, source.exchange)
            for source in result.rate_sources
        ] == [("EUR", "PLN", "walutomat")]

    async def test_pre_first_fill_mark_gap_does_not_demote_walutomat(self) -> None:
        """An unused pre-position mark cannot steer the request-wide FX plane."""
        repo = FakeRepo(
            executions=[_exec_row(_I1, 1, 1, "buy", 1.0, 10.0, 0.0, "")],
            refs=[_ref(_I1, "XRP-EUR", "EUR")],
            candles=[
                _candle(_m(-1), 10.0),
                _candle(_m(0), 10.0),
                _candle(_m(1), 12.0),
            ],
            fx_rows=[
                _fx_row("EUR", "PLN", 0, 2.0, exchange="polygon"),
                _fx_row("EUR", "PLN", 1, 2.0, exchange="polygon"),
                _fx_row("EUR", "PLN", 2, 2.0, exchange="polygon"),
                _fx_row("EUR", "PLN", 1, 4.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 2, 4.0, exchange="walutomat"),
            ],
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _m(2),
            "1m",
            _m(3),
            valuation_ccy="PLN",
        )
        before_fill, fill_point, marked_point = result.points
        assert before_fill.valuation_status == "complete"
        assert before_fill.per_instrument == ()
        assert (
            before_fill.realized_pnl,
            before_fill.fee_pnl,
            before_fill.accrual_pnl,
            before_fill.unrealized_pnl,
            before_fill.net_pnl,
        ) == (0.0, 0.0, 0.0, 0.0, 0.0)
        assert fill_point.unrealized_pnl == 0.0
        assert marked_point.valuation_status == "complete"
        assert marked_point.unrealized_pnl == 8.0
        assert marked_point.net_pnl == 8.0
        assert [
            (source.base_currency, source.quote_currency, source.exchange)
            for source in result.rate_sources
        ] == [("EUR", "PLN", "walutomat")]

    async def test_marks_before_a_future_first_fill_need_no_fx_plane(self) -> None:
        """An instrument absent throughout the window cannot create FX requirements."""
        repo = FakeRepo(
            executions=[_exec_row(_I1, 1, 2, "buy", 1.0, 10.0, 0.0, "")],
            refs=[_ref(_I1, "XRP-EUR", "EUR")],
            candles=[_candle(_m(-1), 10.0), _candle(_m(0), 11.0)],
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _m(1),
            "1m",
            _m(3),
            valuation_ccy="PLN",
        )
        assert [point.valuation_status for point in result.points] == ["complete", "complete"]
        assert all(point.per_instrument == () and point.net_pnl == 0.0 for point in result.points)
        assert result.rate_sources == ()
        assert repo.fx_candidate_pair_calls == []
        assert repo.fx_plane_calls == []

    async def test_complete_rival_beats_gapped_walutomat_for_the_whole_span(
        self,
    ) -> None:
        """A complete Polygon plane prices both fill and mark when Walutomat gaps."""
        repo = FakeRepo(
            executions=[_exec_row(_I1, 1, 0, "buy", 1.0, 10.0, 0.0, "")],
            refs=[_ref(_I1, "XRP-EUR", "EUR")],
            candles=[_candle(_m(-1), 10.0), _candle(_m(0), 12.0)],
            fx_rows=[
                _fx_row("EUR", "PLN", 0, 4.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 0, 2.0, exchange="polygon"),
                _fx_row("EUR", "PLN", 1, 3.0, exchange="polygon"),
            ],
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _m(1),
            "1m",
            _m(2),
            valuation_ccy="PLN",
        )
        assert [point.valuation_status for point in result.points] == ["complete", "complete"]
        assert result.points[0].unrealized_pnl == 0.0
        assert result.points[1].unrealized_pnl == 16.0
        assert result.points[1].net_pnl == 16.0
        assert [source.exchange for source in result.rate_sources] == ["polygon"]

    async def test_conflicting_full_key_does_not_count_as_coverage(self) -> None:
        """A duplicate conflict cannot outrank a genuinely complete venue."""
        repo = FakeRepo(
            executions=[_exec_row(_I1, 1, 0, "buy", 1.0, 10.0, 0.0, "")],
            refs=[_ref(_I1, "XRP-EUR", "EUR")],
            candles=[_candle(_m(-1), 10.0), _candle(_m(0), 12.0)],
            fx_rows=[
                _fx_row("EUR", "USD", 0, 1.0, exchange="alpha"),
                _fx_row("EUR", "USD", 0, 1.1, exchange="alpha"),
                _fx_row("EUR", "USD", 1, 1.0, exchange="alpha"),
                _fx_row("EUR", "USD", 0, 2.0, exchange="zeta"),
                _fx_row("EUR", "USD", 1, 2.0, exchange="zeta"),
            ],
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _m(1),
            "1m",
            _m(2),
            valuation_ccy="USD",
        )
        assert [point.valuation_status for point in result.points] == ["complete", "complete"]
        assert result.points[1].unrealized_pnl == 4.0
        assert result.points[1].net_pnl == 4.0

    async def test_two_held_canonical_venues_each_use_their_own_plane(self) -> None:
        """Divergent venue series independently certify both held instruments."""
        repo = FakeRepo(
            executions=[
                _exec_row(
                    _I1,
                    1,
                    0,
                    "buy",
                    1.0,
                    4.0,
                    0.0,
                    "",
                    exchange="walutomat",
                ),
                _exec_row(
                    _I2,
                    2,
                    0,
                    "buy",
                    1.0,
                    2.0,
                    0.0,
                    "",
                    exchange="polygon",
                ),
            ],
            refs=[
                _ref(_I1, "EUR-PLN", "PLN", exchange="walutomat"),
                _ref(_I2, "EUR-PLN", "PLN", exchange="polygon"),
            ],
            candles=[
                _candle(_m(-1), 4.0),
                _candle(_m(0), 5.0),
                _candle(_m(-1), 2.0, instrument=_I2),
                _candle(_m(0), 3.0, instrument=_I2),
            ],
            fx_rows=[
                _fx_row("EUR", "PLN", 0, 4.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 1, 5.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 0, 2.0, exchange="polygon"),
                _fx_row("EUR", "PLN", 1, 3.0, exchange="polygon"),
            ],
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _m(1),
            "1m",
            _m(2),
            valuation_ccy="EUR",
        )
        assert [
            (
                point.valuation_status,
                point.realized_pnl,
                point.unrealized_pnl,
                point.net_pnl,
            )
            for point in result.points
        ] == [
            ("complete", 0.0, 0.0, 0.0),
            ("complete", 0.0, 0.0, 0.0),
        ]
        assert [
            [
                (contribution.instrument_public_id, contribution.unrealized_pnl)
                for contribution in point.per_instrument
            ]
            for point in result.points
        ] == [
            [(_I1, 0.0), (_I2, 0.0)],
            [(_I1, 0.0), (_I2, 0.0)],
        ]
        assert [
            (source.base_currency, source.quote_currency, source.exchange)
            for source in result.rate_sources
        ] == [
            ("EUR", "PLN", "polygon"),
            ("EUR", "PLN", "walutomat"),
        ]
        assert repo.fx_candidate_pair_calls == [
            [("EUR", "PLN"), ("PLN", "EUR")],
            [("EUR", "PLN"), ("PLN", "EUR")],
        ]

    @pytest.mark.parametrize(
        "valuation_ccy,fx_rows,expected_sources",
        [
            pytest.param(
                "PLN",
                [_fx_row("EUR", "PLN", 0, 4.0, exchange="walutomat")],
                [("EUR", "PLN", "EUR", "PLN", "walutomat")],
                id="pln",
            ),
            pytest.param(
                "EUR",
                [_fx_row("EUR", "PLN", 0, 4.0, exchange="walutomat")],
                [("PLN", "EUR", "EUR", "PLN", "walutomat")],
                id="eur",
            ),
            pytest.param(
                "USD",
                [
                    _fx_row("EUR", "USD", 0, 1.2),
                    _fx_row("USD", "PLN", 0, 4.0, exchange="walutomat"),
                ],
                [
                    ("EUR", "USD", "EUR", "USD", "kraken"),
                    ("PLN", "USD", "USD", "PLN", "walutomat"),
                ],
                id="usd",
            ),
        ],
    )
    async def test_real_wallet_quote_shapes_complete_with_exact_pair_evidence(
        self,
        valuation_ccy: str,
        fx_rows: list[PnlFxRateRow],
        expected_sources: list[tuple[str, str, str, str, str]],
    ) -> None:
        """XRP-EUR and EUR-PLN coexist when each foreign quote has its own pair."""
        repo = FakeRepo(
            executions=[
                _exec_row(_I1, 1, 0, "buy", 1.0, 10.0, 0.0, ""),
                _exec_row(
                    _I2,
                    2,
                    0,
                    "buy",
                    1.0,
                    4.0,
                    0.0,
                    "",
                    exchange="walutomat",
                ),
            ],
            refs=[
                _ref(_I1, "XRP-EUR", "EUR"),
                _ref(_I2, "EUR-PLN", "PLN", exchange="walutomat"),
            ],
            candles=[
                _candle(_m(-1), 10.0),
                _candle(_m(-1), 4.0, instrument=_I2),
            ],
            fx_rows=fx_rows,
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _T0,
            "1m",
            _m(1),
            valuation_ccy=valuation_ccy,
        )
        point = result.points[0]
        assert point.valuation_status == "complete"
        assert point.net_pnl == 0.0
        assert [item.instrument_public_id for item in point.per_instrument] == [_I1, _I2]
        assert all(item.unrealized_pnl == 0.0 for item in point.per_instrument)
        assert [
            (
                source.source_currency,
                source.valuation_currency,
                source.base_currency,
                source.quote_currency,
                source.exchange,
            )
            for source in result.rate_sources
        ] == expected_sources

    async def test_different_buy_mark_and_sell_rates_keep_pool_in_one_currency(self) -> None:
        """Buy basis, later mark, and sell all use their own PLN conversion rate."""
        executions = [
            _exec_row(_I1, 1, 0, "buy", 2.0, 10.0, 0.0, "PLN"),
            _exec_row(_I1, 2, 2, "sell", 2.0, 11.0, 0.0, "PLN"),
        ]
        repo = FakeRepo(
            executions=executions,
            refs=[_ref(_I1, "XRP-EUR", "EUR")],
            candles=[
                _candle(_m(-1), 10.0),
                _candle(_m(0), 12.0),
                _candle(_m(1), 11.0),
            ],
            fx_rows=[
                _fx_row("EUR", "PLN", 0, 4.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 1, 5.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 2, 6.0, exchange="walutomat"),
            ],
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _m(2),
            "1m",
            _m(3),
            valuation_ccy="PLN",
        )
        buy_point, mark_point, sell_point = result.points
        assert buy_point.valuation_status == "complete"
        assert buy_point.unrealized_pnl == 0.0
        assert mark_point.valuation_status == "complete"
        assert mark_point.unrealized_pnl == 40.0
        assert mark_point.net_pnl == 40.0
        assert sell_point.valuation_status == "complete"
        assert sell_point.realized_pnl == 52.0
        assert sell_point.unrealized_pnl == 0.0
        assert sell_point.net_pnl == 52.0

    async def test_inverse_pair_converts_execution_and_mark(self) -> None:
        """A PLN-EUR candle proves both EUR execution and mark values by inverse."""
        repo = FakeRepo(
            executions=[_exec_row(_I1, 1, 0, "buy", 1.0, 10.0, 0.0, "PLN")],
            refs=[_ref(_I1, "XRP-EUR", "EUR")],
            candles=[_candle(_m(-1), 10.0), _candle(_m(0), 12.0)],
            fx_rows=[
                _fx_row("PLN", "EUR", 0, 0.25, exchange="walutomat"),
                _fx_row("PLN", "EUR", 1, 0.2, exchange="walutomat"),
            ],
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _m(1),
            "1m",
            _m(2),
            valuation_ccy="PLN",
        )
        assert [point.valuation_status for point in result.points] == ["complete", "complete"]
        assert result.points[0].unrealized_pnl == 0.0
        assert result.points[1].unrealized_pnl == 20.0
        assert [
            (source.base_currency, source.quote_currency, source.exchange)
            for source in result.rate_sources
        ] == [("PLN", "EUR", "walutomat")]

    async def test_missing_exact_mark_rate_is_mark_incomplete_without_carry_forward(
        self,
    ) -> None:
        """The prior minute's EUR-PLN close cannot price the next minute's mark."""
        repo = FakeRepo(
            executions=[_exec_row(_I1, 1, 0, "buy", 1.0, 10.0, 0.0, "PLN")],
            refs=[_ref(_I1, "XRP-EUR", "EUR")],
            candles=[
                _candle(_m(-1), 10.0),
                _candle(_m(0), 12.0),
                _candle(_m(1), 13.0),
            ],
            fx_rows=[
                _fx_row("EUR", "PLN", 0, 4.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 2, 6.0, exchange="walutomat"),
            ],
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _m(2),
            "1m",
            _m(2),
            valuation_ccy="PLN",
        )
        assert result.points[0].valuation_status == "complete"
        point = result.points[1]
        assert point.valuation_status == "incomplete"
        assert (
            point.realized_pnl,
            point.fee_pnl,
            point.accrual_pnl,
            point.unrealized_pnl,
            point.net_pnl,
        ) == (0.0, 0.0, 0.0, None, None)
        contribution = point.per_instrument[0]
        assert (
            contribution.realized_pnl,
            contribution.fee_pnl,
            contribution.accrual_pnl,
            contribution.unrealized_pnl,
        ) == (0.0, 0.0, 0.0, None)
        recovered = result.points[2]
        assert recovered.valuation_status == "complete"
        assert recovered.unrealized_pnl == 38.0
        assert recovered.net_pnl == 38.0

    async def test_missing_opening_fill_rate_keeps_mark_independent_cumulatives(self) -> None:
        """An unconvertible opening price leaves basis unknown without tainting flows."""
        repo = FakeRepo(
            executions=[_exec_row(_I1, 1, 0, "buy", 1.0, 10.0, 0.0, "PLN")],
            refs=[_ref(_I1, "XRP-EUR", "EUR")],
            candles=[_candle(_m(0), 12.0)],
            fx_rows=[_fx_row("EUR", "PLN", 1, 5.0, exchange="walutomat")],
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _m(1),
            _m(1),
            "1m",
            _m(2),
            valuation_ccy="PLN",
        )
        point = result.points[0]
        assert point.valuation_status == "incomplete"
        assert (
            point.realized_pnl,
            point.fee_pnl,
            point.accrual_pnl,
            point.unrealized_pnl,
            point.net_pnl,
        ) == (0.0, 0.0, 0.0, None, None)

    async def test_missing_same_side_add_rate_keeps_mark_independent_cumulatives(self) -> None:
        """An unconvertible add poisons basis but does not invent a realization."""
        repo = FakeRepo(
            executions=[
                _exec_row(_I1, 1, -1, "buy", 1.0, 10.0, 0.0, "PLN"),
                _exec_row(_I1, 2, 0, "buy", 1.0, 11.0, 0.0, "PLN"),
            ],
            refs=[_ref(_I1, "XRP-EUR", "EUR")],
            candles=[_candle(_m(0), 12.0)],
            fx_rows=[
                _fx_row("EUR", "PLN", -1, 4.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 1, 5.0, exchange="walutomat"),
            ],
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _m(1),
            _m(1),
            "1m",
            _m(2),
            valuation_ccy="PLN",
        )
        point = result.points[0]
        assert point.valuation_status == "incomplete"
        assert (
            point.realized_pnl,
            point.fee_pnl,
            point.accrual_pnl,
            point.unrealized_pnl,
            point.net_pnl,
        ) == (0.0, 0.0, 0.0, None, None)

    @pytest.mark.parametrize(
        "closing_size",
        [
            pytest.param(0.5, id="reduction"),
            pytest.param(1.0, id="close"),
            pytest.param(2.0, id="flip"),
        ],
    )
    async def test_missing_reducing_fill_rate_latches_instrument_untrusted(
        self,
        closing_size: float,
    ) -> None:
        """A close component without its exact FX rate makes realization unknowable."""
        repo = FakeRepo(
            executions=[
                _exec_row(_I1, 1, 0, "buy", 1.0, 10.0, 0.0, "PLN"),
                _exec_row(_I1, 2, 1, "sell", closing_size, 11.0, 0.0, "PLN"),
            ],
            refs=[_ref(_I1, "XRP-EUR", "EUR")],
            candles=[_candle(_m(-1), 10.0), _candle(_m(0), 12.0)],
            fx_rows=[_fx_row("EUR", "PLN", 0, 4.0, exchange="walutomat")],
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _m(1),
            "1m",
            _m(2),
            valuation_ccy="PLN",
        )
        assert result.points[0].valuation_status == "complete"
        point = result.points[1]
        assert point.valuation_status == "incomplete"
        assert (
            point.realized_pnl,
            point.fee_pnl,
            point.accrual_pnl,
            point.unrealized_pnl,
            point.net_pnl,
        ) == (None, None, None, None, None)
        contribution = point.per_instrument[0]
        assert (
            contribution.realized_pnl,
            contribution.fee_pnl,
            contribution.accrual_pnl,
            contribution.unrealized_pnl,
        ) == (None, None, None, None)

    async def test_shared_foreign_quote_loads_one_pair_set_per_rate_plane(self) -> None:
        """Two EUR instruments request pair evidence by currency, not instrument."""
        repo = FakeRepo(
            executions=[
                _exec_row(_I1, 1, 0, "buy", 1.0, 10.0, 0.0, "PLN"),
                _exec_row(_I2, 2, 0, "buy", 1.0, 20.0, 0.0, "PLN"),
            ],
            refs=[_ref(_I1, "XRP-EUR", "EUR"), _ref(_I2, "BTC-EUR", "EUR")],
            candles=[_candle(_m(-1), 10.0), _candle(_m(-1), 20.0, instrument=_I2)],
            fx_rows=[_fx_row("EUR", "PLN", 0, 4.0, exchange="walutomat")],
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _T0,
            "1m",
            _m(1),
            valuation_ccy="PLN",
        )
        assert result.points[0].valuation_status == "complete"
        assert repo.fx_candidate_pair_calls == [
            [("EUR", "PLN"), ("PLN", "EUR")],
            [("EUR", "PLN"), ("PLN", "EUR")],
        ]
        assert repo.fx_pair_calls == [
            [("EUR", "PLN")],
            [("EUR", "PLN")],
        ]

    async def test_pre_window_fill_and_marks_use_separate_fx_ranges(self) -> None:
        """An old basis rate does not widen the current mark-rate candle read."""
        repo = FakeRepo(
            executions=[_exec_row(_I1, 1, -600, "buy", 1.0, 10.0, 0.0, "PLN")],
            refs=[_ref(_I1, "XRP-EUR", "EUR")],
            candles=[_candle(_m(-1), 10.0)],
            fx_rows=[
                _fx_row("EUR", "PLN", -600, 4.0, exchange="walutomat"),
                _fx_row("EUR", "PLN", 0, 5.0, exchange="walutomat"),
            ],
        )
        result = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _T0,
            "1m",
            _m(1),
            valuation_ccy="PLN",
        )
        assert result.points[0].valuation_status == "complete"
        assert result.points[0].unrealized_pnl == 10.0
        assert repo.fx_range_calls == [
            (_m(-1), _T0),
            (_m(-601), _m(-600)),
        ]


class TestBuildWalletPnlTimeline:
    """Cover independent marker sourcing, outcomes, ordering, and capping."""

    async def test_emits_all_marker_kinds_and_preserves_no_fill_decisions(self) -> None:
        """Rejected and never-executed decisions survive without fill lineage."""
        executions = [
            _exec_row(_I1, 1, -1, "buy", 1.0, 99.0, 0.0, "USD"),
            _exec_row(_I1, 2, 1, "buy", 1.0, 100.0, 0.0, "USD"),
        ]
        signals = [
            _signal_row("signal-no-fill", _m(0), False),
            _signal_row("signal-executed", _m(2), True),
        ]
        ai_decisions = [
            _ai_decision_row(
                "ai-reject",
                _m(0),
                "reject",
                "resolved_rejected",
                False,
            ),
            _ai_decision_row(
                "ai-status-reject",
                _m(0) + timedelta(seconds=1),
                7,
                "resolved_rejected",
                True,
                rationale=9,
            ),
            _ai_decision_row(
                "ai-executed",
                _m(1),
                "approve",
                "resolved_approved",
                True,
            ),
            _ai_decision_row(
                "ai-no-fill",
                _m(1) + timedelta(seconds=1),
                "approve",
                "resolved_approved",
                False,
                rationale=None,
            ),
        ]
        refs = [_ref(_I1, "BTC-USD", "USD")]
        candles = [
            _candle(_m(-1), 100.0),
            _candle(_m(0), 101.0),
            _candle(_m(1), 102.0),
        ]
        repo = FakeRepo(
            executions=executions,
            signals=signals,
            ai_decisions=ai_decisions,
            refs=refs,
            candles=candles,
        )
        result = await build_wallet_pnl_timeline(
            repo,
            "w1",
            "live",
            _T0,
            _m(2),
            "1m",
            _m(3),
        )
        assert result.marker_limit == PNL_TIMELINE_MARKER_LIMIT
        assert result.markers_truncated is False
        assert len(result.series.points) == 3
        assert repo.execution_calls == [("w1", "live", _m(3))]
        assert repo.signal_calls == [
            ("w1", "live", _T0, _m(2), _m(3), PNL_TIMELINE_MARKER_LIMIT + 1)
        ]
        assert repo.ai_decision_calls == [
            ("w1", "live", _T0, _m(2), _m(3), PNL_TIMELINE_MARKER_LIMIT + 1)
        ]
        assert [marker.marker_time for marker in result.markers] == sorted(
            marker.marker_time for marker in result.markers
        )
        fills = [marker for marker in result.markers if isinstance(marker, PnlFillMarker)]
        assert len(fills) == 1
        assert fills[0].execution_public_id == f"execution-{_I1}-2"
        assert fills[0].order_public_id == f"order-{_I1}-2"
        assert fills[0].status == "filled"
        assert fills[0].outcome == "executed"
        assert fills[0].price == 100.0
        signal_markers = {
            marker.signal_public_id: marker
            for marker in result.markers
            if isinstance(marker, PnlSignalMarker)
        }
        assert signal_markers["signal-no-fill"].outcome == "no_fill"
        assert signal_markers["signal-no-fill"].status == "no_fill"
        assert signal_markers["signal-executed"].outcome == "executed"
        assert signal_markers["signal-executed"].status == "executed"
        assert signal_markers["signal-executed"].price == 101.0
        ai_markers = {
            marker.event_public_id: marker
            for marker in result.markers
            if isinstance(marker, PnlAiDecisionMarker)
        }
        assert ai_markers["ai-reject"].outcome == "rejected"
        assert ai_markers["ai-status-reject"].outcome == "rejected"
        assert ai_markers["ai-status-reject"].decision is None
        assert ai_markers["ai-status-reject"].rationale is None
        assert ai_markers["ai-executed"].outcome == "executed"
        assert ai_markers["ai-no-fill"].outcome == "no_fill"
        assert ai_markers["ai-no-fill"].rationale is None

    @pytest.mark.parametrize("bad_price", [0.0, -1.25, float("nan")])
    async def test_invalid_opening_price_is_mark_incomplete_until_later_close(
        self, bad_price: float
    ) -> None:
        """Unknown opening basis keeps cumulatives until realization is attempted."""
        executions = [
            _exec_row(_I1, 1, 0, "buy", 1.0, bad_price, 0.25, "USD"),
            _exec_row(_I1, 2, 1, "sell", 1.0, 110.0, 0.5, "USD"),
        ]
        repo = FakeRepo(
            executions=executions,
            refs=[_ref(_I1, "BTC-USD", "USD")],
            candles=[_candle(_m(-1), 100.0), _candle(_m(0), 110.0)],
        )
        result = await build_wallet_pnl_timeline(
            repo,
            "w1",
            "live",
            _T0,
            _m(1),
            "1m",
            _m(2),
        )
        opening_point, closing_point = result.series.points
        assert opening_point.valuation_status == "incomplete"
        assert (
            opening_point.realized_pnl,
            opening_point.fee_pnl,
            opening_point.accrual_pnl,
            opening_point.unrealized_pnl,
            opening_point.net_pnl,
        ) == (0.0, -0.25, 0.0, None, None)
        assert closing_point.valuation_status == "incomplete"
        assert (
            closing_point.realized_pnl,
            closing_point.fee_pnl,
            closing_point.accrual_pnl,
            closing_point.unrealized_pnl,
            closing_point.net_pnl,
        ) == (None, None, None, None, None)
        fills = [marker for marker in result.markers if isinstance(marker, PnlFillMarker)]
        assert [marker.price for marker in fills] == [None, 110.0]

    @pytest.mark.parametrize("bad_price", [0.0, -1.25, float("nan")])
    async def test_invalid_add_price_is_mark_incomplete_not_untrusted(
        self, bad_price: float
    ) -> None:
        """An invalid same-side add poisons basis without inventing realization."""
        executions = [
            _exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.0, "USD"),
            _exec_row(_I1, 2, 1, "buy", 1.0, bad_price, 0.25, "USD"),
        ]
        repo = FakeRepo(
            executions=executions,
            refs=[_ref(_I1, "BTC-USD", "USD")],
            candles=[_candle(_m(-1), 100.0), _candle(_m(0), 110.0)],
        )
        result = await build_wallet_pnl_timeline(
            repo,
            "w1",
            "live",
            _T0,
            _m(1),
            "1m",
            _m(2),
        )
        opening_point, add_point = result.series.points
        assert opening_point.valuation_status == "complete"
        assert add_point.valuation_status == "incomplete"
        assert (
            add_point.realized_pnl,
            add_point.fee_pnl,
            add_point.accrual_pnl,
            add_point.unrealized_pnl,
            add_point.net_pnl,
        ) == (0.0, -0.25, 0.0, None, None)
        fills = [marker for marker in result.markers if isinstance(marker, PnlFillMarker)]
        assert [marker.price for marker in fills] == [100.0, None]

    @pytest.mark.parametrize("bad_price", [0.0, -1.25, float("nan")])
    @pytest.mark.parametrize(
        "closing_size",
        [
            pytest.param(0.5, id="reduction"),
            pytest.param(1.0, id="close"),
            pytest.param(2.0, id="flip"),
        ],
    )
    async def test_invalid_reduction_close_or_flip_price_is_untrusted(
        self,
        bad_price: float,
        closing_size: float,
    ) -> None:
        """Any invalid-price closing quantity makes cumulatives unprovable."""
        executions = [
            _exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.0, "USD"),
            _exec_row(_I1, 2, 1, "sell", closing_size, bad_price, 0.25, "USD"),
        ]
        repo = FakeRepo(
            executions=executions,
            refs=[_ref(_I1, "BTC-USD", "USD")],
            candles=[_candle(_m(-1), 100.0), _candle(_m(0), 110.0)],
        )
        result = await build_wallet_pnl_timeline(
            repo,
            "w1",
            "live",
            _T0,
            _m(1),
            "1m",
            _m(2),
        )
        opening_point, closing_point = result.series.points
        assert opening_point.valuation_status == "complete"
        assert closing_point.valuation_status == "incomplete"
        assert (
            closing_point.realized_pnl,
            closing_point.fee_pnl,
            closing_point.accrual_pnl,
            closing_point.unrealized_pnl,
            closing_point.net_pnl,
        ) == (None, None, None, None, None)
        fills = [marker for marker in result.markers if isinstance(marker, PnlFillMarker)]
        assert [marker.price for marker in fills] == [100.0, None]

    async def test_foreign_fill_and_signal_only_prices_are_withheld(self) -> None:
        """Raw marker prices cannot escape without direct denomination proof."""
        executions = [_exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.0, "EUR")]
        signals = [_signal_row("signal-foreign", _m(0), False, _I2, 250.0)]
        repo = FakeRepo(
            executions=executions,
            signals=signals,
            refs=[
                _ref(_I1, "BTC-USD", "USD"),
                _ref(_I2, "ETH-PLN", "PLN", exchange="walutomat"),
            ],
        )
        result = await build_wallet_pnl_timeline(
            repo,
            "w1",
            "live",
            _T0,
            _T0,
            "1m",
            _m(1),
            valuation_ccy="EUR",
        )
        fill = next(marker for marker in result.markers if isinstance(marker, PnlFillMarker))
        signal = next(marker for marker in result.markers if isinstance(marker, PnlSignalMarker))
        assert fill.price is None
        assert signal.price is None
        assert repo.symbol_ref_calls == [[_I1], [_I1, _I2]]

    async def test_signal_only_same_currency_price_remains_visible(self) -> None:
        """A signal-only instrument gets an independent direct-currency proof."""
        repo = FakeRepo(
            signals=[_signal_row("signal-usd", _T0, False, _I2, 250.0)],
            refs=[_ref(_I2, "ETH-USD", "USD")],
        )
        result = await build_wallet_pnl_timeline(
            repo,
            "w1",
            "live",
            _T0,
            _T0,
            "1m",
            _m(1),
        )
        signal = next(marker for marker in result.markers if isinstance(marker, PnlSignalMarker))
        assert signal.price == 250.0
        assert repo.symbol_ref_calls == [[], [_I2]]

    @pytest.mark.parametrize("bad_price", [0.0, -1.25])
    async def test_signal_price_needs_value_proof_not_only_currency_proof(
        self, bad_price: float
    ) -> None:
        """A proven currency does not make a non-positive signal price publishable.

        Signal prices are captured from the same candle-close plane the mark
        filter rejects, so currency proof alone would republish exactly the
        corruption that filter exists to withhold. The fill marker already
        applies both halves of the gate; a signal marker on the same overlay
        must not disagree with it about whether zero is a real price.
        """
        repo = FakeRepo(
            signals=[_signal_row("signal-bad", _T0, False, _I2, bad_price)],
            refs=[_ref(_I2, "ETH-USD", "USD")],
        )
        result = await build_wallet_pnl_timeline(
            repo,
            "w1",
            "live",
            _T0,
            _T0,
            "1m",
            _m(1),
        )
        signal = next(marker for marker in result.markers if isinstance(marker, PnlSignalMarker))
        assert signal.price is None

    async def test_future_quote_revision_withholds_historical_window(self) -> None:
        """A known future quote correction invalidates historical fill prices."""
        executions = [
            _exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.0, "USD"),
            _exec_row(_I1, 2, 0, "sell", 1.0, 110.0, 0.0, "USD"),
            _exec_row(_I1, 3, 3, "buy", 1.0, 120.0, 0.0, "USD"),
        ]
        repo = FakeRepo(
            executions=executions,
            refs=[
                _ref(_I1, "BTC-USD", "USD", valid_to=_m(2)),
                _ref(_I1, "XBT-EUR", "EUR", valid_from=_m(2)),
            ],
        )
        result = await build_wallet_pnl_timeline(
            repo,
            "w1",
            "live",
            _T0,
            _T0,
            "1m",
            _m(4),
        )
        point = result.series.points[0]
        assert point.valuation_status == "incomplete"
        assert (
            point.realized_pnl,
            point.fee_pnl,
            point.accrual_pnl,
            point.unrealized_pnl,
            point.net_pnl,
        ) == (None, None, None, None, None)
        fills = [marker for marker in result.markers if isinstance(marker, PnlFillMarker)]
        assert len(fills) == 2
        assert [marker.price for marker in fills] == [None, None]

    async def test_exact_cap_is_complete_and_over_cap_keeps_latest(self) -> None:
        """The exact cap is complete; one extra drops the oldest with disclosure."""
        exact_rows = [
            _signal_row(f"signal-{index:04d}", _T0, False)
            for index in reversed(range(PNL_TIMELINE_MARKER_LIMIT))
        ]
        exact = await build_wallet_pnl_timeline(
            FakeRepo(signals=exact_rows),
            "w1",
            "live",
            _T0,
            _T0,
            "1m",
            _m(1),
        )
        assert len(exact.markers) == PNL_TIMELINE_MARKER_LIMIT
        assert exact.markers_truncated is False
        over_rows = [
            _signal_row(f"signal-{index:04d}", _T0, False)
            for index in reversed(range(PNL_TIMELINE_MARKER_LIMIT + 1))
        ]
        over_repo = FakeRepo(signals=over_rows)
        over = await build_wallet_pnl_timeline(
            over_repo,
            "w1",
            "live",
            _T0,
            _T0,
            "1m",
            _m(1),
        )
        assert len(over.markers) == PNL_TIMELINE_MARKER_LIMIT
        assert over.markers_truncated is True
        first = over.markers[0]
        last = over.markers[-1]
        assert isinstance(first, PnlSignalMarker)
        assert isinstance(last, PnlSignalMarker)
        assert first.signal_public_id == "signal-0001"
        assert last.signal_public_id == f"signal-{PNL_TIMELINE_MARKER_LIMIT:04d}"
        assert over_repo.signal_calls[0][-1] == PNL_TIMELINE_MARKER_LIMIT + 1
        assert over_repo.ai_decision_calls[0][-1] == PNL_TIMELINE_MARKER_LIMIT + 1


def test_provenance_constants_are_stable() -> None:
    """Pin the public reconstruction provenance and work-budget constants.

    Given: The P&L timeline service's public constants,
    When: Their values are inspected,
    Then: The documented source, version, and total-work limit remain stable.
    """
    assert PNL_TIMELINE_MARK_SOURCE == "finalized_1m_candle_close"
    assert PNL_TIMELINE_CALC_VERSION == "5A.9"
    assert PNL_TIMELINE_MAX_WORK_UNITS == 131_040
    assert PNL_TIMELINE_MARKER_LIMIT == 2_000


class TestForeignFeeConversion:
    """Cover the live-UAT blocker: a foreign fee must convert, not poison."""

    def _repo(self, fx_rows: list[PnlFxRateRow]) -> FakeRepo:
        """Build a USD-quoted scope whose single fill charges a EUR fee."""
        return FakeRepo(
            executions=[_exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.04, "EUR")],
            refs=[_ref(_I1, "BTC-USD", "USD")],
            candles=[_candle(_m(-1), 100.0), _candle(_m(0), 100.0)],
            fx_rows=fx_rows,
        )

    async def test_foreign_fee_converts_and_keeps_the_series_complete(self) -> None:
        """A EUR fee priced by a EUR-USD candle no longer withholds the series.

        This is the exact production case found in live UAT: a 0.04 EUR fee on a
        USD-valued series withheld 1432 of 1441 points because the flow was
        unconvertible. With the pair available the fee becomes real money and the
        point is complete again.
        """
        repo = self._repo([_fx_row("EUR", "USD", 0, 1.25)])
        result = await build_wallet_pnl_series(repo, "w1", "live", _T0, _m(0), "1m", _T0)
        point = result.points[0]
        assert point.valuation_status == "complete"
        assert point.fee_pnl == pytest.approx(-0.05)
        assert point.realized_pnl == 0.0
        assert repo.fx_candidate_pair_calls == [[("EUR", "USD"), ("USD", "EUR")]]
        assert repo.fx_pair_calls == [[("EUR", "USD")]]

    async def test_inverse_pair_also_converts(self) -> None:
        """Only a USD-EUR listing still prices the fee, by reciprocal."""
        repo = self._repo([_fx_row("USD", "EUR", 0, 0.8)])
        result = await build_wallet_pnl_series(repo, "w1", "live", _T0, _m(0), "1m", _T0)
        assert result.points[0].fee_pnl == pytest.approx(-0.05)
        assert repo.fx_candidate_pair_calls == [[("EUR", "USD"), ("USD", "EUR")]]
        assert repo.fx_pair_calls == [[("USD", "EUR")]]

    async def test_unresolvable_pair_still_withholds(self) -> None:
        """With no pair at all the fee stays unknown and the point is withheld.

        The conversion must not silently substitute a zero or a stale rate — an
        unpriceable cost is still unknown, and saying so is the whole point.
        """
        repo = self._repo([])
        result = await build_wallet_pnl_series(repo, "w1", "live", _T0, _m(0), "1m", _T0)
        assert result.points[0].valuation_status == "incomplete"
        assert result.points[0].fee_pnl is None

    async def test_conflicting_rate_denomination_withholds_nonzero_fee(
        self,
        conflicting_fx_repository: SQLAlchemyRepository,
    ) -> None:
        """A quote conflict known after authoring turns a proven fee unknown."""
        repo = FakeRepo(
            executions=[_exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.04, "GBP")],
            refs=[_ref(_I1, "BTC-PLN", "PLN")],
            candles=[_candle(_m(-1), 100.0)],
            fx_repository=conflicting_fx_repository,
        )
        before = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _T0,
            "1m",
            _m(0),
            valuation_ccy="PLN",
        )
        before_point = before.points[0]
        assert before_point.valuation_status == "complete"
        assert (
            before_point.realized_pnl,
            before_point.fee_pnl,
            before_point.accrual_pnl,
            before_point.unrealized_pnl,
            before_point.net_pnl,
        ) == (0.0, -0.2, 0.0, 0.0, -0.2)
        assert [
            (
                source.base_currency,
                source.quote_currency,
                source.exchange,
            )
            for source in before.rate_sources
        ] == [("GBP", "PLN", "walutomat")]
        after = await build_wallet_pnl_series(
            repo,
            "w1",
            "live",
            _T0,
            _T0,
            "1m",
            _m(2),
            valuation_ccy="PLN",
        )
        after_point = after.points[0]
        assert after_point.valuation_status == "incomplete"
        assert (
            after_point.realized_pnl,
            after_point.fee_pnl,
            after_point.accrual_pnl,
            after_point.unrealized_pnl,
            after_point.net_pnl,
        ) == (None, None, None, None, None)
        assert after.rate_sources == ()

    async def test_rate_from_another_minute_is_not_borrowed(self) -> None:
        """A rate that only covers a later minute cannot price an earlier fee."""
        repo = self._repo([_fx_row("EUR", "USD", 3, 1.25)])
        result = await build_wallet_pnl_series(repo, "w1", "live", _T0, _m(0), "1m", _T0)
        assert result.points[0].valuation_status == "incomplete"


class TestFxRangeCoversPreWindowFlows:
    """Cover the production defect: a foreign fee that predates the window."""

    async def test_fee_before_the_window_still_converts(self) -> None:
        """A fill older than `from_time` is replayed, so its fee needs ITS rate.

        Found on prod: the wallet's only foreign-currency fill happened before the
        requested 24h window. The fill is still replayed — it seeds the pool the
        window opens with — but rates were loaded for the WINDOW only, so its fee
        stayed unconvertible and withheld all 1441 points. The rate range must
        follow the flows, not the window.
        """
        repo = FakeRepo(
            executions=[_exec_row(_I1, 1, -600, "buy", 1.0, 100.0, 0.04, "EUR")],
            refs=[_ref(_I1, "BTC-USD", "USD")],
            candles=[_candle(_m(-1), 100.0), _candle(_m(0), 100.0)],
            fx_rows=[_fx_row("EUR", "USD", -600, 1.25)],
        )
        result = await build_wallet_pnl_series(repo, "w1", "live", _T0, _m(0), "1m", _T0)
        point = result.points[0]
        assert point.valuation_status == "complete"
        assert point.fee_pnl == pytest.approx(-0.05)
        requested_start, requested_end = repo.fx_range_calls[0]
        assert requested_start <= _m(-600)
        assert requested_end >= _m(-600)

    async def test_no_foreign_flow_skips_the_rate_read_entirely(self) -> None:
        """An all-native scope never touches the FX candle read."""
        repo = FakeRepo(
            executions=[_exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.5, "USD")],
            refs=[_ref(_I1, "BTC-USD", "USD")],
            candles=[_candle(_m(-1), 100.0), _candle(_m(0), 100.0)],
        )
        result = await build_wallet_pnl_series(repo, "w1", "live", _T0, _m(0), "1m", _T0)
        assert result.points[0].valuation_status == "complete"
        assert repo.fx_pair_calls == []

    async def test_zero_foreign_fee_needs_no_rate(self) -> None:
        """A zero fee in another currency is invariant and asks for no rate."""
        repo = FakeRepo(
            executions=[_exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.0, "EUR")],
            refs=[_ref(_I1, "BTC-USD", "USD")],
            candles=[_candle(_m(-1), 100.0), _candle(_m(0), 100.0)],
        )
        result = await build_wallet_pnl_series(repo, "w1", "live", _T0, _m(0), "1m", _T0)
        assert result.points[0].valuation_status == "complete"
        assert repo.fx_pair_calls == []

    async def test_nonzero_fee_with_unknown_denomination_is_withheld(self) -> None:
        """A nonzero fee with an EMPTY asset cannot be priced, so it withholds.

        Production writes an empty `fee_asset` for fee-free fills; a NONZERO
        amount carrying no denomination is a different thing entirely — there is
        no currency to convert from, so no pair can be requested and the value
        must stay unknown rather than be guessed at par.
        """
        repo = FakeRepo(
            executions=[_exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.04, "")],
            refs=[_ref(_I1, "BTC-USD", "USD")],
            candles=[_candle(_m(-1), 100.0), _candle(_m(0), 100.0)],
        )
        result = await build_wallet_pnl_series(repo, "w1", "live", _T0, _m(0), "1m", _T0)
        assert result.points[0].valuation_status == "incomplete"
        assert result.points[0].fee_pnl is None
        assert repo.fx_pair_calls == []
