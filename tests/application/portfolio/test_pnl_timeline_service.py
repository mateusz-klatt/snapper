"""Tests for the async P&L timeline orchestrator + mark builder (Phase 5A).

Covers the seam between the repository reads and the pure builder: zero-safe
flow mapping, canonical-source PAPER marks, batched candle loading, total-work
budgeting, durable fill-gap withholding, and end-to-end orchestration.
"""

import math
from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest

from snapper.application.portfolio.pnl_timeline_service import PNL_TIMELINE_CALC_VERSION
from snapper.application.portfolio.pnl_timeline_service import PNL_TIMELINE_MARK_SOURCE
from snapper.application.portfolio.pnl_timeline_service import PNL_TIMELINE_MARKER_LIMIT
from snapper.application.portfolio.pnl_timeline_service import PNL_TIMELINE_MAX_WORK_UNITS
from snapper.application.portfolio.pnl_timeline_service import PnlAiDecisionMarker
from snapper.application.portfolio.pnl_timeline_service import PnlFillMarker
from snapper.application.portfolio.pnl_timeline_service import PnlSignalMarker
from snapper.application.portfolio.pnl_timeline_service import PnlTimelineWorkBudgetError
from snapper.application.portfolio.pnl_timeline_service import _to_timeline_accrual
from snapper.application.portfolio.pnl_timeline_service import _to_timeline_execution
from snapper.application.portfolio.pnl_timeline_service import build_marks
from snapper.application.portfolio.pnl_timeline_service import build_wallet_pnl_series
from snapper.application.portfolio.pnl_timeline_service import build_wallet_pnl_timeline
from snapper.data.repository_types import InstrumentSymbolRefRow
from snapper.data.repository_types import PnlFxRateRow
from snapper.data.repository_types import PnlTimelineAccrualRow
from snapper.data.repository_types import PnlTimelineAiDecisionMarkerRow
from snapper.data.repository_types import PnlTimelineCandleRow
from snapper.data.repository_types import PnlTimelineExecutionRow
from snapper.data.repository_types import PnlTimelineSignalMarkerRow

_T0 = datetime(2026, 7, 20, 10, 0, tzinfo=UTC)
_I1 = "00000000-0000-7000-8000-000000000b01"
_I2 = "00000000-0000-7000-8000-000000000b02"


def _m(minute: int) -> datetime:
    """Return the grid minute ``_T0 + minute``."""
    return _T0 + timedelta(minutes=minute)


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


def _signal_row(
    public_id: str,
    marker_time: datetime,
    has_execution: bool,
) -> PnlTimelineSignalMarkerRow:
    """Build one independently sourced signal-marker row."""
    return {
        "public_id": public_id,
        "instrument_public_id": _I1,
        "fired_at": marker_time,
        "side": "buy",
        "strategy_name": "momentum",
        "strength": 0.8,
        "reason": "breakout",
        "price": 101.0,
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
) -> InstrumentSymbolRefRow:
    """Build one symbol reference row as ``get_instrument_symbol_refs`` returns it."""
    return {
        "instrument_public_id": instrument,
        "native_symbol": native_symbol,
        "exchange": exchange,
        "quote_currency": quote,
    }


def _candle(
    open_at: datetime,
    close: float,
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
        fill_shard_keys: Sequence[str] = (),
        gapped_shards: set[str] | None = None,
        fx_rows: Sequence[PnlFxRateRow] | None = None,
    ) -> None:
        """Store the canned read results and record the calls made."""
        self._executions = list(executions)
        self._accruals = list(accruals)
        self._refs = list(refs)
        self._candles = list(candles)
        self._signals = list(signals)
        self._ai_decisions = list(ai_decisions)
        self._fill_shard_keys = list(fill_shard_keys)
        self._gapped_shards = gapped_shards or set()
        self._fx_rows: list[PnlFxRateRow] = list(fx_rows or [])
        self.fx_pair_calls: list[list[tuple[str, str]]] = []
        self.symbol_ref_calls: list[list[str]] = []
        self.candle_calls: list[
            tuple[list[InstrumentSymbolRefRow], datetime, datetime, datetime]
        ] = []
        self.fill_scope_calls: list[tuple[str, str, datetime]] = []
        self.fill_gap_calls: list[tuple[str, str, str, datetime]] = []
        self.execution_calls: list[tuple[str, str, datetime]] = []
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

    async def get_pnl_fx_rate_candles(
        self,
        pairs: Sequence[tuple[str, str]],
        start: datetime,
        end: datetime,
        as_of: datetime,
    ) -> list[PnlFxRateRow]:
        """Record the requested pairs and return the canned FX candles."""
        self.fx_pair_calls.append(list(pairs))
        requested = set(pairs)
        return [row for row in self._fx_rows if (row["base"], row["quote"]) in requested]


class TestToTimelineExecution:
    """Cover the execution row mapping and fee currency discipline."""

    def test_maps_fields_and_uses_timestamp_as_event_time(self) -> None:
        """Row fields map through and ``event_time`` is the timestamp axis."""
        row = _exec_row(_I1, 1, 3, "buy", 2.0, 100.0, 0.5, "USD")
        mapped = _to_timeline_execution(row, "USD", {})
        assert mapped.instrument_public_id == _I1
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
        refs = [_ref(_I1, "BTC-USD", "USD")]
        candles = [_candle(_m(-1), 105.0), _candle(_m(0), 110.0), _candle(_m(1), 120.0)]
        repo = FakeRepo(
            executions=executions,
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
        assert repo.fill_scope_calls == [("w1", "live", as_of)]
        assert repo.fill_gap_calls == [("clean-shard", "w1", "live", as_of)]
        assert repo.execution_calls == [("w1", "live", as_of)]
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
        refs = [_ref(_I1, "BTC-USD", "USD", exchange="kraken")]
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
        signal_markers = {
            marker.signal_public_id: marker
            for marker in result.markers
            if isinstance(marker, PnlSignalMarker)
        }
        assert signal_markers["signal-no-fill"].outcome == "no_fill"
        assert signal_markers["signal-no-fill"].status == "no_fill"
        assert signal_markers["signal-executed"].outcome == "executed"
        assert signal_markers["signal-executed"].status == "executed"
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
    assert PNL_TIMELINE_CALC_VERSION == "5A.2"
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
        assert repo.fx_pair_calls[0] == [("EUR", "USD"), ("USD", "EUR")]

    async def test_inverse_pair_also_converts(self) -> None:
        """Only a USD-EUR listing still prices the fee, by reciprocal."""
        repo = self._repo([_fx_row("USD", "EUR", 0, 0.8)])
        result = await build_wallet_pnl_series(repo, "w1", "live", _T0, _m(0), "1m", _T0)
        assert result.points[0].fee_pnl == pytest.approx(-0.05)

    async def test_unresolvable_pair_still_withholds(self) -> None:
        """With no pair at all the fee stays unknown and the point is withheld.

        The conversion must not silently substitute a zero or a stale rate — an
        unpriceable cost is still unknown, and saying so is the whole point.
        """
        repo = self._repo([])
        result = await build_wallet_pnl_series(repo, "w1", "live", _T0, _m(0), "1m", _T0)
        assert result.points[0].valuation_status == "incomplete"
        assert result.points[0].fee_pnl is None

    async def test_rate_from_another_minute_is_not_borrowed(self) -> None:
        """A rate that only covers a later minute cannot price an earlier fee."""
        repo = self._repo([_fx_row("EUR", "USD", 3, 1.25)])
        result = await build_wallet_pnl_series(repo, "w1", "live", _T0, _m(0), "1m", _T0)
        assert result.points[0].valuation_status == "incomplete"
