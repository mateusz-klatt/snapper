"""Tests for ContinuousContractBuilder service."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock

import pytest

from snapper.application.services.continuous_contract_builder import ContinuousContractBuilder
from snapper.application.services.continuous_contract_builder import _timeframe_to_seconds
from snapper.data.repository_types import CandleRow
from snapper.data.repository_types import InstrumentContractRow


def _contract(symbol: str, expiry_days: int, family: str = "ES") -> InstrumentContractRow:
    """Build a test contract row."""
    return {
        "instrument_public_id": f"inst-{symbol}",
        "native_symbol": symbol,
        "exchange": "kraken_equities",
        "expiry_at": datetime(2026, 1, 1, tzinfo=UTC) + timedelta(days=expiry_days),
        "instrument_kind": "future",
        "relationship_type": "derivative",
        "contract_family": family,
        "is_front_month": False,
    }


def _candle(
    open_at: datetime,
    close: float,
    open_price: float = 100.0,
    high: float = 110.0,
    low: float = 90.0,
    volume: float = 1000.0,
) -> CandleRow:
    """Build a test candle row."""
    return {
        "open_at": open_at,
        "timeframe": "1d",
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "vwap": close,
        "trades": 100,
        "public_id": "candle-1",
        "timestamp": open_at,
        "session_id": "s1",
        "sequence_id": 1,
    }


def _mock_repo(
    contracts: list[InstrumentContractRow],
    candles_by_symbol: dict[str, list[CandleRow]],
) -> Any:
    """Build a mock repository."""
    repo = AsyncMock()
    repo.get_contracts_for_underlying = AsyncMock(return_value=contracts)

    async def _get_candles(
        instrument: str, timeframe: str, start: Any, end: Any, exchange: str, as_of: Any, **kw: Any
    ) -> list[CandleRow]:
        return candles_by_symbol.get(instrument, [])

    repo.get_candles = AsyncMock(side_effect=_get_candles)
    return repo


D1 = datetime(2026, 1, 1, tzinfo=UTC)
D2 = datetime(2026, 1, 2, tzinfo=UTC)
D3 = datetime(2026, 1, 3, tzinfo=UTC)
D4 = datetime(2026, 1, 4, tzinfo=UTC)
D5 = datetime(2026, 1, 5, tzinfo=UTC)
D6 = datetime(2026, 1, 6, tzinfo=UTC)
D7 = datetime(2026, 1, 7, tzinfo=UTC)
D8 = datetime(2026, 1, 8, tzinfo=UTC)


class TestTwoContractStitch:
    """Basic 2-contract stitching with all adjustment methods."""

    @pytest.mark.asyncio
    async def test_unadjusted_has_gap(self) -> None:
        """Unadjusted method shows raw prices with gap at rollover.

        Given: C1 (expiry D4) with close=100, C2 (expiry D8) with close=110 at roll,
        When: build with method=unadjusted,
        Then: bars from C1 show raw prices, gap visible at transition.
        """
        c1 = _contract("ESM6", expiry_days=3)
        c2 = _contract("ESU6", expiry_days=7)
        candles = {
            "ESM6": [_candle(D1, 100.0), _candle(D2, 102.0), _candle(D3, 104.0)],
            "ESU6": [_candle(D3, 114.0), _candle(D4, 116.0), _candle(D5, 118.0)],
        }
        repo = _mock_repo([c1, c2], candles)
        builder = ContinuousContractBuilder(repository=repo)
        result = await builder.build(
            underlying_public_id="u1",
            exchange="kraken_equities",
            contract_family="ES",
            timeframe="1d",
            start=D1,
            end=D5 + timedelta(hours=23),
            method="unadjusted",
        )
        assert len(result.candles) == 5
        assert result.candles[0]["source_contract"] == "ESM6"
        assert result.candles[0]["close"] == 100.0
        assert result.candles[0]["adjustment_factor"] is None
        assert result.candles[-1]["source_contract"] == "ESU6"

    @pytest.mark.asyncio
    async def test_panama_preserves_absolute_difference(self) -> None:
        """Panama method adjusts early bars by cumulative delta.

        Given: C1 close=100 at roll, C2 close=110 at roll �� delta=10,
        When: build with method=panama,
        Then: C1 bars shifted by +10.
        """
        c1 = _contract("ESM6", expiry_days=3)
        c2 = _contract("ESU6", expiry_days=7)
        candles = {
            "ESM6": [_candle(D1, 100.0), _candle(D2, 102.0)],
            "ESU6": [_candle(D2, 112.0), _candle(D4, 116.0), _candle(D5, 118.0)],
        }
        repo = _mock_repo([c1, c2], candles)
        builder = ContinuousContractBuilder(repository=repo)
        result = await builder.build(
            underlying_public_id="u1",
            exchange="kraken_equities",
            contract_family="ES",
            timeframe="1d",
            start=D1,
            end=D5 + timedelta(hours=23),
            method="panama",
        )
        delta = 112.0 - 102.0
        assert result.candles[0]["close"] == pytest.approx(100.0 + delta)
        assert result.candles[0]["adjustment_factor"] == pytest.approx(delta)
        assert result.candles[-1]["adjustment_factor"] is None

    @pytest.mark.asyncio
    async def test_ratio_preserves_percentage_returns(self) -> None:
        """Ratio method multiplies early bars by cumulative ratio.

        Given: C1 close=100 at roll, C2 close=110 → ratio=1.1,
        When: build with method=ratio,
        Then: C1 bars multiplied by 1.1.
        """
        c1 = _contract("ESM6", expiry_days=3)
        c2 = _contract("ESU6", expiry_days=7)
        candles = {
            "ESM6": [_candle(D1, 100.0), _candle(D2, 100.0)],
            "ESU6": [_candle(D2, 110.0), _candle(D4, 116.0)],
        }
        repo = _mock_repo([c1, c2], candles)
        builder = ContinuousContractBuilder(repository=repo)
        result = await builder.build(
            underlying_public_id="u1",
            exchange="kraken_equities",
            contract_family="ES",
            timeframe="1d",
            start=D1,
            end=D5,
            method="ratio",
        )
        ratio = 110.0 / 100.0
        assert result.candles[0]["close"] == pytest.approx(100.0 * ratio)
        assert result.candles[0]["adjustment_factor"] == pytest.approx(ratio)

    @pytest.mark.asyncio
    async def test_source_contract_field(self) -> None:
        """Each bar has correct source_contract field."""
        c1 = _contract("ESM6", expiry_days=3)
        c2 = _contract("ESU6", expiry_days=7)
        candles = {
            "ESM6": [_candle(D1, 100.0)],
            "ESU6": [_candle(D2, 110.0), _candle(D4, 116.0)],
        }
        repo = _mock_repo([c1, c2], candles)
        builder = ContinuousContractBuilder(repository=repo)
        result = await builder.build(
            underlying_public_id="u1",
            exchange="kraken_equities",
            contract_family="ES",
            timeframe="1d",
            start=D1,
            end=D5,
            method="unadjusted",
        )
        contracts = {c["source_contract"] for c in result.candles}
        assert "ESM6" in contracts
        assert "ESU6" in contracts


class TestThreeContractStitch:
    """3-contract stitching verifies cumulative adjustments."""

    @pytest.mark.asyncio
    async def test_cumulative_panama(self) -> None:
        """Panama adjustments accumulate across 2 roll points.

        Given: C1→C2 delta=10, C2→C3 delta=5,
        When: build with panama,
        Then: C1 bars get +15, C2 bars get +5, C3 bars unadjusted.
        """
        c1 = _contract("ESH6", expiry_days=3)
        c2 = _contract("ESM6", expiry_days=6)
        c3 = _contract("ESU6", expiry_days=9)
        candles = {
            "ESH6": [_candle(D1, 100.0)],
            "ESM6": [_candle(D2, 110.0), _candle(D4, 112.0)],
            "ESU6": [_candle(D5, 117.0), _candle(D7, 120.0)],
        }
        repo = _mock_repo([c1, c2, c3], candles)
        builder = ContinuousContractBuilder(repository=repo)
        result = await builder.build(
            underlying_public_id="u1",
            exchange="kraken_equities",
            contract_family="ES",
            timeframe="1d",
            start=D1,
            end=D8,
            method="panama",
        )
        delta_1 = 110.0 - 100.0
        delta_2 = 117.0 - 112.0
        assert result.candles[0]["close"] == pytest.approx(100.0 + delta_1 + delta_2)
        assert result.roll_points[0].from_contract == "ESH6"
        assert result.roll_points[1].from_contract == "ESM6"
        assert result.candles[-1]["adjustment_factor"] is None


class TestEdgeCases:
    """Edge cases: single contract, empty data, roll failures."""

    @pytest.mark.asyncio
    async def test_single_contract_no_stitching(self) -> None:
        """Single contract returns raw candles, no roll points."""
        c1 = _contract("ESM6", expiry_days=30)
        candles = {"ESM6": [_candle(D1, 100.0), _candle(D2, 102.0)]}
        repo = _mock_repo([c1], candles)
        builder = ContinuousContractBuilder(repository=repo)
        result = await builder.build(
            underlying_public_id="u1",
            exchange="kraken_equities",
            contract_family="ES",
            timeframe="1d",
            start=D1,
            end=D3,
        )
        assert len(result.candles) == 2
        assert len(result.roll_points) == 0
        assert result.failed_roll is None

    @pytest.mark.asyncio
    async def test_empty_candle_data(self) -> None:
        """No candle data returns empty result."""
        c1 = _contract("ESM6", expiry_days=30)
        repo = _mock_repo([c1], {})
        builder = ContinuousContractBuilder(repository=repo)
        result = await builder.build(
            underlying_public_id="u1",
            exchange="kraken_equities",
            contract_family="ES",
            timeframe="1d",
            start=D1,
            end=D3,
        )
        assert result.candles == []

    @pytest.mark.asyncio
    async def test_no_common_bar_truncates(self) -> None:
        """Missing common bar at roll point truncates series."""
        c1 = _contract("ESM6", expiry_days=3)
        c2 = _contract("ESU6", expiry_days=7)
        candles = {
            "ESM6": [_candle(D1, 100.0)],
            "ESU6": [_candle(D7, 120.0)],
        }
        repo = _mock_repo([c1, c2], candles)
        builder = ContinuousContractBuilder(repository=repo)
        result = await builder.build(
            underlying_public_id="u1",
            exchange="kraken_equities",
            contract_family="ES",
            timeframe="1d",
            start=D1,
            end=D8,
            method="panama",
        )
        assert result.failed_roll is not None
        assert result.failed_roll.from_contract == "ESM6"
        for bar in result.candles:
            assert bar["open_at"] < result.failed_roll.roll_at

    @pytest.mark.asyncio
    async def test_contracts_used_truncated_on_failed_roll(self) -> None:
        """R1: contracts_used reports only the truncated chain on failed roll.

        Given: a 3-contract chain where the 2->3 roll fails (no common
        bar between ESU6 and ESZ6),
        When: build runs with method='panama',
        Then: result.failed_roll identifies ESU6 as from_contract AND
        result.contracts_used enumerates only the contributing prefix
        [ESM6, ESU6] — NOT the full active chain [ESM6, ESU6, ESZ6].
        The truncated candle series consistently reflects the same
        prefix: no bar has open_at >= failed_roll.roll_at.
        """
        c1 = _contract("ESM6", expiry_days=3)
        c2 = _contract("ESU6", expiry_days=7)
        c3 = _contract("ESZ6", expiry_days=11)
        candles = {
            "ESM6": [_candle(D1, 100.0), _candle(D2, 101.0), _candle(D3, 102.0)],
            "ESU6": [_candle(D3, 102.5), _candle(D4, 103.0), _candle(D5, 104.0)],
            "ESZ6": [_candle(D8, 120.0)],
        }
        repo = _mock_repo([c1, c2, c3], candles)
        builder = ContinuousContractBuilder(repository=repo)
        result = await builder.build(
            underlying_public_id="u1",
            exchange="kraken_equities",
            contract_family="ES",
            timeframe="1d",
            start=D1,
            end=D8,
            method="panama",
        )
        assert result.failed_roll is not None
        assert result.failed_roll.from_contract == "ESU6"
        assert result.contracts_used == ["ESM6", "ESU6"]
        for bar in result.candles:
            assert bar["open_at"] < result.failed_roll.roll_at

    @pytest.mark.asyncio
    async def test_unadjusted_no_overlap_succeeds(self) -> None:
        """Unadjusted method does not require common bar at roll point.

        Given: two contracts with no overlapping bars near roll,
        When: build with method=unadjusted,
        Then: succeeds without failed_roll (gap is expected for unadjusted).
        """
        c1 = _contract("ESM6", expiry_days=3)
        c2 = _contract("ESU6", expiry_days=7)
        candles = {
            "ESM6": [_candle(D1, 100.0)],
            "ESU6": [_candle(D7, 120.0)],
        }
        repo = _mock_repo([c1, c2], candles)
        builder = ContinuousContractBuilder(repository=repo)
        result = await builder.build(
            underlying_public_id="u1",
            exchange="kraken_equities",
            contract_family="ES",
            timeframe="1d",
            start=D1,
            end=D8,
            method="unadjusted",
        )
        assert result.failed_roll is None
        assert len(result.candles) == 2

    @pytest.mark.asyncio
    async def test_missing_middle_contract_truncates(self) -> None:
        """Missing candles for a middle contract truncates at that point.

        Given: contracts A, B, C where B has no candles,
        When: build runs,
        Then: only A's candles are returned (chain truncated at B).
        """
        c1 = _contract("ESH6", expiry_days=3)
        c2 = _contract("ESM6", expiry_days=6)
        c3 = _contract("ESU6", expiry_days=9)
        candles = {
            "ESH6": [_candle(D1, 100.0)],
            "ESU6": [_candle(D7, 120.0)],
        }
        repo = _mock_repo([c1, c2, c3], candles)
        builder = ContinuousContractBuilder(repository=repo)
        result = await builder.build(
            underlying_public_id="u1",
            exchange="kraken_equities",
            contract_family="ES",
            timeframe="1d",
            start=D1,
            end=D8,
            method="panama",
        )
        assert len(result.candles) == 1
        assert result.candles[0]["source_contract"] == "ESH6"

    @pytest.mark.asyncio
    async def test_ratio_rejects_nonpositive_price(self) -> None:
        """Ratio method raises ValueError on non-positive roll price."""
        c1 = _contract("ESM6", expiry_days=3)
        c2 = _contract("ESU6", expiry_days=7)
        candles = {
            "ESM6": [_candle(D1, 0.0), _candle(D2, 0.0)],
            "ESU6": [_candle(D2, 110.0), _candle(D4, 116.0)],
        }
        repo = _mock_repo([c1, c2], candles)
        builder = ContinuousContractBuilder(repository=repo)
        with pytest.raises(ValueError, match="Non-positive"):
            await builder.build(
                underlying_public_id="u1",
                exchange="kraken_equities",
                contract_family="ES",
                timeframe="1d",
                start=D1,
                end=D5,
                method="ratio",
            )

    @pytest.mark.asyncio
    async def test_build_rejects_negative_rollover_days(self) -> None:
        """R2: builder raises ValueError when rollover_days_before < 0."""
        c1 = _contract("ESM6", expiry_days=3)
        repo = _mock_repo([c1], {"ESM6": [_candle(D1, 100.0)]})
        builder = ContinuousContractBuilder(repository=repo)
        with pytest.raises(ValueError, match=r"rollover_days_before must be in \[0, 365\]"):
            await builder.build(
                underlying_public_id="u1",
                exchange="kraken_equities",
                contract_family="ES",
                timeframe="1d",
                start=D1,
                end=D8,
                method="panama",
                rollover_days_before=-1,
            )

    @pytest.mark.asyncio
    async def test_build_rejects_excessive_rollover_days(self) -> None:
        """R2: builder raises ValueError when rollover_days_before > 365."""
        c1 = _contract("ESM6", expiry_days=3)
        repo = _mock_repo([c1], {"ESM6": [_candle(D1, 100.0)]})
        builder = ContinuousContractBuilder(repository=repo)
        with pytest.raises(ValueError, match=r"rollover_days_before must be in \[0, 365\]"):
            await builder.build(
                underlying_public_id="u1",
                exchange="kraken_equities",
                contract_family="ES",
                timeframe="1d",
                start=D1,
                end=D8,
                method="panama",
                rollover_days_before=366,
            )

    @pytest.mark.asyncio
    async def test_invalid_method_raises(self) -> None:
        """Invalid adjustment method raises ValueError."""
        repo = _mock_repo([], {})
        builder = ContinuousContractBuilder(repository=repo)
        with pytest.raises(ValueError, match="Invalid method"):
            await builder.build(
                underlying_public_id="u1",
                exchange="kraken_equities",
                contract_family="ES",
                timeframe="1d",
                start=D1,
                end=D3,
                method="invalid",
            )

    @pytest.mark.asyncio
    async def test_volume_not_adjusted(self) -> None:
        """Volume and trade_count are NOT adjusted."""
        c1 = _contract("ESM6", expiry_days=3)
        c2 = _contract("ESU6", expiry_days=7)
        candles = {
            "ESM6": [_candle(D1, 100.0, volume=500.0), _candle(D2, 100.0, volume=600.0)],
            "ESU6": [_candle(D2, 110.0, volume=700.0), _candle(D4, 116.0, volume=800.0)],
        }
        repo = _mock_repo([c1, c2], candles)
        builder = ContinuousContractBuilder(repository=repo)
        result = await builder.build(
            underlying_public_id="u1",
            exchange="kraken_equities",
            contract_family="ES",
            timeframe="1d",
            start=D1,
            end=D5,
            method="panama",
        )
        assert result.candles[0]["volume"] == 500.0
        assert result.candles[0]["trades"] == 100

    @pytest.mark.asyncio
    async def test_rollover_days_before(self) -> None:
        """Rolling N days before expiry shifts the roll point earlier.

        Given: C1 expiry at D4, rollover_days_before=1 → roll at D3,
        When: build runs,
        Then: bar at D3 belongs to C2 (not C1).
        """
        c1 = _contract("ESM6", expiry_days=3)
        c2 = _contract("ESU6", expiry_days=7)
        candles = {
            "ESM6": [_candle(D1, 100.0), _candle(D2, 102.0), _candle(D3, 104.0)],
            "ESU6": [_candle(D2, 112.0), _candle(D3, 114.0), _candle(D4, 116.0)],
        }
        repo = _mock_repo([c1, c2], candles)
        builder = ContinuousContractBuilder(repository=repo)
        result = await builder.build(
            underlying_public_id="u1",
            exchange="kraken_equities",
            contract_family="ES",
            timeframe="1d",
            start=D1,
            end=D5,
            method="unadjusted",
            rollover_days_before=1,
        )
        d3_bar = [c for c in result.candles if c["open_at"] == D3]
        assert len(d3_bar) == 1
        assert d3_bar[0]["source_contract"] == "ESU6"

    @pytest.mark.asyncio
    async def test_no_futures_returns_empty(self) -> None:
        """No futures contracts (no expiry) returns empty result."""
        repo = _mock_repo([], {})
        builder = ContinuousContractBuilder(repository=repo)
        result = await builder.build(
            underlying_public_id="u1",
            exchange="kraken_equities",
            contract_family="ES",
            timeframe="1d",
            start=D1,
            end=D3,
        )
        assert result.candles == []
        assert result.contracts_used == []

    @pytest.mark.asyncio
    async def test_leading_empty_contracts_skipped(self) -> None:
        """Query window after old contracts skips leading empties.

        Given: C1 (old, no candles in window), C2 (has candles in window),
        When: build runs,
        Then: C2 candles returned, C1 silently skipped.
        """
        c1 = _contract("ESH6", expiry_days=3)
        c2 = _contract("ESM6", expiry_days=30)
        candles = {
            "ESM6": [_candle(D5, 110.0), _candle(D6, 112.0)],
        }
        repo = _mock_repo([c1, c2], candles)
        builder = ContinuousContractBuilder(repository=repo)
        result = await builder.build(
            underlying_public_id="u1",
            exchange="kraken_equities",
            contract_family="ES",
            timeframe="1d",
            start=D5,
            end=D7,
            method="panama",
        )
        assert len(result.candles) == 2
        assert result.candles[0]["source_contract"] == "ESM6"
        assert result.failed_roll is None


class TestTimeframeConversion:
    """Timeframe string to seconds conversion."""

    def test_minutes(self) -> None:
        """Minute timeframes convert correctly."""
        assert _timeframe_to_seconds("1m") == 60
        assert _timeframe_to_seconds("5m") == 300

    def test_hours(self) -> None:
        """Hour timeframes convert correctly."""
        assert _timeframe_to_seconds("1h") == 3600
        assert _timeframe_to_seconds("4h") == 14400

    def test_days(self) -> None:
        """Day timeframes convert correctly."""
        assert _timeframe_to_seconds("1d") == 86400

    def test_weeks(self) -> None:
        """Week timeframes convert correctly."""
        assert _timeframe_to_seconds("1w") == 604800


class TestFindCommonBarFallback:
    """Tests for _find_common_bar 3-day cutoff fallback path."""

    @pytest.mark.asyncio
    async def test_fallback_matches_within_one_bar_width(self) -> None:
        """Fallback finds nearest bars within one bar width after exact match fails.

        Given: Two contracts where bars don't align exactly but are within
               one timeframe duration (1h), and outside the initial reverse scan
               (the old bar is before the 3-day cutoff for the primary loop but
               within the 3-day cutoff for the fallback),
        When: _find_common_bar is called,
        Then: Falls back to the 3-day cutoff path and finds a nearby pair.
        """
        roll_at = datetime(2026, 1, 10, tzinfo=UTC)
        cutoff = roll_at - timedelta(days=3)
        bar_time_old = cutoff + timedelta(hours=1)
        bar_time_new = bar_time_old + timedelta(minutes=30)

        old_candles = [_candle(bar_time_old, 100.0)]
        new_candles = [_candle(bar_time_new, 110.0)]

        result = ContinuousContractBuilder._find_common_bar(old_candles, new_candles, roll_at, "1h")
        assert result is not None
        assert result == (100.0, 110.0)

    @pytest.mark.asyncio
    async def test_fallback_returns_none_beyond_cutoff(self) -> None:
        """Fallback returns None when old bars are beyond 3-day cutoff.

        Given: Old bars more than 3 days before the roll point, new bars
               at a completely different time,
        When: _find_common_bar is called,
        Then: Returns None (no match within cutoff).
        """
        roll_at = datetime(2026, 1, 10, tzinfo=UTC)
        bar_time_old = roll_at - timedelta(days=5)
        bar_time_new = roll_at - timedelta(days=1)

        old_candles = [_candle(bar_time_old, 100.0)]
        new_candles = [_candle(bar_time_new, 110.0)]

        result = ContinuousContractBuilder._find_common_bar(old_candles, new_candles, roll_at, "1d")
        assert result is None

    @pytest.mark.asyncio
    async def test_fallback_triggered_when_primary_loop_finds_no_match(self) -> None:
        """Fallback path is used during full build when bars are close but not exact.

        Given: C1 has a bar 2 days before roll, C2 has a bar offset by 30min,
               within one bar width (1h) but not an exact timestamp match,
               and the primary loop's inner scan doesn't match because bars are
               far apart for the primary pass but within range for fallback,
        When: build is called with panama method,
        Then: The build succeeds using the fallback path and produces adjusted bars.
        """
        roll_at = datetime(2026, 1, 4, tzinfo=UTC)
        near_roll = roll_at - timedelta(days=1)
        near_roll_offset = near_roll + timedelta(minutes=30)

        c1 = _contract("ESM6", expiry_days=3)
        c2 = _contract("ESU6", expiry_days=7)

        candles = {
            "ESM6": [
                _candle(D1, 100.0),
                _candle(near_roll, 104.0),
            ],
            "ESU6": [
                _candle(near_roll_offset, 114.0),
                _candle(D5, 118.0),
            ],
        }
        repo = _mock_repo([c1, c2], candles)
        builder = ContinuousContractBuilder(repository=repo)
        result = await builder.build(
            underlying_public_id="u1",
            exchange="kraken_equities",
            contract_family="ES",
            timeframe="1h",
            start=D1,
            end=D5 + timedelta(hours=23),
            method="panama",
        )
        assert result.failed_roll is None
        assert len(result.roll_points) == 1
        delta = 114.0 - 104.0
        assert result.candles[0]["close"] == pytest.approx(100.0 + delta)
