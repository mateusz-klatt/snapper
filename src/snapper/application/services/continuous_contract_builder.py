"""On-demand continuous contract series builder.

Stitches historical candle data from multiple expired futures contracts
into a single continuous price series using configurable adjustment
methods (unadjusted, ratio, panama canal).
"""

from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast

from loguru import logger

from snapper.core.types import AllExchange
from snapper.data.repository import Repository
from snapper.data.repository_types import CandleRow
from snapper.data.repository_types import ContinuousCandleRow
from snapper.data.repository_types import InstrumentContractRow


@dataclass
class RollPointInfo:
    """Metadata for a single roll transition between contracts."""

    from_contract: str
    to_contract: str
    roll_at: datetime
    adjustment: float | None


@dataclass
class BuildResult:
    """Result of continuous contract series computation."""

    candles: list[ContinuousCandleRow]
    contracts_used: list[str]
    roll_points: list[RollPointInfo]
    failed_roll: RollPointInfo | None


class ContinuousContractBuilder:
    """Builds continuous contract candle series on demand.

    Queries contract metadata and candle data from the repository,
    determines roll points from expiry dates, computes adjustment
    factors at each roll, and returns a stitched series anchored
    to the latest contract.
    """

    def __init__(self, repository: Repository) -> None:
        """Initialize the builder.

        Args:
            repository: Database repository for contract and candle queries.
        """
        self._repo = repository

    async def build(
        self,
        underlying_public_id: str,
        exchange: str,
        contract_family: str,
        timeframe: str,
        start: datetime,
        end: datetime,
        method: str = "panama",
        rollover_days_before: int = 0,
        as_of: datetime | None = None,
    ) -> BuildResult:
        """Build a continuous contract series for one contract family.

        Args:
            underlying_public_id: Public ID of the underlying asset.
            exchange: Exchange to source contracts from.
            contract_family: Product root (e.g., "ES", "GC").
            timeframe: Candle timeframe (e.g., "1h", "1d").
            start: Series start time (inclusive).
            end: Series end time (inclusive).
            method: Adjustment method: "unadjusted", "ratio", "panama".
            rollover_days_before: Days before expiry to roll (0 = on expiry).
            as_of: Temporal query point. Defaults to utc_now().

        Returns:
            BuildResult with stitched candles, contracts used, and roll info.

        Raises:
            ValueError: If method is invalid or ratio method encounters
                non-positive prices at a roll point.
        """
        if method not in ("unadjusted", "ratio", "panama"):
            msg = f"Invalid method: {method}. Must be unadjusted, ratio, or panama."
            raise ValueError(msg)

        if not 0 <= rollover_days_before <= 365:
            raise ValueError(
                f"rollover_days_before must be in [0, 365], got {rollover_days_before}"
            )

        now = as_of or datetime.now(UTC)

        contracts = await self._repo.get_contracts_for_underlying(
            underlying_public_id=underlying_public_id,
            as_of=now,
            exchange=exchange,
            contract_family=contract_family,
            include_expired=True,
        )

        futures = [c for c in contracts if c["expiry_at"] is not None]
        futures.sort(key=lambda c: cast(datetime, c["expiry_at"]))

        if not futures:
            return BuildResult(candles=[], contracts_used=[], roll_points=[], failed_roll=None)

        contract_candles = await self._load_candles(futures, exchange, timeframe, start, end, now)

        roll_timestamps = self._compute_roll_timestamps(futures, rollover_days_before)

        active, active_rolls = self._select_active_chain(futures, roll_timestamps, contract_candles)
        if not active:
            return BuildResult(candles=[], contracts_used=[], roll_points=[], failed_roll=None)

        adjustments, failed = self._compute_adjustments(
            active, active_rolls, contract_candles, method, timeframe
        )

        if failed is not None:
            fail_idx = next(
                i for i, c in enumerate(active) if c["native_symbol"] == failed.from_contract
            )
            truncated = active[: fail_idx + 1]
            truncated_rolls = active_rolls[:fail_idx]
            candles = self._stitch(
                truncated,
                truncated_rolls,
                adjustments,
                contract_candles,
                method,
                truncate_at=failed.roll_at,
            )
            return BuildResult(
                candles=candles,
                contracts_used=[c["native_symbol"] for c in truncated],
                roll_points=list(adjustments),
                failed_roll=failed,
            )

        candles = self._stitch(active, active_rolls, adjustments, contract_candles, method)
        return BuildResult(
            candles=candles,
            contracts_used=[c["native_symbol"] for c in active],
            roll_points=adjustments,
            failed_roll=None,
        )

    async def _load_candles(
        self,
        contracts: list[InstrumentContractRow],
        exchange: str,
        timeframe: str,
        start: datetime,
        end: datetime,
        as_of: datetime,
    ) -> dict[str, list[CandleRow]]:
        """Load candle data for each contract in the family.

        Returns:
            Dict mapping native_symbol to list of CandleRow sorted by open_at.
        """
        result: dict[str, list[CandleRow]] = {}
        for contract in contracts:
            symbol = contract["native_symbol"]
            candles = await self._repo.get_candles(
                instrument=symbol,
                timeframe=timeframe,
                start=start,
                end=end,
                exchange=cast(AllExchange, exchange),
                as_of=as_of,
            )
            if candles:
                result[symbol] = sorted(candles, key=lambda c: c["open_at"])
        return result

    @staticmethod
    def _select_active_chain(
        futures: list[InstrumentContractRow],
        roll_timestamps: list[datetime],
        contract_candles: dict[str, list[CandleRow]],
    ) -> tuple[list[InstrumentContractRow], list[datetime]]:
        """Select the contiguous chain of contracts with candle data.

        Skips leading contracts without candles (pre-window history),
        then truncates at the first interior gap to avoid misassigning
        bar ownership by skipping a middle contract.

        Returns:
            (active_contracts, active_roll_timestamps) — contiguous
            sub-chain with candle data, plus matching roll timestamps.
        """
        first_with_data = None
        for i, contract in enumerate(futures):
            if contract["native_symbol"] in contract_candles:
                first_with_data = i
                break
        if first_with_data is None:
            return [], []

        active: list[InstrumentContractRow] = []
        for contract in futures[first_with_data:]:
            if contract["native_symbol"] in contract_candles:
                active.append(contract)
            else:
                break

        start_roll_idx = max(first_with_data, 0)
        end_roll_idx = start_roll_idx + max(len(active) - 1, 0)
        active_rolls = roll_timestamps[start_roll_idx:end_roll_idx]
        return active, active_rolls

    @staticmethod
    def _compute_roll_timestamps(
        contracts: list[InstrumentContractRow],
        rollover_days_before: int,
    ) -> list[datetime]:
        """Compute roll timestamps for each contract except the last.

        Returns:
            List of roll timestamps r_i for i=0..n-2 where n=len(contracts).
            Roll at r_i means: bars at t < r_i belong to contracts[i],
            bars at t >= r_i belong to contracts[i+1].
        """
        rolls: list[datetime] = []
        for contract in contracts[:-1]:
            expiry = contract["expiry_at"]
            assert expiry is not None
            rolls.append(expiry - timedelta(days=rollover_days_before))
        return rolls

    @staticmethod
    def _find_common_bar(
        old_candles: list[CandleRow],
        new_candles: list[CandleRow],
        roll_at: datetime,
        timeframe: str,
    ) -> tuple[float, float] | None:
        """Find the common bar at or before the roll point.

        Looks for the last bar in old_candles at t <= roll_at that has
        a matching bar (same timestamp) in new_candles. Falls back to
        nearest pair within one bar width.

        Returns:
            (old_close, new_close) or None if no match within 3 days.
        """
        tf_seconds = _timeframe_to_seconds(timeframe)
        old_by_time = {c["open_at"]: c["close"] for c in old_candles if c["open_at"] <= roll_at}
        new_by_time = {c["open_at"]: c["close"] for c in new_candles}

        for t in sorted(old_by_time.keys(), reverse=True):
            if t in new_by_time:
                return old_by_time[t], new_by_time[t]

        cutoff = roll_at - timedelta(days=3)
        old_near = {t: v for t, v in old_by_time.items() if t >= cutoff}
        for t in sorted(old_near.keys(), reverse=True):
            for nt in sorted(new_by_time.keys(), reverse=True):
                if abs((t - nt).total_seconds()) <= tf_seconds:
                    return old_near[t], new_by_time[nt]

        return None

    def _compute_adjustments(
        self,
        contracts: list[InstrumentContractRow],
        roll_timestamps: list[datetime],
        contract_candles: dict[str, list[CandleRow]],
        method: str,
        timeframe: str,
    ) -> tuple[list[RollPointInfo], RollPointInfo | None]:
        """Compute adjustment factors at each roll point.

        Returns:
            (roll_infos, failed_roll) — failed_roll is set if a gap
            is too large and the series should be truncated.
        """
        rolls: list[RollPointInfo] = []
        for i, roll_at in enumerate(roll_timestamps):
            old_sym = contracts[i]["native_symbol"]
            new_sym = contracts[i + 1]["native_symbol"]
            old_candles = contract_candles.get(old_sym, [])
            new_candles = contract_candles.get(new_sym, [])
            roll_info, failed = self._resolve_roll_info(
                old_sym=old_sym,
                new_sym=new_sym,
                roll_at=roll_at,
                old_candles=old_candles,
                new_candles=new_candles,
                method=method,
                timeframe=timeframe,
            )
            if failed is not None:
                return rolls, failed
            assert roll_info is not None
            rolls.append(roll_info)
        return rolls, None

    def _resolve_roll_info(
        self,
        *,
        old_sym: str,
        new_sym: str,
        roll_at: datetime,
        old_candles: list[CandleRow],
        new_candles: list[CandleRow],
        method: str,
        timeframe: str,
    ) -> tuple[RollPointInfo | None, RollPointInfo | None]:
        """Resolve the adjustment metadata for a single contract roll."""
        if method == "unadjusted":
            return (
                RollPointInfo(
                    from_contract=old_sym,
                    to_contract=new_sym,
                    roll_at=roll_at,
                    adjustment=0.0,
                ),
                None,
            )

        pair = self._find_common_bar(old_candles, new_candles, roll_at, timeframe)
        if pair is None:
            failed = RollPointInfo(
                from_contract=old_sym,
                to_contract=new_sym,
                roll_at=roll_at,
                adjustment=None,
            )
            logger.error(
                f"Continuous: no common bar for roll {old_sym} -> {new_sym} "
                f"at {roll_at}, truncating series"
            )
            return None, failed

        old_close, new_close = pair
        adjustment = self._compute_roll_adjustment(
            method=method,
            old_sym=old_sym,
            new_sym=new_sym,
            old_close=old_close,
            new_close=new_close,
        )
        return (
            RollPointInfo(
                from_contract=old_sym,
                to_contract=new_sym,
                roll_at=roll_at,
                adjustment=adjustment,
            ),
            None,
        )

    @staticmethod
    def _compute_roll_adjustment(
        *,
        method: str,
        old_sym: str,
        new_sym: str,
        old_close: float,
        new_close: float,
    ) -> float:
        """Compute the numeric adjustment value for a resolved roll pair."""
        if method == "ratio":
            if old_close <= 0 or new_close <= 0:
                msg = (
                    f"Non-positive price at roll {old_sym}->{new_sym}: "
                    f"old={old_close}, new={new_close}"
                )
                raise ValueError(msg)
            return new_close / old_close
        return new_close - old_close

    @staticmethod
    def _stitch(
        contracts: list[InstrumentContractRow],
        roll_timestamps: list[datetime],
        roll_infos: list[RollPointInfo],
        contract_candles: dict[str, list[CandleRow]],
        method: str,
        truncate_at: datetime | None = None,
    ) -> list[ContinuousCandleRow]:
        """Stitch candles from multiple contracts into a continuous series.

        Anchor is the last contract (contracts[-1]). Earlier contracts
        get cumulative adjustments toward the anchor.

        Args:
            contracts: Ordered list of contracts to stitch.
            roll_timestamps: Roll points between consecutive contracts.
            roll_infos: Adjustment info per roll point.
            contract_candles: Raw candles keyed by native_symbol.
            method: Adjustment method (unadjusted/ratio/panama).
            truncate_at: If set, clamp the last contract's upper bound
                (used for failed-roll partial results).
        """
        n = len(contracts)
        result: list[ContinuousCandleRow] = []

        for bar_idx, contract in enumerate(contracts):
            sym = contract["native_symbol"]
            candles = contract_candles.get(sym, [])
            is_anchor = bar_idx == n - 1
            start_bound, end_bound = ContinuousContractBuilder._resolve_stitch_bounds(
                bar_idx,
                n,
                roll_timestamps,
                is_anchor,
                truncate_at,
            )
            cum_factor = ContinuousContractBuilder._cumulative_adjustment(
                method,
                roll_infos,
                bar_idx,
            )
            result.extend(
                ContinuousContractBuilder._stitch_contract_candles(
                    candles,
                    sym,
                    method,
                    cum_factor,
                    is_anchor,
                    start_bound,
                    end_bound,
                )
            )

        result.sort(key=lambda c: c["open_at"])
        return result

    @staticmethod
    def _resolve_stitch_bounds(
        bar_idx: int,
        contract_count: int,
        roll_timestamps: list[datetime],
        is_anchor: bool,
        truncate_at: datetime | None,
    ) -> tuple[datetime | None, datetime | None]:
        """Resolve the candle window assigned to a single contract."""
        start_bound = roll_timestamps[bar_idx - 1] if bar_idx > 0 else None
        end_bound = roll_timestamps[bar_idx] if bar_idx < contract_count - 1 else None
        if is_anchor and truncate_at is not None:
            end_bound = truncate_at
        return start_bound, end_bound

    @staticmethod
    def _cumulative_adjustment(
        method: str,
        roll_infos: list[RollPointInfo],
        bar_idx: int,
    ) -> float:
        """Compute the cumulative adjustment applied to one contract segment."""
        if method == "ratio":
            cum_factor = 1.0
            for roll_info in roll_infos[bar_idx:]:
                cum_factor *= roll_info.adjustment or 1.0
            return cum_factor
        if method == "panama":
            return sum((roll_info.adjustment or 0.0) for roll_info in roll_infos[bar_idx:])
        return 0.0

    @staticmethod
    def _stitch_contract_candles(
        candles: list[CandleRow],
        source_contract: str,
        method: str,
        cum_factor: float,
        is_anchor: bool,
        start_bound: datetime | None,
        end_bound: datetime | None,
    ) -> list[ContinuousCandleRow]:
        """Transform one contract's candles into continuous-series rows."""
        result: list[ContinuousCandleRow] = []
        for candle in candles:
            if not ContinuousContractBuilder._candle_within_bounds(
                candle["open_at"],
                start_bound,
                end_bound,
            ):
                continue
            result.append(
                ContinuousContractBuilder._build_stitched_candle(
                    candle,
                    source_contract,
                    method,
                    cum_factor,
                    is_anchor,
                )
            )
        return result

    @staticmethod
    def _candle_within_bounds(
        open_at: datetime,
        start_bound: datetime | None,
        end_bound: datetime | None,
    ) -> bool:
        """Check whether a candle belongs to the active contract window."""
        return (start_bound is None or open_at >= start_bound) and (
            end_bound is None or open_at < end_bound
        )

    @staticmethod
    def _build_stitched_candle(
        candle: CandleRow,
        source_contract: str,
        method: str,
        cum_factor: float,
        is_anchor: bool,
    ) -> ContinuousCandleRow:
        """Build one continuous candle row from a raw contract candle."""
        adj_open, adj_high, adj_low, adj_close, adj_vwap, adj_factor = (
            ContinuousContractBuilder._adjust_candle_prices(
                candle,
                method,
                cum_factor,
                is_anchor,
            )
        )
        return {
            "open_at": candle["open_at"],
            "timeframe": candle["timeframe"],
            "open": adj_open,
            "high": adj_high,
            "low": adj_low,
            "close": adj_close,
            "volume": candle["volume"],
            "vwap": adj_vwap,
            "trades": candle["trades"],
            "source_contract": source_contract,
            "adjustment_factor": adj_factor,
        }

    @staticmethod
    def _adjust_candle_prices(
        candle: CandleRow,
        method: str,
        cum_factor: float,
        is_anchor: bool,
    ) -> tuple[float, float, float, float, float | None, float | None]:
        """Adjust OHLCV price fields for one stitched candle."""
        if is_anchor or method == "unadjusted":
            return (
                candle["open"],
                candle["high"],
                candle["low"],
                candle["close"],
                candle["vwap"],
                None,
            )
        if method == "ratio":
            return (
                candle["open"] * cum_factor,
                candle["high"] * cum_factor,
                candle["low"] * cum_factor,
                candle["close"] * cum_factor,
                ContinuousContractBuilder._adjust_optional_price(
                    candle["vwap"],
                    cum_factor,
                    method,
                ),
                cum_factor,
            )
        return (
            candle["open"] + cum_factor,
            candle["high"] + cum_factor,
            candle["low"] + cum_factor,
            candle["close"] + cum_factor,
            ContinuousContractBuilder._adjust_optional_price(
                candle["vwap"],
                cum_factor,
                method,
            ),
            cum_factor,
        )

    @staticmethod
    def _adjust_optional_price(
        value: float | None,
        cum_factor: float,
        method: str,
    ) -> float | None:
        """Adjust an optional price-like field using the selected method."""
        if value is None:
            return None
        if method == "ratio":
            return value * cum_factor
        return value + cum_factor


def _timeframe_to_seconds(timeframe: str) -> float:
    """Convert timeframe string to seconds.

    Supports: 1m, 5m, 15m, 30m, 1h, 4h, 1d, 1w.
    """
    multipliers = {
        "m": 60,
        "h": 3600,
        "d": 86400,
        "w": 604800,
    }
    unit = timeframe[-1]
    value = int(timeframe[:-1])
    return value * multipliers.get(unit, 60)
