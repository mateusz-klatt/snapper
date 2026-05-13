"""Backtest API schemas — request bodies and response payloads.

Defines Pydantic models for backtest REST endpoints:
create, list, detail, cancel, rerun, trades, signals, events.
"""

from datetime import datetime
from typing import Any
from typing import Literal

from pydantic import Field
from pydantic import field_validator

from snapper.api.schemas.base import PayloadListResponse
from snapper.api.schemas.base import PayloadRequest
from snapper.api.schemas.base import PayloadResponse
from snapper.api.schemas.base import StrictBody
from snapper.api.schemas.base import StrictDataSchema
from snapper.core.json_types import JsonObject
from snapper.core.types import OrderExchange
from snapper.strategies.factory import StrategyFactory


class BacktestCreateBody(StrictBody):
    """Request body for POST /api/backtests.

    Attributes:
        strategy_class: Registered strategy name.
        instrument_public_id: Source-feed instrument to backtest.
        exchange: Source-feed exchange name (where candles are read from).
        timeframe: Candle timeframe (e.g., "1h").
        start_date: Backtest period start.
        end_date: Backtest period end.
        initial_cash: Starting cash balance.
        strategy_params: Strategy-specific parameters.
        target_execution_exchange: Optional order-capable venue that
            simulated fills are attributed to. ``None`` (default) keeps
            fills on the source feed (single-exchange backtest, byte-
            identical to legacy behaviour). When set, the engine
            attributes simulated trades to this venue at fill time —
            enables observe-on-feed-A / trade-on-venue-B (cross-asset)
            runs from the public REST surface. Must be one of the
            order-capable values: ``paper`` / ``kraken`` /
            ``kraken_futures`` / ``walutomat``.
    """

    strategy_class: str
    instrument_public_id: str
    exchange: str
    timeframe: str = "1h"
    start_date: datetime
    end_date: datetime
    initial_cash: float = 10_000.0
    strategy_params: JsonObject = {}
    execution_mode: str = "direct_db"
    fill_model: str = "market"
    slippage_bps: float = 0.0
    commission_bps: float = 0.0
    target_execution_exchange: OrderExchange | None = None

    @field_validator("strategy_class")
    @classmethod
    def validate_strategy_class(cls, v: str) -> str:
        """Validate strategy_class is registered.

        Args:
            v: Strategy class name.

        Returns:
            Validated strategy class name.
        """
        if v not in StrategyFactory.STRATEGY_CLASSES:
            available = ", ".join(sorted(StrategyFactory.STRATEGY_CLASSES.keys()))
            raise ValueError(
                f"Unknown strategy class '{v}'. Available: {available or 'none registered'}"
            )
        return v

    @field_validator("initial_cash")
    @classmethod
    def validate_initial_cash(cls, v: float) -> float:
        """Initial cash must be positive.

        Args:
            v: Cash value.

        Returns:
            Validated cash value.
        """
        if v <= 0:
            raise ValueError("initial_cash must be positive")
        return v

    @field_validator("execution_mode")
    @classmethod
    def validate_execution_mode(cls, v: str) -> str:
        """Reject execution modes the engine does not implement.

        Args:
            v: Execution mode value.

        Returns:
            Validated execution mode.
        """
        if v not in ("direct_db", "zmq_replay"):
            raise ValueError(f"execution_mode must be 'direct_db' or 'zmq_replay'; got '{v}'")
        return v

    @field_validator("fill_model")
    @classmethod
    def validate_fill_model(cls, v: str) -> str:
        """Only ships the 'market' fill model.

        Args:
            v: Fill model value.

        Returns:
            Validated fill model.
        """
        if v != "market":
            raise ValueError(f"fill_model must be 'market'; got '{v}'")
        return v

    @field_validator("slippage_bps", "commission_bps")
    @classmethod
    def validate_bps_bounds(cls, v: float) -> float:
        """Basis-point fields must be in [0, 500].

        Args:
            v: Basis-point value.

        Returns:
            Validated value.
        """
        if v < 0 or v > 500:
            raise ValueError("bps fields must be in [0, 500]")
        return v

    @field_validator("end_date")
    @classmethod
    def validate_date_range(cls, v: datetime, info: object) -> datetime:
        """End date must be after start date.

        Args:
            v: End date.
            info: Pydantic validation info.

        Returns:
            Validated end date.
        """
        data = getattr(info, "data", None)
        if data is not None:
            start = data.get("start_date")
            if start is not None and v <= start:
                raise ValueError("end_date must be after start_date")
        return v


class BacktestCreateCommand(
    PayloadRequest[Literal["backtest_create_command"], BacktestCreateBody],
):
    """Request envelope for POST /api/backtests."""

    type: Literal["backtest_create_command"] = "backtest_create_command"


class BacktestCancelBody(StrictBody):
    """Request body for POST /api/backtests/{id}/cancel.

    Attributes:
        reason: Optional cancellation reason.
    """

    reason: str = ""


class BacktestCancelCommand(
    PayloadRequest[Literal["backtest_cancel_command"], BacktestCancelBody],
):
    """Request envelope for POST /api/backtests/{id}/cancel."""

    type: Literal["backtest_cancel_command"] = "backtest_cancel_command"


class BacktestRunData(StrictDataSchema[Literal["backtest_run"]]):
    """Backtest run detail payload.

    Attributes:
        type: Payload discriminator.
        wallet_public_id: Owning wallet.
        strategy_name: Strategy class name.
        strategy_params: Strategy parameters.
        instrument_public_id: Source-feed instrument.
        instrument: Resolved native ticker (e.g. ``BTC-USD-PERP``)
            joined from the symbol table when available; ``None`` when
            the instrument was archived or the symbol JOIN failed.
        exchange: Source-feed exchange name.
        target_execution_exchange: Target order venue when set; ``None``
            attributes fills to ``exchange`` (single-exchange run).
        timeframe: Candle timeframe.
        start_date: Period start.
        end_date: Period end.
        initial_cash: Starting balance.
        status: Run lifecycle status.
        started_at: When execution started.
        completed_at: When execution finished.
        error: Error message if failed.
    """

    type: Literal["backtest_run"] = "backtest_run"
    wallet_public_id: str
    strategy_name: str
    strategy_params: JsonObject = {}
    instrument_public_id: str
    instrument: str | None = None
    exchange: str
    timeframe: str
    start_date: datetime
    end_date: datetime
    initial_cash: float
    status: str
    execution_mode: str = "direct_db"
    fill_model: str = "market"
    slippage_bps: float = 0.0
    commission_bps: float = 0.0
    config_hash: str | None = None
    target_execution_exchange: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None


class BacktestResultInline(StrictBody):
    """Inline backtest aggregate metrics embedded in BacktestRunDetailData.

     Promotes 5 metrics from ``extra_metrics`` to typed
    nullable-float columns and adds 3 new metrics. Mixed-vintage reads
    are handled at the route layer via an explicit null-coalescing
    fallback (typed column else ``extra_metrics.get(name)``) — see
    ``backtest_routes.py`` inline projection.

    Attributes:
        total_trades: Total exit trades.
        winning_trades: Profitable trades.
        losing_trades: Losing trades.
        total_pnl: Net PnL.
        max_drawdown: Maximum drawdown fraction.
        sharpe_ratio: Annualized Sharpe.
        win_rate: Win rate fraction.
        profit_factor: Gross profit divided by gross loss.
        final_equity: Final equity value.
        max_equity: Peak equity value.
        sortino_ratio: Annualized Sortino ratio.
        cagr: Compound annual growth rate.
        calmar_ratio: CAGR divided by max drawdown.
        expectancy: Mean per-trade PnL.
        avg_trade_pnl: Average PnL per exit trade.
        max_drawdown_duration_seconds: Longest peak-to-recovery duration.
        exposure_ratio: Fraction of run time holding a non-zero position.
        turnover_ratio: Total notional traded divided by mean equity.
        extra_metrics: Any non-promoted additional computed metrics.
    """

    total_trades: int
    winning_trades: int
    losing_trades: int
    total_pnl: float
    max_drawdown: float
    sharpe_ratio: float | None = None
    win_rate: float | None = None
    profit_factor: float | None = None
    final_equity: float
    max_equity: float
    sortino_ratio: float | None = None
    cagr: float | None = None
    calmar_ratio: float | None = None
    expectancy: float | None = None
    avg_trade_pnl: float | None = None
    max_drawdown_duration_seconds: float | None = None
    exposure_ratio: float | None = None
    turnover_ratio: float | None = None
    extra_metrics: JsonObject = Field(default={})


class BacktestEquityPointInline(StrictBody):
    """Inline equity-curve point used by GET /api/backtests/{id}/equity.

    Attributes:
        point_time: Timestamp of the equity sample.
        equity: Total portfolio equity.
        cash: Cash component.
        position_value: Open-position value.
        drawdown: Drawdown fraction at this point.
    """

    point_time: datetime
    equity: float
    cash: float
    position_value: float = 0.0
    drawdown: float = 0.0


class BacktestEquityPointListResponse(
    PayloadListResponse[Literal["backtest_equity_point_list"], BacktestEquityPointInline]
):
    """Paginated response for GET /api/backtests/{id}/equity."""

    type: Literal["backtest_equity_point_list"] = "backtest_equity_point_list"


class BacktestRunDetailData(BacktestRunData):
    """Detail-only payload — adds inline result for completed runs.

    Used exclusively by GET /api/backtests/{id}. Other endpoints
    (create/list/cancel/rerun) keep emitting the lighter BacktestRunData
    so they do not leak a permanent ``result: null`` field.

    Attributes:
        result: Inline aggregate metrics if status == 'completed', else None.
    """

    result: BacktestResultInline | None = None


class BacktestRunResponse(PayloadResponse[Literal["backtest_run_response"], BacktestRunData]):
    """Single backtest run response (lightweight; no inline result)."""

    type: Literal["backtest_run_response"] = "backtest_run_response"


class BacktestRunDetailResponse(
    PayloadResponse[Literal["backtest_run_detail_response"], BacktestRunDetailData]
):
    """Detail backtest run response — used by GET /api/backtests/{id}."""

    type: Literal["backtest_run_detail_response"] = "backtest_run_detail_response"


class BacktestRunListResponse(PayloadListResponse[Literal["backtest_run_list"], BacktestRunData]):
    """List of backtest runs response."""

    type: Literal["backtest_run_list"] = "backtest_run_list"


class BacktestResultData(StrictDataSchema[Literal["backtest_result"]]):
    """Backtest result metrics payload.

    See ``BacktestResultInline`` for the 8
    advanced metrics; this schema mirrors them so future standalone
    result endpoints surface the same shape.

    Attributes:
        type: Payload discriminator.
        run_public_id: Associated run.
        total_trades: Total exit trades.
        winning_trades: Profitable trades.
        losing_trades: Losing trades.
        total_pnl: Net PnL.
        max_drawdown: Maximum drawdown fraction.
        sharpe_ratio: Annualized Sharpe.
        win_rate: Win rate fraction.
        profit_factor: Gross profit / gross loss.
        final_equity: Final equity value.
        max_equity: Peak equity value.
        sortino_ratio: Annualized Sortino ratio.
        cagr: Compound annual growth rate.
        calmar_ratio: CAGR divided by max drawdown.
        expectancy: Mean per-trade PnL.
        avg_trade_pnl: Average PnL per exit trade.
        max_drawdown_duration_seconds: Longest peak-to-recovery duration.
        exposure_ratio: Fraction of run time holding a non-zero position.
        turnover_ratio: Total notional traded divided by mean equity.
        extra_metrics: Any non-promoted additional computed metrics.
    """

    type: Literal["backtest_result"] = "backtest_result"
    run_public_id: str
    total_trades: int
    winning_trades: int
    losing_trades: int
    total_pnl: float
    max_drawdown: float
    sharpe_ratio: float | None = None
    win_rate: float | None = None
    profit_factor: float | None = None
    final_equity: float
    max_equity: float
    sortino_ratio: float | None = None
    cagr: float | None = None
    calmar_ratio: float | None = None
    expectancy: float | None = None
    avg_trade_pnl: float | None = None
    max_drawdown_duration_seconds: float | None = None
    exposure_ratio: float | None = None
    turnover_ratio: float | None = None
    extra_metrics: dict[str, Any] = Field(default={})


class BacktestCompareBody(StrictBody):
    """Compare-request body.

    Auto-mode requires ``config_hash``; manual-mode requires both
    ``run_a_public_id`` and ``run_b_public_id``.
    """

    mode: Literal["manual", "auto"]
    run_a_public_id: str | None = None
    run_b_public_id: str | None = None
    config_hash: str | None = None
    anchor_run_public_id: str | None = None


class BacktestCompareRequest(
    PayloadRequest[Literal["backtest_compare_request"], BacktestCompareBody]
):
    """Compare-request envelope."""

    type: Literal["backtest_compare_request"] = "backtest_compare_request"


class BacktestComparisonData(StrictDataSchema[Literal["backtest_comparison"]]):
    """Comparison metadata — the immutable side of a compare request."""

    type: Literal["backtest_comparison"] = "backtest_comparison"
    wallet_public_id: str
    run_a_public_id: str
    run_b_public_id: str
    config_hash: str | None = None
    pairing_mode: str
    anchor_run_public_id: str | None = None


class MetricDiffRow(StrictBody):
    """One row in the side-by-side metrics diff."""

    name: str
    run_a: float | None = None
    run_b: float | None = None
    delta: float | None = None
    pct: float | None = None


class EquityOverlayPoint(StrictBody):
    """Aligned equity sample across both runs (one-sided legs nullable)."""

    point_time: datetime
    equity_a: float | None = None
    equity_b: float | None = None


class TradeDiffEntry(StrictBody):
    """Matched trade from either leg (for common entries both legs set)."""

    instrument: str
    executed_at: datetime
    side: str
    quantity: float
    price: float
    leg: Literal["a", "b", "common"]
    pnl_a: float | None = None
    pnl_b: float | None = None
    pnl_delta: float | None = None


class SignalDiffEntry(StrictBody):
    """Matched signal from either leg."""

    instrument: str
    signal_time: datetime
    signal_type: str
    leg: Literal["a", "b", "common"]


class BacktestComparisonDetailResponseData(StrictDataSchema[Literal["backtest_comparison_detail"]]):
    """Response payload for GET /api/backtests/compare/{id}."""

    type: Literal["backtest_comparison_detail"] = "backtest_comparison_detail"
    comparison: BacktestComparisonData
    run_a: BacktestRunData
    run_b: BacktestRunData
    metrics_diff: list[MetricDiffRow]
    equity_overlay: list[EquityOverlayPoint]
    trades_diff: list[TradeDiffEntry]
    signals_diff: list[SignalDiffEntry]


class BacktestComparisonResponse(
    PayloadResponse[Literal["backtest_comparison_response"], BacktestComparisonData]
):
    """Envelope for POST /api/backtests/compare."""

    type: Literal["backtest_comparison_response"] = "backtest_comparison_response"


class BacktestComparisonDetailResponse(
    PayloadResponse[
        Literal["backtest_comparison_detail_response"], BacktestComparisonDetailResponseData
    ]
):
    """Envelope for GET /api/backtests/compare/{comparison_public_id}."""

    type: Literal["backtest_comparison_detail_response"] = "backtest_comparison_detail_response"


class BacktestComparisonListResponse(
    PayloadListResponse[Literal["backtest_comparison_list"], BacktestComparisonData]
):
    """Envelope for GET /api/backtests/compare (wallet-scoped list)."""

    type: Literal["backtest_comparison_list"] = "backtest_comparison_list"


class BacktestTradeData(StrictDataSchema[Literal["backtest_trade"]]):
    """Backtest trade payload.

    Attributes:
        type: Payload discriminator.
        run_public_id: Parent run.
        executed_at: Trade execution time.
        instrument: Instrument name.
        side: Trade direction.
        quantity: Trade size.
        price: Fill price.
        fee: Commission fee.
        pnl: Per-fill PnL (None for entries).
        position_after: Portfolio position after fill.
        signal_public_id: ``public_id`` of the originating ``backtest_signal``
            None for synthetic fills with no triggering signal. Surfaces the
            FK-style linkage required by parity tests.
    """

    type: Literal["backtest_trade"] = "backtest_trade"
    run_public_id: str
    executed_at: datetime
    instrument: str
    side: str
    quantity: float
    price: float
    fee: float
    pnl: float | None = None
    position_after: float = 0.0
    signal_public_id: str | None = None


class BacktestTradeListResponse(
    PayloadListResponse[Literal["backtest_trade_list"], BacktestTradeData]
):
    """List of backtest trades response."""

    type: Literal["backtest_trade_list"] = "backtest_trade_list"


class BacktestSignalData(StrictDataSchema[Literal["backtest_signal"]]):
    """Backtest signal payload.

    Attributes:
        type: Payload discriminator.
        run_public_id: Parent run.
        signal_time: When signal was generated.
        signal_type: Signal direction.
        instrument: Target instrument.
        price: Price at signal time.
        indicators: Strategy indicator values.
    """

    type: Literal["backtest_signal"] = "backtest_signal"
    run_public_id: str
    signal_time: datetime
    signal_type: str
    instrument: str
    price: float
    indicators: dict[str, Any] = Field(default={})


class BacktestSignalListResponse(
    PayloadListResponse[Literal["backtest_signal_list"], BacktestSignalData]
):
    """List of backtest signals response."""

    type: Literal["backtest_signal_list"] = "backtest_signal_list"


class BacktestEventData(StrictDataSchema[Literal["backtest_event"]]):
    """Backtest event payload.

    Attributes:
        type: Payload discriminator.
        run_public_id: Parent run.
        event_type: Event classification.
        detail: Event-specific data.
    """

    type: Literal["backtest_event"] = "backtest_event"
    run_public_id: str
    event_type: str
    detail: dict[str, Any] = Field(default={})


class BacktestEventListResponse(
    PayloadListResponse[Literal["backtest_event_list"], BacktestEventData]
):
    """List of backtest events response."""

    type: Literal["backtest_event_list"] = "backtest_event_list"
