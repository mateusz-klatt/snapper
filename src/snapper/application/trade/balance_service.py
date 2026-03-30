"""Balance domain service — projection of cash, equity, and exposure.

BalanceService consumes PositionChanged events from TradeService and
MarkPrice updates from the quote pipeline. It computes derived metrics
(equity, exposure, drawdown) and provides a read model for
TradingEngineService and API endpoints.

Phase 1b: service class + in-memory projection.
Phase 1c: wired to TradeService events + mark prices.
"""

from dataclasses import dataclass

from loguru import logger


@dataclass
class BalanceProjection:
    """In-memory balance projection for a single shard.

    Updated on position changes and mark price updates.
    """

    cash: float = 0.0
    position_qty: float = 0.0
    entry_price: float | None = None
    mark_price: float | None = None
    peak_equity: float = 0.0
    realized_pnl: float = 0.0

    @property
    def unrealized_pnl(self) -> float:
        """Calculate unrealized PnL from current mark price.

        Returns:
            Unrealized PnL based on position quantity and mark-to-entry
            price difference, or 0.0 if no position or missing prices.
        """
        if self.mark_price is None or self.entry_price is None or self.position_qty == 0.0:
            return 0.0
        return self.position_qty * (self.mark_price - self.entry_price)

    @property
    def equity(self) -> float:
        """Calculate total equity: cash + position mark-to-market value.

        Cash is free cash (reduced by position cost on entry). Equity is
        cash plus position valued at current mark price.

        Returns:
            Cash plus position value at mark price, or just cash if
            mark price is unavailable.
        """
        if self.mark_price is None or self.position_qty == 0.0:
            return self.cash
        return self.cash + self.position_qty * self.mark_price

    @property
    def exposure(self) -> float:
        """Calculate absolute notional exposure.

        Returns:
            Absolute position quantity times mark price, or 0.0 if
            no mark price is available.
        """
        if self.mark_price is None:
            return 0.0
        return abs(self.position_qty) * self.mark_price

    @property
    def drawdown(self) -> float:
        """Calculate current drawdown from peak equity.

        Returns:
            Fractional drawdown (0.0 to 1.0) from peak equity, or 0.0 if
            peak equity is non-positive.
        """
        if self.peak_equity <= 0:
            return 0.0
        return max(0.0, 1.0 - self.equity / self.peak_equity)


class BalanceService:
    """Balance projection service.

    Maintains per-shard balance state derived from TradeService position
    changes and quote pipeline mark prices.
    """

    def __init__(self) -> None:
        """Initialize balance service."""
        self._shards: dict[str, BalanceProjection] = {}

    def _get_or_create_shard(self, shard_key: str) -> BalanceProjection:
        """Get existing shard projection or create a new one."""
        if shard_key not in self._shards:
            self._shards[shard_key] = BalanceProjection()
        return self._shards[shard_key]

    def get_equity(self, shard_key: str) -> float:
        """Read model: current equity for engine drawdown calculations.

        Args:
            shard_key: Unique identifier for the trading shard.

        Returns:
            Current equity (cash + unrealized PnL) for the shard.
        """
        return self._get_or_create_shard(shard_key).equity

    def get_cash(self, shard_key: str) -> float:
        """Read model: available cash for engine sizing.

        Args:
            shard_key: Unique identifier for the trading shard.

        Returns:
            Current cash balance for the shard.
        """
        return self._get_or_create_shard(shard_key).cash

    def get_peak_equity(self, shard_key: str) -> float:
        """Read model: peak equity for drawdown calculation.

        Args:
            shard_key: Unique identifier for the trading shard.

        Returns:
            Historical peak equity for the shard.
        """
        return self._get_or_create_shard(shard_key).peak_equity

    def get_exposure(self, shard_key: str) -> float:
        """Read model: current notional exposure.

        Args:
            shard_key: Unique identifier for the trading shard.

        Returns:
            Absolute notional exposure for the shard.
        """
        return self._get_or_create_shard(shard_key).exposure

    def get_drawdown(self, shard_key: str) -> float:
        """Read model: current drawdown from peak.

        Args:
            shard_key: Unique identifier for the trading shard.

        Returns:
            Fractional drawdown from peak equity for the shard.
        """
        return self._get_or_create_shard(shard_key).drawdown

    def get_projection(self, shard_key: str) -> BalanceProjection:
        """Read model: full balance projection for a shard.

        Args:
            shard_key: Unique identifier for the trading shard.

        Returns:
            Complete BalanceProjection dataclass for the shard.
        """
        return self._get_or_create_shard(shard_key)

    def on_position_changed(
        self,
        shard_key: str,
        position_qty: float,
        entry_price: float | None,
        cash: float,
        peak_equity: float,
        realized_pnl: float,
    ) -> None:
        """Handle PositionChanged event from TradeService.

        Updates the balance projection with the latest position and cash
        state from the trade domain.

        Args:
            shard_key: Unique identifier for the trading shard.
            position_qty: Current net position quantity.
            entry_price: Weighted average entry price, or None if flat.
            cash: Current cash balance after the position change.
            peak_equity: Historical peak equity watermark.
            realized_pnl: Cumulative realized profit and loss.
        """
        proj = self._get_or_create_shard(shard_key)
        proj.position_qty = position_qty
        proj.entry_price = entry_price
        proj.cash = cash
        proj.peak_equity = peak_equity
        proj.realized_pnl = realized_pnl
        if proj.mark_price is not None:
            equity = proj.equity
            if equity > proj.peak_equity:
                proj.peak_equity = equity

    def on_mark_price(self, shard_key: str, price: float) -> None:
        """Handle MarkPrice update from quote pipeline.

        Updates the mark price used for MTM calculations.

        Args:
            shard_key: Unique identifier for the trading shard.
            price: Latest mark price from the quote pipeline.
        """
        proj = self._get_or_create_shard(shard_key)
        proj.mark_price = price
        equity = proj.equity
        if equity > proj.peak_equity:
            proj.peak_equity = equity

    def restore_from_checkpoint(
        self,
        shard_key: str,
        cash: float,
        position_qty: float,
        entry_price: float | None,
        peak_equity: float,
        realized_pnl: float,
    ) -> None:
        """Restore shard balance from a checkpoint during recovery.

        Args:
            shard_key: Unique identifier for the trading shard.
            cash: Cash balance at checkpoint time.
            position_qty: Net position quantity at checkpoint time.
            entry_price: Weighted average entry price, or None if flat.
            peak_equity: Historical peak equity watermark.
            realized_pnl: Cumulative realized profit and loss.
        """
        proj = self._get_or_create_shard(shard_key)
        proj.cash = cash
        proj.position_qty = position_qty
        proj.entry_price = entry_price
        proj.peak_equity = peak_equity
        proj.realized_pnl = realized_pnl
        logger.debug(f"BalanceService: restored {shard_key} from checkpoint")
