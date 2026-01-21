"""Zonda exchange API response schemas.

Pydantic models for parsing Zonda (BitBay) REST and WebSocket responses.
"""

from datetime import UTC
from datetime import datetime
from typing import Any

from pydantic import BaseModel
from pydantic import Field
from pydantic import field_validator

from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import OrderStatusEnum
from snapper.infrastructure.exchanges.contracts import OrderTypeEnum
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.exchanges.schemas.base import EXCHANGE_SCHEMA_CONFIG


class ZondaMarketInfo(BaseModel):
    """Market information from Zonda exchange."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    code: str = ""


class ZondaTickerData(BaseModel):
    """Ticker data from Zonda exchange WebSocket."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    market: ZondaMarketInfo = Field(default_factory=ZondaMarketInfo)
    highest_bid: str = Field(default="0", alias="highestBid")
    lowest_ask: str = Field(default="0", alias="lowestAsk")
    rate: str = "0"
    time: str = ""
    previous_rate: str = Field(default="0", alias="previousRate")

    @property
    def bid(self) -> float:
        """Return the highest bid price as float.

        Returns:
            Highest bid price, or 0.0 if not available.
        """
        return float(self.highest_bid) if self.highest_bid else 0.0

    @property
    def ask(self) -> float:
        """Return the lowest ask price as float.

        Returns:
            Lowest ask price, or 0.0 if not available.
        """
        return float(self.lowest_ask) if self.lowest_ask else 0.0

    @property
    def last(self) -> float:
        """Return the last trade price as float.

        Returns:
            Last trade price, or 0.0 if not available.
        """
        return float(self.rate) if self.rate else 0.0


class ZondaTickerMessage(BaseModel):
    """WebSocket message containing ticker data from Zonda."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    action: str = ""
    topic: str = ""
    message: ZondaTickerData = Field(default_factory=ZondaTickerData)
    seq_no: int = Field(default=0, alias="seqNo")

    def extract_symbol(self) -> str:
        """Extract trading symbol from the message topic.

        Returns:
            The uppercase trading symbol extracted from the topic or market code.
        """
        topic_parts = self.topic.split("/")
        if len(topic_parts) >= 3:
            return topic_parts[-1].upper()
        return self.message.market.code.upper()


class ZondaStatsData(BaseModel):
    """24-hour statistics data from Zonda exchange."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    m: str = ""
    h: float = 0.0
    l: float = 0.0
    v: float = 0.0
    r24h: float = 0.0

    @field_validator("h", "l", "v", "r24h", mode="before")
    @classmethod
    def coerce_to_float(cls, v: Any) -> float:
        """Coerce a value to float, returning 0.0 for empty values.

        Args:
            v: The value to coerce to float.

        Returns:
            The value as float, or 0.0 if the value is None or empty.
        """
        if v is None or v == "":
            return 0.0
        return float(v)

    @property
    def symbol(self) -> str:
        """Return the market symbol in uppercase.

        Returns:
            Market symbol string in uppercase.
        """
        return self.m.upper()

    @property
    def high(self) -> float:
        """Return the 24-hour high price.

        Returns:
            24-hour high price.
        """
        return self.h

    @property
    def low(self) -> float:
        """Return the 24-hour low price.

        Returns:
            24-hour low price.
        """
        return self.l

    @property
    def volume(self) -> float:
        """Return the 24-hour trading volume.

        Returns:
            24-hour trading volume.
        """
        return self.v

    @property
    def rate_24h(self) -> float:
        """Return the 24-hour rate change.

        Returns:
            24-hour rate change value.
        """
        return self.r24h


class ZondaStatsMessage(BaseModel):
    """WebSocket message containing statistics data from Zonda."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    action: str = ""
    topic: str = ""
    message: list[ZondaStatsData] = Field(default_factory=list)
    seq_no: int = Field(default=0, alias="seqNo")


class ZondaTransactionData(BaseModel):
    """Single transaction data from Zonda exchange."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    id: str = ""
    t: str = "0"
    a: str = "0"
    r: str = "0"
    ty: str = ""

    @property
    def trade_id(self) -> int:
        """Return a unique trade ID derived from the transaction ID.

        Returns:
            Hash value of the transaction ID.
        """
        return hash(self.id)

    @property
    def timestamp_ms(self) -> int:
        """Return the timestamp in milliseconds.

        Returns:
            Timestamp in milliseconds, or 0 if not available.
        """
        return int(self.t) if self.t else 0

    @property
    def timestamp(self) -> datetime:
        """Return the timestamp as a datetime object.

        Returns:
            UTC datetime of the transaction.
        """
        return datetime.fromtimestamp(self.timestamp_ms / 1000.0, tz=UTC)

    @property
    def amount(self) -> float:
        """Return the transaction amount as float.

        Returns:
            Transaction amount, or 0.0 if not available.
        """
        return float(self.a) if self.a else 0.0

    @property
    def price(self) -> float:
        """Return the transaction price as float.

        Returns:
            Transaction price, or 0.0 if not available.
        """
        return float(self.r) if self.r else 0.0

    @property
    def side(self) -> str:
        """Return the transaction side (buy/sell) in lowercase.

        Returns:
            Side string in lowercase (buy or sell).
        """
        return self.ty.lower()

    def to_trade_update(self, symbol: str) -> TradeUpdate:
        """Convert transaction data to a TradeUpdate object.

        Args:
            symbol: The trading symbol for the trade update.

        Returns:
            A TradeUpdate instance populated with transaction data.
        """
        return TradeUpdate(
            symbol=symbol,
            side=self.side,
            quantity=self.amount,
            price=self.price,
            ord_type="unknown",
            trade_id=self.trade_id,
            timestamp=self.timestamp,
        )


class ZondaTransactionsPayload(BaseModel):
    """Payload containing a list of transactions from Zonda."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    transactions: list[ZondaTransactionData] = Field(default_factory=list)


class ZondaTransactionsMessage(BaseModel):
    """WebSocket message containing transactions from Zonda."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    action: str = ""
    topic: str = ""
    message: ZondaTransactionsPayload = Field(default_factory=ZondaTransactionsPayload)
    timestamp: str = ""
    seq_no: int = Field(default=0, alias="seqNo")

    def extract_symbol(self) -> str:
        """Extract trading symbol from the message topic.

        Returns:
            The uppercase trading symbol, or empty string if not found.
        """
        if "/" in self.topic:
            return self.topic.split("/")[-1].upper()
        return ""


class ZondaExecutionData(BaseModel):
    """Single execution data from Zonda exchange."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    id: str = ""
    market: str = ""
    time: str = "0"
    amount: str = "0"
    rate: str = "0"
    initialized_by: str = Field(default="", alias="initializedBy")
    was_taker: bool = Field(default=False, alias="wasTaker")
    user_action: str = Field(default="", alias="userAction")
    offer_id: str = Field(default="", alias="offerId")
    commission_value: str = Field(default="0", alias="commissionValue")

    @property
    def timestamp_ms(self) -> int:
        """Return the execution timestamp in milliseconds.

        Returns:
            Timestamp in milliseconds, or 0 if not available.
        """
        return int(self.time) if self.time else 0

    @property
    def timestamp(self) -> datetime:
        """Return the execution timestamp as a datetime object.

        Returns:
            UTC datetime of the execution.
        """
        return datetime.fromtimestamp(self.timestamp_ms / 1000.0, tz=UTC)

    @property
    def quantity(self) -> float:
        """Return the executed quantity as float.

        Returns:
            Executed quantity, or 0.0 if not available.
        """
        return float(self.amount) if self.amount else 0.0

    @property
    def price(self) -> float:
        """Return the execution price as float.

        Returns:
            Execution price, or 0.0 if not available.
        """
        return float(self.rate) if self.rate else 0.0

    @property
    def side(self) -> OrderSideEnum:
        """Return the order side (buy/sell) as enum.

        Returns:
            OrderSideEnum.BUY for buy orders, OrderSideEnum.SELL otherwise.
        """
        return OrderSideEnum.BUY if self.user_action.lower() == "buy" else OrderSideEnum.SELL

    def to_execution_update(self) -> ExecutionUpdate:
        """Convert execution data to an ExecutionUpdate object.

        Returns:
            An ExecutionUpdate instance populated with execution data.
        """
        return ExecutionUpdate(
            order_id=self.offer_id,
            exec_type="trade",
            symbol=self.market,
            side=self.side,
            order_type=OrderTypeEnum.LIMIT,
            order_status=OrderStatusEnum.FILLED,
            timestamp=self.timestamp,
            cum_qty=self.quantity,
            cum_cost=self.quantity * self.price,
        )


class ZondaExecutionsPayload(BaseModel):
    """Payload containing execution history from Zonda."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    history: list[ZondaExecutionData] = Field(default_factory=list)


class ZondaExecutionsMessage(BaseModel):
    """WebSocket message containing executions from Zonda."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    action: str = ""
    topic: str = ""
    message: ZondaExecutionsPayload = Field(default_factory=ZondaExecutionsPayload)
    timestamp: str = ""
    seq_no: int = Field(default=0, alias="seqNo")


def create_ticker_update_from_caches(
    symbol: str,
    ticker_cache: dict[str, Any],
    stats_cache: dict[str, Any],
) -> TickerUpdate:
    """Create a TickerUpdate by combining ticker and stats cache data.

    Args:
        symbol: The trading symbol for the ticker update.
        ticker_cache: Cached ticker data containing bid, ask, and last prices.
        stats_cache: Cached statistics data containing high, low, volume, and rate.

    Returns:
        A TickerUpdate instance with combined ticker and statistics data.
    """
    bid = ticker_cache.get("bid", 0.0)
    ask = ticker_cache.get("ask", 0.0)
    last = ticker_cache.get("last", 0.0)
    high = stats_cache.get("high", 0.0)
    low = stats_cache.get("low", 0.0)
    volume = stats_cache.get("volume", 0.0)
    rate_24h = stats_cache.get("rate_24h", 0.0)
    change = last - rate_24h if rate_24h > 0 else 0.0
    change_pct = (change / rate_24h * 100) if rate_24h > 0 else 0.0
    return TickerUpdate(
        symbol=symbol,
        bid=bid,
        bid_qty=0.0,
        ask=ask,
        ask_qty=0.0,
        last=last,
        volume=volume,
        vwap=0.0,
        low=low,
        high=high,
        change=change,
        change_pct=change_pct,
    )


__all__ = [
    "ZondaExecutionData",
    "ZondaExecutionsMessage",
    "ZondaExecutionsPayload",
    "ZondaMarketInfo",
    "ZondaStatsData",
    "ZondaStatsMessage",
    "ZondaTickerData",
    "ZondaTickerMessage",
    "ZondaTransactionData",
    "ZondaTransactionsMessage",
    "ZondaTransactionsPayload",
    "create_ticker_update_from_caches",
]
