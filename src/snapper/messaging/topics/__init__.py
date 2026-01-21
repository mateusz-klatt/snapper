"""Topic validation for ZMQ pub/sub messaging.

This package provides topic string validation for the Snapper messaging system.
Topics follow a hierarchical structure that ensures messages are routed correctly
and subscriptions are semantically valid.

Topic Hierarchy
---------------
The messaging system uses dot-separated topic strings:

**Market Data** (market.{exchange}.{instrument}.{type})::

    market.kraken.BTC-USD.ticks     # Real-time tick data
    market.kraken.BTC-USD.candles.1m # OHLCV bars by timeframe
    market.kraken.BTC-USD.trades    # Trade feed

**Orders** (orders.{exchange}.{instrument}.{type})::

    orders.kraken.BTC-USD.requests  # Order submission requests
    orders.kraken.BTC-USD.status    # Order status updates

**Executions** (executions.{exchange}.{instrument}.fill)::

    executions.kraken.BTC-USD.fill  # Order fill notifications

**Signals** (signals.{exchange}.{instrument}.live|{strategy})::

    signals.kraken.BTC-USD.live     # Live trading signals
    signals.paper.BTC-USD.backtest1 # Paper trading signals

**System** (system.{type}[.{component}[.{name}]])::

    system.heartbeats               # Component health checks
    system.heartbeats.feed.kraken   # Specific feed heartbeat
    system.symbol_mappings          # Symbol cache invalidation
    system.settings                 # Settings change notifications

**Admin** (admin.{resource})::

    admin.command                   # Administrative commands

Subscription Patterns
---------------------
Subscribers can use prefix patterns (ending with '.') to receive
multiple topics::

    market.kraken.          # All Kraken market data
    system.                 # All system messages
    orders.                 # All order-related messages

Modules
-------
validation
    Topic and subscription pattern validation functions.

Example:
-------
Validate a topic before publishing::

    from snapper.messaging.topics.validation import validate_topic

    is_valid, error = validate_topic("market.kraken.BTC-USD.ticks")
    if is_valid:
        publisher.send(topic, payload)
    else:
        logger.error(f"Invalid topic: {error}")
"""
