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

**Order Commands** (orders.commands.{exchange}.{instrument}.{command})::

    orders.commands.kraken.BTC-USD.submit   # Submit new order
    orders.commands.kraken.BTC-USD.cancel   # Cancel existing order
    orders.commands.kraken.BTC-USD.replace  # Modify order (price/qty)

**Order Events** (orders.events.{exchange}.{instrument}.{event})::

    orders.events.kraken.BTC-USD.submitted  # Order submitted to exchange (local)
    orders.events.kraken.BTC-USD.accepted   # Order accepted by exchange (ACK)
    orders.events.kraken.BTC-USD.rejected   # Order rejected
    orders.events.kraken.BTC-USD.fill       # Order fill (partial/full)
    orders.events.kraken.BTC-USD.cancelled  # Order cancelled
    orders.events.kraken.BTC-USD.expired    # Order expired

**Signals** (signals.{exchange}.{instrument}.live|{strategy})::

    signals.kraken.BTC-USD.live     # Live trading signals
    signals.paper.BTC-USD.backtest1 # Paper trading signals

**System** (system.{type}[.{component}[.{name}]])::

    system.heartbeats               # Component health checks
    system.heartbeats.feed.kraken   # Specific feed heartbeat
    system.symbol_aliases          # Symbol cache invalidation
    system.settings                 # Settings change notifications

**Admin** (admin.{resource})::

    admin.command                   # Administrative commands

Subscription Patterns
---------------------
Subscribers can use prefix patterns (ending with '.') to receive
multiple topics::

    market.kraken.          # All Kraken market data
    orders.commands.        # All order commands (for executors)
    orders.events.          # All order events (for traders/UI)
    orders.events.kraken.   # All Kraken order events
    system.                 # All system messages

Modules
-------
validation
    Topic and subscription pattern validation functions.

builders
    Functions for constructing valid topic strings.

Example:
-------
Validate a topic before publishing::

    from snapper.messaging.topics.validation import validate_topic

    is_valid, error = validate_topic("orders.commands.kraken.BTC-USD.submit")
    if is_valid:
        publisher.send(topic, payload)
    else:
        logger.error(f"Invalid topic: {error}")

Build a topic with builders::

    from snapper.messaging.topics.builders import order_command_topic

    topic = order_command_topic("kraken", "BTC-USD", "submit")
    publisher.send(topic, payload)
"""
