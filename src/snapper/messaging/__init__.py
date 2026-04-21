"""ZeroMQ messaging system for Snapper.

This package provides the complete ZMQ-based messaging infrastructure for
the Snapper trading platform, enabling real-time communication between
components through a pub/sub architecture.
Architecture
The messaging system uses a central broker pattern
    ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
    │ Publisher │───────> │ Broker │ <───────│ Subscriber │
    │ (Market │ XSUB │ (XPUB/XSUB │ XPUB │ (Strategy, │
    │ Data) │ │ proxy) │ │ Executor) │
    └─────────────┘ └─────────────┘ └─────────────┘
Messages flow through topic-filtered channels
Publishers send to XSUB with topic strings
Broker forwards to XPUB
Subscribers receive based on topic prefix matching
Packages
infrastructure
    Core ZMQ components: broker, validated sockets, message logger.
topics
    Topic string validation for structured messaging hierarchy.
publishers
    Market data feed publishers for various exchanges.
executors
    Order execution services connecting to exchange APIs.
schemas
    Data class definitions and message serialization.
Topic Hierarchy
    market.{exchange}.{instrument}.{type} # Market data
    orders.commands.{exchange}.{instrument}.{cmd} # Order commands
    orders.events.{exchange}.{instrument}.{event} # Order events/executions
    signals.{exchange}.{instrument}.{type} # Trading signals
    system.{type}[.{component}[.{name}]] # System messages
    admin.{resource} # Administration
Message Types
MarketDataMessage
    Union of TickData, TradeData, CandleData.
OrderRequestData
    Order submission requests.
OrderData
    Order status updates.
ExecutionData
    Trade execution results.
HeartbeatData
    Component health monitoring.
SettingChangedData
    Configuration change notifications.

Example:
Basic publisher/subscriber setup
    from snapper.messaging.schemas.data import TickData
    broker = ZmqBrokerProcess()
    await broker.start()
    publisher = ValidatedPublisher(pub_socket)
    msg = TickData(exchange="kraken", instrument="BTC-USD",...)
    await publisher.send_multipart(
        "market.kraken.BTC-USD.ticks"
        msg.to_json().encode()
    # Subscriber
    subscriber = ValidatedSubscriber(sub_socket)
    subscriber.subscribe("market.kraken.")
    topic, payload = await subscriber.recv_multipart()
"""
