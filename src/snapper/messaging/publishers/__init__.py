"""Market data publisher services.

This package provides market data feed publishers that connect to exchange
WebSocket APIs and publish normalized market data to the ZMQ messaging bus.

Architecture
------------
Each publisher:
1. Connects to an exchange WebSocket API (public, anonymous)
2. Subscribes to ticks, trades, and candles for configured instruments
3. Normalizes data to common envelope formats
4. Publishes to the ZMQ broker with validated topics
5. Persists candle data to the database

Publishers run as RegisterableProcess instances managed by the process manager.

Classes
-------
MarketDataPublisherService
    Abstract base class for all feed publishers.
KrakenMarketDataPublisher
    Kraken exchange feed publisher.

Message Flow
------------
::

    Exchange WebSocket ──> Publisher ──> ZMQ Broker ──> Subscribers
                              │
                              └──> Database (candles)

Topics Published
----------------
- market.{exchange}.{instrument}.ticks
- market.{exchange}.{instrument}.trades
- market.{exchange}.{instrument}.candles.{timeframe}
- system.heartbeats.feed.{exchange}

Example:
-------
Register and run a publisher::

    publisher = KrakenMarketDataPublisher(symbols=["BTC-USD", "ETH-USD"])
    await publisher.start()
    # Publisher runs until stopped
    await publisher.stop()
"""
