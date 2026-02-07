"""Order execution services.

This package provides execution services that receive order commands from
the ZMQ messaging bus, execute them on exchanges, and publish execution
events back to the bus.

Architecture
------------
Each executor:
1. Subscribes to orders.commands.{exchange}.* topics
2. Receives OrderRequestEnvelope messages
3. Executes orders via exchange API (authenticated)
4. Publishes FillEnvelope and OrderStatusEnvelope results
5. Monitors WebSocket execution streams (where supported)

Executors run as RegisterableProcess instances managed by the process manager.

Classes
-------
ExchangeExecutorService
    Abstract base class for all order executors.
KrakenOrderExecutor
    Kraken exchange order execution service.
PaperOrderExecutor
    Paper trading (simulation) execution service.

Message Flow
------------
::

    Strategy ──> ZMQ [orders.commands.{exchange}.{instrument}.submit]
                      │
                      v
                  Executor ──> Exchange API
                      │
                      v
    Strategy <── ZMQ [orders.events.{exchange}.{instrument}.fill]
                     [orders.events.{exchange}.{instrument}.submitted]

Topics Subscribed
-----------------
- orders.commands.{exchange}.{instrument}.submit
- orders.commands.{exchange}.{instrument}.cancel
- system.symbol_aliases
- system.settings

Topics Published
----------------
- orders.events.{exchange}.{instrument}.submitted
- orders.events.{exchange}.{instrument}.rejected
- orders.events.{exchange}.{instrument}.fill
- orders.events.{exchange}.{instrument}.cancelled
- orders.events.{exchange}.{instrument}.expired
- system.heartbeats.executor.{exchange}

Example:
-------
Register and run an executor::

    executor = KrakenOrderExecutor()
    await executor.start()
    # Executor listens for orders until stopped
    await executor.stop()
"""
