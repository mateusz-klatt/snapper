"""Order execution services.

This package provides execution services that receive order requests from
the ZMQ messaging bus, execute them on exchanges, and publish execution
results back to the bus.

Architecture
------------
Each executor:
1. Subscribes to orders.{exchange}.* topics
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

    Strategy ──> ZMQ [orders.{exchange}.requests]
                      │
                      v
                  Executor ──> Exchange API
                      │
                      v
    Strategy <── ZMQ [executions.{exchange}.{instrument}.fill]
                     [orders.{exchange}.{instrument}.status]

Topics Subscribed
-----------------
- orders.{exchange}.requests
- orders.{exchange}.{instrument}.new
- system.symbol_mappings
- system.settings

Topics Published
----------------
- executions.{exchange}.{instrument}.fill
- orders.{exchange}.{instrument}.status
- system.heartbeats.executor.{exchange}

Example:
-------
Register and run an executor::

    executor = KrakenOrderExecutor()
    await executor.start()
    # Executor listens for orders until stopped
    await executor.stop()
"""
