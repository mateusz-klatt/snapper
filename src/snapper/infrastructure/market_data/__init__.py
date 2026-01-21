"""Market data snapshot services.

This package provides services for collecting and persisting real-time market
data snapshots from various exchanges. Each exchange has a dedicated updater
service that implements the common MarketSnapshotUpdaterService interface.

Modules:
    base: Abstract base class MarketSnapshotUpdaterService defining the update
        interface.
    kraken: Kraken-specific snapshot updater using WebSocket connections.
    zonda: Zonda-specific snapshot updater using REST API polling.
    walutomat: Walutomat-specific snapshot updater using REST API polling.

All updaters persist data to the database and support configurable update
intervals and symbol filtering.
"""
