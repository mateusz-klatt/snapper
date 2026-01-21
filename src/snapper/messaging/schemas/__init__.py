"""ZMQ messaging schema definitions for inter-process communication.

This package contains Pydantic models for all message types exchanged
between Snapper processes over ZeroMQ pub/sub messaging.

Modules:
    data: Core data structures (TickData, CandleData, SignalData, etc.).
    messages: Message envelopes wrapping data for ZMQ transport.
"""
