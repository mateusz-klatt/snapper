"""Logging infrastructure and handlers.

This package provides custom logging handlers and utilities for integrating
Python's standard logging with Loguru.

Modules:
    handlers: Custom logging handlers including InterceptStdLogHandler for
        routing stdlib logs to Loguru.

The package enables unified logging across the application by intercepting
logs from third-party libraries that use the standard logging module.
"""
