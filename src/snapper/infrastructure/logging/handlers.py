"""Custom logging handlers for unified log management.

This module provides handlers for integrating Python's standard logging module
with Loguru. The InterceptStdLogHandler allows third-party libraries that use
stdlib logging to have their logs routed through Loguru.

Example:
    >>> import logging
    >>> from snapper.infrastructure.logging.handlers import InterceptStdLogHandler
    >>> logging.basicConfig(handlers=[InterceptStdLogHandler()], level=logging.DEBUG)
    >>> logging.info("This will be routed to Loguru")
"""

import logging

from loguru import logger


class InterceptStdLogHandler(logging.Handler):
    """Handler that intercepts stdlib logging and routes to Loguru.

    Captures log records from Python's standard logging module and re-emits
    them through Loguru. This enables unified logging configuration when
    using third-party libraries that use stdlib logging.

    The handler preserves log levels and exception information. Module name
    is bound to the log context for filtering.

    Example:
        >>> handler = InterceptStdLogHandler()
        >>> logging.getLogger("uvicorn").addHandler(handler)
    """

    def emit(self, record: logging.LogRecord) -> None:
        """Emit a log record through Loguru.

        Args:
            record: Standard library LogRecord to process.
        """
        try:
            level = record.levelname
        except Exception:
            level = "INFO"
        logger.bind(module=record.module).opt(depth=6, exception=record.exc_info).log(
            level, record.getMessage()
        )
