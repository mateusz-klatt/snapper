"""Logging configuration with colorized output and context tracking.

This module provides centralized logging setup for the Snapper application
using Loguru with features:

- **Colorized terminal output**: Level-based colors and context-aware
  background coloring for easy visual separation of log sources.
- **Context tracking**: Per-task/process context labels via ContextVar.
- **JSON mode**: Structured JSON logging for production environments.
- **File logging**: Optional file output with rotation support.
- **uvicorn integration**: Intercepts standard library logging.

Color scheme:
    - Each log context gets a unique background color based on hash.
    - Log levels have distinct foreground colors (green=INFO, yellow=WARNING, etc.).

Example:
    Basic setup::

        from snapper.utils.logging import setup_logging, set_log_context

        setup_logging(level="DEBUG")
        set_log_context("my-process")
        logger.info("Starting process")  # Shows with context label

    JSON logging for production::

        setup_logging(level="INFO", json_logs=True)
"""

import asyncio
import colorsys
import contextvars
import json
import logging
import os
import sys
import traceback
import zlib
from collections.abc import Mapping
from typing import Any
from typing import cast

from loguru import logger

from snapper.infrastructure.logging.handlers import InterceptStdLogHandler

__all__ = [
    "setup_logging",
    "set_log_context",
    "get_log_context",
]
_MSG_ONLY_FMT = "{message}"
_RESET = "\033[0m"
_WHITE = "\033[38;2;255;255;255m"
_ACCENT = "\033[38;2;140;140;255m"
_EXCEPTION_RED = "\033[38;2;255;90;90m"
_LEVEL_COLORS: dict[str, str] = {
    "TRACE": "\033[38;2;150;150;150m",
    "DEBUG": "\033[38;2;100;150;200m",
    "INFO": "\033[38;2;80;200;120m",
    "SUCCESS": "\033[38;2;0;255;127m",
    "WARNING": "\033[38;2;255;200;0m",
    "ERROR": "\033[38;2;255;90;90m",
    "CRITICAL": "\033[38;2;255;0;255m",
}
_CONTEXT_BG_SATURATION = 0.25
_CONTEXT_BG_LIGHTNESS = 0.13
_CONTEXT_COLOR_CACHE: dict[str, str] = {}
_log_context: contextvars.ContextVar[str] = contextvars.ContextVar("log_context", default="main")


def _get_context_bg_color(context: str) -> str:
    if context in _CONTEXT_COLOR_CACHE:
        return _CONTEXT_COLOR_CACHE[context]
    hash_val = zlib.crc32(context.encode()) & 0xFFFFFFFF
    hue = (hash_val % 360) / 360.0
    r, g, b = colorsys.hls_to_rgb(hue, _CONTEXT_BG_LIGHTNESS, _CONTEXT_BG_SATURATION)
    r_int, g_int, b_int = int(r * 255), int(g * 255), int(b * 255)
    color = f"\033[48;2;{r_int};{g_int};{b_int}m"
    _CONTEXT_COLOR_CACHE[context] = color
    return color


_CONTEXT_MAX_LEN = 16


def _truncate(text: str, max_len: int = _CONTEXT_MAX_LEN) -> str:
    if len(text) <= max_len:
        return text.ljust(max_len)
    return text[:max_len]


def _get_level_color(level: str) -> str:
    return _LEVEL_COLORS.get(level, _RESET)


def _colorize_record(record: Mapping[str, Any]) -> str:
    time_str = record["time"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    level = record["level"].name
    process = record["process"]
    context = _log_context.get()
    module = record["module"]
    function = record["function"]
    line = record["line"]
    message = record["message"]
    level_color = _get_level_color(level)
    bg = _get_context_bg_color(context)
    if level in ("WARNING", "ERROR", "CRITICAL"):
        colored_message = f"{bg}{level_color}{message}"
    else:
        colored_message = f"{bg}{_WHITE}{message}"
    result = (
        f"{bg}{level_color}{level:<8} | "
        f"{bg}{_WHITE}{_truncate(context)} | "
        f"{bg}{_ACCENT}{time_str} | "
        f"{bg}{_ACCENT}{process} | "
        f"{bg}{_ACCENT}{module}:{function}:{line} | "
        f"{colored_message}"
        f"{_RESET}\n"
    )
    if record.get("exception"):
        exception_text = record["exception"]
        if hasattr(exception_text, "type") and hasattr(exception_text, "value"):
            exc_lines = traceback.format_exception(
                exception_text.type, exception_text.value, exception_text.traceback
            )
            result += f"{_EXCEPTION_RED}{''.join(exc_lines)}{_RESET}"
    return result


def set_log_context(context: str) -> None:
    """Set the current logging context label.

    The context is stored in a ContextVar and appears in all log messages
    from the current task/thread.

    Args:
        context: Context label (e.g., 'api', 'proc:strategy-1').
    """
    _log_context.set(context)


def get_log_context() -> str:
    """Get the current logging context label.

    Returns:
        Current context string, defaults to 'main'.
    """
    return _log_context.get()


def _filter_cancelled_errors(record: Any) -> bool:
    """Filter out asyncio.CancelledError from logs.

    Args:
        record: Log record to check.

    Returns:
        False if record contains CancelledError, True otherwise.
    """
    if "exception" in record and record["exception"] is not None:
        exc_type = record["exception"].type
        if exc_type is asyncio.CancelledError:
            return False
    return True


def setup_logging(level: str = "INFO", json_logs: bool = False, logfile: str | None = None) -> None:
    """Configure application-wide logging.

    Sets up Loguru with appropriate handlers based on environment:
        - TTY: Colorized output with context-aware backgrounds.
        - Non-TTY: Plain text format suitable for log aggregators.
        - JSON mode: Structured JSON for machine parsing.

    Also configures standard library logging interception for uvicorn
    and other libraries using stdlib logging.

    Aggressively replaces every stdlib logger's handlers with an
    :class:`InterceptStdLogHandler` so third-party libraries that
    transitively install :class:`rich.logging.RichHandler` (observed
    via ``python-kraken-sdk`` ``_recover_subscriptions`` consuming
    >50% of GIL time rendering ``rich.table`` / ``rich.text`` per log
    line) cannot route logs through their own formatters. Without
    this every kraken WS subscription ack would format through rich,
    blocking the publisher consumer for milliseconds at a time.

    Args:
        level: Minimum log level ('DEBUG', 'INFO', 'WARNING', etc.).
        json_logs: If True, output JSON instead of formatted text.
        logfile: Optional file path for additional file logging.
    """
    logging.basicConfig(
        handlers=[InterceptStdLogHandler()],
        level=logging.INFO if level == "INFO" else logging.DEBUG,
        force=True,
    )
    for log_name in list(logging.Logger.manager.loggerDict.keys()):
        log_obj = logging.getLogger(log_name)
        log_obj.handlers = []
        log_obj.propagate = True
    for logger_name in ("uvicorn", "uvicorn.access", "uvicorn.error"):
        uvicorn_logger = logging.getLogger(logger_name)
        uvicorn_logger.handlers = [InterceptStdLogHandler()]
        uvicorn_logger.propagate = False
    logger.remove()
    logger.configure(extra={"context": "main"})

    def _format_message(record: Mapping[str, Any]) -> str:
        time_str = record["time"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        level_name = record["level"].name
        process = record["process"]
        context = _log_context.get()
        module = record["module"]
        function = record["function"]
        line = record["line"]
        message = record["message"]
        formatted = (
            f"{level_name:<8} | {_truncate(context)} | {time_str} | "
            f"{process} | {module}:{function}:{line} | {message}\n"
        )
        if record.get("exception"):
            exception_text = record["exception"]
            if hasattr(exception_text, "type") and hasattr(exception_text, "value"):
                exc_lines = traceback.format_exception(
                    exception_text.type, exception_text.value, exception_text.traceback
                )
                formatted += "".join(exc_lines)
        return formatted

    if json_logs:

        def _serialize(record: object) -> str:
            rec = cast(Mapping[str, Any], record)
            r = {
                "ts": rec["time"].timestamp(),
                "level": rec["level"].name,
                "message": rec["message"],
                "module": rec["module"],
                "function": rec["function"],
                "line": rec["line"],
                "context": _log_context.get(),
            }
            return json.dumps(r)

        logger.add(
            lambda msg: print(msg, end=""),
            level=level,
            format=_serialize,
            filter=_filter_cancelled_errors,
        )
    else:
        if sys.stdout.isatty():

            def colorized_sink(message: Any) -> None:
                formatted = _colorize_record(message.record)
                print(formatted, end="")

            logger.add(
                colorized_sink,
                level=level,
                format=_MSG_ONLY_FMT,
                colorize=False,
                filter=_filter_cancelled_errors,
            )
        else:
            logger.add(
                lambda msg: print(_format_message(msg.record), end=""),
                level=level,
                format=_MSG_ONLY_FMT,
                filter=_filter_cancelled_errors,
            )
    if logfile:
        os.makedirs(os.path.dirname(logfile) or ".", exist_ok=True)

        def file_sink(message: Any) -> None:
            formatted = _format_message(message.record)
            with open(logfile, "a", encoding="utf-8") as f:
                f.write(formatted)

        logger.add(
            file_sink,
            level=level,
            format=_MSG_ONLY_FMT,
            filter=_filter_cancelled_errors,
        )
