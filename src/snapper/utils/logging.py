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
from collections.abc import Callable
from collections.abc import Mapping
from typing import Any
from typing import cast

from loguru import logger

from snapper.infrastructure.logging.handlers import InterceptStdLogHandler

__all__ = [
    "setup_logging",
    "set_log_context",
    "get_log_context",
    "resolve_logfile_from_environment",
    "resolve_subprocess_logfile",
    "LOGFILE_ENV_VAR",
    "DEFAULT_LOGFILE",
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


def _configure_stdlib_logging(level: str) -> None:
    """Route standard library logging through Loguru interception.

    Args:
        level: Minimum application log level.
    """
    logging.basicConfig(
        handlers=[InterceptStdLogHandler()],
        level=logging.INFO if level == "INFO" else logging.DEBUG,
        force=True,
    )
    for log_name in logging.Logger.manager.loggerDict:
        log_obj = logging.getLogger(log_name)
        log_obj.handlers = []
        log_obj.propagate = True
    for logger_name in ("uvicorn", "uvicorn.access", "uvicorn.error"):
        uvicorn_logger = logging.getLogger(logger_name)
        uvicorn_logger.handlers = [InterceptStdLogHandler()]
        uvicorn_logger.propagate = False


def _format_message(record: Mapping[str, Any]) -> str:
    """Format a plain text loguru record.

    Args:
        record: Loguru record mapping.

    Returns:
        Plain text log line, including traceback text when present.
    """
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


def _serialize_json_record(record: object) -> str:
    """Serialize a Loguru record to compact JSON.

    Args:
        record: Raw Loguru record object.

    Returns:
        JSON string containing the stable structured fields.
    """
    rec = cast(Mapping[str, Any], record)
    payload = {
        "ts": rec["time"].timestamp(),
        "level": rec["level"].name,
        "message": rec["message"],
        "module": rec["module"],
        "function": rec["function"],
        "line": rec["line"],
        "context": _log_context.get(),
    }
    return json.dumps(payload)


def _add_json_logging(level: str) -> None:
    """Add the JSON stdout Loguru sink.

    Args:
        level: Minimum log level.
    """
    logger.add(
        lambda msg: print(msg, end=""),
        level=level,
        format=_serialize_json_record,
        filter=_filter_cancelled_errors,
    )


def _add_text_logging(level: str) -> None:
    """Add the terminal-appropriate text Loguru sink.

    Args:
        level: Minimum log level.
    """
    if sys.stdout.isatty():
        logger.add(
            _colorized_sink,
            level=level,
            format=_MSG_ONLY_FMT,
            colorize=False,
            filter=_filter_cancelled_errors,
        )
        return
    logger.add(
        lambda msg: print(_format_message(msg.record), end=""),
        level=level,
        format=_MSG_ONLY_FMT,
        filter=_filter_cancelled_errors,
    )


def _colorized_sink(message: Any) -> None:
    """Print a colorized Loguru message.

    Args:
        message: Loguru message object with a record attribute.
    """
    formatted = _colorize_record(message.record)
    print(formatted, end="")


def _add_file_logging(level: str, logfile: str) -> None:
    """Add an append-only file sink.

    Args:
        level: Minimum log level.
        logfile: Destination file path.
    """
    os.makedirs(os.path.dirname(logfile) or ".", exist_ok=True)
    logger.add(
        _file_sink(logfile),
        level=level,
        format=_MSG_ONLY_FMT,
        filter=_filter_cancelled_errors,
    )


def _file_sink(logfile: str) -> Callable[[Any], None]:
    """Build a file sink callable for a specific path.

    Args:
        logfile: Destination file path.

    Returns:
        Callable sink accepted by Loguru.
    """

    def sink(message: Any) -> None:
        formatted = _format_message(message.record)
        with open(logfile, "a", encoding="utf-8") as f:
            f.write(formatted)

    return sink


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
    _FILE_SINK_READY[0] = False
    _configure_stdlib_logging(level)
    logger.remove()
    logger.configure(extra={"context": "main"})
    if json_logs:
        _add_json_logging(level)
    else:
        _add_text_logging(level)
    if logfile:
        _add_file_logging(level, logfile)
        _FILE_SINK_READY[0] = True


LOGFILE_ENV_VAR = "SNAPPER_LOG_FILE"
DEFAULT_LOGFILE = "data/log/snapper/snapper.log"
_LOGFILE_PATH_MAX_LENGTH = 4096


def _is_valid_logfile_path(logfile: str) -> bool:
    """Return whether a configured logfile has a safe filesystem shape.

    Args:
        logfile: Relative or absolute path supplied through the environment.

    Returns:
        True for a printable ``.log`` file path without traversal segments.
    """
    if (
        not logfile
        or len(logfile) > _LOGFILE_PATH_MAX_LENGTH
        or logfile != logfile.strip()
        or not logfile.isprintable()
    ):
        return False
    normalized = logfile.replace("\\", "/")
    parts = normalized.split("/")
    if "://" in normalized or normalized.endswith("/") or "." in parts or ".." in parts:
        return False
    filename = parts[-1]
    return filename != ".log" and filename.endswith(".log")


def resolve_logfile_from_environment(default_logfile: str) -> str:
    """Prefer a validated explicit logfile over a command default.

    Args:
        default_logfile: Command-specific fallback used when the environment
            variable is absent or invalid.

    Returns:
        The validated :data:`LOGFILE_ENV_VAR` value when explicitly supplied,
        otherwise ``default_logfile``.
    """
    configured_logfile = os.environ.get(LOGFILE_ENV_VAR)
    if configured_logfile is not None and _is_valid_logfile_path(configured_logfile):
        return configured_logfile
    return default_logfile


def resolve_subprocess_logfile() -> str:
    """Return the logfile a spawned subprocess should append to.

    A managed subprocess (``python -m snapper.server.process_runner``)
    cannot derive its container from ``sys.argv`` the way the package
    entry point does, so the container entry point exports its resolved
    logfile in :data:`LOGFILE_ENV_VAR` and every child inherits it
    through the process environment. This keeps a feed-container
    publisher writing to ``data/log/snapper-feed/snapper-feed.log``
    (its parent's file) instead of the API container's
    ``data/log/snapper/snapper.log``, so each
    container — and its children — own a single dedicated log file.
    Falls back to :data:`DEFAULT_LOGFILE` when the variable is unset
    (e.g. a child spawned outside the managed entry point, or a test).

    Returns:
        The logfile path the current subprocess should write to.
    """
    return resolve_logfile_from_environment(DEFAULT_LOGFILE)


_FILE_SINK_READY: list[bool] = [False]
"""Single-element list flag tracking whether ``setup_logging`` has wired
the file sink. Read by callers that emit boot-time confirmation logs
which would otherwise be lost when fired before the file sink existed
(see :func:`snapper.infrastructure.exchanges.kraken_sdk_patches.log_kraken_sdk_patches_status`).
"""


def is_file_sink_ready() -> bool:
    """Return ``True`` once ``setup_logging`` has installed the file sink.

    Used by boot-time confirmation helpers that must not emit until the
    file sink is live. Calling code that has nothing to lose from
    stderr-only emission can ignore this flag; calling code whose
    audience is the persistent log file (i.e. anything an operator
    greps after the fact) should gate emission on this returning
    ``True``.

    Returns:
        ``True`` after ``setup_logging(...)`` ran with a non-empty
        ``logfile``; ``False`` until then (or for the rare invocation
        that disables file logging entirely).
    """
    return _FILE_SINK_READY[0]
