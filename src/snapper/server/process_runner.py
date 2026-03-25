"""Subprocess entry point for running processes in isolation.

This module provides the entry point for processes spawned as subprocesses
by the ProcessLauncherService. It handles:

1. JSON configuration parsing from command line
2. Dynamic class loading and instantiation
3. Async/sync method invocation
4. Logging setup with process-specific context

Usage:
    Called by ProcessLauncherService when mode='subprocess'::

        python -m snapper.server.process_runner --config '{...}'

Configuration JSON:
    {
        "name": "process-name",
        "class_path": "snapper.strategies.rsi.RSIReversion",
        "method": "start",
        "kwargs": {"symbols": ["BTC-USD"]}
    }

The subprocess runs independently with its own Python interpreter,
allowing true parallelism and isolation from the main server process.
"""

import argparse
import asyncio
import importlib
import inspect
import json
import os
from collections.abc import Awaitable
from collections.abc import Callable
from typing import Any

from loguru import logger

from snapper.utils.logging import set_log_context
from snapper.utils.logging import setup_logging


async def _await_result(awaitable: Awaitable[Any]) -> Any:
    """Await and return result from an awaitable.

    Args:
        awaitable: Coroutine or awaitable object.

    Returns:
        Result of the awaited operation.
    """
    return await awaitable


async def _run_async_method(method: Callable[[], Awaitable[Any]]) -> Any:
    """Run an async method and handle nested awaitables.

    Args:
        method: Async callable to invoke.

    Returns:
        Result from the method, awaiting if necessary.
    """
    result = await method()
    if inspect.isawaitable(result):
        result = await result
    return result


def main() -> int:
    """Main entry point for subprocess process runner.

    Parses command-line arguments, loads the specified class,
    instantiates it with provided kwargs, and invokes
    the target method (sync or async).

    Returns:
        Exit code: 0 for success, 1 for configuration error or process failure.
    """
    setup_logging(level="INFO", json_logs=False, logfile="data/snapper.log")
    parser = argparse.ArgumentParser(description="Snapper process runner")
    parser.add_argument(
        "--config",
        type=str,
        required=True,
        help="JSON config with class_path, method, kwargs",
    )
    args = parser.parse_args()
    try:
        config = json.loads(args.config)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse config JSON: {e}")
        return 1
    name = config.get("name", "unknown")
    class_path = config["class_path"]
    method = config["method"]
    class_kwargs = config.get("kwargs", {})
    set_log_context(f"proc:{name}")
    logger.info(f"Process '{name}' starting (PID: {os.getpid()})")
    try:
        module_path, class_name = class_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        process_class = getattr(module, class_name)
        instance = process_class(**class_kwargs)
        target_method = getattr(instance, method)
        logger.info(f"Process '{name}' calling {class_path}.{method}()")
        if inspect.iscoroutinefunction(target_method):
            asyncio.run(_run_async_method(target_method))
        else:
            result = target_method()
            if inspect.isawaitable(result):
                asyncio.run(_await_result(result))
        logger.info(f"Process '{name}' completed successfully")
        return 0
    except Exception as e:
        logger.error(f"Process '{name}' failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
