"""Subprocess entry point for running processes in isolation.

This module provides the entry point for processes spawned as subprocesses
by the ProcessLauncherService. It handles
1. JSON configuration parsing from command line
2. Dynamic class loading and instantiation
3. Async/sync method invocation
4. Logging setup with process-specific context
5. AiReviewService decision-only bus listener wiring (process-mode
   strategy fast-path closure)
Usage
    Called by ProcessLauncherService when mode='subprocess'
        python -m snapper.server.process_runner --config '{...}'
Configuration JSON
        "name": "process-name"
        "class_path": "snapper.strategies.rsi.RSIReversion"
        "method": "start"
        "parameters": {"symbols": ["BTC-USD"]}
The subprocess runs independently with its own Python interpreter
allowing true parallelism and isolation from the main server process.

AI Review fast-path: subprocess
strategies that invoke ``create_ai_review_and_await`` register an
``asyncio.Future`` on the subprocess's :class:`AiReviewService`
singleton. Without a bus listener that future would never fire (the
FastAPI server's listener resolves futures on its OWN singleton) and
the strategy primitive would always fall back to the slower DB-poll
path. This module wraps the async target invocation in an
``_ai_review_decision_listener`` context manager that subscribes to
the ``bus.ai_review_decision`` topic only — explicitly NOT to
``bus.delegate_offline`` (needs a repository factory the subprocess
lacks) or ``bus.caps_violation_after_ai_approve`` (the FastAPI
server with ShardOwnership gating owns that fanout; a subprocess
re-publish would N-duplicate the external WS frame).
"""

import argparse
import asyncio
import contextlib
import importlib
import inspect
import json
import os
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from typing import Any

from loguru import logger

from snapper.application.ai_review.service import _BUS_AI_REVIEW_DECISION_TOPIC
from snapper.application.ai_review.service import get_ai_review_service
from snapper.config.settings import get_settings
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


@contextlib.asynccontextmanager
async def _ai_review_decision_listener() -> AsyncIterator[None]:
    """Wire the subprocess decision-only bus listener.

    Subscribes the subprocess's :class:`AiReviewService` singleton to
    ``bus.ai_review_decision`` ONLY so the strategy primitive's Future
    fast-path wakes immediately when an AI delegate submits a
    decision. Restricted to that single topic on purpose:

    - ``bus.delegate_offline``: handler needs a repository factory the
      subprocess lacks; would log warning + skip on every event.
    - ``bus.caps_violation_after_ai_approve``: handler re-publishes
      the external ``ai_reviews.*.caps_violation`` WS frame. The
      FastAPI server (with ShardOwnership gating) is the SOLE
      publisher; a subprocess re-publish would N-duplicate the frame.

    Empty broker XPUB skips the listener entirely (test fixtures +
    process-runner unit tests rely on this fast-fail path). On exit
    we always stop the listener so the subprocess can shut down
    cleanly without leaking the ZMQ context.
    """
    settings = get_settings()
    broker_xpub = settings.zmq_broker_xpub
    if not broker_xpub:
        yield
        return
    service = get_ai_review_service()
    await service.start_bus_listener(
        broker_xpub,
        topics=(_BUS_AI_REVIEW_DECISION_TOPIC,),
    )
    try:
        yield
    finally:
        await service.stop_bus_listener()


async def _run_async_method_with_listener(method: Callable[[], Awaitable[Any]]) -> Any:
    """Run ``method`` with the subprocess decision-only bus listener active."""
    async with _ai_review_decision_listener():
        return await _run_async_method(method)


async def _await_result_with_listener(awaitable: Awaitable[Any]) -> Any:
    """Drive ``awaitable`` with the subprocess decision-only bus listener active."""
    async with _ai_review_decision_listener():
        return await _await_result(awaitable)


def main() -> int:
    """Main entry point for subprocess process runner.

    Parses command-line arguments, loads the specified class,
    instantiates it with provided parameters, and invokes
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
        help="JSON config with class_path, method, parameters",
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
    class_parameters = config.get("parameters", {})
    set_log_context(f"proc:{name}")
    logger.info(f"Process '{name}' starting (PID: {os.getpid()})")
    try:
        module_path, class_name = class_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        process_class = getattr(module, class_name)
        instance = process_class(**class_parameters)
        target_method = getattr(instance, method)
        logger.info(f"Process '{name}' calling {class_path}.{method}()")
        if inspect.iscoroutinefunction(target_method):
            asyncio.run(_run_async_method_with_listener(target_method))
        else:
            result = target_method()
            if inspect.isawaitable(result):
                asyncio.run(_await_result_with_listener(result))
        logger.info(f"Process '{name}' completed successfully")
        return 0
    except Exception as e:
        logger.error(f"Process '{name}' failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
