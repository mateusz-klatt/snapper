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
The async target is driven on a uvloop event loop where uvloop is
available so a ``mode=PROCESS`` publisher gets the same libuv IO
speedup the FastAPI server enjoys (``uvicorn --loop uvloop``). Windows
workers fall back to ``asyncio.run`` because uvloop is not published
for that platform.

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
from typing import cast

from loguru import logger

from snapper.application.ai_review.service import _BUS_AI_REVIEW_DECISION_TOPIC
from snapper.application.ai_review.service import get_ai_review_service
from snapper.application.process_manager.registry import discover_processes
from snapper.application.process_manager.registry import get_registered_processes
from snapper.config.settings import get_settings
from snapper.infrastructure.exchanges.kraken_sdk_patches import log_kraken_sdk_patches_status
from snapper.utils.logging import resolve_subprocess_logfile
from snapper.utils.logging import set_log_context
from snapper.utils.logging import setup_logging


async def _await_result[Result](awaitable: Awaitable[Result]) -> Result:
    """Await and return result from an awaitable.

    Args:
        awaitable: Coroutine or awaitable object.

    Returns:
        Result of the awaited operation.
    """
    return await awaitable


def _resolve_process_class(class_path: str, name: str, template_name: str | None) -> type:
    """Resolve the process class: registry template first, then dynamic import.

    Configs created FROM a registered template persist a function-local
    wrapper class_path that can never be imported dynamically; the runner
    resolves such classes by running process discovery and reading the
    template's registry entry. A plain importable class_path keeps the
    legacy dynamic import (no discovery cost).

    Args:
        class_path: Fully qualified class path from the runner config.
        name: Process name (log context only).
        template_name: Optional source-template registry name.

    Returns:
        The resolved process class.

    Raises:
        ImportError: When neither the registry nor the dynamic import
            can produce the class.
    """
    if template_name:
        discover_processes()
        entry = get_registered_processes().get(template_name)
        if entry is not None and isinstance(entry.class_ref, type):
            logger.info(f"Process '{name}' resolved class via registry template '{template_name}'")
            return entry.class_ref
    module_path, class_name = class_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    resolved = getattr(module, class_name)
    if not isinstance(resolved, type):
        raise ImportError(f"{class_path} is not a class")
    return resolved


def _use_asyncio_runner() -> bool:
    """Return whether the subprocess runner must avoid uvloop."""
    return os.name == "nt"


def _run_event_loop[Result](awaitable: Awaitable[Result]) -> Result:
    """Run an awaitable with uvloop where supported.

    Returns:
        Result returned by the awaited operation.
    """
    if _use_asyncio_runner():
        return asyncio.run(_await_result(awaitable))
    event_loop_module = importlib.import_module("uvloop")
    run_event_loop = cast(
        Callable[[Awaitable[Result]], Result],
        event_loop_module.run,
    )
    return run_event_loop(_await_result(awaitable))


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


async def _await_result_with_listener[Result](awaitable: Awaitable[Result]) -> Result:
    """Drive ``awaitable`` with the subprocess decision-only bus listener active."""
    async with _ai_review_decision_listener():
        return await _await_result(awaitable)


def main() -> int:
    """Main entry point for subprocess process runner.

    Parses command-line arguments, loads the specified class,
    instantiates it with provided parameters, and invokes
    the target method (sync or async).

    Logging is routed to the container's dedicated file resolved by
    :func:`snapper.utils.logging.resolve_subprocess_logfile` (inherited
    from the parent via ``SNAPPER_LOG_FILE``) so a feed-container
    publisher logs to ``data/snapper-feed.log`` rather than the API
    container's ``data/snapper.log``.

    Returns:
        Exit code: 0 for success, 1 for configuration error or process failure.
    """
    setup_logging(level="INFO", json_logs=False, logfile=resolve_subprocess_logfile())
    log_kraken_sdk_patches_status()
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
    template_name = config.get("template_name")
    set_log_context(f"proc:{name}")
    logger.info(f"Process '{name}' starting (PID: {os.getpid()})")
    try:
        process_class = _resolve_process_class(class_path, name, template_name)
        instance = process_class(**class_parameters)
        target_method = getattr(instance, method)
        logger.info(f"Process '{name}' calling {class_path}.{method}()")
        if inspect.iscoroutinefunction(target_method):
            _run_event_loop(_run_async_method_with_listener(target_method))
        else:
            result = target_method()
            if inspect.isawaitable(result):
                _run_event_loop(_await_result_with_listener(result))
        logger.info(f"Process '{name}' completed successfully")
        return 0
    except Exception as e:
        logger.error(f"Process '{name}' failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
