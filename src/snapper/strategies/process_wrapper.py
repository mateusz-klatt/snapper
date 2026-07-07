"""Strategy process wrapper.

This module provides functionality to wrap strategies as managed processes
that can be controlled via the process manager.
"""

import asyncio
import copy
from collections.abc import Mapping
from typing import Any

from loguru import logger

from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.process_parameters import StrategyProcessParameters
from snapper.application.process_manager.registry import register_process
from snapper.config.settings import AppSettings
from snapper.core.types import ExchangeEnum
from snapper.core.types import OrderExchange
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRoleEnum
from snapper.strategies.base import BaseStrategy
from snapper.strategies.base import StrategyConfig
from snapper.strategies.factory import StrategyFactory
from snapper.utils.logging import set_log_context


def create_strategy_process(
    process_name: str,
    strategy_class: str,
    default_config: dict[str, Any],
    reference_identity_params: Mapping[str, str] | None = None,
    seeded_identity_params: tuple[str, ...] = (),
) -> type:
    """Create a process class wrapper for a strategy.

    Dynamically creates a RegisterableProcess subclass that wraps
    a strategy for management by the process manager.

    Args:
        process_name: Name for the process.
        strategy_class: Name of the strategy class.
        default_config: Default configuration for the strategy.
        reference_identity_params: Scoped-strategy nested identity references
            (name -> kind) read from the strategy class, empty otherwise.
        seeded_identity_params: Scoped-strategy nested seeded-identity keys
            read from the strategy class, empty otherwise.

    Returns:
        The dynamically created process class.
    """

    @register_process(
        process_name,
        description=f"Strategy: {strategy_class} ({default_config.get('name', 'unnamed')})",
        priority=50,
        role=ProcessRoleEnum.STRATEGY,
        tags=("strategy", strategy_class.lower()),
        parameters_model=StrategyProcessParameters,
        enabled=False,
        mode=ProcessModeEnum.THREAD,
        reference_identity_params=reference_identity_params,
        seeded_identity_params=seeded_identity_params,
    )
    class StrategyProcess(RegisterableProcess):
        @staticmethod
        def get_default_parameters(settings: AppSettings) -> dict[str, Any]:
            """Return a fresh copy of the strategy's default config.

            A copy — never the shared ``default_config`` template captured in
            this closure — so a caller that mutates the result (for example
            the persistence layer minting a seeded-identity token in place)
            can never pollute the template and leak that value into a later
            instance created from the same strategy.
            """
            return copy.deepcopy(default_config)

        def __init__(
            self,
            name: str,
            inputs: list[str],
            outputs: list[str],
            exchange: OrderExchange = ExchangeEnum.PAPER,
            params: dict[str, Any] | None = None,
            wallet_public_id: str = "",
            operator_public_id: str = "",
        ) -> None:
            self.process_name = process_name
            self.config = StrategyConfig(
                name=name,
                strategy_class=strategy_class,
                inputs=inputs,
                outputs=outputs,
                exchange=exchange,
                params=params or {},
                wallet_public_id=wallet_public_id,
                operator_public_id=operator_public_id,
            )
            self.strategy: BaseStrategy | None = None
            self.factory = StrategyFactory()
            self._stop_event: asyncio.Event | None = None
            logger.info(
                f"Strategy process '{process_name}' initialized with config: "
                f"name={name}, inputs={inputs}, outputs={outputs}, exchange={exchange}"
            )

        async def start(self) -> None:
            set_log_context(f"strat:{self.config.name}")
            logger.info(f"Starting strategy process: {self.process_name}")
            self._stop_event = asyncio.Event()
            try:
                self.strategy = self.factory.create_strategy(self.config)
                await self.factory.start_strategy(self.config.name)
                logger.success(
                    f"Strategy '{self.config.name}' started successfully "
                    f"(class: {self.config.strategy_class})"
                )
                try:
                    await self._wait_stop_or_listen_death()
                except asyncio.CancelledError:
                    logger.info(f"Strategy process '{self.process_name}' start task cancelled")
                    raise
            except Exception as e:
                logger.error(f"Failed to start strategy '{self.config.name}': {e}")
                raise
            finally:
                self._stop_event = None
                logger.debug(f"Strategy process '{self.process_name}' start coroutine exiting")

        async def _wait_stop_or_listen_death(self) -> None:
            """Block until stop is requested or the listen loop dies.

            The strategy's listen loop is a fire-and-forget task; before
            this guard its unexpected death produced a ZOMBIE (P7 soak
            finding: heartbeats kept reporting running=True while the
            strategy consumed nothing). Waiting on BOTH the stop event
            and the listen task converts an unexpected loop exit into a
            raised error, so the launcher's task-exception watchdog
            restarts the whole strategy process.

            Raises:
                RuntimeError: The listen loop exited while the process
                    was still supposed to run (chained to the loop's own
                    exception when it raised one).
            """
            assert self._stop_event is not None
            stop_wait = asyncio.ensure_future(self._stop_event.wait())
            listen_task = getattr(self.strategy, "_listen_task", None)
            if listen_task is None:
                await stop_wait
                return
            try:
                done, _ = await asyncio.wait(
                    {stop_wait, listen_task}, return_when=asyncio.FIRST_COMPLETED
                )
            finally:
                if not stop_wait.done():
                    stop_wait.cancel()
            if listen_task in done and not self._stop_event.is_set():
                exc = listen_task.exception() if not listen_task.cancelled() else None
                raise RuntimeError(
                    f"Strategy '{self.config.name}' listen loop exited unexpectedly"
                ) from exc

        async def stop(self) -> None:
            logger.info(f"Stopping strategy process: {self.process_name}")
            if self._stop_event and not self._stop_event.is_set():
                self._stop_event.set()
            if self.strategy:
                await self.factory.stop_strategy(self.config.name)
                self.strategy = None
                logger.success(f"Strategy '{self.config.name}' stopped")

        def get_status(self) -> dict[str, Any]:
            is_running = self.strategy is not None and self.strategy.is_running
            return {
                "process_name": self.process_name,
                "strategy_name": self.config.name,
                "strategy_class": self.config.strategy_class,
                "inputs": self.config.inputs,
                "outputs": self.config.outputs,
                "exchange": self.config.exchange,
                "params": self.config.params,
                "running": is_running,
            }

    return StrategyProcess
