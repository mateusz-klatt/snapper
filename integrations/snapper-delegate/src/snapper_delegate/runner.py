"""Generic idle runner lifecycle for delegate workloads.

The current runner intentionally provides lifecycle plumbing only. It
does not perform model calls, network requests, tool execution, or consult
handling. The class is dependency-light so the runner-only container can
import it without loading Snapper's coordinator, database, message bus, or
broker stack. Managed-process registration lives in a separate module.
"""

import asyncio
import signal

from loguru import logger

_HEARTBEAT_INTERVAL_SECONDS = 30.0


class DelegateRunner:
    """Run an idle delegate lifecycle directly or under process management.

    The configuration values are retained for later increments but are
    deliberately unused here. This first increment only proves that the
    workload can stay alive, report its local state, and stop cleanly.
    """

    def __init__(
        self,
        model_alias: str,
        base_url: str,
        api_key_file: str,
        delegate_token_file: str,
        max_tool_rounds: int,
    ) -> None:
        """Initialize the lifecycle-only runner.

        Args:
            model_alias: Operator-facing model route alias.
            base_url: Base endpoint reserved for the later client seam.
            api_key_file: Path reserved for later credential loading.
            delegate_token_file: Path reserved for later token loading.
            max_tool_rounds: Positive limit reserved for later tool orchestration.
        """
        self.model_alias = model_alias
        self.base_url = base_url
        self.api_key_file = api_key_file
        self.delegate_token_file = delegate_token_file
        self.max_tool_rounds = max_tool_rounds
        self._stop_event = asyncio.Event()
        self._running = False
        self._heartbeat_count = 0

    async def start(self) -> None:
        """Start the runner and remain idle until a clean stop is requested."""
        self._stop_event.clear()
        self._running = True
        stop_signals: tuple[signal.Signals, ...] = ()
        try:
            stop_signals = self._install_stop_signal_handlers()
            logger.info("Delegate runner started")
            await self._idle_heartbeat_loop()
        finally:
            self._running = False
            self._remove_stop_signal_handlers(stop_signals)
            logger.info("Delegate runner stopped")

    def _install_stop_signal_handlers(self) -> tuple[signal.Signals, ...]:
        """Route managed-process termination signals through the stop event."""
        loop = asyncio.get_running_loop()
        installed: list[signal.Signals] = []
        for stop_signal in (signal.SIGTERM,):
            try:
                loop.add_signal_handler(stop_signal, self._stop_event.set)
            except (NotImplementedError, RuntimeError):
                continue
            installed.append(stop_signal)
        return tuple(installed)

    @staticmethod
    def _remove_stop_signal_handlers(stop_signals: tuple[signal.Signals, ...]) -> None:
        """Remove lifecycle signal handlers installed for this runner."""
        loop = asyncio.get_running_loop()
        for stop_signal in stop_signals:
            loop.remove_signal_handler(stop_signal)

    async def _idle_heartbeat_loop(self) -> None:
        """Stay alive while emitting local idle heartbeats.

        Later increments may perform model and consult work between
        heartbeats. Quota exhaustion belongs in that future seam as a
        degraded state while this loop remains alive; it must not escape
        as a runner crash. No client, consult, or egress behavior exists
        in this lifecycle stub.
        """
        while not self._stop_event.is_set():
            self._heartbeat_count += 1
            logger.debug("Delegate runner idle heartbeat")
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=_HEARTBEAT_INTERVAL_SECONDS)
            except TimeoutError:
                continue

    async def stop(self) -> None:
        """Request a clean exit from the idle lifecycle loop."""
        self._stop_event.set()
        await asyncio.sleep(0)

    def get_status(self) -> dict[str, object]:
        """Return the runner's local lifecycle status.

        Returns:
            JSON-compatible state and heartbeat fields.
        """
        return {
            "state": "idle" if self._running else "stopped",
            "running": self._running,
            "heartbeat_count": self._heartbeat_count,
        }
