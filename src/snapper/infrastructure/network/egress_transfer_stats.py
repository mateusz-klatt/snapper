"""WireGuard transfer sampling for the snapper-egress sidecar.

The sidecar owns the WireGuard interfaces, so it is the only process
that can safely call ``wg show <iface> dump``. This module converts that
tool output into small typed samples and computes byte rates from a
previous monotonic sample without influencing route selection.
"""

import subprocess
import time
from collections.abc import Callable
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime

from snapper.infrastructure.network.egress_models import EgressTransferInterfaceSnapshot


@dataclass(frozen=True, slots=True)
class WireGuardTransferCounters:
    """Cumulative transfer counters for one WireGuard interface."""

    interface: str
    rx_bytes: int
    tx_bytes: int
    latest_handshake_at: datetime | None


@dataclass(frozen=True, slots=True)
class EgressTransferTunnel:
    """Tunnel fields required to publish transfer samples."""

    interface: str
    socks5_listen_port: int


@dataclass(frozen=True, slots=True)
class _PreviousTransferCounters:
    """Previous sample used to compute byte rates."""

    monotonic_time: float
    rx_bytes: int
    tx_bytes: int


WireGuardDumpRunner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]
WireGuardReader = Callable[[str], WireGuardTransferCounters | None]
MonotonicClock = Callable[[], float]
WallClock = Callable[[], datetime]


def parse_wg_dump(interface: str, dump: str) -> WireGuardTransferCounters:
    """Parse ``wg show <iface> dump`` output into summed transfer counters.

    Given tab-separated WireGuard dump output, when peer rows contain
    valid handshake and transfer fields, then received and transmitted
    bytes are summed across peers and the newest handshake is retained.
    Malformed peer rows are skipped so one bad line does not hide the
    rest of the interface.

    Args:
        interface: WireGuard interface name associated with the dump.
        dump: Raw tab-separated ``wg show <iface> dump`` output.

    Returns:
        Parsed cumulative counters for the requested interface.
    """
    rx_total = 0
    tx_total = 0
    latest_handshake = 0
    for line in dump.splitlines()[1:]:
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 7:
            continue
        try:
            handshake = int(parts[4])
            rx_bytes = int(parts[5])
            tx_bytes = int(parts[6])
        except ValueError:
            continue
        rx_total += rx_bytes
        tx_total += tx_bytes
        if handshake > latest_handshake:
            latest_handshake = handshake
    latest_handshake_at = (
        datetime.fromtimestamp(latest_handshake, tz=UTC) if latest_handshake > 0 else None
    )
    return WireGuardTransferCounters(
        interface=interface,
        rx_bytes=rx_total,
        tx_bytes=tx_total,
        latest_handshake_at=latest_handshake_at,
    )


def _run_wg_dump(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    """Execute a ``wg`` command and return the completed process."""
    return subprocess.run(
        list(command),
        check=False,
        capture_output=True,
        text=True,
    )


def read_wg_transfer_counters(
    interface: str,
    runner: WireGuardDumpRunner = _run_wg_dump,
) -> WireGuardTransferCounters | None:
    """Read one WireGuard interface's cumulative transfer counters.

    Given a WireGuard interface name, when ``wg show <iface> dump``
    succeeds, then parsed counters are returned. Command failures are
    treated as a missing interface and reported as ``None`` so the
    sampler can emit a reset sample instead of crashing the sidecar.

    Args:
        interface: WireGuard interface name to read.
        runner: Injectable command runner for tests.

    Returns:
        Parsed counters, or ``None`` when the command fails.
    """
    result = runner(("wg", "show", interface, "dump"))
    if result.returncode != 0:
        return None
    return parse_wg_dump(interface, result.stdout)


class EgressTransferSampler:
    """Compute sidecar transfer snapshots from WireGuard counters."""

    def __init__(
        self,
        *,
        tunnels: Sequence[EgressTransferTunnel],
        reader: WireGuardReader = read_wg_transfer_counters,
        monotonic_clock: MonotonicClock = time.monotonic,
        wall_clock: WallClock | None = None,
    ) -> None:
        """Initialize sampler state for configured tunnels.

        Given stable tunnel descriptors and a reader, when ``sample`` is
        called repeatedly, then each output row includes cumulative byte
        counters and a rate only when the current and previous counters
        form a reliable positive-time delta.
        """
        self._tunnels = list(tunnels)
        self._reader = reader
        self._monotonic_clock = monotonic_clock
        self._wall_clock = wall_clock or self._utc_now
        self._previous: dict[str, _PreviousTransferCounters] = {}

    @staticmethod
    def _utc_now() -> datetime:
        """Return the current UTC wall-clock timestamp."""
        return datetime.now(UTC)

    def sample(self) -> list[EgressTransferInterfaceSnapshot]:
        """Return transfer snapshots for all configured tunnels.

        Given the current WireGuard counters, when this is the first
        sample, the interface is missing, the interval is invalid, or a
        counter decreased, then rates are ``None`` and ``counter_reset``
        is true. Otherwise rates are byte deltas divided by elapsed
        monotonic seconds.

        Returns:
            Per-interface transfer snapshots in configured tunnel order.
        """
        monotonic_time = self._monotonic_clock()
        sampled_at = self._wall_clock()
        rows: list[EgressTransferInterfaceSnapshot] = []
        for tunnel in self._tunnels:
            counters = self._reader(tunnel.interface)
            if counters is None:
                self._previous.pop(tunnel.interface, None)
                rows.append(
                    EgressTransferInterfaceSnapshot(
                        interface=tunnel.interface,
                        socks5_listen_port=tunnel.socks5_listen_port,
                        rx_bytes=0,
                        tx_bytes=0,
                        rx_rate_bytes_per_second=None,
                        tx_rate_bytes_per_second=None,
                        latest_handshake_at=None,
                        counter_reset=True,
                        sampled_at=sampled_at,
                    )
                )
                continue
            previous = self._previous.get(tunnel.interface)
            rx_rate: float | None = None
            tx_rate: float | None = None
            counter_reset = True
            if previous is not None:
                delta_seconds = monotonic_time - previous.monotonic_time
                counters_decreased = (
                    counters.rx_bytes < previous.rx_bytes or counters.tx_bytes < previous.tx_bytes
                )
                if delta_seconds > 0.0 and not counters_decreased:
                    rx_rate = (counters.rx_bytes - previous.rx_bytes) / delta_seconds
                    tx_rate = (counters.tx_bytes - previous.tx_bytes) / delta_seconds
                    counter_reset = False
            self._previous[tunnel.interface] = _PreviousTransferCounters(
                monotonic_time=monotonic_time,
                rx_bytes=counters.rx_bytes,
                tx_bytes=counters.tx_bytes,
            )
            rows.append(
                EgressTransferInterfaceSnapshot(
                    interface=tunnel.interface,
                    socks5_listen_port=tunnel.socks5_listen_port,
                    rx_bytes=counters.rx_bytes,
                    tx_bytes=counters.tx_bytes,
                    rx_rate_bytes_per_second=rx_rate,
                    tx_rate_bytes_per_second=tx_rate,
                    latest_handshake_at=counters.latest_handshake_at,
                    counter_reset=counter_reset,
                    sampled_at=sampled_at,
                )
            )
        return rows
