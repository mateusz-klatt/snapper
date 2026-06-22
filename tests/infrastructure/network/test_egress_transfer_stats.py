"""Tests for sidecar WireGuard transfer parsing and rate sampling."""

import subprocess
from collections.abc import Sequence
from datetime import UTC
from datetime import datetime

import pytest

from snapper.infrastructure.network import egress_transfer_stats as stats
from snapper.infrastructure.network.egress_transfer_stats import EgressTransferSampler
from snapper.infrastructure.network.egress_transfer_stats import EgressTransferTunnel
from snapper.infrastructure.network.egress_transfer_stats import WireGuardTransferCounters


class _FakeMonotonicClock:
    """Mutable monotonic clock for deterministic sampler tests."""

    def __init__(self, value: float) -> None:
        """Store the initial monotonic time."""
        self.value = value

    def __call__(self) -> float:
        """Return the current monotonic time."""
        return self.value


class _FakeWallClock:
    """Mutable wall clock for deterministic sampler tests."""

    def __init__(self, value: datetime) -> None:
        """Store the initial wall-clock time."""
        self.value = value

    def __call__(self) -> datetime:
        """Return the current wall-clock time."""
        return self.value


def _counters(
    interface: str,
    rx_bytes: int,
    tx_bytes: int,
    latest_handshake_at: datetime | None = None,
) -> WireGuardTransferCounters:
    """Build WireGuard counters for sampler tests."""
    return WireGuardTransferCounters(
        interface=interface,
        rx_bytes=rx_bytes,
        tx_bytes=tx_bytes,
        latest_handshake_at=latest_handshake_at,
    )


def test_parse_wg_dump_sums_peers_and_keeps_latest_handshake() -> None:
    """Spec — valid peer rows sum bytes and malformed rows are skipped.

    Given a WireGuard dump with two valid peers, empty lines, and bad rows,
    When the dump is parsed,
    Then transfer bytes are summed and the newest handshake is retained.
    """
    dump = "\n".join(
        [
            "wg-pl\tprivate\tpublic\t51820\toff",
            "peer-a\tpsk\t203.0.113.10:51820\t0.0.0.0/0\t1710000000\t100\t200\t25",
            "",
            "bad-peer\ttoo-short",
            "peer-b\tpsk\t203.0.113.11:51820\t0.0.0.0/0\t1710000030\t300\t400\t25",
            "peer-c\tpsk\t203.0.113.12:51820\t0.0.0.0/0\tbad\t5\t6\t25",
        ]
    )

    parsed = stats.parse_wg_dump("wg-pl", dump)

    assert parsed.interface == "wg-pl"
    assert parsed.rx_bytes == 400
    assert parsed.tx_bytes == 600
    assert parsed.latest_handshake_at == datetime.fromtimestamp(1710000030, tz=UTC)


def test_parse_wg_dump_returns_zeroes_for_empty_or_no_handshake_dump() -> None:
    """Spec — empty dumps and zero handshakes produce zero counters.

    Given dump output with no valid non-zero peer counters,
    When the parser runs,
    Then it returns a zero-valued sample with no handshake timestamp.
    """
    parsed_empty = stats.parse_wg_dump("wg-empty", "")
    parsed_zero = stats.parse_wg_dump(
        "wg-zero",
        "\n".join(
            [
                "wg-zero\tprivate\tpublic\t51820\toff",
                "peer\tpsk\t203.0.113.10:51820\t0.0.0.0/0\t0\t0\t0\t25",
            ]
        ),
    )

    assert parsed_empty.rx_bytes == 0
    assert parsed_empty.tx_bytes == 0
    assert parsed_empty.latest_handshake_at is None
    assert parsed_zero.rx_bytes == 0
    assert parsed_zero.tx_bytes == 0
    assert parsed_zero.latest_handshake_at is None


def test_read_wg_transfer_counters_parses_success_and_reports_missing() -> None:
    """Spec — command success parses stdout and command failure returns None.

    Given injected ``wg`` command results,
    When the reader is called,
    Then successful dumps parse and failed dumps represent a missing interface.
    """

    def _runner(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
        assert command == ("wg", "show", "wg-pl", "dump")
        return subprocess.CompletedProcess(
            args=list(command),
            returncode=0,
            stdout="wg-pl\tprivate\tpublic\t51820\toff\npeer\tpsk\tep\tips\t1\t2\t3\t25",
            stderr="",
        )

    def _failing_runner(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=list(command),
            returncode=1,
            stdout="",
            stderr="missing",
        )

    parsed = stats.read_wg_transfer_counters("wg-pl", runner=_runner)
    missing = stats.read_wg_transfer_counters("wg-pl", runner=_failing_runner)

    assert parsed is not None
    assert parsed.rx_bytes == 2
    assert parsed.tx_bytes == 3
    assert missing is None


def test_run_wg_dump_invokes_subprocess_with_safe_capture(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Spec — the default runner shells out without raising on non-zero status.

    Given a monkeypatched ``subprocess.run``,
    When the internal command wrapper runs,
    Then it passes the exact command with text capture and no check.
    """
    calls: list[tuple[list[str], bool, bool, bool]] = []

    def _fake_run(
        args: list[str],
        *,
        check: bool,
        capture_output: bool,
        text: bool,
    ) -> subprocess.CompletedProcess[str]:
        calls.append((args, check, capture_output, text))
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")

    monkeypatch.setattr(stats.subprocess, "run", _fake_run)

    result = stats._run_wg_dump(("wg", "show", "wg-pl", "dump"))

    assert result.returncode == 0
    assert calls == [(["wg", "show", "wg-pl", "dump"], False, True, True)]


def test_sampler_computes_rates_after_first_sample() -> None:
    """Spec — normal counter deltas produce byte-per-second rates.

    Given a sampler with one tunnel and increasing counters,
    When two samples are taken ten monotonic seconds apart,
    Then the second sample contains cumulative bytes and computed rates.
    """
    clock = _FakeMonotonicClock(100.0)
    wall = _FakeWallClock(datetime(2026, 6, 22, 10, 0, tzinfo=UTC))
    counters = [_counters("wg-pl", 100, 200), _counters("wg-pl", 160, 260)]

    def _reader(_interface: str) -> WireGuardTransferCounters | None:
        return counters.pop(0)

    sampler = EgressTransferSampler(
        tunnels=[EgressTransferTunnel(interface="wg-pl", socks5_listen_port=1084)],
        reader=_reader,
        monotonic_clock=clock,
        wall_clock=wall,
    )

    first = sampler.sample()[0]
    clock.value = 110.0
    second = sampler.sample()[0]

    assert first.rx_rate_bytes_per_second is None
    assert first.tx_rate_bytes_per_second is None
    assert first.counter_reset is True
    assert second.rx_bytes == 160
    assert second.tx_bytes == 260
    assert second.rx_rate_bytes_per_second == 6.0
    assert second.tx_rate_bytes_per_second == 6.0
    assert second.counter_reset is False


def test_sampler_marks_counter_decrease_and_non_positive_delta_as_reset() -> None:
    """Spec — unreliable deltas emit cumulative bytes with null rates.

    Given previous counters exist,
    When the next counter decreases or elapsed time is not positive,
    Then rates are omitted and counter_reset remains true.
    """
    clock = _FakeMonotonicClock(100.0)
    wall = _FakeWallClock(datetime(2026, 6, 22, 10, 0, tzinfo=UTC))
    counters = [
        _counters("wg-pl", 100, 200),
        _counters("wg-pl", 90, 210),
        _counters("wg-pl", 95, 215),
    ]

    def _reader(_interface: str) -> WireGuardTransferCounters | None:
        return counters.pop(0)

    sampler = EgressTransferSampler(
        tunnels=[EgressTransferTunnel(interface="wg-pl", socks5_listen_port=1084)],
        reader=_reader,
        monotonic_clock=clock,
        wall_clock=wall,
    )

    sampler.sample()
    clock.value = 110.0
    decreased = sampler.sample()[0]
    same_time = sampler.sample()[0]

    assert decreased.rx_bytes == 90
    assert decreased.rx_rate_bytes_per_second is None
    assert decreased.tx_rate_bytes_per_second is None
    assert decreased.counter_reset is True
    assert same_time.rx_bytes == 95
    assert same_time.rx_rate_bytes_per_second is None
    assert same_time.tx_rate_bytes_per_second is None
    assert same_time.counter_reset is True


def test_sampler_emits_reset_row_for_missing_interface() -> None:
    """Spec — a missing interface produces a reset sample and clears history.

    Given a sampler with one tunnel and a reader returning ``None``,
    When a sample is taken,
    Then the route still receives cumulative zeroes with null rates.
    """
    clock = _FakeMonotonicClock(100.0)
    sampled_at = datetime(2026, 6, 22, 10, 0, tzinfo=UTC)
    wall = _FakeWallClock(sampled_at)

    def _reader(_interface: str) -> WireGuardTransferCounters | None:
        return None

    sampler = EgressTransferSampler(
        tunnels=[EgressTransferTunnel(interface="wg-pl", socks5_listen_port=1084)],
        reader=_reader,
        monotonic_clock=clock,
        wall_clock=wall,
    )

    row = sampler.sample()[0]

    assert row.interface == "wg-pl"
    assert row.socks5_listen_port == 1084
    assert row.rx_bytes == 0
    assert row.tx_bytes == 0
    assert row.rx_rate_bytes_per_second is None
    assert row.tx_rate_bytes_per_second is None
    assert row.latest_handshake_at is None
    assert row.counter_reset is True
    assert row.sampled_at == sampled_at


def test_sampler_returns_empty_list_without_tunnels() -> None:
    """Spec — no configured tunnels publish no transfer rows.

    Given a sampler with no configured tunnels,
    When a sample is taken,
    Then no transfer rows are returned.
    """
    sampler = EgressTransferSampler(tunnels=[])

    assert sampler.sample() == []
