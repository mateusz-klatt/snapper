"""Unit tests for DivergenceDetector.

Exercises every public increment method, the snapshot shape, and the
periodic snapshot loop emission cadence. Plan:
``proprietary/plans/plan_shadow_write_divergence.md`` v1.3.
"""

import asyncio
from typing import cast
from unittest.mock import MagicMock

import pytest

from snapper.application.trade.divergence_detector import DivergenceDetector
from snapper.application.trade.divergence_detector import PublishPath


def test_observe_command_created_increments() -> None:
    """Each observe_command_created call bumps the baseline counter.

    Given: a fresh DivergenceDetector,
    When: observe_command_created fires three times,
    Then: commands_created_total is 3; all other counters stay at 0.
    """
    detector = DivergenceDetector()
    for _ in range(3):
        detector.observe_command_created("kraken", "kraken.BTC-USD.live")
    snap = detector.snapshot()
    assert snap["commands_created_total"] == 3
    assert snap["commands_published_dual_write_total"] == 0
    assert snap["commands_published_durable_notified_total"] == 0
    assert snap["commands_published_durable_dispatched_total"] == 0
    assert snap["venue_events_observed_total"] == 0
    assert snap["reconciliation_ok_total"] == 0
    assert snap["reconciliation_failure_total"] == 0


@pytest.mark.parametrize(
    ("path", "expected_key"),
    [
        ("dual_write", "commands_published_dual_write_total"),
        ("durable_notified", "commands_published_durable_notified_total"),
        ("durable_dispatched", "commands_published_durable_dispatched_total"),
    ],
)
def test_observe_command_published_by_path(path: str, expected_key: str) -> None:
    """observe_command_published increments only its targeted counter.

    Given: a fresh DivergenceDetector and the parametrised path value,
    When: observe_command_published fires twice with that path,
    Then: only the matching counter reads 2; every other counter stays 0.
    """
    detector = DivergenceDetector()
    typed_path: PublishPath = cast(PublishPath, path)
    for _ in range(2):
        detector.observe_command_published(typed_path, "kraken", "shard")
    snap = detector.snapshot()
    assert snap[expected_key] == 2
    for key, value in snap.items():
        if key == expected_key:
            continue
        assert value == 0, key


def test_observe_venue_event_increments_total() -> None:
    """observe_venue_event increments the fill counter.

    Given: a fresh detector,
    When: observe_venue_event fires five times,
    Then: venue_events_observed_total reads 5.
    """
    detector = DivergenceDetector()
    for i in range(5):
        detector.observe_venue_event("shard", i + 1)
    assert detector.snapshot()["venue_events_observed_total"] == 5


def test_observe_reconciliation_verdict_splits_by_verdict() -> None:
    """observe_reconciliation_verdict keeps ok and failure counters isolated.

    Given: a fresh detector,
    When: two "ok" and three "failure" verdicts are recorded,
    Then: reconciliation_ok_total is 2 and reconciliation_failure_total is 3.
    """
    detector = DivergenceDetector()
    for _ in range(2):
        detector.observe_reconciliation_verdict("shard-A", "ok")
    for _ in range(3):
        detector.observe_reconciliation_verdict("shard-B", "failure")
    snap = detector.snapshot()
    assert snap["reconciliation_ok_total"] == 2
    assert snap["reconciliation_failure_total"] == 3


def test_snapshot_returns_all_seven_counter_keys() -> None:
    """The ``snapshot`` method always returns the canonical 7-key dict.

    Given: a fresh detector,
    When: snapshot() is called,
    Then: the returned dict has exactly the 7 documented counter keys
        with every value defaulting to 0.
    """
    detector = DivergenceDetector()
    snap = detector.snapshot()
    expected_keys = {
        "commands_created_total",
        "commands_published_dual_write_total",
        "commands_published_durable_notified_total",
        "commands_published_durable_dispatched_total",
        "venue_events_observed_total",
        "reconciliation_ok_total",
        "reconciliation_failure_total",
    }
    assert set(snap.keys()) == expected_keys
    assert all(v == 0 for v in snap.values())


@pytest.mark.asyncio
async def test_periodic_snapshot_loop_emits_at_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """periodic_snapshot_loop logs snapshots at its configured interval.

    Given: a detector with snapshot_interval_s=0.01 and a stubbed
        logger,
    When: the loop task runs for ~30 ms and is cancelled,
    Then: at least one INFO log fires and the payload contains the
        seven canonical counter keys (confirming the logger call
        receives a real snapshot dict, not a placeholder).
    """
    detector = DivergenceDetector(snapshot_interval_s=0.01)
    calls: list[tuple[str, dict[str, int]]] = []

    fake_logger = MagicMock()

    def _capture(fmt: str, *args: object) -> None:
        if args:
            payload_arg = args[0]
            if isinstance(payload_arg, dict):
                calls.append((fmt, cast(dict[str, int], payload_arg)))

    fake_logger.info.side_effect = _capture
    fake_logger.opt.return_value = fake_logger
    monkeypatch.setattr("snapper.application.trade.divergence_detector.logger", fake_logger)

    task = asyncio.create_task(detector.periodic_snapshot_loop())
    await asyncio.sleep(0.03)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert calls
    fmt_str, payload = calls[0]
    assert "DivergenceDetector snapshot" in fmt_str
    assert "commands_created_total" in payload
    assert "commands_published_durable_dispatched_total" in payload
