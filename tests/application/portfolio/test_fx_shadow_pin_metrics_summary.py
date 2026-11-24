"""Periodic INFO summary of the process-local FX shadow pin counters."""

import logging

import pytest

from snapper.application.portfolio import fx_conversion_shadow
from snapper.application.portfolio.fx_conversion_shadow import FX_SHADOW_PIN_SUMMARY_EVERY_TICKS
from snapper.application.portfolio.fx_conversion_shadow import FxShadowPinMetrics
from snapper.application.portfolio.fx_conversion_shadow import advance_fx_shadow_pin_summary_tick
from snapper.application.portfolio.fx_conversion_shadow import format_fx_shadow_pin_metrics_summary
from snapper.application.portfolio.fx_conversion_shadow import reset_fx_shadow_pin_metrics


def test_summary_line_lists_every_counter_in_a_fixed_order() -> None:
    """Format all seven counters on one deterministic line.

    Given: A metrics snapshot with a distinct value per counter.
    When: The summary line is formatted.
    Then: Every counter appears once, in declaration order, after a stable prefix.
    """
    metrics = FxShadowPinMetrics(
        creation=1,
        reuse=2,
        conflict=3,
        upgrade_required=4,
        mismatch=5,
        failure=6,
        dropped=7,
    )

    assert format_fx_shadow_pin_metrics_summary(metrics) == (
        "fx_shadow_pin_metrics_summary creation=1 reuse=2 conflict=3 "
        "upgrade_required=4 mismatch=5 failure=6 dropped=7"
    )


def test_summary_tick_logs_the_cumulative_counters_every_period(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Emit one INFO line at each full period and none in between.

    Given: Reset counters, then two failed flushes that dropped three evaluations.
    When: The tick helper runs through two full periods.
    Then: Only ticks N and 2N log, each with the cumulative counters at that moment.

    Args:
        caplog: Capture the summary lines.
    """
    reset_fx_shadow_pin_metrics()
    assert FX_SHADOW_PIN_SUMMARY_EVERY_TICKS == 10
    test_logger = logging.getLogger("test.fx_shadow_pin.summary")

    with caplog.at_level(logging.INFO, logger=test_logger.name):
        tick = 0
        for _ in range(FX_SHADOW_PIN_SUMMARY_EVERY_TICKS - 1):
            tick = advance_fx_shadow_pin_summary_tick(tick, test_logger)
        assert caplog.records == []

        tick = advance_fx_shadow_pin_summary_tick(tick, test_logger)
        assert tick == FX_SHADOW_PIN_SUMMARY_EVERY_TICKS
        fx_conversion_shadow._METRICS.failure += 2
        fx_conversion_shadow._METRICS.dropped += 3
        for _ in range(FX_SHADOW_PIN_SUMMARY_EVERY_TICKS):
            tick = advance_fx_shadow_pin_summary_tick(tick, test_logger)

    reset_fx_shadow_pin_metrics()
    assert tick == 2 * FX_SHADOW_PIN_SUMMARY_EVERY_TICKS
    assert [record.levelname for record in caplog.records] == ["INFO", "INFO"]
    assert [record.getMessage() for record in caplog.records] == [
        format_fx_shadow_pin_metrics_summary(FxShadowPinMetrics()),
        format_fx_shadow_pin_metrics_summary(FxShadowPinMetrics(failure=2, dropped=3)),
    ]
