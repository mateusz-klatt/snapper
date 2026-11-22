"""Tests for event conversions whose evidence came only from mark requirements."""

from collections.abc import Mapping
from datetime import datetime

import pytest

from snapper.application.portfolio.pnl_timeline import TimelineExecution
from snapper.application.portfolio.pnl_timeline import TimelineWindow
from snapper.application.portfolio.pnl_timeline import build_pnl_timeline
from snapper.application.portfolio.pnl_timeline_service import (
    _accrual_with_conversion_mark_evidence,
)
from snapper.application.portfolio.pnl_timeline_service import (
    _execution_with_conversion_mark_evidence,
)
from snapper.application.portfolio.pnl_timeline_service import _FxInstrumentRequirements
from snapper.application.portfolio.pnl_timeline_service import _to_timeline_accrual
from snapper.application.portfolio.pnl_timeline_service import _to_timeline_execution
from snapper.data.repository_types import PnlTimelineOpeningExecutionRow
from tests.application.portfolio import test_pnl_opening_checkpoint as checkpoint_fixtures
from tests.application.portfolio import test_pnl_timeline_service as fixtures

_PAIR = ("EUR", "USD")


def _window(*, capture: bool) -> TimelineWindow:
    """Build one exact recovery window with optional engine capture.

    Args:
        capture: Whether the engine should attempt its closing checkpoint.

    Returns:
        A pointwise-recovery timeline window ending after the test event.
    """
    return TimelineWindow(
        from_time=fixtures._m(0),
        to_time=fixtures._m(5),
        granularity="1m",
        valuation_ccy="USD",
        recovery_budget_per_point=True,
        capture_checkpoint=capture,
    )


def _requirements(minute: datetime | None) -> _FxInstrumentRequirements:
    """Build an event requirement set optionally containing the test conversion.

    Args:
        minute: Event rate minute to include, or ``None`` for a mark-only read.

    Returns:
        Exact event requirements grouped by instrument and currency pair.
    """
    if minute is None:
        return {}
    return {fixtures._I1: {_PAIR: {minute}}}


def _converted_execution(
    role: str,
    rates: Mapping[tuple[str, str, str, datetime], float],
) -> tuple[TimelineExecution, PnlTimelineOpeningExecutionRow]:
    """Convert one execution whose fee or price uses foreign currency.

    Args:
        role: Either ``"fee"`` or ``"price"``.
        rates: Exact FX rate map available to the converter.

    Returns:
        The converted engine execution and its source repository row.
    """
    row = fixtures._exec_row(
        fixtures._I1,
        1,
        3,
        "buy",
        1.0,
        100.0,
        2.0 if role == "fee" else 0.0,
        "EUR" if role == "fee" else "USD",
    )
    converted = _to_timeline_execution(
        row,
        "USD",
        rates,
        base_asset="BTC",
        price_currency="EUR" if role == "price" else "USD",
    )
    return converted, row


def _flagged_execution(
    role: str,
    rates: Mapping[tuple[str, str, str, datetime], float],
    requirements: _FxInstrumentRequirements,
) -> TimelineExecution:
    """Convert and stamp one execution against the event requirement set.

    Args:
        role: Either ``"fee"`` or ``"price"``.
        rates: Exact FX rate map available to the converter.
        requirements: Event-produced requirements for the same series.

    Returns:
        The converted execution with its mark-evidence dependency flag.
    """
    converted, row = _converted_execution(role, rates)
    return _execution_with_conversion_mark_evidence(
        converted,
        row,
        "EUR" if role == "price" else "USD",
        requirements,
        "USD",
    )


@pytest.mark.parametrize("role", ["fee", "price"])
@pytest.mark.parametrize("rate_available", [False, True])
def test_execution_mark_only_conversion_refuses_engine_capture(
    role: str,
    rate_available: bool,
) -> None:
    """Flag successful and failed execution conversions absent from event requirements.

    Given: A fee or price conversion available only through a mark-pinned minute.
    When: The converted row is stamped and the engine attempts capture.
    Then: The flag is true and capture refuses with ``conversion_uses_mark_evidence``.

    Args:
        role: Execution conversion role being tested.
        rate_available: Whether the conversion has a usable rate or fails.
    """
    minute = fixtures._m(3)
    rates = {("EUR", "USD", "kraken", minute): 2.0} if rate_available else {}
    flagged = _flagged_execution(role, rates, _requirements(None))
    unflagged = _flagged_execution(role, rates, _requirements(minute))
    assert flagged.conversion_uses_mark_evidence
    assert not unflagged.conversion_uses_mark_evidence
    captured = build_pnl_timeline((flagged,), (), {}, _window(capture=True))
    assert captured.closing_checkpoint is None
    assert captured.closing_checkpoint_refusal == "conversion_uses_mark_evidence"
    default_flagged = build_pnl_timeline((flagged,), (), {}, _window(capture=False))
    default_unflagged = build_pnl_timeline((unflagged,), (), {}, _window(capture=False))
    checkpoint_fixtures._assert_points_bit_identical(
        default_unflagged.points,
        default_flagged.points,
    )


def test_execution_event_requirement_allows_engine_capture() -> None:
    """Leave an event-requested conversion eligible for capture.

    Given: A successful foreign fee conversion present in event requirements.
    When: The converted row is stamped and the engine attempts capture.
    Then: The flag remains false and the engine produces a closing checkpoint.
    """
    minute = fixtures._m(3)
    converted = _flagged_execution(
        "fee",
        {("EUR", "USD", "kraken", minute): 2.0},
        _requirements(minute),
    )
    captured = build_pnl_timeline((converted,), (), {}, _window(capture=True))
    assert not converted.conversion_uses_mark_evidence
    assert captured.closing_checkpoint is not None
    assert captured.closing_checkpoint_refusal is None


@pytest.mark.parametrize("rate_available", [False, True])
def test_accrual_mark_only_conversion_refuses_engine_capture(rate_available: bool) -> None:
    """Flag accrual conversions absent from event requirements whether they succeed or fail.

    Given: An accrual conversion available only through a mark-pinned minute.
    When: The converted row is stamped and the engine attempts capture.
    Then: The flag is true, capture refuses, and non-capture points are unchanged.

    Args:
        rate_available: Whether the conversion has a usable rate or fails.
    """
    minute = fixtures._m(3)
    rates = {("EUR", "USD", "kraken", minute): 2.0} if rate_available else {}
    row = fixtures._accrual_row(fixtures._I1, 3, 2.0, "EUR")
    converted = _to_timeline_accrual(row, "USD", rates)
    flagged = _accrual_with_conversion_mark_evidence(
        converted,
        row,
        _requirements(None),
        "USD",
    )
    unflagged = _accrual_with_conversion_mark_evidence(
        converted,
        row,
        _requirements(minute),
        "USD",
    )
    assert flagged.conversion_uses_mark_evidence
    assert not unflagged.conversion_uses_mark_evidence
    captured = build_pnl_timeline((), (flagged,), {}, _window(capture=True))
    assert captured.closing_checkpoint is None
    assert captured.closing_checkpoint_refusal == "conversion_uses_mark_evidence"
    default_flagged = build_pnl_timeline((), (flagged,), {}, _window(capture=False))
    default_unflagged = build_pnl_timeline((), (unflagged,), {}, _window(capture=False))
    checkpoint_fixtures._assert_points_bit_identical(
        default_unflagged.points,
        default_flagged.points,
    )


def test_native_and_zero_values_do_not_use_mark_evidence() -> None:
    """Do not flag identity or exact-zero values that need no foreign conversion.

    Given: A native-currency execution and accrual with empty event requirements.
    When: Their converted rows are stamped.
    Then: Both mark-evidence flags remain false.
    """
    execution_row = fixtures._exec_row(
        fixtures._I1,
        1,
        3,
        "buy",
        1.0,
        100.0,
        0.0,
        "USD",
    )
    execution = _to_timeline_execution(
        execution_row,
        "USD",
        {},
        base_asset="BTC",
        price_currency="USD",
    )
    accrual_row = fixtures._accrual_row(fixtures._I1, 3, 0.0, "EUR")
    accrual = _to_timeline_accrual(accrual_row, "USD", {})
    assert not _execution_with_conversion_mark_evidence(
        execution,
        execution_row,
        "USD",
        {},
        "USD",
    ).conversion_uses_mark_evidence
    assert not _accrual_with_conversion_mark_evidence(
        accrual,
        accrual_row,
        {},
        "USD",
    ).conversion_uses_mark_evidence
