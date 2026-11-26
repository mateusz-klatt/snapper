"""Tests for the read-only Kraken Spot trade recovery planner."""

from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from decimal import Decimal

import pytest

from snapper.application.updaters.historical.kraken_spot_trade_evidence import (
    KrakenSpotTradeEvidence,
)
from snapper.application.updaters.historical.kraken_spot_trade_reconciliation import (
    StoredKrakenSpotTrade,
)
from snapper.application.updaters.historical.kraken_spot_trade_reconciliation import (
    plan_kraken_spot_trade_recovery,
)

_BTC = "31a25729-27da-4c18-8101-78635da341af"
_ETH = "3f8d595a-5ac1-4a16-85b6-c324d996bf3e"
_EXECUTED_AT = datetime(2026, 11, 26, 18, 27, 0, 123456, tzinfo=UTC)
_EVIDENCE = KrakenSpotTradeEvidence(
    symbol="BTC/USD",
    instrument_public_id=_BTC,
    trade_id="118134708",
    executed_at=_EXECUTED_AT,
    price=Decimal("90000.10000000000000000001"),
    size=Decimal("0.001000000000000000000001"),
    side="buy",
)
_STORED = StoredKrakenSpotTrade(
    instrument_public_id=_BTC,
    trade_id="118134708",
    executed_at=_EXECUTED_AT,
    price=90000.1,
    size=0.001,
    side="buy",
)


def test_classify_by_instrument_and_venue_id() -> None:
    """Match the instrument and venue ID together.

    Given the same ID on two instruments and repeated unrelated rows,
    When only one instrument has a matching stored trade,
    Then the candidate for the other instrument remains absent.
    """
    same_id_on_ether = replace(_EVIDENCE, symbol="ETH/USD", instrument_public_id=_ETH)
    later_bitcoin = replace(_EVIDENCE, trade_id="118134709")
    source = (same_id_on_ether, _EVIDENCE, later_bitcoin)
    unrelated_row = replace(_STORED, trade_id="118134710", price=7.0)
    present_row = replace(
        _STORED, executed_at=_EXECUTED_AT.astimezone(timezone(timedelta(hours=2)))
    )

    plan = plan_kraken_spot_trade_recovery(
        source, iter((unrelated_row, present_row, unrelated_row))
    )

    assert plan.missing == (same_id_on_ether, later_bitcoin)
    assert plan.already_present == 1


def test_empty_baseline_reports_all_candidates_in_input_order() -> None:
    """Preserve candidate order for an empty baseline.

    Given two candidates supplied in reverse venue ID order,
    When the baseline is empty,
    Then both candidates are missing in their original order.
    """
    earlier_id = replace(_EVIDENCE, trade_id="118134707")
    source = (_EVIDENCE, earlier_id)

    plan = plan_kraken_spot_trade_recovery(source, ())

    assert plan.missing == source
    assert plan.already_present == 0


def test_repeated_source_identity_is_rejected() -> None:
    """Reject repeated source trade identities.

    Given two candidates with the same instrument and venue ID,
    When the candidates have different execution times,
    Then the planner rejects the duplicate source identity.
    """
    duplicate = replace(_EVIDENCE, executed_at=_EXECUTED_AT + timedelta(seconds=1))

    with pytest.raises(
        ValueError, match=r"Duplicate Kraken Spot evidence for BTC/USD trade 118134708"
    ):
        plan_kraken_spot_trade_recovery((_EVIDENCE, duplicate), ())


def test_repeated_identical_stored_row_is_rejected() -> None:
    """Reject identical matching stored rows.

    Given one candidate and two identical stored matches,
    When the planner receives both stored rows,
    Then it rejects the duplicate and returns no plan.
    """
    with pytest.raises(
        ValueError, match=r"Duplicate stored Kraken Spot trade for BTC/USD trade 118134708"
    ):
        plan_kraken_spot_trade_recovery((_EVIDENCE,), (_STORED, _STORED))


@pytest.mark.parametrize(
    "row",
    (
        replace(_STORED, executed_at=_EXECUTED_AT + timedelta(microseconds=1)),
        replace(_STORED, executed_at=_EXECUTED_AT.replace(tzinfo=None)),
        replace(_STORED, price=90000.2),
        replace(_STORED, size=0.002),
        replace(_STORED, side="sell"),
    ),
    ids=("different-time", "naive-time", "price", "size", "side"),
)
def test_conflicting_baseline_row_is_rejected(row: StoredKrakenSpotTrade) -> None:
    """Reject conflicting stored values.

    Given a candidate and a stored row with the same venue identity,
    When the row differs in execution time, price, size, or side,
    Then the planner rejects the conflicting row.
    """
    with pytest.raises(
        ValueError, match=r"Conflicting stored Kraken Spot trade for BTC/USD trade 118134708"
    ):
        plan_kraken_spot_trade_recovery((_EVIDENCE,), (row,))


def test_additional_row_with_different_time_is_rejected() -> None:
    """Reject a conflicting stored timestamp after an exact match.

    Given two stored rows for the same candidate identity,
    When the row after an exact match has another execution time,
    Then the planner rejects the conflicting row.
    """
    conflicting_row = replace(_STORED, executed_at=_EXECUTED_AT + timedelta(hours=1))

    with pytest.raises(
        ValueError, match=r"Conflicting stored Kraken Spot trade for BTC/USD trade 118134708"
    ):
        plan_kraken_spot_trade_recovery((_EVIDENCE,), (_STORED, conflicting_row))
