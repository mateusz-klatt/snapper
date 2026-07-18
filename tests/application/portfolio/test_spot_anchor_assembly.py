"""Tests for the pure raw-read to witness/bootstrap mappers."""

import json
from decimal import Decimal

from snapper.application.portfolio.spot_anchor_assembly import build_witnesses_from_reads
from snapper.application.portfolio.spot_anchor_assembly import parse_observed_balances
from snapper.application.portfolio.spot_anchor_assembly import venue_history_tip_from_raw
from snapper.application.portfolio.spot_anchor_bootstrap import VenueHistoryItem
from snapper.infrastructure.exchanges.contracts import VenueAccountHistoryItem
from snapper.infrastructure.exchanges.contracts import VenueAccountHistoryTip
from snapper.infrastructure.exchanges.contracts import VenueOrderFillLegs


def _item(
    item_id: int,
    operation_type: str,
    amount: str,
    currency: str,
    *,
    order_id: str | None = "O1",
    transaction_id: str | None = "T1",
    ordered_by: str = "API/key",
    balance_after: str = "0",
) -> VenueAccountHistoryItem:
    """Build one raw account-history item for a mapper scenario."""
    return VenueAccountHistoryItem(
        item_id=item_id,
        operation_type=operation_type,
        operation_amount=Decimal(amount),
        balance_after=Decimal(balance_after),
        currency=currency,
        transaction_id=transaction_id,
        ordered_by=ordered_by,
        order_id=order_id,
    )


def _buy_totals() -> dict[str, VenueOrderFillLegs]:
    """Return the canonical BUY EURPLN order totals keyed by order id."""
    return {
        "O1": VenueOrderFillLegs(
            order_id="O1",
            bought_amount=Decimal("10"),
            sold_amount=Decimal("50"),
            commission_amount=Decimal("0"),
            bought_currency="EUR",
            sold_currency="PLN",
            commission_currency="EUR",
            buy_sell="BUY",
        )
    }


def test_build_witnesses_from_reads_composes_the_map() -> None:
    """Given: A two-fill BUY tip page, its exec-id prefix, and order totals.

    When: The witnesses are composed from the raw reads,
    Then: Each execution maps to its fill's MARKET_FX legs.
    """
    tip = VenueAccountHistoryTip(
        item_id=102,
        items=(
            _item(102, "MARKET_FX", "4", "EUR", transaction_id="T2"),
            _item(101, "MARKET_FX", "-20", "PLN", transaction_id="T2"),
            _item(100, "MARKET_FX", "6", "EUR", transaction_id="T1"),
            _item(99, "MARKET_FX", "-30", "PLN", transaction_id="T1"),
        ),
    )
    outcome = build_witnesses_from_reads(
        tip,
        [(1, "O1", 600_000_000, False), (2, "O1", 1_000_000_000, False)],
        _buy_totals(),
    )
    assert outcome.refusals == ()
    assert outcome.witnesses == {1: frozenset({100, 99}), 2: frozenset({102, 101})}


def test_build_witnesses_from_reads_passes_refusals_through() -> None:
    """Given: An order whose legs do not sum to its reported total.

    When: The witnesses are composed,
    Then: The witness builder's refusal surfaces and the map is empty.
    """
    tip = VenueAccountHistoryTip(
        item_id=100,
        items=(
            _item(100, "MARKET_FX", "6", "EUR", transaction_id="T1"),
            _item(99, "MARKET_FX", "-30", "PLN", transaction_id="T1"),
        ),
    )
    outcome = build_witnesses_from_reads(tip, [(1, "O1", 600_000_000, False)], _buy_totals())
    assert outcome.witnesses == {}
    assert outcome.refusals == ("order_base_legs_disagree_with_total",)


def test_venue_history_tip_from_raw_normalizes_markers() -> None:
    """Given: A tip mixing a MARKET_FX our-key row, a fee row, and a manual row.

    When: The tip is normalized,
    Then: Each item's MARKET_FX marker and API attribution project faithfully and
        the descending page order is preserved.
    """
    tip = VenueAccountHistoryTip(
        item_id=100,
        items=(
            _item(100, "MARKET_FX", "6", "EUR", balance_after="106"),
            _item(99, "COMMISSION", "-0.02", "EUR", ordered_by="API/key"),
            _item(98, "MARKET_FX", "5", "EUR", ordered_by="GUI/user@example.com"),
        ),
    )
    normalized = venue_history_tip_from_raw(tip)
    assert normalized.item_id == 100
    assert normalized.page[0] == VenueHistoryItem(
        item_id=100,
        is_market_fx=True,
        is_api_attributed=True,
        currency="EUR",
        balance_after=Decimal("106"),
    )
    assert normalized.page[1].is_market_fx is False
    assert normalized.page[2].is_api_attributed is False


def test_parse_observed_balances_venue_raw() -> None:
    """Given: A venue-raw balances payload with exact decimal mirrors.

    When: It is parsed,
    Then: Totals and reserved amounts are exact and the read is venue-raw.
    """
    payload = json.dumps(
        [
            {
                "currency": "BTC",
                "total": 1.5,
                "free": 1.0,
                "used": 0.5,
                "total_decimal": "1.50000000",
                "free_decimal": "1.0",
                "used_decimal": "0.50000000",
                "numeric_provenance": "venue_raw",
            }
        ]
    )
    balances, reserved, venue_raw = parse_observed_balances(payload)
    assert balances == {"BTC": Decimal("1.50000000")}
    assert reserved == {"BTC": Decimal("0.50000000")}
    assert venue_raw is True


def test_parse_observed_balances_legacy_float_is_not_venue_raw() -> None:
    """Given: A balances entry without the exact decimal mirror.

    When: It is parsed,
    Then: The entry is skipped and the read is marked not-venue-raw.
    """
    payload = json.dumps([{"currency": "BTC", "total": 1.5, "free": 1.0, "used": 0.5}])
    balances, reserved, venue_raw = parse_observed_balances(payload)
    assert balances == {}
    assert reserved == {}
    assert venue_raw is False


def test_parse_observed_balances_empty_is_not_venue_raw() -> None:
    """Given: An empty balances payload.

    When: It is parsed,
    Then: The maps are empty and the read is not venue-raw.
    """
    balances, reserved, venue_raw = parse_observed_balances("[]")
    assert balances == {}
    assert reserved == {}
    assert venue_raw is False
