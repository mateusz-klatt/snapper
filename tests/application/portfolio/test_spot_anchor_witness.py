"""Tests for the pure fill-identity witness builder.

Every refusal is proven with a scenario that constructs the exact failing
composition (a dropped venue leg, a mis-aligned cumulative, a total mismatch, an
un-ingested fill), and the happy paths prove the partition handles multi-partial
fills, poll snapshots spanning several fills, sell orders, and terminal
order-close emissions that share their predecessor's legs.
"""

from decimal import Decimal

from snapper.application.portfolio.spot_anchor_witness import OwnedWitnessScope
from snapper.application.portfolio.spot_anchor_witness import WitnessExecution
from snapper.application.portfolio.spot_anchor_witness import WitnessHistoryLeg
from snapper.application.portfolio.spot_anchor_witness import WitnessOrderTotals
from snapper.application.portfolio.spot_anchor_witness import build_execution_witnesses
from snapper.application.portfolio.spot_anchor_witness import build_owned_execution_witnesses
from snapper.application.portfolio.spot_anchor_witness import decode_execution_cumulative


def _leg(
    item_id: int,
    currency: str,
    amount: str,
    *,
    order_id: str | None = "O1",
    transaction_id: str | None = "T1",
    is_api_attributed: bool = True,
) -> WitnessHistoryLeg:
    """Build one MARKET_FX history leg for a test scenario."""
    return WitnessHistoryLeg(
        item_id=item_id,
        order_id=order_id,
        transaction_id=transaction_id,
        currency=currency,
        amount=Decimal(amount),
        is_api_attributed=is_api_attributed,
    )


def _execution(
    scope_sequence: int,
    cumulative: str,
    *,
    venue_order_id: str = "O1",
    is_terminal: bool = False,
) -> WitnessExecution:
    """Build one sealed-prefix execution for a test scenario."""
    return WitnessExecution(
        scope_sequence=scope_sequence,
        venue_order_id=venue_order_id,
        cumulative=Decimal(cumulative),
        is_terminal=is_terminal,
    )


def _totals(
    *,
    bought: str = "10",
    sold: str = "50",
    bought_currency: str = "EUR",
    sold_currency: str = "PLN",
    is_buy: bool = True,
    commission: str = "0",
) -> WitnessOrderTotals:
    """Build one order's market_fx/orders cumulative totals for a test scenario."""
    return WitnessOrderTotals(
        bought_amount=Decimal(bought),
        sold_amount=Decimal(sold),
        bought_currency=bought_currency,
        sold_currency=sold_currency,
        is_buy=is_buy,
        commission_amount=Decimal(commission),
    )


def _two_fill_buy() -> tuple[list[WitnessHistoryLeg], dict[str, WitnessOrderTotals]]:
    """Return the canonical two-partial-fill BUY EURPLN scenario legs and totals."""
    legs = [
        _leg(100, "EUR", "6", transaction_id="T1"),
        _leg(99, "PLN", "-30", transaction_id="T1"),
        _leg(102, "EUR", "4", transaction_id="T2"),
        _leg(101, "PLN", "-20", transaction_id="T2"),
    ]
    return legs, {"O1": _totals(bought="10", sold="50")}


def test_single_fill_order_maps_the_one_fill() -> None:
    """Given: A BUY order with one fill and one execution at its cumulative.

    When: The witness map is built,
    Then: The execution witnesses both currency legs of the fill.
    """
    legs = [_leg(100, "EUR", "6"), _leg(99, "PLN", "-30")]
    outcome = build_execution_witnesses(
        [_execution(1, "6")], legs, {"O1": _totals(bought="6", sold="30")}, 100, {}
    )
    assert outcome.refusals == ()
    assert outcome.witnesses == {1: frozenset({100, 99})}


def test_two_partial_fills_partition_by_cumulative() -> None:
    """Given: A BUY order with two partial fills and two executions.

    When: The witness map is built,
    Then: Each execution witnesses exactly its own fill's legs.
    """
    legs, totals = _two_fill_buy()
    outcome = build_execution_witnesses(
        [_execution(1, "6"), _execution(2, "10")], legs, totals, 102, {}
    )
    assert outcome.refusals == ()
    assert outcome.witnesses == {1: frozenset({100, 99}), 2: frozenset({102, 101})}


def test_skipped_fill_execution_sweeps_every_fill_in_its_interval() -> None:
    """Given: One execution whose poll snapshot spans two venue fills.

    When: The witness map is built,
    Then: The single execution witnesses both fills' legs (Gemini R1).
    """
    legs, totals = _two_fill_buy()
    outcome = build_execution_witnesses([_execution(1, "10")], legs, totals, 102, {})
    assert outcome.refusals == ()
    assert outcome.witnesses == {1: frozenset({100, 99, 102, 101})}


def test_sell_order_uses_the_sold_currency_as_base() -> None:
    """Given: A SELL EURPLN order whose base cumulative is the sold EUR leg.

    When: The witness map is built,
    Then: The negative EUR leg is the base volume and the fill maps.
    """
    legs = [_leg(100, "EUR", "-6"), _leg(99, "PLN", "30")]
    totals = {
        "O1": _totals(
            bought="30", sold="6", bought_currency="PLN", sold_currency="EUR", is_buy=False
        )
    }
    outcome = build_execution_witnesses([_execution(1, "6")], legs, totals, 100, {})
    assert outcome.refusals == ()
    assert outcome.witnesses == {1: frozenset({100, 99})}


def test_terminal_execution_shares_predecessor_legs() -> None:
    """Given: An order that fills once then closes with a zero-delta terminal.

    When: The witness map is built,
    Then: The terminal execution shares its predecessor's legs (Gemini finding 2).
    """
    legs = [_leg(100, "EUR", "6"), _leg(99, "PLN", "-30")]
    outcome = build_execution_witnesses(
        [_execution(1, "6"), _execution(2, "6", is_terminal=True)],
        legs,
        {"O1": _totals(bought="6", sold="30")},
        100,
        {},
    )
    assert outcome.refusals == ()
    assert outcome.witnesses == {1: frozenset({100, 99}), 2: frozenset({100, 99})}


def test_dropped_leg_makes_the_cumulative_miss_a_fill_boundary() -> None:
    """Given: An execution cumulative that lands between fill boundaries.

    When: The witness map is built,
    Then: The cumulative is refused as off a fill boundary (Gemini finding 1).
    """
    legs, totals = _two_fill_buy()
    outcome = build_execution_witnesses(
        [_execution(1, "7"), _execution(2, "10")], legs, totals, 102, {}
    )
    assert outcome.witnesses == {}
    assert outcome.refusals == ("execution_cumulative_off_fill_boundary",)


def test_order_without_totals_is_refused() -> None:
    """Given: An executed order with no market_fx/orders totals.

    When: The witness map is built,
    Then: The missing totals are refused.
    """
    legs = [_leg(100, "EUR", "6"), _leg(99, "PLN", "-30")]
    outcome = build_execution_witnesses([_execution(1, "6")], legs, {}, 100, {})
    assert outcome.witnesses == {}
    assert outcome.refusals == ("execution_order_totals_missing",)


def test_unidentified_attributed_leg_is_refused() -> None:
    """Given: An attributed MARKET_FX leg with no transaction id.

    When: The witness map is built,
    Then: The unidentified leg is refused.
    """
    legs = [_leg(100, "EUR", "6", transaction_id=None)]
    outcome = build_execution_witnesses([], legs, {}, 100, {})
    assert outcome.witnesses == {}
    assert outcome.refusals == ("history_leg_unidentified",)


def test_fill_missing_its_base_leg_is_refused() -> None:
    """Given: A fill whose only leg is the quote currency (no base leg).

    When: The witness map is built,
    Then: The incomplete fill is refused.
    """
    legs = [_leg(100, "PLN", "-30")]
    outcome = build_execution_witnesses(
        [_execution(1, "6")], legs, {"O1": _totals(bought="6", sold="30")}, 100, {}
    )
    assert outcome.witnesses == {}
    assert outcome.refusals == ("history_fill_leg_incomplete",)


def test_base_legs_not_summing_to_the_total_is_refused() -> None:
    """Given: Base legs that sum to less than the order's reported total.

    When: The witness map is built,
    Then: The disagreement with the total is refused (Gemini finding 1).
    """
    legs = [_leg(100, "EUR", "6"), _leg(99, "PLN", "-30")]
    outcome = build_execution_witnesses(
        [_execution(1, "6")], legs, {"O1": _totals(bought="12", sold="60")}, 100, {}
    )
    assert outcome.witnesses == {}
    assert outcome.refusals == ("order_base_legs_disagree_with_total",)


def test_fill_beyond_the_last_execution_is_unassigned() -> None:
    """Given: An order whose second fill has no ingested execution.

    When: The witness map is built,
    Then: The un-ingested fill is refused as unassigned.
    """
    legs, totals = _two_fill_buy()
    outcome = build_execution_witnesses([_execution(1, "6")], legs, totals, 102, {})
    assert outcome.witnesses == {}
    assert outcome.refusals == ("unassigned_venue_fill",)


def test_two_non_terminal_executions_at_one_cumulative_collide() -> None:
    """Given: Two non-terminal executions sharing one cumulative.

    When: The witness map is built,
    Then: The collision is refused (only a terminal may repeat a cumulative).
    """
    legs = [_leg(100, "EUR", "6"), _leg(99, "PLN", "-30")]
    outcome = build_execution_witnesses(
        [_execution(1, "6"), _execution(2, "6")],
        legs,
        {"O1": _totals(bought="6", sold="30")},
        100,
        {},
    )
    assert outcome.witnesses == {}
    assert outcome.refusals == ("non_terminal_cumulative_collision",)


def test_non_api_legs_are_excluded_from_the_attributed_set() -> None:
    """Given: A manual (non-API) MARKET_FX leg alongside our own fill.

    When: The witness map is built,
    Then: The manual leg is not in the attributed set and does not map.
    """
    legs = [
        _leg(100, "EUR", "6"),
        _leg(99, "PLN", "-30"),
        _leg(98, "EUR", "5", transaction_id="TX", is_api_attributed=False),
    ]
    outcome = build_execution_witnesses(
        [_execution(1, "6")], legs, {"O1": _totals(bought="6", sold="30")}, 100, {}
    )
    assert outcome.refusals == ()
    assert outcome.witnesses == {1: frozenset({100, 99})}


def test_legs_above_the_tip_are_excluded() -> None:
    """Given: A leg whose item id is above the sealed tip H0.

    When: The witness map is built,
    Then: The above-tip leg is excluded from the attributed set.
    """
    legs = [
        _leg(100, "EUR", "6"),
        _leg(99, "PLN", "-30"),
        _leg(150, "EUR", "9", transaction_id="TX"),
    ]
    outcome = build_execution_witnesses(
        [_execution(1, "6")], legs, {"O1": _totals(bought="6", sold="30")}, 100, {}
    )
    assert outcome.refusals == ()
    assert outcome.witnesses == {1: frozenset({100, 99})}


def test_all_reasons_are_returned_not_first_fail() -> None:
    """Given: One order missing totals and another with a mismatched total.

    When: The witness map is built,
    Then: Both distinct refusals are returned, alphabetically ordered.
    """
    legs = [
        _leg(100, "EUR", "6", order_id="O1", transaction_id="T1"),
        _leg(99, "PLN", "-30", order_id="O1", transaction_id="T1"),
        _leg(80, "EUR", "6", order_id="O2", transaction_id="T2"),
        _leg(79, "PLN", "-30", order_id="O2", transaction_id="T2"),
    ]
    outcome = build_execution_witnesses(
        [_execution(1, "6", venue_order_id="O1"), _execution(2, "6", venue_order_id="O2")],
        legs,
        {"O2": _totals(bought="12", sold="60")},
        100,
        {},
    )
    assert outcome.witnesses == {}
    assert outcome.refusals == (
        "execution_order_totals_missing",
        "order_base_legs_disagree_with_total",
    )


def test_no_executions_yields_an_empty_map() -> None:
    """Given: No sealed-prefix executions.

    When: The witness map is built,
    Then: The map is empty with no refusals.
    """
    outcome = build_execution_witnesses([], [], {}, 100, {})
    assert outcome.refusals == ()
    assert outcome.witnesses == {}


def test_decode_execution_cumulative_is_exact() -> None:
    """Given: A basis-unit cumulative from a venue exec id.

    When: It is decoded,
    Then: The result is the exact Decimal with no float artifact.
    """
    assert decode_execution_cumulative(700_000_000) == Decimal("7")
    assert decode_execution_cumulative(7_000_000) == Decimal("0.07")
    assert decode_execution_cumulative(1) == Decimal("0.00000001")


def test_fill_missing_its_quote_leg_is_refused() -> None:
    """Given: A fill whose quote leg is absent (in flight or page-cut).

    When: The witness map is built,
    Then: The one-legged fill is refused — certifying it would seal balances
        whose missing leg posts later as a post-cursor item replay re-applies.
    """
    legs = [_leg(100, "EUR", "6")]
    outcome = build_execution_witnesses(
        [_execution(1, "6")], legs, {"O1": _totals(bought="6", sold="30")}, 100, {}
    )
    assert outcome.witnesses == {}
    assert outcome.refusals == ("history_fill_leg_incomplete",)


def test_commission_sum_short_of_the_order_total_is_refused() -> None:
    """Given: An order whose reported commission exceeds the witnessed fee items.

    When: The witness map is built,
    Then: The in-flight fee is refused — the fee-leg completeness obligation is
        discharged by the exact commission-sum cross-check.
    """
    legs = [_leg(100, "EUR", "6"), _leg(99, "PLN", "-30")]
    outcome = build_execution_witnesses(
        [_execution(1, "6")],
        legs,
        {"O1": _totals(bought="6", sold="30", commission="0.02")},
        100,
        {},
    )
    assert outcome.witnesses == {}
    assert outcome.refusals == ("commission_sum_disagrees_with_order_total",)


def test_commission_sum_matching_the_order_total_certifies() -> None:
    """Given: An order whose witnessed COMMISSION items sum to its reported fee.

    When: The witness map is built,
    Then: The composition certifies with the fee evidence complete.
    """
    legs = [_leg(100, "EUR", "6"), _leg(99, "PLN", "-30")]
    outcome = build_execution_witnesses(
        [_execution(1, "6")],
        legs,
        {"O1": _totals(bought="6", sold="30", commission="0.02")},
        100,
        {"O1": Decimal("0.02")},
    )
    assert outcome.refusals == ()
    assert outcome.witnesses == {1: frozenset({100, 99})}


def _scope(
    *,
    owned: frozenset[str] = frozenset({"O1"}),
    authoritative: dict[int, str | None] | None = None,
    tip_item_id: int = 100,
) -> OwnedWitnessScope:
    """Build the bootstrap's ownership evidence for a witness scenario."""
    return OwnedWitnessScope(
        tip_item_id=tip_item_id,
        owned_order_ids=owned,
        authoritative_order_ids={1: "O1"} if authoritative is None else authoritative,
    )


def test_owned_entry_selects_legs_by_the_owned_order_set() -> None:
    """The bootstrap's legs are chosen by ``S``, never by ``orderedBy``.

    Given: Non-API legs of the owned order ``O1`` and an API leg of foreign
        order ``M1``, with one execution of ``O1``.
    When: The owned witness map is built,
    Then: The non-API owned legs are witnessed and the foreign API leg is not.
    """
    legs = [
        _leg(100, "EUR", "6", is_api_attributed=False),
        _leg(99, "PLN", "-30", is_api_attributed=False),
        _leg(98, "EUR", "5", order_id="M1", transaction_id="TX"),
    ]
    outcome = build_owned_execution_witnesses(
        [_execution(1, "6")], legs, {"O1": _totals(bought="6", sold="30")}, {}, _scope()
    )
    assert outcome.refusals == ()
    assert outcome.witnesses == {1: frozenset({100, 99})}


def test_certificate_entry_keeps_its_api_selection() -> None:
    """The certificate's entry point still witnesses only API-attributed legs.

    Given: The same non-API legs of order ``O1`` with one execution of ``O1``.
    When: The certificate's witness map is built,
    Then: The non-API legs are not witnessable, so the order's base legs
        disagree with its total exactly as before.
    """
    legs = [
        _leg(100, "EUR", "6", is_api_attributed=False),
        _leg(99, "PLN", "-30", is_api_attributed=False),
    ]
    outcome = build_execution_witnesses(
        [_execution(1, "6")], legs, {"O1": _totals(bought="6", sold="30")}, 100, {}
    )
    assert outcome.witnesses == {}
    assert outcome.refusals == ("order_base_legs_disagree_with_total",)


def test_owned_entry_excludes_legs_above_the_tip() -> None:
    """An owned leg above the seal tip is not part of the seal page.

    Given: The owned fill of ``O1`` plus a later owned leg above the tip.
    When: The owned witness map is built,
    Then: Only the at-or-below-tip fill is witnessed.
    """
    legs = [
        _leg(100, "EUR", "6"),
        _leg(99, "PLN", "-30"),
        _leg(150, "EUR", "9", transaction_id="TX"),
    ]
    outcome = build_owned_execution_witnesses(
        [_execution(1, "6")], legs, {"O1": _totals(bought="6", sold="30")}, {}, _scope()
    )
    assert outcome.refusals == ()
    assert outcome.witnesses == {1: frozenset({100, 99})}


def test_an_execution_decoding_another_owned_order_is_foreign() -> None:
    """An execution linked to Order A but decoding Order B is refused.

    Given: Both ``O1`` and ``O2`` in ``S``, and an execution whose exec id decodes
        ``O2`` while its Order's authoritative venue order id is ``O1``.
    When: The owned witness map is built,
    Then: ``execution_order_id_foreign`` is refused and no map is returned.
    """
    legs = [
        _leg(100, "EUR", "6", order_id="O2"),
        _leg(99, "PLN", "-30", order_id="O2"),
    ]
    outcome = build_owned_execution_witnesses(
        [_execution(1, "6", venue_order_id="O2")],
        legs,
        {"O2": _totals(bought="6", sold="30")},
        {},
        _scope(owned=frozenset({"O1", "O2"}), authoritative={1: "O1"}),
    )
    assert outcome.witnesses == {}
    assert outcome.refusals == ("execution_order_id_foreign",)


def test_an_execution_without_an_authoritative_order_is_foreign() -> None:
    """An execution whose Order does not resolve is refused.

    Given: An execution of ``O1`` whose Order resolved to no venue order id.
    When: The owned witness map is built,
    Then: ``execution_order_id_foreign`` is refused.
    """
    legs = [_leg(100, "EUR", "6"), _leg(99, "PLN", "-30")]
    outcome = build_owned_execution_witnesses(
        [_execution(1, "6")],
        legs,
        {"O1": _totals(bought="6", sold="30")},
        {},
        _scope(authoritative={1: None}),
    )
    assert outcome.refusals == ("execution_order_id_foreign",)


def test_an_execution_whose_authoritative_order_is_outside_s_is_foreign() -> None:
    """An execution matching its Order but outside ``S`` is refused.

    Given: An execution decoding ``O1`` whose Order's venue order id is ``O1``,
        while ``O1`` is not in ``S``.
    When: The owned witness map is built,
    Then: ``execution_order_id_foreign`` is refused.
    """
    outcome = build_owned_execution_witnesses(
        [_execution(1, "6")],
        [],
        {"O1": _totals(bought="6", sold="30")},
        {},
        _scope(owned=frozenset({"O9"})),
    )
    assert "execution_order_id_foreign" in outcome.refusals


def test_an_owned_commission_of_an_order_without_an_execution_is_unproven() -> None:
    """An owned fee whose order has no ingested execution cannot pass.

    Given: An owned order ``O2`` with venue totals and a COMMISSION on the page,
        but no committed execution and no witnessed fill.
    When: The owned witness map is built,
    Then: ``owned_commission_unproven`` is refused.
    """
    legs = [_leg(100, "EUR", "6"), _leg(99, "PLN", "-30")]
    outcome = build_owned_execution_witnesses(
        [_execution(1, "6")],
        legs,
        {
            "O1": _totals(bought="6", sold="30"),
            "O2": _totals(bought="1", sold="5", commission="0.02"),
        },
        {"O2": Decimal("0.02")},
        _scope(owned=frozenset({"O1", "O2"})),
    )
    assert outcome.witnesses == {}
    assert outcome.refusals == ("owned_commission_unproven",)


def test_an_owned_commission_of_an_order_without_totals_is_unproven() -> None:
    """An owned fee whose order has no venue totals cannot pass.

    Given: An owned order ``O2`` with a COMMISSION on the page and no totals.
    When: The owned witness map is built,
    Then: ``owned_commission_unproven`` is refused.
    """
    legs = [_leg(100, "EUR", "6"), _leg(99, "PLN", "-30")]
    outcome = build_owned_execution_witnesses(
        [_execution(1, "6")],
        legs,
        {"O1": _totals(bought="6", sold="30")},
        {"O2": Decimal("0.02")},
        _scope(owned=frozenset({"O1", "O2"})),
    )
    assert outcome.refusals == ("owned_commission_unproven",)


def test_an_owned_commission_disagreeing_with_its_totals_is_unproven() -> None:
    """An owned fee whose page sum disagrees with the order's total refuses.

    Given: An executed owned order ``O1`` whose page commissions fall short of
        its reported cumulative commission.
    When: The owned witness map is built,
    Then: Both the composition's commission disagreement and the owned
        commission proof refuse.
    """
    legs = [_leg(100, "EUR", "6"), _leg(99, "PLN", "-30")]
    outcome = build_owned_execution_witnesses(
        [_execution(1, "6")],
        legs,
        {"O1": _totals(bought="6", sold="30", commission="0.05")},
        {"O1": Decimal("0.02")},
        _scope(),
    )
    assert outcome.refusals == (
        "commission_sum_disagrees_with_order_total",
        "owned_commission_unproven",
    )


def test_an_owned_commission_with_totals_and_a_witnessed_fill_is_proven() -> None:
    """An owned fee with totals, a witnessed fill, and an exact sum certifies.

    Given: An executed owned order ``O1`` whose page commissions sum exactly to
        its reported cumulative commission, plus a foreign order's commission.
    When: The owned witness map is built,
    Then: The map certifies and the foreign commission plays no part.
    """
    legs = [_leg(100, "EUR", "6"), _leg(99, "PLN", "-30")]
    outcome = build_owned_execution_witnesses(
        [_execution(1, "6")],
        legs,
        {"O1": _totals(bought="6", sold="30", commission="0.02")},
        {"O1": Decimal("0.02"), "M1": Decimal("0.07")},
        _scope(),
    )
    assert outcome.refusals == ()
    assert outcome.witnesses == {1: frozenset({100, 99})}
