"""Unit tests for the pure spot reconciliation anchor bootstrap authority."""

from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal

import pytest

from snapper.application.portfolio.spot_anchor_bootstrap import SpotAnchorNotCertifiableError
from snapper.application.portfolio.spot_anchor_bootstrap import SpotAnchorObservation
from snapper.application.portfolio.spot_anchor_bootstrap import SpotAnchorRefusal
from snapper.application.portfolio.spot_anchor_bootstrap import VenueHistoryItem
from snapper.application.portfolio.spot_anchor_bootstrap import VenueHistoryTip
from snapper.application.portfolio.spot_anchor_bootstrap import _history_refusals
from snapper.application.portfolio.spot_anchor_bootstrap import build_spot_anchor
from snapper.application.portfolio.spot_anchor_bootstrap import spot_anchor_bootstrap_refusals
from snapper.data.repository_types import SpotOwnedOrderPopulation
from snapper.data.repository_types import SpotOwnedOrderRow
from snapper.data.repository_types import SpotOwnedSubmitCommandRow

_T = datetime(2026, 7, 18, 8, 0, tzinfo=UTC)
_SCHEME = "walutomat:api-v2.0.0:account/history:v1"
_BALANCES = {"BTC": Decimal("1.5"), "USD": Decimal("100")}
_ORDER_PUBLIC_ID = "00000000-0000-7000-8000-000000000601"
_COMMAND_PUBLIC_ID = "00000000-0000-7000-8000-000000000701"
_EXTERNAL_ONLY_OPERATIONS = ("PAYIN", "PAYOUT", "TRANSFER", "DIRECT_FX")


def _item(
    item_id: int,
    currency: str,
    balance_after: str,
    *,
    operation_type: str = "MARKET_FX",
    order_id: str | None = "O1",
) -> VenueHistoryItem:
    """Build one normalized API-attributed seal-page row for a bootstrap scenario."""
    return VenueHistoryItem(
        item_id=item_id,
        is_market_fx=operation_type == "MARKET_FX",
        is_api_attributed=True,
        currency=currency,
        balance_after=Decimal(balance_after),
        operation_type=operation_type,
        order_id=order_id,
        correcting_entry=False,
    )


def _command(
    status: str = "filled", exchange_order_id: str | None = "O1"
) -> SpotOwnedSubmitCommandRow:
    """Build one in-scope submit command for an owned-population scenario."""
    return {
        "public_id": _COMMAND_PUBLIC_ID,
        "status": status,
        "exchange_order_id": exchange_order_id,
    }


def _population(
    *,
    owned: frozenset[str] = frozenset({"O1"}),
    commands: tuple[SpotOwnedSubmitCommandRow, ...] | None = None,
) -> SpotOwnedOrderPopulation:
    """Build an owned-order population whose resolved Orders carry ``owned``."""
    resolved: list[SpotOwnedOrderRow] = [
        {"order_public_id": f"{_ORDER_PUBLIC_ID[:-3]}{index:03d}", "exchange_order_id": order_id}
        for index, order_id in enumerate(sorted(owned), start=1)
    ]
    return {
        "owned_order_ids": owned,
        "resolved_orders": tuple(resolved),
        "unresolved_lineage": (),
        "blocked_order_ids": frozenset(),
        "commands": (_command(),) if commands is None else commands,
    }


_PAGE = (_item(100, "BTC", "1.5"), _item(99, "USD", "100"))
_BASELINE = SpotAnchorObservation(
    public_id="00000000-0000-7000-8000-000000000301",
    wallet_public_id="00000000-0000-7000-8000-000000000101",
    exchange="walutomat",
    mode="live",
    anchor_exists=False,
    balance_status="observed",
    balances_are_venue_raw=True,
    balance_read_bound_to_scope=True,
    venue_account_state_public_id="00000000-0000-7000-8000-000000000201",
    balance_observation_id=41,
    session_id="00000000-0000-7000-8000-000000000501",
    sequence_id=1,
    balances_1=dict(_BALANCES),
    balances_2=dict(_BALANCES),
    balances_reserved={"BTC": Decimal("0"), "USD": Decimal("0")},
    source_watermark=1,
    watermark_unchanged=True,
    source_chain_tip="a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2",
    tip_0=VenueHistoryTip(item_id=100, page=_PAGE),
    tip_1_item_id=100,
    history_window_reached_genesis=True,
    precision_certified={"BTC": True, "USD": True},
    margin_signal=False,
    execution_witnesses={1: frozenset({100, 99})},
    venue_cursor_requested_at=_T,
    venue_cursor_observed_at=_T + timedelta(seconds=1),
    source_watermark_requested_at=_T + timedelta(seconds=2),
    source_watermark_captured_at=_T + timedelta(seconds=3),
    first_request_started_at=_T + timedelta(seconds=4),
    first_request_completed_at=_T + timedelta(seconds=5),
    second_request_started_at=_T + timedelta(seconds=6),
    second_request_completed_at=_T + timedelta(seconds=7),
    venue_cursor_confirmed_at=_T + timedelta(seconds=8),
    timestamp=_T + timedelta(seconds=9),
    balances_reserved_2={"BTC": Decimal("0"), "USD": Decimal("0")},
    ownership=_population(),
    ownership_guarantee_version="ownership-v1:" + "c" * 64,
    external_only_operations_initiated=False,
    witness_refusals=(),
)


def _with_rows(*rows: VenueHistoryItem) -> SpotAnchorObservation:
    """Return the baseline with extra older rows appended below the seal page."""
    return replace(_BASELINE, tip_0=VenueHistoryTip(item_id=100, page=(*_PAGE, *rows)))


def _balance_chain_break_tip() -> VenueHistoryTip:
    """Build a tip whose newest BTC balance disagrees with the read."""
    broken = replace(_PAGE[0], balance_after=Decimal("2"))
    return VenueHistoryTip(item_id=100, page=(broken, _PAGE[1]))


def test_certifiable_observation_has_no_refusals() -> None:
    """A fully proven observation is sealed with an empty refusal tuple.

    Given: An observation whose every obligation is discharged.
    When: The refusals are computed.
    Then: The tuple is empty.
    """
    assert spot_anchor_bootstrap_refusals(_BASELINE) == ()


def test_terminal_shared_witness_legs_are_deduplicated() -> None:
    """A terminal execution sharing its predecessor's legs still certifies.

    Given: A two-execution sealed prefix where the terminal (order-close)
        execution witnesses the same fill legs as its active predecessor.
    When: The refusals are computed.
    Then: The shared item ids are deduplicated so the bijection holds and the
        observation is certifiable (a zero-delta terminal never blocks an anchor).
    """
    observation = replace(
        _BASELINE,
        source_watermark=2,
        execution_witnesses={1: frozenset({100, 99}), 2: frozenset({100, 99})},
    )
    assert spot_anchor_bootstrap_refusals(observation) == ()


_REFUSAL_CASES = [
    pytest.param(replace(_BASELINE, anchor_exists=True), "anchor_already_exists", id="anchor"),
    pytest.param(
        replace(_BASELINE, precision_certified={"BTC": True, "USD": False}),
        "asset_precision_uncertified",
        id="precision",
    ),
    pytest.param(
        replace(_BASELINE, balance_read_bound_to_scope=False),
        "balance_read_not_current",
        id="not_current",
    ),
    pytest.param(
        replace(_BASELINE, balances_2={"BTC": Decimal("2"), "USD": Decimal("100")}),
        "balance_reads_disagree",
        id="reads_disagree",
    ),
    pytest.param(
        replace(_BASELINE, balances_are_venue_raw=False), "balances_not_venue_raw", id="not_raw"
    ),
    pytest.param(
        replace(_BASELINE, timestamp=_T - timedelta(seconds=1)),
        "boundary_window_inverted",
        id="inverted",
    ),
    pytest.param(
        replace(_BASELINE, witness_refusals=("execution_order_id_foreign",)),
        "execution_order_id_foreign",
        id="foreign_order_id",
    ),
    pytest.param(
        replace(_BASELINE, balance_status="stale"), "inventory_not_observed", id="not_observed"
    ),
    pytest.param(
        replace(_BASELINE, execution_witnesses={1: frozenset()}),
        "local_execution_not_in_venue_history",
        id="no_witness",
    ),
    pytest.param(replace(_BASELINE, margin_signal=True), "margin_not_proven_cash", id="margin"),
    pytest.param(
        replace(_BASELINE, ownership=_population(commands=(_command("dispatched"),))),
        "own_submit_unresolved",
        id="own_submit",
    ),
    pytest.param(
        replace(_BASELINE, source_watermark=0), "scope_without_committed_execution", id="virgin"
    ),
    pytest.param(replace(_BASELINE, tip_1_item_id=99), "venue_cursor_regressed", id="regressed"),
    pytest.param(
        replace(_BASELINE, exchange="kraken"),
        "venue_cursor_scheme_unregistered",
        id="unregistered",
    ),
    pytest.param(replace(_BASELINE, tip_0=None), "venue_cursor_unavailable", id="no_tip"),
    pytest.param(
        replace(_BASELINE, execution_witnesses={1: frozenset({99})}),
        "venue_fill_not_ingested",
        id="not_ingested",
    ),
    pytest.param(replace(_BASELINE, tip_1_item_id=101), "venue_history_advanced", id="advanced"),
    pytest.param(
        replace(_BASELINE, tip_0=_balance_chain_break_tip()),
        "venue_history_balance_chain_mismatch",
        id="chain_mismatch",
    ),
    pytest.param(
        _with_rows(replace(_item(98, "USD", "90"), correcting_entry=True)),
        "venue_history_correcting_entry",
        id="correcting",
    ),
    pytest.param(
        _with_rows(_item(98, "USD", "90", order_id=None)),
        "venue_history_manual_unattributed",
        id="manual",
    ),
    pytest.param(
        _with_rows(_item(98, "USD", "90", operation_type="LOAN", order_id=None)),
        "venue_history_unsupported_operation",
        id="unsupported",
    ),
    pytest.param(
        replace(_BASELINE, ownership_guarantee_version=None),
        "venue_ownership_guarantee_unrecorded",
        id="guarantee",
    ),
    pytest.param(
        replace(_BASELINE, balances_reserved={"BTC": Decimal("0.5"), "USD": Decimal("0")}),
        "venue_reserved_funds_present",
        id="reserved",
    ),
    pytest.param(
        replace(_BASELINE, watermark_unchanged=False), "watermark_advanced", id="watermark"
    ),
]


@pytest.mark.parametrize("observation, expected", _REFUSAL_CASES)
def test_each_obligation_refuses_its_violation(
    observation: SpotAnchorObservation, expected: SpotAnchorRefusal
) -> None:
    """Each named refusal fires for exactly the violation it guards.

    Given: A certifiable observation with one obligation broken.
    When: The refusals are computed.
    Then: The expected refusal is present.

    Args:
        observation: The baseline with one obligation broken.
        expected: The refusal that obligation names.
    """
    assert expected in spot_anchor_bootstrap_refusals(observation)


def test_zero_or_negative_tip_id_is_malformed() -> None:
    """A non-positive venue history tip id is refused as malformed.

    Given: An observation whose tip id and confirming id are zero.
    When: The refusals are computed.
    Then: ``venue_cursor_malformed`` is present.
    """
    zero_tip = VenueHistoryTip(item_id=0, page=())
    observation = replace(_BASELINE, tip_0=zero_tip, tip_1_item_id=0)
    assert "venue_cursor_malformed" in spot_anchor_bootstrap_refusals(observation)


def test_a_regressed_tip_is_both_regressed_and_advanced() -> None:
    """A confirming tip below the first is both regressed and advanced.

    Given: An observation whose confirming H1 is below H0.
    When: The refusals are computed.
    Then: Both ``venue_cursor_regressed`` and ``venue_history_advanced`` fire.
    """
    refusals = spot_anchor_bootstrap_refusals(replace(_BASELINE, tip_1_item_id=99))
    assert "venue_cursor_regressed" in refusals
    assert "venue_history_advanced" in refusals


def test_an_un_ingested_partial_fill_on_the_page_is_refused() -> None:
    """A second same-order partial on the page with no local execution is refused.

    Given: A page carrying an extra owned fill leg whose history item is absent
        from the ingested set (a later partial of an already-ingested order,
        not yet a local execution).
    When: The refusals are computed.
    Then: ``venue_fill_not_ingested`` is present — order identity never masks it.
    """
    later_partial = _item(102, "BTC", "1.5")
    tip = VenueHistoryTip(item_id=102, page=(later_partial, *_PAGE))
    observation = replace(_BASELINE, tip_0=tip, tip_1_item_id=102)
    assert "venue_fill_not_ingested" in spot_anchor_bootstrap_refusals(observation)


def test_all_applicable_reasons_are_returned_sorted() -> None:
    """An observation violating several obligations returns all of them, sorted.

    Given: An observation with the anchor present, a margin signal, and
        non-venue-raw balances.
    When: The refusals are computed.
    Then: All three names are returned in sorted order.
    """
    observation = replace(
        _BASELINE, anchor_exists=True, margin_signal=True, balances_are_venue_raw=False
    )
    refusals = spot_anchor_bootstrap_refusals(observation)
    assert refusals == (
        "anchor_already_exists",
        "balances_not_venue_raw",
        "margin_not_proven_cash",
    )


def test_build_spot_anchor_produces_a_certified_row() -> None:
    """A certifiable observation builds a fully certified anchor row.

    Given: The certifiable baseline observation, whose page is entirely owned.
    When: The anchor is built.
    Then: The row carries the single certified literals, the verbatim cursor
        value, the registered scheme and derived provenance without an
        external suffix, and canonical sorted balances.
    """
    row = build_spot_anchor(_BASELINE)
    assert row["boundary_status"] == "cursor_certified"
    assert row["inventory_status"] == "venue_reported_full"
    assert row["margin_status"] == "cash"
    assert row["source_watermark_kind"] == "scope_sequence"
    assert row["source_watermark"] == 1
    assert row["venue_cursor_kind"] == "account_history_item_id"
    assert row["venue_cursor_value"] == "100"
    assert row["venue_cursor_scheme"] == _SCHEME
    assert row["provenance"] == f"spot_anchor_bootstrap:v1:{_SCHEME}:genesis"
    assert row["balances_json"] == '{"BTC":"1.5","USD":"100"}'
    assert row["source_chain_tip"] == _BASELINE.source_chain_tip


def test_build_spot_anchor_records_a_windowed_history_claim() -> None:
    """A saturated history page is recorded as a window-scoped claim.

    Given: The certifiable baseline whose history page did not reach genesis.
    When: The anchor is built.
    Then: The provenance carries the ``:windowed`` claim strength — a durable
        per-anchor record that activity older than the page was unobserved.
    """
    observation = replace(_BASELINE, history_window_reached_genesis=False)
    row = build_spot_anchor(observation)
    assert row["provenance"] == f"spot_anchor_bootstrap:v1:{_SCHEME}:windowed"


def test_build_spot_anchor_raises_with_the_refusal_tuple() -> None:
    """An uncertifiable observation raises carrying its exact refusal tuple.

    Given: An observation carrying a durable margin signal.
    When: The anchor build is attempted.
    Then: ``SpotAnchorNotCertifiableError`` is raised carrying that refusal.
    """
    observation = replace(_BASELINE, margin_signal=True)
    with pytest.raises(SpotAnchorNotCertifiableError) as caught:
        build_spot_anchor(observation)
    assert caught.value.refusals == ("margin_not_proven_cash",)


def test_a_naive_read_instant_inverts_the_boundary_window() -> None:
    """A tz-naive read instant fails the read-order chain closed.

    Given: An observation whose timestamp is tz-naive.
    When: The refusals are computed.
    Then: ``boundary_window_inverted`` is present.
    """
    naive = (_T + timedelta(seconds=9)).replace(tzinfo=None)
    observation = replace(_BASELINE, timestamp=naive)
    assert "boundary_window_inverted" in spot_anchor_bootstrap_refusals(observation)


def test_a_duplicate_currency_page_checks_only_its_newest_balance() -> None:
    """A currency appearing twice on the page is checked at its newest row only.

    Given: A descending page carrying an older owned BTC row below the newest one.
    When: The refusals are computed.
    Then: The observation stays certifiable (the older duplicate is skipped).
    """
    older = _item(98, "BTC", "0.9")
    tip = VenueHistoryTip(item_id=100, page=(_PAGE[0], _PAGE[1], older))
    observation = replace(_BASELINE, tip_0=tip, execution_witnesses={1: frozenset({100, 99, 98})})
    assert spot_anchor_bootstrap_refusals(observation) == ()


def test_reverse_coverage_refuses_an_unwitnessed_earlier_execution() -> None:
    """Every sealed-prefix execution must be witnessed, not just the tip one.

    Given: A two-execution prefix whose earlier commit has no venue history
        items (commit order is not venue fill order, so a later-filled execution
        can be committed first).
    When: The refusals are computed.
    Then: ``local_execution_not_in_venue_history`` fires — witnessing only the
        tip execution would have missed it.
    """
    observation = replace(
        _BASELINE,
        source_watermark=2,
        execution_witnesses={1: frozenset(), 2: frozenset({100, 99})},
    )
    assert "local_execution_not_in_venue_history" in spot_anchor_bootstrap_refusals(observation)


def test_build_refuses_when_the_tip_is_absent() -> None:
    """Building with no venue tip fails closed on the unavailable cursor.

    Given: An observation whose tip read is absent.
    When: The anchor build is attempted.
    Then: It raises carrying ``venue_cursor_unavailable``.
    """
    observation = replace(_BASELINE, tip_0=None)
    with pytest.raises(SpotAnchorNotCertifiableError) as caught:
        build_spot_anchor(observation)
    assert "venue_cursor_unavailable" in caught.value.refusals


def test_build_refuses_when_the_exchange_scheme_is_unregistered() -> None:
    """Building for an exchange with no registered scheme fails closed.

    Given: An observation for an exchange absent from the cursor scheme registry.
    When: The anchor build is attempted.
    Then: It raises carrying ``venue_cursor_scheme_unregistered``.
    """
    observation = replace(_BASELINE, exchange="kraken")
    with pytest.raises(SpotAnchorNotCertifiableError) as caught:
        build_spot_anchor(observation)
    assert "venue_cursor_scheme_unregistered" in caught.value.refusals


def test_build_refuses_when_the_ownership_population_is_absent() -> None:
    """Building without an owned-order population fails closed on the gate.

    Given: An otherwise certifiable observation whose population was never read.
    When: The anchor build is attempted.
    Then: It raises carrying exactly ``own_submit_unresolved``.
    """
    observation = replace(_BASELINE, ownership=None)
    with pytest.raises(SpotAnchorNotCertifiableError) as caught:
        build_spot_anchor(observation)
    assert caught.value.refusals == ("own_submit_unresolved",)


def test_an_owned_fill_that_is_not_witnessed_is_not_ingested() -> None:
    """A MARKET_FX row of an order in ``S`` without a witness refuses.

    Given: A seal page with an extra owned MARKET_FX row of order ``O1`` that
        no committed execution witnesses.
    When: The refusals are computed.
    Then: Exactly ``venue_fill_not_ingested`` is returned.
    """
    observation = _with_rows(_item(98, "USD", "90"))
    assert spot_anchor_bootstrap_refusals(observation) == ("venue_fill_not_ingested",)


def test_a_null_order_market_fx_refuses_as_manual() -> None:
    """A MARKET_FX row without an order id refuses whatever its attribution.

    Given: A seal page with an extra API-attributed MARKET_FX row whose order id
        is missing, and another whose order id is blank.
    When: The refusals are computed.
    Then: Exactly ``venue_history_manual_unattributed`` is returned for each.
    """
    for order_id in (None, " "):
        observation = _with_rows(_item(98, "USD", "90", order_id=order_id))
        assert spot_anchor_bootstrap_refusals(observation) == ("venue_history_manual_unattributed",)


def test_a_foreign_order_market_fx_is_external_and_marks_the_provenance() -> None:
    """A MARKET_FX row whose order id lies outside ``S`` is external.

    Given: A seal page with an extra manual MARKET_FX row of foreign order ``M1``.
    When: The anchor is built.
    Then: It seals, and the provenance gains ``:external_observed`` within the
        128-character column.
    """
    observation = _with_rows(
        replace(_item(98, "USD", "90", order_id="M1"), is_api_attributed=False)
    )
    assert spot_anchor_bootstrap_refusals(observation) == ()
    provenance = build_spot_anchor(observation)["provenance"]
    assert provenance == f"spot_anchor_bootstrap:v1:{_SCHEME}:genesis:external_observed"
    windowed = build_spot_anchor(replace(observation, history_window_reached_genesis=False))
    assert len(windowed["provenance"]) <= 128


def test_an_api_row_outside_the_owned_set_is_external() -> None:
    """``orderedBy`` never decides ownership: an ``API/`` row outside ``S`` is external.

    Given: An extra API-attributed MARKET_FX row whose order id is not in ``S``.
    When: The anchor is built.
    Then: It seals as external activity, unwitnessed and unrefused.
    """
    observation = _with_rows(_item(98, "USD", "90", order_id="M1"))
    assert build_spot_anchor(observation)["provenance"].endswith(":external_observed")


def test_a_non_api_row_inside_the_owned_set_is_owned() -> None:
    """A row in ``S`` is owned even without an ``API/`` attribution.

    Given: A seal page whose two owned fill legs carry no API attribution and are
        witnessed.
    When: The anchor is built.
    Then: It seals with no external suffix.
    """
    page = tuple(replace(item, is_api_attributed=False) for item in _PAGE)
    observation = replace(_BASELINE, tip_0=VenueHistoryTip(item_id=100, page=page))
    assert build_spot_anchor(observation)["provenance"].endswith(":genesis")


def test_an_owned_commission_with_its_proof_is_owned() -> None:
    """A COMMISSION of an order in ``S`` is owned when the commission proof holds.

    Given: A seal page with an extra COMMISSION row of order ``O1`` and no
        witness refusal.
    When: The anchor is built.
    Then: It seals with no external suffix.
    """
    observation = _with_rows(_item(98, "USD", "99.98", operation_type="COMMISSION"))
    assert build_spot_anchor(observation)["provenance"].endswith(":genesis")


def test_an_owned_commission_without_its_proof_is_not_ingested() -> None:
    """An unproven owned COMMISSION refuses as an un-ingested fill.

    Given: A seal page with an owned COMMISSION row and the witness builder's
        ``owned_commission_unproven`` refusal.
    When: The refusals are computed.
    Then: Exactly ``venue_fill_not_ingested`` is returned.
    """
    observation = replace(
        _with_rows(_item(98, "USD", "99.98", operation_type="COMMISSION")),
        witness_refusals=("owned_commission_unproven",),
    )
    assert spot_anchor_bootstrap_refusals(observation) == ("venue_fill_not_ingested",)


def test_a_null_order_commission_refuses_as_manual() -> None:
    """A COMMISSION row without an order id refuses at bootstrap too.

    Given: A seal page with an extra COMMISSION row whose order id is missing.
    When: The refusals are computed.
    Then: Exactly ``venue_history_manual_unattributed`` is returned.
    """
    observation = _with_rows(_item(98, "USD", "99.98", operation_type="COMMISSION", order_id=None))
    assert spot_anchor_bootstrap_refusals(observation) == ("venue_history_manual_unattributed",)


def test_a_foreign_order_commission_is_external() -> None:
    """A COMMISSION row whose order id lies outside ``S`` is external.

    Given: A seal page with an extra COMMISSION row of foreign order ``M1``.
    When: The anchor is built.
    Then: It seals with the ``:external_observed`` suffix.
    """
    observation = _with_rows(_item(98, "USD", "99.98", operation_type="COMMISSION", order_id="M1"))
    assert build_spot_anchor(observation)["provenance"].endswith(":external_observed")


@pytest.mark.parametrize("operation_type", _EXTERNAL_ONLY_OPERATIONS)
def test_a_null_order_external_only_operation_is_external(operation_type: str) -> None:
    """An external-only operation without an order id is external while pinned.

    Given: A seal page with an extra external-only row without an order id and
        the adapter capability pinned to no outbound writer.
    When: The anchor is built.
    Then: It seals with the ``:external_observed`` suffix.

    Args:
        operation_type: One of the four external-only operations.
    """
    observation = _with_rows(_item(98, "USD", "40", operation_type=operation_type, order_id=None))
    assert build_spot_anchor(observation)["provenance"].endswith(":external_observed")


@pytest.mark.parametrize("operation_type", _EXTERNAL_ONLY_OPERATIONS)
def test_an_external_only_operation_refuses_without_the_pinned_capability(
    operation_type: str,
) -> None:
    """An external-only operation refuses when the capability is not pinned.

    Given: A seal page with an extra external-only row without an order id, and
        an adapter that does not pin its external-only capability to false.
    When: The refusals are computed.
    Then: Exactly ``venue_history_unsupported_operation`` is returned.

    Args:
        operation_type: One of the four external-only operations.
    """
    observation = replace(
        _with_rows(_item(98, "USD", "40", operation_type=operation_type, order_id=None)),
        external_only_operations_initiated=True,
    )
    assert spot_anchor_bootstrap_refusals(observation) == ("venue_history_unsupported_operation",)


@pytest.mark.parametrize("order_id", ["M1", ""])
@pytest.mark.parametrize("operation_type", _EXTERNAL_ONLY_OPERATIONS)
def test_an_external_only_operation_with_an_order_id_refuses(
    operation_type: str, order_id: str
) -> None:
    """An external-only operation carrying any order id refuses as unsupported.

    Given: A seal page with an extra external-only row whose order id is present,
        even blank, and outside ``S``.
    When: The refusals are computed.
    Then: Exactly ``venue_history_unsupported_operation`` is returned.

    Args:
        operation_type: One of the four external-only operations.
        order_id: A non-null order id outside the owned set.
    """
    observation = _with_rows(
        _item(98, "USD", "40", operation_type=operation_type, order_id=order_id)
    )
    assert spot_anchor_bootstrap_refusals(observation) == ("venue_history_unsupported_operation",)


def test_an_unknown_operation_refuses_as_unsupported() -> None:
    """An operation outside the whitelist refuses whatever its order id.

    Given: A seal page with an extra unknown operation row of foreign order ``M1``.
    When: The refusals are computed.
    Then: Exactly ``venue_history_unsupported_operation`` is returned.
    """
    observation = _with_rows(_item(98, "USD", "40", operation_type="UNKNOWN", order_id="M1"))
    assert spot_anchor_bootstrap_refusals(observation) == ("venue_history_unsupported_operation",)


def test_an_owned_looking_payout_trips_the_ownership_guard() -> None:
    """A PAYOUT carrying an order id in ``S`` can never fall through to external.

    Given: A seal page with an extra PAYOUT row whose order id is the owned ``O1``,
        with the external-only capability pinned.
    When: The refusals are computed.
    Then: Exactly ``venue_history_unsupported_operation`` is returned.
    """
    observation = _with_rows(_item(98, "USD", "40", operation_type="PAYOUT", order_id="O1"))
    assert spot_anchor_bootstrap_refusals(observation) == ("venue_history_unsupported_operation",)


def test_a_correcting_entry_refuses_even_on_an_owned_witnessed_row() -> None:
    """A correcting entry refuses before any ownership classification.

    Given: A seal page whose witnessed owned USD leg is flagged as a correcting
        entry.
    When: The refusals are computed.
    Then: Exactly ``venue_history_correcting_entry`` is returned.
    """
    page = (_PAGE[0], replace(_PAGE[1], correcting_entry=True))
    observation = replace(_BASELINE, tip_0=VenueHistoryTip(item_id=100, page=page))
    assert spot_anchor_bootstrap_refusals(observation) == ("venue_history_correcting_entry",)


def test_an_unresolved_submit_refuses_before_any_classification() -> None:
    """The unresolved-submit gate exits before the page is classified.

    Given: A page holding a null-order MARKET_FX row and a correcting entry, and
        a population whose only command is still dispatched.
    When: The history refusals are computed.
    Then: ``own_submit_unresolved`` is returned and no classification refusal.
    """
    observation = replace(
        _with_rows(
            _item(98, "USD", "90", order_id=None),
            replace(_item(97, "USD", "80"), correcting_entry=True),
        ),
        ownership=_population(commands=(_command("dispatched"),)),
        witness_refusals=("execution_order_id_foreign",),
    )
    assert spot_anchor_bootstrap_refusals(observation) == ("own_submit_unresolved",)


def test_an_unread_population_refuses_before_any_classification() -> None:
    """An observation without a population is refused by the gate alone.

    Given: A page holding a foreign MARKET_FX row and no population.
    When: The history refusals are computed directly.
    Then: Only ``own_submit_unresolved`` is returned.
    """
    observation = replace(
        _with_rows(_item(98, "USD", "90", order_id="M1")),
        ownership=None,
    )
    assert _history_refusals(observation) == {"own_submit_unresolved"}


def test_an_absent_guarantee_refuses_an_otherwise_certifiable_seal() -> None:
    """The seal refuses while the venue order-id guarantee is unrecorded.

    Given: The certifiable baseline whose guarantee version is absent.
    When: The anchor build is attempted.
    Then: It raises carrying exactly ``venue_ownership_guarantee_unrecorded``.
    """
    observation = replace(_BASELINE, ownership_guarantee_version=None)
    with pytest.raises(SpotAnchorNotCertifiableError) as caught:
        build_spot_anchor(observation)
    assert caught.value.refusals == ("venue_ownership_guarantee_unrecorded",)


def test_reserved_funds_on_the_second_read_refuse() -> None:
    """Reserved funds seen only by the second balance read refuse the seal.

    Given: A first read with zero reservations and a second read showing a
        reserved amount, as when a manual order opens between the reads.
    When: The refusals are computed.
    Then: Exactly ``venue_reserved_funds_present`` is returned.
    """
    observation = replace(
        _BASELINE, balances_reserved_2={"BTC": Decimal("0"), "USD": Decimal("10")}
    )
    assert spot_anchor_bootstrap_refusals(observation) == ("venue_reserved_funds_present",)


def test_an_unobserved_second_read_reservation_refuses() -> None:
    """A second read without a reservation reading cannot prove zero reservations.

    Given: The baseline whose second read reported no reservation map.
    When: The refusals are computed.
    Then: Exactly ``venue_reserved_funds_present`` is returned.
    """
    observation = replace(_BASELINE, balances_reserved_2=None)
    assert spot_anchor_bootstrap_refusals(observation) == ("venue_reserved_funds_present",)


def test_a_complete_short_genesis_page_is_accepted() -> None:
    """A short page that reached genesis is a complete page, not a failure.

    Given: A two-row page that holds the account's entire history.
    When: The anchor is built.
    Then: It seals with the ``:genesis`` claim.
    """
    row = build_spot_anchor(replace(_BASELINE, history_window_reached_genesis=True))
    assert row["provenance"] == f"spot_anchor_bootstrap:v1:{_SCHEME}:genesis"


def test_insufficient_witness_coverage_refuses() -> None:
    """A committed execution whose fills fell off the page refuses.

    Given: A sealed prefix of two executions whose second has no witnessed legs
        on the page.
    When: The refusals are computed.
    Then: ``local_execution_not_in_venue_history`` is returned.
    """
    observation = replace(
        _BASELINE,
        source_watermark=2,
        execution_witnesses={1: frozenset({100, 99}), 2: frozenset()},
    )
    assert spot_anchor_bootstrap_refusals(observation) == ("local_execution_not_in_venue_history",)


def test_an_observation_built_without_ownership_fields_fails_closed() -> None:
    """Every ownership field defaults to its fail-closed value.

    Given: An observation constructed with only the read-order fields, as an
        older caller would build it.
    When: The refusals are computed.
    Then: The gate, the guarantee, and the second-read reservation all refuse.
    """
    observation = SpotAnchorObservation(
        **{
            name: getattr(_BASELINE, name)
            for name in _BASELINE.__dataclass_fields__
            if name
            not in {
                "balances_reserved_2",
                "ownership",
                "ownership_guarantee_version",
                "external_only_operations_initiated",
                "witness_refusals",
            }
        }
    )
    assert spot_anchor_bootstrap_refusals(observation) == (
        "own_submit_unresolved",
        "venue_ownership_guarantee_unrecorded",
        "venue_reserved_funds_present",
    )
