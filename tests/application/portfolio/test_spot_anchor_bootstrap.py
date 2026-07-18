"""Unit tests for the pure spot reconciliation anchor bootstrap authority."""

from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal

import pytest

from snapper.application.portfolio.spot_anchor_bootstrap import SpotAnchorNotCertifiableError
from snapper.application.portfolio.spot_anchor_bootstrap import SpotAnchorObservation
from snapper.application.portfolio.spot_anchor_bootstrap import VenueHistoryItem
from snapper.application.portfolio.spot_anchor_bootstrap import VenueHistoryTip
from snapper.application.portfolio.spot_anchor_bootstrap import build_spot_anchor
from snapper.application.portfolio.spot_anchor_bootstrap import spot_anchor_bootstrap_refusals

_T = datetime(2026, 7, 18, 8, 0, tzinfo=UTC)
_SCHEME = "walutomat:api-v2.0.0:account/history:v1"
_BALANCES = {"BTC": Decimal("1.5"), "USD": Decimal("100")}
_PAGE = (
    VenueHistoryItem(
        item_id=100,
        is_market_fx=True,
        is_api_attributed=True,
        currency="BTC",
        balance_after=Decimal("1.5"),
    ),
    VenueHistoryItem(
        item_id=99,
        is_market_fx=True,
        is_api_attributed=True,
        currency="USD",
        balance_after=Decimal("100"),
    ),
)
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
)


def _unattributed_tip() -> VenueHistoryTip:
    """Build a tip whose newest fill was not created by our API key."""
    manual = replace(_PAGE[0], is_api_attributed=False)
    return VenueHistoryTip(item_id=100, page=(manual, _PAGE[1]))


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
        replace(_BASELINE, balance_status="stale"), "inventory_not_observed", id="not_observed"
    ),
    pytest.param(
        replace(_BASELINE, execution_witnesses={1: frozenset()}),
        "local_execution_not_in_venue_history",
        id="no_witness",
    ),
    pytest.param(replace(_BASELINE, margin_signal=True), "margin_not_proven_cash", id="margin"),
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
        replace(_BASELINE, tip_0=_unattributed_tip()),
        "venue_history_manual_unattributed",
        id="manual",
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
    observation: SpotAnchorObservation, expected: str
) -> None:
    """Each named refusal fires for exactly the violation it guards.

    Given: A certifiable observation with one obligation broken.
    When: The refusals are computed.
    Then: The expected refusal is present.
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

    Given: A page carrying an extra attributed fill leg whose history item is
        absent from the ingested set (a later partial of an already-ingested
        order, not yet a local execution).
    When: The refusals are computed.
    Then: ``venue_fill_not_ingested`` is present — order identity never masks it.
    """
    later_partial = VenueHistoryItem(
        item_id=102,
        is_market_fx=True,
        is_api_attributed=True,
        currency="BTC",
        balance_after=Decimal("1.5"),
    )
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

    Given: The certifiable baseline observation.
    When: The anchor is built.
    Then: The row carries the single certified literals, the verbatim cursor
        value, the registered scheme and derived provenance, and canonical
        sorted balances.
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
    assert row["provenance"] == f"spot_anchor_bootstrap:v1:{_SCHEME}"
    assert row["balances_json"] == '{"BTC":"1.5","USD":"100"}'
    assert row["source_chain_tip"] == _BASELINE.source_chain_tip


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

    Given: A descending page carrying an older BTC row below the newest one.
    When: The refusals are computed.
    Then: The observation stays certifiable (the older duplicate is skipped).
    """
    older = VenueHistoryItem(
        item_id=98,
        is_market_fx=True,
        is_api_attributed=True,
        currency="BTC",
        balance_after=Decimal("0.9"),
    )
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
