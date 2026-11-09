"""Observe existing FX refusal branches without changing conversion results."""

import json
import math
from typing import Literal
from typing import cast

import pytest

from snapper.application.portfolio import pnl_timeline_service
from snapper.application.portfolio.fx_rates import FxRateMap
from snapper.application.portfolio.fx_rates import FxVenueMap
from snapper.application.portfolio.pnl_fx_diagnostics import observe_pnl_fx_diagnostics
from snapper.application.portfolio.pnl_fx_diagnostics import pnl_fx_diagnostics_text
from snapper.application.portfolio.pnl_timeline import PnlIncompletenessReason
from snapper.application.portfolio.pnl_timeline_service import PnlAnchorEvidenceError
from snapper.application.portfolio.pnl_timeline_service import _build_marks_and_reasons_from_candles
from snapper.application.portfolio.pnl_timeline_service import _opening_marks_from_candles
from snapper.application.portfolio.pnl_timeline_service import _resolve_fx_planes
from snapper.application.portfolio.pnl_timeline_service import _resolve_identity_fx_planes
from snapper.application.portfolio.pnl_timeline_service import _to_timeline_accrual
from snapper.application.portfolio.pnl_timeline_service import _to_timeline_execution
from snapper.application.portfolio.pnl_timeline_service import build_fx_rates
from snapper.application.portfolio.pnl_timeline_service import build_wallet_pnl_series
from snapper.core.json_types import JsonObject
from snapper.data.repository import Repository
from snapper.data.repository_types import PnlTimelineAccrualRow
from tests.application.portfolio.test_pnl_timeline_service import _I1
from tests.application.portfolio.test_pnl_timeline_service import _W1
from tests.application.portfolio.test_pnl_timeline_service import FakeRepo
from tests.application.portfolio.test_pnl_timeline_service import _accrual_row
from tests.application.portfolio.test_pnl_timeline_service import _candle
from tests.application.portfolio.test_pnl_timeline_service import _exec_row
from tests.application.portfolio.test_pnl_timeline_service import _fx_row
from tests.application.portfolio.test_pnl_timeline_service import _m
from tests.application.portfolio.test_pnl_timeline_service import _ref

type _Role = Literal["price", "fee", "accrual", "mark"]
type _Reason = Literal[
    "no_pin", "missing_exact_minute", "invalid_or_conflicting_close", "conversion_nonfinite"
]

_PAIR = ("EUR", "USD")
_PLANE = ("EUR", "USD", "kraken")
_PRIVATE_VALUE = 987654.125


def _evidence(reason: _Reason) -> tuple[FxRateMap, FxVenueMap]:
    """Supply each existing refusal cause using plane-qualified materialized data."""
    if reason == "no_pin":
        return {}, {}
    if reason == "missing_exact_minute":
        return {}, {_PAIR: _PLANE}
    if reason == "invalid_or_conflicting_close":
        rows = [_fx_row("EUR", "USD", 2, 1.1), _fx_row("EUR", "USD", 2, 1.2)]
        return build_fx_rates(rows), {_PAIR: _PLANE}
    return {(*_PLANE, _m(2)): 1e308}, {_PAIR: _PLANE}


def _reject_role(
    role: _Role, rates: FxRateMap, venues: FxVenueMap
) -> PnlIncompletenessReason | None:
    """Invoke the actual service mapper and retain its unchanged public cause."""
    if role == "accrual":
        accrual = _to_timeline_accrual(
            _accrual_row(_I1, 2, _PRIVATE_VALUE, "EUR"), "USD", rates, venues
        )
        assert math.isnan(accrual.amount_usd)
        return accrual.incompleteness_reason
    if role == "mark":
        marks, reasons = _build_marks_and_reasons_from_candles(
            [_candle(_m(1), _PRIVATE_VALUE)], {_I1: "EUR"}, "USD", rates, {_I1: venues}
        )
        assert marks == {}
        return reasons[(_I1, _m(2))]
    row = _exec_row(_I1, 1, 2, "buy", 1.0, _PRIVATE_VALUE, 0.0, "USD")
    if role == "fee":
        row["fee"] = _PRIVATE_VALUE
        row["fee_asset"] = "EUR"
    execution = _to_timeline_execution(
        row, "USD", rates, price_currency="EUR" if role == "price" else "USD", venues=venues
    )
    if role == "fee":
        assert math.isnan(execution.fee)
        return execution.fee_incompleteness_reason
    assert math.isnan(execution.price)
    return execution.price_incompleteness_reason


@pytest.mark.parametrize("role", ["price", "fee", "accrual", "mark"])
@pytest.mark.parametrize(
    "reason",
    ["no_pin", "missing_exact_minute", "invalid_or_conflicting_close", "conversion_nonfinite"],
)
def test_every_role_reports_each_existing_fx_refusal(role: _Role, reason: _Reason) -> None:
    """Every conversion seam describes its existing refusal without monetary data.

    Given: A finite foreign input and one of the four existing failure causes.
    When: A real service conversion rejects that input inside a request collector.
    Then: Its public cause remains unchanged and diagnostics name only safe metadata.
    """
    rates, venues = _evidence(reason)
    with observe_pnl_fx_diagnostics() as diagnostics:
        assert _reject_role(role, rates, venues) == "fx_conversion_unproven"
    text = pnl_fx_diagnostics_text(diagnostics)
    summary = cast(JsonObject, json.loads(text.removeprefix(" fx_diagnostics=")))
    failure = cast(JsonObject, summary["first_failure"])
    assert failure == {
        "instrument": _I1,
        "role": role,
        "event_time": _m(2).isoformat(),
        "rate_minute": _m(2).isoformat(),
        "from_currency": "EUR",
        "to_currency": "USD",
        "selected_plane": None if reason == "no_pin" else list(_PLANE),
        "reason": reason,
    }
    assert str(_PRIVATE_VALUE) not in text
    assert "1e+308" not in text
    assert "order-" not in text
    assert "shard" not in text
    assert "\n" not in text


def test_causal_first_failure_overrides_the_service_conversion_loop_order() -> None:
    """An earlier accrual supersedes marks and fills converted by earlier loops.

    Given: Service preparation converts marks first, then executions, then accruals.
    When: All fail, but the accrual has the earliest causal event timestamp.
    Then: The request identifies that accrual rather than the first callback.
    """
    with observe_pnl_fx_diagnostics() as diagnostics:
        _build_marks_and_reasons_from_candles([_candle(_m(5), 100.0)], {_I1: "EUR"}, "USD", {}, {})
        _to_timeline_execution(
            _exec_row(_I1, 1, 4, "buy", 1.0, 100.0, 2.0, "EUR"),
            "USD",
            {},
            price_currency="EUR",
            venues={},
        )
        _to_timeline_accrual(_accrual_row(_I1, 2, 2.0, "EUR"), "USD", {}, {})
    summary = cast(JsonObject, json.loads(pnl_fx_diagnostics_text(diagnostics).split("=", 1)[1]))
    failure = cast(JsonObject, summary["first_failure"])
    assert failure["role"] == "accrual"
    assert failure["event_time"] == _m(2).isoformat()


def test_successful_fill_clamps_a_regressed_failure_after_an_intervening_accrual() -> None:
    """A regressed economic timestamp retains its later causal replay placement.

    Given: A successful fill precedes a timestamp-regressed fill in one durable pool.
    When: The regressed fill fails before an earlier accrual is converted by the service.
    Then: The accrual wins because the fill replays at its pool's running maximum.
    """
    with observe_pnl_fx_diagnostics() as diagnostics:
        _to_timeline_execution(_exec_row(_I1, 1, 10, "buy", 1.0, 100.0, 0.0, "USD"), "USD", {})
        _to_timeline_execution(
            _exec_row(_I1, 2, 1, "sell", 1.0, 100.0, 2.0, "EUR"), "USD", {}, venues={}
        )
        _to_timeline_accrual(_accrual_row(_I1, 5, 2.0, "EUR"), "USD", {}, {})
    summary = cast(JsonObject, json.loads(pnl_fx_diagnostics_text(diagnostics).split("=", 1)[1]))
    assert cast(JsonObject, summary["first_failure"])["role"] == "accrual"


@pytest.mark.parametrize("include_accrual", [False, True])
def test_equal_time_failures_follow_accrual_fee_price_mark_replay_order(
    include_accrual: bool,
) -> None:
    """Equal-time conversions keep the builder's event and within-fill precedence.

    Given: Mark, fee and price refusals at one minute, optionally with an accrual.
    When: Service callbacks run in their existing preparation order.
    Then: Accrual precedes fee, and fee precedes price and mark.
    """
    with observe_pnl_fx_diagnostics() as diagnostics:
        _build_marks_and_reasons_from_candles([_candle(_m(1), 100.0)], {_I1: "EUR"}, "USD", {}, {})
        _to_timeline_execution(
            _exec_row(_I1, 1, 2, "buy", 1.0, 100.0, 2.0, "EUR"),
            "USD",
            {},
            price_currency="EUR",
            venues={},
        )
        if include_accrual:
            _to_timeline_accrual(_accrual_row(_I1, 2, 2.0, "EUR"), "USD", {}, {})
    summary = cast(JsonObject, json.loads(pnl_fx_diagnostics_text(diagnostics).split("=", 1)[1]))
    assert cast(JsonObject, summary["first_failure"])["role"] == (
        "accrual" if include_accrual else "fee"
    )


def test_shared_election_reports_exact_coverage_before_candidate_restriction() -> None:
    """A complete event plane and its denser partial rival both remain observable.

    Given: Kraken proves both events while Walutomat proves one event and more marks.
    When: The unchanged election restricts ranking to complete event planes.
    Then: Its summary reports every original candidate and the existing winner.
    """
    rival = ("EUR", "USD", "walutomat")
    rows = [
        _fx_row("EUR", "USD", 1, 1.1),
        _fx_row("EUR", "USD", 2, 1.1),
        *(_fx_row("EUR", "USD", minute, 1.2, "walutomat") for minute in (1, 3, 4, 5)),
    ]
    with observe_pnl_fx_diagnostics() as diagnostics:
        selected = _resolve_fx_planes(
            {_PAIR: {_m(minute) for minute in range(1, 6)}},
            {_PAIR: {_PLANE, rival}},
            rows,
            "USD",
            {_PAIR: {_m(1), _m(2)}},
        )
    assert selected == {_PAIR: _PLANE}
    summary = cast(JsonObject, json.loads(pnl_fx_diagnostics_text(diagnostics).split("=", 1)[1]))
    assert summary["first_failure"] is None
    elections = summary["elections"]
    assert isinstance(elections, list)
    assert elections == [
        {
            "pair": list(_PAIR),
            "instrument": None,
            "event_count": 2,
            "complete_plane_exists": True,
            "elected_plane": list(_PLANE),
            "candidates": [
                {"plane": list(_PLANE), "exact_event_count": 2, "first_missing_event_minute": None},
                {
                    "plane": list(rival),
                    "exact_event_count": 1,
                    "first_missing_event_minute": _m(2).isoformat(),
                },
            ],
            "candidates_omitted": 0,
        }
    ]


def test_election_observation_construction_failures_preserve_both_elections(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Failed diagnostic assembly cannot alter shared or canonical selection.

    Given: Existing exact coverage and a recorder that raises during observation.
    When: Both service election helpers attempt their request-local summaries.
    Then: Both preserve the same elected planes and propagate no logging failure.
    """

    def fail(*_args: object) -> None:
        """Represent a diagnostic construction failure before the sink is reached."""
        raise RuntimeError("observation unavailable")

    monkeypatch.setattr(pnl_timeline_service, "record_pnl_fx_election", fail)
    rows = [_fx_row("EUR", "USD", 2, 1.1)]
    with observe_pnl_fx_diagnostics() as diagnostics:
        shared = _resolve_fx_planes(
            {_PAIR: {_m(2)}}, {_PAIR: {_PLANE}}, rows, "USD", {_PAIR: {_m(2)}}
        )
        canonical = _resolve_identity_fx_planes(
            {_I1: {_PAIR: {_m(2)}}}, {_I1: _PLANE}, rows, {_I1: {_PAIR: {_m(2)}}}
        )
    assert shared == {_PAIR: _PLANE}
    assert canonical == {_I1: _PLANE}
    assert pnl_fx_diagnostics_text(diagnostics) == ""


def test_opening_mark_refusal_keeps_its_exception_and_records_the_failed_conversion() -> None:
    """An opening mark records the existing FX refusal before anchor derivation stops.

    Given: A nonflat opening instrument has a valid raw mark and no pinned FX plane.
    When: Existing opening-mark construction rejects the unavailable conversion.
    Then: Its existing exception is retained and the request names that mark failure.
    """
    with (
        observe_pnl_fx_diagnostics() as diagnostics,
        pytest.raises(PnlAnchorEvidenceError, match="opening mark or FX evidence is unavailable"),
    ):
        _opening_marks_from_candles(
            [_candle(_m(1), _PRIVATE_VALUE)], {_I1}, {_I1: "EUR"}, "USD", {}, {}, _m(2)
        )
    summary = cast(JsonObject, json.loads(pnl_fx_diagnostics_text(diagnostics).split("=", 1)[1]))
    failure = cast(JsonObject, summary["first_failure"])
    assert failure["role"] == "mark"
    assert failure["event_time"] == _m(2).isoformat()
    assert failure["reason"] == "no_pin"


async def test_real_series_excludes_marks_before_the_instrument_becomes_held() -> None:
    """Unused early candle conversions cannot displace a later causal mark failure.

    Given: Early marks lack FX before the first fill and its exact event rate is proven.
    When: The real series later reaches a held mark whose exact FX minute is missing.
    Then: The request names the held mark rather than earlier unused candle conversions.
    """
    repo = FakeRepo(
        executions=[_exec_row(_I1, 1, 2, "buy", 1.0, 100.0, 0.0, "USD")],
        refs=[_ref(_I1, "BTC-EUR", "EUR")],
        candles=[_candle(_m(minute), 105.0) for minute in (-1, 0, 1, 2)],
        fx_rows=[_fx_row("EUR", "USD", 2, 1.1)],
    )
    with observe_pnl_fx_diagnostics() as diagnostics:
        result = await build_wallet_pnl_series(
            cast(Repository, repo), _W1, "live", _m(0), _m(3), "1m", _m(4)
        )
    assert result.points[0].net_pnl == 0.0
    summary = cast(JsonObject, json.loads(pnl_fx_diagnostics_text(diagnostics).split("=", 1)[1]))
    failure = cast(JsonObject, summary["first_failure"])
    assert failure["role"] == "mark"
    assert failure["event_time"] == _m(3).isoformat()
    assert failure["reason"] == "missing_exact_minute"


@pytest.mark.parametrize("role", ["price", "mark"])
def test_positive_conversion_underflow_uses_the_closed_arithmetic_reason(
    role: Literal["price", "mark"],
) -> None:
    """A positive input rounded to zero retains the caller's existing refusal.

    Given: A valid positive exact rate whose multiplication underflows to zero.
    When: A price or mark mapper applies its existing positive-result requirement.
    Then: It keeps the FX cause and diagnostics use the arithmetic failure category.
    """
    rates = {(*_PLANE, _m(2)): 0.5}
    venues = {_PAIR: _PLANE}
    with observe_pnl_fx_diagnostics() as diagnostics:
        if role == "price":
            execution = _to_timeline_execution(
                _exec_row(_I1, 1, 2, "buy", 1.0, 5e-324, 0.0, "USD"),
                "USD",
                rates,
                price_currency="EUR",
                venues=venues,
            )
            assert execution.price_incompleteness_reason == "fx_conversion_unproven"
        else:
            marks, reasons = _build_marks_and_reasons_from_candles(
                [_candle(_m(1), 5e-324)], {_I1: "EUR"}, "USD", rates, {_I1: venues}
            )
            assert marks == {}
            assert reasons[(_I1, _m(2))] == "fx_conversion_unproven"
    summary = cast(JsonObject, json.loads(pnl_fx_diagnostics_text(diagnostics).split("=", 1)[1]))
    assert cast(JsonObject, summary["first_failure"])["reason"] == "conversion_nonfinite"


def test_identity_zero_and_invalid_raw_values_do_not_create_fx_diagnostics() -> None:
    """Only actual finite nonzero foreign conversion refusals create a failure record.

    Given: Native values, exact-zero foreign flows and invalid raw price evidence.
    When: The unchanged service mappers perform their existing guards and conversions.
    Then: No rejected foreign conversion is attributed to these bypassed cases.
    """
    with observe_pnl_fx_diagnostics() as diagnostics:
        _to_timeline_execution(
            _exec_row(_I1, 1, 2, "buy", 1.0, 100.0, 2.0, "USD"),
            "USD",
            {},
            price_currency="USD",
        )
        _to_timeline_execution(_exec_row(_I1, 2, 2, "buy", 1.0, 100.0, 0.0, "EUR"), "USD", {})
        _to_timeline_execution(
            _exec_row(_I1, 3, 2, "buy", 1.0, math.nan, 0.0, "USD"),
            "USD",
            {},
            price_currency="EUR",
        )
        _to_timeline_accrual(_accrual_row(_I1, 2, 0.0, "EUR"), "USD", {}, {})
        _to_timeline_accrual(_accrual_row(_I1, 2, 2.0, "USD"), "USD", {}, {})
        _build_marks_and_reasons_from_candles([_candle(_m(1), 100.0)], {_I1: "USD"}, "USD", {}, {})
        _build_marks_and_reasons_from_candles(
            [_candle(_m(1), math.nan)], {_I1: "EUR"}, "USD", {}, {}
        )
    assert pnl_fx_diagnostics_text(diagnostics) == ""


@pytest.mark.parametrize("case", ["empty_fee", "empty_accrual", "invalid_size_fee"])
async def test_real_series_reports_rejected_flows_without_a_load_requirement(
    case: Literal["empty_fee", "empty_accrual", "invalid_size_fee"],
) -> None:
    """Existing event rejection branches remain observable when planning omits their pair.

    Given: A nonzero empty-denomination flow or a foreign fee on an invalid-size fill.
    When: Existing requirement guards omit the pair but conversion still fails closed.
    Then: The real series retains its refusal and diagnostics name that rejected flow.
    """
    row = _exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.0, "USD")
    accruals: list[PnlTimelineAccrualRow] = []
    if case == "empty_accrual":
        accruals = [_accrual_row(_I1, 1, 2.0, "")]
    elif case == "empty_fee":
        row["fee"], row["fee_asset"] = 2.0, ""
    else:
        row["size"], row["fee"], row["fee_asset"] = math.nan, 2.0, "EUR"
    repo = FakeRepo(
        executions=[row],
        accruals=accruals,
        refs=[_ref(_I1, "BTC-USD", "USD")],
        candles=[_candle(_m(-1), 105.0), _candle(_m(0), 105.0)],
    )
    with observe_pnl_fx_diagnostics() as diagnostics:
        result = await build_wallet_pnl_series(
            cast(Repository, repo), _W1, "live", _m(0), _m(1), "1m", _m(2)
        )
    assert result.points[-1].net_pnl is None
    assert any(
        reason.reason == "fx_conversion_unproven"
        for reason in result.points[-1].incompleteness_reasons
    )
    summary = cast(JsonObject, json.loads(pnl_fx_diagnostics_text(diagnostics).split("=", 1)[1]))
    failure = cast(JsonObject, summary["first_failure"])
    assert failure["role"] == ("accrual" if case == "empty_accrual" else "fee")
    assert failure["from_currency"] == ("EUR" if case == "invalid_size_fee" else "")
    assert failure["reason"] == "no_pin"
