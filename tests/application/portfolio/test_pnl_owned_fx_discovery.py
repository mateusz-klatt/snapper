"""Owned FX evidence must avoid global discovery without widening financial authority."""

from datetime import timedelta
from typing import cast
from unittest.mock import AsyncMock

import pytest

from snapper.application.portfolio.fx_conversion_shadow import FxShadowEvaluation
from snapper.application.portfolio.fx_conversion_shadow import FxShadowPinContext
from snapper.application.portfolio.fx_conversion_shadow import _build_artifact
from snapper.application.portfolio.fx_conversion_shadow import activate_fx_shadow_context
from snapper.application.portfolio.fx_rates import convert_amount
from snapper.application.portfolio.fx_rates import currency_pair_key
from snapper.application.portfolio.pnl_timeline_service import _load_request_fx_rates
from snapper.data.repository import Repository
from tests.application.portfolio.test_pnl_timeline_service import FakeRepo
from tests.application.portfolio.test_pnl_timeline_service import _fx_row
from tests.application.portfolio.test_pnl_timeline_service import _m

_PAIR = currency_pair_key("EUR", "PLN")
_PLANE = ("EUR", "PLN", "walutomat")


@pytest.mark.parametrize(("marks", "events"), [(True, True), (True, False), (False, True)])
async def test_owned_phases_skip_discovery_and_keep_separate_exact_reads(
    marks: bool, events: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Owned conversions retain their exact source windows without discovery.

    Given: Only canonical owned requirements in either or both request phases.
    When: The request loads FX rates and discovery is unavailable.
    Then: Both global calls are skipped and original bounded exact-plane reads remain.
    """
    repo = FakeRepo(
        fx_rows=[
            _fx_row("EUR", "PLN", 10, 4.0, "walutomat"),
            _fx_row("EUR", "PLN", -50, 5.0, "walutomat"),
        ]
    )
    discovery = AsyncMock(side_effect=AssertionError("Owned FX must not discover rival planes"))
    monkeypatch.setattr(repo, "get_pnl_fx_rate_exchanges", discovery)
    exact_read = AsyncMock(wraps=repo.get_pnl_fx_rate_candles)
    monkeypatch.setattr(repo, "get_pnl_fx_rate_candles", exact_read)
    rates, selected, used = await _load_request_fx_rates(
        cast(Repository, repo),
        {"owner": {_PAIR: {_m(10)}}} if marks else {},
        {"owner": {_PAIR: {_m(-50)}}} if events else {},
        {"owner": _PLANE},
        _m(200),
        "EUR",
    )
    discovery.assert_not_awaited()
    assert all(call.args[3] == _m(200) for call in exact_read.await_args_list)
    assert repo.fx_plane_calls == [[_PLANE]] * (int(marks) + int(events))
    assert repo.fx_range_calls == ([(_m(-11), _m(10))] if marks else []) + (
        [(_m(-71), _m(-50))] if events else []
    )
    assert selected == {"owner": {_PAIR: _PLANE}}
    assert used == {(_PAIR, _PLANE)}
    if marks:
        assert convert_amount(4.0, "PLN", "EUR", _m(10), rates, selected["owner"]) == 1.0
    if events:
        assert convert_amount(5.0, "PLN", "EUR", _m(-50), rates, selected["owner"]) == 1.0


@pytest.mark.parametrize("same_instrument", [False, True])
@pytest.mark.parametrize("owned_mark_phase", [False, True])
async def test_any_general_consumer_preserves_both_full_phase_discoveries(
    same_instrument: bool, owned_mark_phase: bool
) -> None:
    """Per-instrument ownership cannot suppress another required conversion.

    Given: An owned pair plus a same-pair peer or the owner's third-currency flow.
    When: Mark and event requirements are merged in either order.
    Then: Both original discovery calls keep their complete phase pairs and windows.
    """
    general_pair = currency_pair_key("GBP", "PLN") if same_instrument else _PAIR
    general_plane = ("GBP", "PLN", "alpha") if same_instrument else ("EUR", "PLN", "alpha")
    general_owner = "owner" if same_instrument else "peer"
    owned = {"owner": {_PAIR: {_m(10)}}}
    general = {general_owner: {general_pair: {_m(-50)}}}
    repo = FakeRepo(
        fx_rows=[
            _fx_row("EUR", "PLN", 10, 4.0, "walutomat"),
            _fx_row(general_plane[0], general_plane[1], -50, 5.0, "alpha"),
        ]
    )
    _, selected, _ = await _load_request_fx_rates(
        cast(Repository, repo),
        owned if owned_mark_phase else general,
        general if owned_mark_phase else owned,
        {"owner": _PLANE},
        _m(200),
        "EUR",
    )
    own_pairs = [("EUR", "PLN"), ("PLN", "EUR")]
    general_pairs = sorted([general_pair, tuple(reversed(general_pair))])
    assert repo.fx_candidate_pair_calls == (
        [own_pairs, general_pairs] if owned_mark_phase else [general_pairs, own_pairs]
    )
    ranges = [(_m(-11), _m(10)), (_m(-71), _m(-50))]
    assert repo.fx_candidate_range_calls == (ranges if owned_mark_phase else list(reversed(ranges)))
    assert selected["owner"][_PAIR] == _PLANE
    assert selected[general_owner][general_pair] == general_plane


@pytest.mark.parametrize("close", [None, 0.0, -1.0, float("nan"), float("inf")])
async def test_unusable_owned_evidence_never_discovers_a_complete_rival(
    close: float | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A canonical evidence refusal cannot trigger a rival or inverse fallback.

    Given: Missing or invalid owned evidence and complete rival and inverse rows.
    When: An owned request resolves its exact canonical plane.
    Then: Conversion stays withheld and discovery is never used to rescue it.
    """
    rows = [_fx_row("EUR", "PLN", 0, 2.0, "alpha"), _fx_row("PLN", "EUR", 0, 0.25, "walutomat")]
    if close is not None:
        rows.append(_fx_row("EUR", "PLN", 0, close, "walutomat"))
    repo = FakeRepo(fx_rows=rows)
    discovery = AsyncMock(
        side_effect=AssertionError("Canonical refusal must not discover a fallback")
    )
    monkeypatch.setattr(repo, "get_pnl_fx_rate_exchanges", discovery)
    rates, selected, used = await _load_request_fx_rates(
        cast(Repository, repo), {"owner": {_PAIR: {_m(0)}}}, {}, {"owner": _PLANE}, _m(2), "EUR"
    )
    discovery.assert_not_awaited()
    assert selected == {"owner": {}}
    assert rates == {}
    assert used == set()
    assert repo.fx_plane_calls == [[_PLANE]]


@pytest.mark.parametrize("age", [20, 21])
async def test_owned_carry_retains_its_exact_twenty_minute_boundary(
    age: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Skipping discovery cannot extend canonical evidence freshness.

    Given: An owned close exactly at or beyond the existing carry bound.
    When: The request needs a later conversion minute.
    Then: Only the existing twenty-minute boundary retains the canonical plane.
    """
    repo = FakeRepo(fx_rows=[_fx_row("EUR", "PLN", -age, 4.0, "walutomat")])
    monkeypatch.setattr(
        repo, "get_pnl_fx_rate_exchanges", AsyncMock(side_effect=AssertionError("No global read"))
    )
    _, selected, _ = await _load_request_fx_rates(
        cast(Repository, repo), {"owner": {_PAIR: {_m(0)}}}, {}, {"owner": _PLANE}, _m(2), "EUR"
    )
    assert selected == {"owner": ({_PAIR: _PLANE} if age == 20 else {})}
    assert repo.fx_range_calls == [(_m(0) - timedelta(minutes=21), _m(0))]


async def test_owned_shadow_artifacts_preserve_canonical_version_and_operation() -> None:
    """Unused rival rows cannot affect the owned evidence artifact.

    Given: Canonical, rival, and reverse rows with an active shadow collector.
    When: Owned discovery is omitted while the same canonical rows are retained.
    Then: Normalized election and proof fields match the original full-row artifact.
    """
    rows = [
        _fx_row("EUR", "PLN", 0, 4.0, "walutomat"),
        _fx_row("EUR", "PLN", 0, 2.0, "alpha"),
        _fx_row("PLN", "EUR", 0, 0.2, "walutomat"),
    ]
    repo = FakeRepo(fx_rows=rows)
    context = FxShadowPinContext(calculation_version="5B.2", evaluations=[])
    with activate_fx_shadow_context(context):
        rates, _, _ = await _load_request_fx_rates(
            cast(Repository, repo), {"owner": {_PAIR: {_m(0)}}}, {}, {"owner": _PLANE}, _m(2), "EUR"
        )
    assert repo.fx_candidate_pair_calls == []
    assert len(context.evaluations) == 1
    actual = context.evaluations[0]
    expected = FxShadowEvaluation(
        scope_kind="instrument_owned",
        consumer_instrument_public_id="owner",
        pair=_PAIR,
        target_currency="EUR",
        required_minutes=frozenset({_m(0)}),
        candidate_planes=frozenset({_PLANE}),
        selected_plane=_PLANE,
        requested_knowledge_at=_m(2),
        rows=tuple(rows),
        authoritative_rates=rates,
    )
    actual_election, actual_proofs = _build_artifact(actual, _m(2), "5B.2")
    expected_election, expected_proofs = _build_artifact(expected, _m(2), "5B.2")
    generated = {"public_id", "session_id", "election_public_id"}
    assert {key: value for key, value in actual_election.items() if key not in generated} == {
        key: value for key, value in expected_election.items() if key not in generated
    }
    assert [
        {key: value for key, value in proof.items() if key not in generated}
        for proof in actual_proofs
    ] == [
        {key: value for key, value in proof.items() if key not in generated}
        for proof in expected_proofs
    ]
