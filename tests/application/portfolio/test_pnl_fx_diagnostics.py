"""Amount-free FX observations remain bounded, causal, and isolated per request."""

import asyncio
import json
import math
from collections.abc import Iterator
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast

import pytest

from snapper.application.portfolio import pnl_fx_diagnostics
from snapper.application.portfolio.fx_rates import FxRateMap
from snapper.application.portfolio.fx_rates import FxRatePlane
from snapper.application.portfolio.pnl_fx_diagnostics import PnlFxDiagnostics
from snapper.application.portfolio.pnl_fx_diagnostics import PnlFxFailure
from snapper.application.portfolio.pnl_fx_diagnostics import PnlFxFailureReason
from snapper.application.portfolio.pnl_fx_diagnostics import PnlFxRole
from snapper.application.portfolio.pnl_fx_diagnostics import observe_pnl_fx_diagnostics
from snapper.application.portfolio.pnl_fx_diagnostics import pnl_fx_conversion_scope
from snapper.application.portfolio.pnl_fx_diagnostics import pnl_fx_diagnostics_active
from snapper.application.portfolio.pnl_fx_diagnostics import pnl_fx_diagnostics_text
from snapper.application.portfolio.pnl_fx_diagnostics import pnl_fx_execution_time
from snapper.application.portfolio.pnl_fx_diagnostics import record_pnl_fx_election
from snapper.application.portfolio.pnl_fx_diagnostics import record_pnl_fx_failure
from snapper.application.portfolio.pnl_fx_diagnostics import reset_pnl_fx_execution_order
from snapper.core.json_types import JsonObject

_TIME = datetime(2026, 11, 9, 12, tzinfo=UTC)
_PAIR = ("EUR", "USD")
_PLANE = ("EUR", "USD", "kraken")
_VENUES = {_PAIR: _PLANE}


def _failure(role: PnlFxRole = "price", minute: int = 0) -> PnlFxFailure:
    """Build safe metadata and an explicitly private chronology key."""
    event_time = _TIME + timedelta(minutes=minute, seconds=12)
    return PnlFxFailure(
        instrument="instrument-public",
        role=role,
        event_time=event_time,
        rate_minute=_TIME + timedelta(minutes=minute),
        from_currency="EUR",
        to_currency="USD",
        causal_order=(event_time, 1, "private-exchange-key", 9, "private-pool-key", 0),
    )


def _payload(diagnostics: PnlFxDiagnostics) -> JsonObject:
    """Decode the actual log-text suffix produced for the request."""
    suffix = pnl_fx_diagnostics_text(diagnostics)
    assert suffix.startswith(" fx_diagnostics=")
    return cast(JsonObject, json.loads(suffix.removeprefix(" fx_diagnostics=")))


@pytest.mark.parametrize("role", ["price", "fee", "accrual", "mark"])
def test_each_role_retains_only_amount_free_conversion_metadata(role: PnlFxRole) -> None:
    """Each rejected role records public evidence identity without conversion values.

    Given: One rejected conversion carrying public identities and chronology.
    When: The active request records each supported conversion role.
    Then: The rendered failure contains only the permitted evidence metadata.
    """
    failure = _failure(role)
    with observe_pnl_fx_diagnostics() as diagnostics:
        record_pnl_fx_failure(failure, {}, None)
    assert _payload(diagnostics)["first_failure"] == {
        "instrument": "instrument-public",
        "role": role,
        "event_time": failure.event_time.isoformat(),
        "rate_minute": failure.rate_minute.isoformat(),
        "from_currency": "EUR",
        "to_currency": "USD",
        "selected_plane": None,
        "reason": "no_pin",
    }


@pytest.mark.parametrize(
    ("plane", "close", "expected"),
    [
        (None, None, "no_pin"),
        (_PLANE, None, "missing_exact_minute"),
        (_PLANE, math.nan, "invalid_or_conflicting_close"),
        (_PLANE, math.inf, "invalid_or_conflicting_close"),
        (_PLANE, 0.0, "invalid_or_conflicting_close"),
        (_PLANE, -1.0, "invalid_or_conflicting_close"),
        (("GBP", "USD", "kraken"), 1.0, "invalid_or_conflicting_close"),
        (_PLANE, 1.2, "conversion_nonfinite"),
        (("USD", "EUR", "kraken"), 0.8, "conversion_nonfinite"),
    ],
)
def test_rejected_evidence_uses_closed_reasons_without_reconverting(
    plane: FxRatePlane | None, close: float | None, expected: PnlFxFailureReason
) -> None:
    """Exact-key presence distinguishes gaps from invalid evidence and arithmetic.

    Given: Missing pins, missing minutes, invalid closes, or usable exact evidence.
    When: The collector classifies an already rejected conversion.
    Then: The recorded reason belongs to the closed set and preserves the selected plane.
    """
    rates: dict[tuple[str, str, str, datetime], float] = {}
    venues: dict[tuple[str, str], FxRatePlane] = {}
    if plane is not None:
        venues[_PAIR] = plane
        if close is not None:
            rates[(*plane, _TIME)] = close
    with observe_pnl_fx_diagnostics() as diagnostics:
        record_pnl_fx_failure(_failure(), rates, venues)
    first = cast(JsonObject, _payload(diagnostics)["first_failure"])
    assert first["reason"] == expected
    assert first["selected_plane"] == (None if plane is None else list(plane))


def test_first_failure_uses_causal_order_instead_of_conversion_visit_order() -> None:
    """An earlier causal event replaces a visited mark while equal keys remain stable.

    Given: Failures visited in a different order from their causal replay keys.
    When: Earlier, equal, and later observations reach the request collector.
    Then: Only the earliest failure survives and private ordering fields are absent.
    """
    late = _failure("mark", 10)
    early = _failure("accrual", -2)
    with observe_pnl_fx_diagnostics() as diagnostics:
        record_pnl_fx_failure(late, {}, None)
        record_pnl_fx_failure(early, {}, None)
        record_pnl_fx_failure(replace(early, instrument="equal-key-rival"), {}, None)
        record_pnl_fx_failure(_failure("fee", 2), {}, None)
    assert diagnostics.first_failure_order == early.causal_order
    first = cast(JsonObject, _payload(diagnostics)["first_failure"])
    assert first["role"] == "accrual"
    assert first["instrument"] == "instrument-public"
    rendered = pnl_fx_diagnostics_text(diagnostics)
    assert "private-exchange-key" not in rendered
    assert "private-pool-key" not in rendered


def test_election_retains_all_candidates_exact_coverage_and_first_gap() -> None:
    """Partial rivals remain visible even when a complete plane wins the election.

    Given: Complete, partial, and empty candidates for three exact event minutes.
    When: The existing winning plane and coverage are recorded.
    Then: Every candidate retains its exact count and earliest missing event minute.
    """
    later = _TIME + timedelta(minutes=2)
    middle = _TIME + timedelta(minutes=1)
    rival = ("USD", "EUR", "walutomat")
    absent = ("EUR", "USD", "absent")
    with observe_pnl_fx_diagnostics() as diagnostics:
        record_pnl_fx_election(
            _PAIR,
            {_TIME, middle, later},
            {_PLANE: {_TIME, middle, later}, rival: {later}, absent: set()},
            _PLANE,
        )
    summary = cast(list[JsonObject], _payload(diagnostics)["elections"])[0]
    assert summary["event_count"] == 3
    assert summary["complete_plane_exists"] is True
    assert summary["elected_plane"] == list(_PLANE)
    assert summary["instrument"] is None
    assert summary["candidates"] == [
        {
            "plane": list(absent),
            "exact_event_count": 0,
            "first_missing_event_minute": _TIME.isoformat(),
        },
        {
            "plane": list(_PLANE),
            "exact_event_count": 3,
            "first_missing_event_minute": None,
        },
        {
            "plane": list(rival),
            "exact_event_count": 1,
            "first_missing_event_minute": _TIME.isoformat(),
        },
    ]


@pytest.mark.parametrize("empty_events", [False, True])
def test_election_distinguishes_empty_event_sets_and_no_complete_candidate(
    empty_events: bool,
) -> None:
    """A mark-only candidate is vacuously complete; a missing event remains incomplete.

    Given: A candidate without exact coverage and an empty or nonempty event set.
    When: The collector also observes an election without any candidates.
    Then: Completeness distinguishes a vacuous candidate from missing required proof.
    """
    events = set() if empty_events else {_TIME}
    with observe_pnl_fx_diagnostics() as diagnostics:
        record_pnl_fx_election(_PAIR, events, {_PLANE: set()}, None, "canonical-owner")
        record_pnl_fx_election(_PAIR, events, {}, None)
    records = cast(list[JsonObject], _payload(diagnostics)["elections"])
    assert records[0]["complete_plane_exists"] is empty_events
    assert records[0]["instrument"] == "canonical-owner"
    assert records[0]["elected_plane"] is None
    assert records[1]["complete_plane_exists"] is False


def test_output_caps_elections_candidates_and_metadata_with_omission_counts() -> None:
    """Large requests produce bounded text and explicitly disclose omitted detail.

    Given: More elections and candidates than the retention limits permit.
    When: Long public identity strings accompany those observations.
    Then: Counts disclose omitted entries and retained strings and output remain bounded.
    """
    candidates: dict[FxRatePlane, set[datetime]] = {
        ("EUR", "USD", f"venue-{index:03}"): set() for index in range(19)
    }
    long_text = "X" * 300
    with observe_pnl_fx_diagnostics() as diagnostics:
        record_pnl_fx_failure(replace(_failure(), instrument=long_text), {}, None)
        for _ in range(20):
            record_pnl_fx_election(_PAIR, {_TIME}, candidates, _PLANE, long_text)
    payload = _payload(diagnostics)
    records = cast(list[JsonObject], payload["elections"])
    assert len(records) == 16
    assert payload["elections_omitted"] == 4
    assert len(cast(list[JsonObject], records[0]["candidates"])) == 16
    assert records[0]["candidates_omitted"] == 3
    assert records[0]["instrument"] == "X" * 96
    assert cast(JsonObject, payload["first_failure"])["instrument"] == "X" * 96
    assert len(pnl_fx_diagnostics_text(diagnostics)) < 50_000


def test_numeric_rate_values_and_unsafe_identity_text_never_escape_one_line() -> None:
    """Only allowed metadata is rendered; rate values and chronology keys stay private.

    Given: A unique numeric rate and a public identity containing unsafe text characters.
    When: The failure is serialized into the request log suffix.
    Then: JSON escapes the identity onto one line without rendering monetary evidence.
    """
    close = 1234567.89123
    with observe_pnl_fx_diagnostics() as diagnostics:
        record_pnl_fx_failure(
            replace(_failure(), instrument='instrument\n"quoted"'),
            {(*_PLANE, _TIME): close},
            _VENUES,
        )
    rendered = pnl_fx_diagnostics_text(diagnostics)
    assert str(close) not in rendered
    assert "\n" not in rendered
    assert 'instrument\\n\\"quoted\\"' in rendered
    assert "amount" not in rendered
    assert "balance" not in rendered
    assert "wallet" not in rendered
    assert "credential" not in rendered
    assert cast(JsonObject, _payload(diagnostics)["first_failure"])["selected_plane"] == list(
        _PLANE
    )


def test_inactive_observation_does_not_inspect_evidence() -> None:
    """Background work incurs no evidence observation and owns no request state.

    Given: No observed handler and mappings that raise if inspected.
    When: Background callers invoke failure, election, and chronology helpers.
    Then: All helpers return safely without reading evidence or creating log text.
    """
    assert pnl_fx_diagnostics_active() is False
    record_pnl_fx_failure(_failure(), _BrokenRates(), _VENUES)
    record_pnl_fx_election(_PAIR, {_TIME}, _BrokenCandidates(), _PLANE)
    assert pnl_fx_execution_time("instrument", "shard", _TIME) == _TIME
    reset_pnl_fx_execution_order()
    assert pnl_fx_diagnostics_text(PnlFxDiagnostics()) == ""


def test_nested_contexts_restore_parent_and_reset_after_failure() -> None:
    """Nested observed handlers have independent collectors even when one raises.

    Given: An outer request with a retained fee failure.
    When: A nested request records an accrual failure and raises its original exception.
    Then: The outer collector is restored and both requests retain independent records.
    """
    with observe_pnl_fx_diagnostics() as outer:
        record_pnl_fx_failure(_failure("fee"), {}, None)
        with (
            pytest.raises(ValueError, match="handler failed"),
            observe_pnl_fx_diagnostics() as inner,
        ):
            record_pnl_fx_failure(_failure("accrual"), {}, None)
            raise ValueError("handler failed")
        assert pnl_fx_diagnostics_active() is True
        record_pnl_fx_election(_PAIR, set(), {}, None)
    assert pnl_fx_diagnostics_active() is False
    assert cast(JsonObject, _payload(outer)["first_failure"])["role"] == "fee"
    assert cast(JsonObject, _payload(inner)["first_failure"])["role"] == "accrual"
    assert len(outer.elections) == 1
    assert inner.elections == []


async def test_simultaneous_requests_keep_failure_records_independent() -> None:
    """Task-local contexts prevent one request from replacing a peer's first failure.

    Given: Concurrent request tasks recording different conversion roles.
    When: Both tasks yield while their collectors are active.
    Then: Each request renders only its own first failure.
    """

    async def observe(role: PnlFxRole) -> JsonObject:
        """Yield once while another request has an active independent collector."""
        with observe_pnl_fx_diagnostics() as diagnostics:
            record_pnl_fx_failure(_failure(role), {}, None)
            await asyncio.sleep(0)
        return cast(JsonObject, _payload(diagnostics)["first_failure"])

    fee, accrual = await asyncio.gather(observe("fee"), observe("accrual"))
    assert fee["role"] == "fee"
    assert accrual["role"] == "accrual"


def test_nested_handler_resets_and_restores_parent_conversion_constraints() -> None:
    """A nested handler owns an independent mark scope and replay horizon.

    Given: An outer request whose conversion scope excludes marks and later events.
    When: A nested request observes a mark beyond the parent's horizon.
    Then: The child records its failure and restores the parent's constraints on exit.
    """
    with observe_pnl_fx_diagnostics() as outer:
        with pnl_fx_conversion_scope({}, _TIME):
            with observe_pnl_fx_diagnostics() as inner:
                record_pnl_fx_failure(_failure("mark"), {}, None)
            record_pnl_fx_failure(_failure("mark"), {}, None)
            record_pnl_fx_failure(_failure("fee"), {}, None)
            assert outer.first_failure is None
        record_pnl_fx_failure(_failure("fee"), {}, None)
    assert cast(JsonObject, _payload(inner)["first_failure"])["role"] == "mark"
    assert cast(JsonObject, _payload(outer)["first_failure"])["role"] == "fee"


class _BrokenRates(dict[tuple[str, str, str, datetime], float]):
    """Model an observation-only mapping failure without invoking conversion."""

    def __contains__(self, key: object) -> bool:
        """Raise while the collector inspects exact-key presence."""
        raise RuntimeError("private evidence failure")


class _BrokenCandidates(dict[FxRatePlane, set[datetime]]):
    """Model an election observation failure before it mutates request state."""

    def __iter__(self) -> Iterator[FxRatePlane]:
        """Raise while the collector enumerates candidate identities."""
        raise RuntimeError("private candidate failure")


class _BrokenTimes:
    """Model private causal cursor read and reset failures."""

    def get(self, key: tuple[str, str], default: datetime | None = None) -> datetime:
        """Raise while reading a cursor regardless of the supplied fallback."""
        raise RuntimeError("private cursor failure")

    def clear(self) -> None:
        """Raise while resetting the diagnostic cursor."""
        raise RuntimeError("private cursor reset failure")


def test_observation_failures_are_isolated_and_cannot_replace_good_records() -> None:
    """A broken evidence object drops only its own observation.

    Given: Rate and candidate mappings that fail during diagnostic inspection.
    When: Broken observations precede and follow a valid recorded failure.
    Then: No error escapes and the valid record remains unchanged.
    """
    with observe_pnl_fx_diagnostics() as diagnostics:
        record_pnl_fx_failure(_failure(), cast(FxRateMap, _BrokenRates()), _VENUES)
        record_pnl_fx_election(_PAIR, {_TIME}, _BrokenCandidates(), _PLANE)
        assert diagnostics.first_failure is None
        assert diagnostics.elections == []
        record_pnl_fx_failure(_failure("fee", 1), {}, None)
        previous = diagnostics.first_failure
        record_pnl_fx_failure(_failure("price", 0), _BrokenRates(), _VENUES)
        assert diagnostics.first_failure is previous


def test_serialization_failure_drops_only_the_optional_suffix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The caller keeps its result even when the JSON encoder fails.

    Given: A request collector with a valid failure record.
    When: The JSON encoding boundary raises during message rendering.
    Then: Only the optional diagnostic suffix is dropped without propagating the error.
    """
    with observe_pnl_fx_diagnostics() as diagnostics:
        record_pnl_fx_failure(_failure(), {}, None)

    def reject_encoding(*args: object, **kwargs: object) -> str:
        """Model a failing configured serialization boundary."""
        raise RuntimeError("private encoder failure")

    monkeypatch.setattr(pnl_fx_diagnostics.json, "dumps", reject_encoding)
    assert pnl_fx_diagnostics_text(diagnostics) == ""


def test_execution_cursor_clamps_each_pool_and_can_restart_a_batch() -> None:
    """Successful earlier fills still clamp a later failed fill's causal time.

    Given: Several instrument and shard pools with independent event clocks.
    When: One pool regresses and a subsequent conversion batch resets its cursors.
    Then: The regression clamps within its pool and the new batch starts independently.
    """
    later = _TIME + timedelta(minutes=3)
    with observe_pnl_fx_diagnostics():
        assert pnl_fx_execution_time("instrument", "shard", later) == later
        assert pnl_fx_execution_time("instrument", "shard", _TIME) == later
        assert pnl_fx_execution_time("instrument", "other-shard", _TIME) == _TIME
        assert pnl_fx_execution_time("other-instrument", "shard", _TIME) == _TIME
        reset_pnl_fx_execution_order()
        assert pnl_fx_execution_time("instrument", "shard", _TIME) == _TIME


def test_execution_cursor_failure_keeps_original_event_time() -> None:
    """Cursor failures cannot affect the actual replay or conversion timestamp.

    Given: A request whose diagnostic cursor raises on reads and resets.
    When: The service requests effective chronology and clears the batch.
    Then: Observation returns the original timestamp and isolates reset failures.
    """
    with observe_pnl_fx_diagnostics() as diagnostics:
        diagnostics.execution_times = cast(dict[tuple[str, str], datetime], _BrokenTimes())
        assert pnl_fx_execution_time("instrument", "shard", _TIME) == _TIME
        reset_pnl_fx_execution_order()


def test_conversion_scope_records_only_required_mark_instrument_pair_and_minute() -> None:
    """Unused historical mark conversions cannot become the first failure.

    Given: A causal mark requirement for one instrument, currency pair, and minute.
    When: Unused minutes, instruments, and pairs fail before that required mark.
    Then: Only the required mark enters the request's first failure record.
    """
    failure = _failure("mark")
    requirements = {failure.instrument: {_PAIR: {failure.rate_minute}}}
    with observe_pnl_fx_diagnostics() as diagnostics, pnl_fx_conversion_scope(requirements):
        record_pnl_fx_failure(_failure("mark", -1), {}, None)
        record_pnl_fx_failure(replace(failure, instrument="unused-instrument"), {}, None)
        record_pnl_fx_failure(replace(failure, from_currency="GBP"), {}, None)
        assert diagnostics.first_failure is None
        record_pnl_fx_failure(failure, {}, None)
    assert diagnostics.first_failure_order == failure.causal_order


@pytest.mark.parametrize("role", ["price", "fee", "accrual"])
def test_conversion_scope_keeps_actual_event_failures_without_requirements(
    role: PnlFxRole,
) -> None:
    """The actual failed event conversion remains authoritative without discovery proof.

    Given: An actual event failure with an empty source denomination.
    When: The event is recorded inside a scope with no mark requirements.
    Then: Missing discovery requirements cannot hide the causal conversion failure.
    """
    failure = replace(_failure(role), from_currency="")
    with observe_pnl_fx_diagnostics() as diagnostics, pnl_fx_conversion_scope({}):
        record_pnl_fx_failure(failure, {}, None)
    assert diagnostics.first_failure_order == failure.causal_order


@pytest.mark.parametrize("role", ["mark", "price", "fee", "accrual"])
def test_conversion_scope_suppresses_failures_beyond_effective_replay_horizon(
    role: PnlFxRole,
) -> None:
    """Raw economic time cannot admit an execution clamped beyond the consumed prefix.

    Given: A failure whose causal time is later than the replay's effective horizon.
    When: It is observed before an otherwise identical failure at the horizon.
    Then: The future failure is ignored and the inclusive horizon failure is retained.
    """
    failure = _failure(role)
    requirements = {failure.instrument: {_PAIR: {failure.rate_minute}}}
    horizon = failure.event_time
    later = replace(
        failure,
        causal_order=(horizon + timedelta(minutes=1), 1, "exchange", 1, "instrument", 0),
    )
    with (
        observe_pnl_fx_diagnostics() as diagnostics,
        pnl_fx_conversion_scope(requirements, horizon),
    ):
        record_pnl_fx_failure(later, {}, None)
        assert diagnostics.first_failure is None
        record_pnl_fx_failure(failure, {}, None)
    assert diagnostics.first_failure_order == failure.causal_order


def test_conversion_scope_nests_and_resets_after_exceptions() -> None:
    """Each conversion batch retains its own replay horizon and restores the parent.

    Given: An outer conversion scope admitting an event's effective timestamp.
    When: A nested restrictive scope raises after excluding that same event.
    Then: Both nested scope restoration and leaving the outer scope restore admission.
    """
    failure = _failure()
    with observe_pnl_fx_diagnostics() as diagnostics:
        with pnl_fx_conversion_scope({}, _TIME + timedelta(minutes=1)):
            with (
                pytest.raises(RuntimeError, match="batch failed"),
                pnl_fx_conversion_scope({}, _TIME),
            ):
                record_pnl_fx_failure(failure, {}, None)
                assert diagnostics.first_failure is None
                raise RuntimeError("batch failed")
            record_pnl_fx_failure(failure, {}, None)
        record_pnl_fx_failure(_failure("accrual", -1), {}, None)
    assert cast(JsonObject, _payload(diagnostics)["first_failure"])["role"] == "accrual"


class _BrokenRequirements(Mapping[str, Mapping[tuple[str, str], set[datetime]]]):
    """Model a diagnostic-only failure while examining causal scope membership."""

    def __getitem__(self, key: str) -> Mapping[tuple[str, str], set[datetime]]:
        """Fail the requirement lookup before observing any rate evidence."""
        raise RuntimeError("private requirement failure")

    def __iter__(self) -> Iterator[str]:
        """Expose an empty iterator for the mapping protocol."""
        return iter(())

    def __len__(self) -> int:
        """Expose an empty cardinality for the mapping protocol."""
        return 0


def test_conversion_scope_lookup_failure_is_isolated() -> None:
    """Broken scope observation cannot change conversion control flow.

    Given: A mark requirement mapping that raises during membership lookup.
    When: The collector inspects a failed mark inside that scope.
    Then: The observation is omitted and the mapping error does not escape.
    """
    with (
        observe_pnl_fx_diagnostics() as diagnostics,
        pnl_fx_conversion_scope(_BrokenRequirements()),
    ):
        record_pnl_fx_failure(_failure("mark"), {}, None)
    assert diagnostics.first_failure is None
