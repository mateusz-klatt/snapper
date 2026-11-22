"""Tests for the strict versioned P&L opening-checkpoint state envelope."""

import json
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast

import pytest

from snapper.application.portfolio.pnl_opening_checkpoint import PnlOpeningCheckpointState
from snapper.application.portfolio.pnl_opening_checkpoint import checkpoint_to_payload
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import (
    PnlCheckpointBundleIdentityPayload,
)
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import PnlCheckpointStateBundle
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import PnlEventFxCandlePlanePayload
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import PnlFxRateSourcePayload
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import PnlIdAggregatePayload
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import PnlPrefixFxCoveragePayload
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import PnlPrefixFxElectionPayload
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import PnlPrefixRefBindingPayload
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import PnlRefBindingPayload
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import bundle_from_json
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import bundle_to_json
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import event_walk_state_from_replay
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import event_walk_state_to_replay
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import (
    is_unsupported_bundle_version,
)
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import mark_walk_state_from_replay
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import mark_walk_state_to_replay
from snapper.application.portfolio.pnl_timeline import TimelineWindow
from snapper.application.portfolio.pnl_timeline import build_pnl_timeline
from snapper.application.portfolio.pnl_timeline_service import PnlFxRateSource
from snapper.application.portfolio.pnl_timeline_service import _effective_mark_events
from snapper.application.portfolio.pnl_timeline_service import _eligible_mark_minutes
from snapper.application.portfolio.pnl_timeline_service import _eligible_mark_minutes_and_state
from snapper.application.portfolio.pnl_timeline_service import _event_fx_minutes
from snapper.application.portfolio.pnl_timeline_service import _EventFxContext
from snapper.application.portfolio.pnl_timeline_service import _EventFxReplayState
from snapper.application.portfolio.pnl_timeline_service import _MarkFxWindow
from snapper.application.portfolio.pnl_timeline_service import _MarkReplayState
from snapper.application.portfolio.pnl_timeline_service import _replay_event_fx_state
from snapper.data.repository_types import PnlTimelineAccrualRow
from snapper.data.repository_types import PnlTimelineCandleRow
from snapper.data.repository_types import PnlTimelineOpeningExecutionRow
from tests.application.portfolio import test_pnl_opening_checkpoint as checkpoint_fixtures

_BASE = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)
_INSTRUMENT = "I1"
_POOL = (_INSTRUMENT, "shard-I1")


def _m(minute: int) -> datetime:
    """Return one UTC fixture minute.

    Args:
        minute: Minute offset from the fixture base.

    Returns:
        The corresponding aware UTC datetime.
    """
    return _BASE + timedelta(minutes=minute)


def _execution(
    minute: int,
    sequence: int,
    *,
    side: str = "buy",
    price: float = 100.0,
    instrument: str = _INSTRUMENT,
) -> PnlTimelineOpeningExecutionRow:
    """Build one replay execution with durable pool and lineage identities.

    Args:
        minute: Event timestamp offset.
        sequence: Exchange-local scope sequence.
        side: Executed order side.
        price: Execution price, with zero representing invalid evidence.
        instrument: Instrument public identifier.

    Returns:
        A complete service-layer opening execution row.
    """
    return {
        "public_id": f"execution-{sequence}",
        "instrument_public_id": instrument,
        "exchange": "kraken",
        "scope_sequence": sequence,
        "order_public_id": f"order-{sequence}",
        "side": side,
        "status": "filled",
        "size": 1.0,
        "price": price,
        "fee": 0.0,
        "fee_asset": "EUR",
        "executed_at": _m(minute),
        "timestamp": _m(minute),
        "exec_id": f"fill-{sequence}",
        "trade_id": None,
        "client_order_id": f"client-{sequence}",
        "shard_key": f"shard-{instrument}",
    }


def _accrual() -> PnlTimelineAccrualRow:
    """Build one accrual processed after the instrument's invalidation.

    Returns:
        A complete foreign-currency accrual row for the replay walk.
    """
    return {
        "instrument_public_id": _INSTRUMENT,
        "exchange": "kraken",
        "mode": "live",
        "accrual_type": "funding",
        "accrued_at": _m(7),
        "amount": 2.0,
        "amount_asset": "EUR",
    }


def _candle(minute: int, instrument: str = _INSTRUMENT) -> PnlTimelineCandleRow:
    """Build one positive finalized mark candle.

    Args:
        minute: Candle opening minute offset.
        instrument: Instrument public identifier.

    Returns:
        A candle row consumed at the following minute.
    """
    return {"instrument_public_id": instrument, "open_at": _m(minute), "close": 101.0}


def _engine_state() -> PnlOpeningCheckpointState:
    """Capture a fully populated engine state through the existing codec.

    Returns:
        The existing strict opening-checkpoint payload model.
    """
    opening, executions, accruals, marks, lineage = checkpoint_fixtures._multi_instrument_fixture()
    result = build_pnl_timeline(
        executions,
        accruals,
        marks,
        TimelineWindow(
            from_time=checkpoint_fixtures._M(0),
            to_time=checkpoint_fixtures._M(5),
            granularity="1m",
            valuation_ccy="USD",
            recovery_budget_per_point=True,
            capture_checkpoint=True,
        ),
        opening=opening,
        lineage=lineage,
    )
    assert result.closing_checkpoint is not None
    return checkpoint_to_payload(result.closing_checkpoint)


def _real_walk_states() -> tuple[
    _EventFxReplayState,
    dict[str, _MarkReplayState],
    tuple[tuple[datetime, datetime], ...],
]:
    """Run real event and mark internals over a regression-crossing constellation.

    Returns:
        The final event state, per-instrument mark states, and global shadows.
    """
    rows = (
        _execution(5, 1),
        _execution(4, 2, price=0.0),
        _execution(6, 3, side="sell", price=0.0),
        _execution(2, 4, instrument="I2"),
    )
    context = _EventFxContext(
        base_by_instrument={_INSTRUMENT: "BTC", "I2": "ETH"},
        quote_by_instrument={_INSTRUMENT: "EUR", "I2": "EUR"},
        valuation_ccy="USD",
    )
    accruals = (_accrual(),)
    event_state = _replay_event_fx_state(rows, accruals, context)
    event_requirements = _event_fx_minutes(
        rows,
        accruals,
        context,
    )
    assert event_requirements == event_state.requirements
    effective_events, shadows = _effective_mark_events(
        rows,
        context.base_by_instrument,
        context.quote_by_instrument,
    )
    assert len(shadows) == 1
    boundary = _m(4) + timedelta(seconds=30)
    assert shadows[0][0] < boundary < shadows[0][1]
    candles = tuple(_candle(minute) for minute in range(3, 8))
    window = _MarkFxWindow(grid_start=_m(0), to_time=_m(8), regression_shadows=shadows)
    opening_quantities = {("I2", "shard-I2"): 1.0}
    minutes, mark_state = _eligible_mark_minutes_and_state(
        _INSTRUMENT,
        candles,
        tuple(sorted(effective_events[_INSTRUMENT])),
        opening_quantities,
        window,
    )
    assert minutes == _eligible_mark_minutes(
        _INSTRUMENT,
        candles,
        tuple(sorted(effective_events[_INSTRUMENT])),
        opening_quantities,
        window,
    )
    second_candles = tuple(_candle(minute, "I2") for minute in range(3, 8))
    second_minutes, second_state = _eligible_mark_minutes_and_state(
        "I2",
        second_candles,
        tuple(sorted(effective_events["I2"])),
        opening_quantities,
        window,
    )
    assert second_minutes
    return event_state, {_INSTRUMENT: mark_state, "I2": second_state}, tuple(shadows)


def _rate_source_payload() -> PnlFxRateSourcePayload:
    """Build one canonical serialized FX rate source.

    Returns:
        A strict payload containing every ``PnlFxRateSource`` field.
    """
    source = PnlFxRateSource(
        source_currency="EUR",
        valuation_currency="USD",
        base_currency="EUR",
        quote_currency="USD",
        exchange="kraken",
    )
    return PnlFxRateSourcePayload(
        source_currency=source.source_currency,
        valuation_currency=source.valuation_currency,
        base_currency=source.base_currency,
        quote_currency=source.quote_currency,
        exchange=source.exchange,
    )


def _candle_plane_payload() -> PnlEventFxCandlePlanePayload:
    """Build one populated per-plane exact candle aggregate.

    Returns:
        A strict plane entry with an exact decimal identity sum.
    """
    return PnlEventFxCandlePlanePayload(
        base="EUR",
        quote="USD",
        exchange="kraken",
        aggregate=PnlIdAggregatePayload(
            count=2,
            id_sum="3",
            max_id=2,
            max_timestamp=_m(5),
        ),
    )


def _identity_payload() -> PnlCheckpointBundleIdentityPayload:
    """Build one canonical embedded bundle identity payload.

    Returns:
        A strict identity payload binding scope, calc version, and generation.
    """
    return PnlCheckpointBundleIdentityPayload(
        wallet_public_id="00000000-0000-7000-8000-000000000b04",
        mode="live",
        valuation_ccy="USD",
        epoch_public_id="00000000-0000-7000-8000-000000000e01",
        calc_version="5C.6",
        source_generation=2,
    )


def _populated_bundle() -> PnlCheckpointStateBundle:
    """Build an envelope populated from real replay and engine states.

    Returns:
        A fully populated strict checkpoint state bundle.
    """
    event_state, mark_states, shadows = _real_walk_states()
    return PnlCheckpointStateBundle(
        bundle_version=4,
        identity=_identity_payload(),
        engine_state=_engine_state(),
        price_ref_spans_by_instrument={
            "I2": (_m(1), _m(5)),
            _INSTRUMENT: (_m(0), _m(6)),
        },
        display_activity_spans_by_instrument={_INSTRUMENT: (_m(2), _m(6))},
        rate_sources_prefix=(_rate_source_payload(),),
        prefix_lineage_order_ids=("order-1", "order-2"),
        event_walk_state=event_walk_state_from_replay(event_state),
        mark_walk_state=mark_walk_state_from_replay(mark_states, shadows),
        event_fx_candles_by_plane=(_candle_plane_payload(),),
        prefix_fx_elections=(
            PnlPrefixFxElectionPayload(
                instrument_public_id=_INSTRUMENT,
                pair=("EUR", "USD"),
                minute=_m(3),
                elected_plane=("EUR", "USD", "kraken"),
            ),
        ),
        prefix_ref_bindings=(
            PnlPrefixRefBindingPayload(
                instrument_public_id=_INSTRUMENT,
                binding=PnlRefBindingPayload(
                    native_symbol="BTC-USD",
                    exchange="kraken",
                    instrument_exchange="kraken",
                    base_currency="BTC",
                    quote_currency="USD",
                ),
            ),
        ),
        prefix_fx_coverage=(
            PnlPrefixFxCoveragePayload(
                base="EUR",
                quote="USD",
                exchange="kraken",
                minutes=(_m(3),),
            ),
        ),
    )


def _json_object(raw: str) -> dict[str, object]:
    """Decode canonical bundle JSON into a mutable top-level object.

    Args:
        raw: Serialized bundle JSON.

    Returns:
        The decoded top-level JSON object.
    """
    return cast(dict[str, object], json.loads(raw))


def _nested_object(parent: dict[str, object], key: str) -> dict[str, object]:
    """Return one nested JSON object with a concrete mutable type.

    Args:
        parent: Parent JSON object.
        key: Nested object field name.

    Returns:
        The selected nested JSON object.
    """
    return cast(dict[str, object], parent[key])


def test_fully_populated_bundle_json_round_trip_is_bit_identical() -> None:
    """Preserve every populated envelope field through canonical JSON.

    Given: A bundle with real engine, event-walk, mark-walk, span, and plane state.
    When: It is encoded, decoded, and encoded again.
    Then: The model is equal and both JSON strings are bit-identical.
    """
    bundle = _populated_bundle()
    encoded = bundle_to_json(bundle)
    decoded = bundle_from_json(encoded)
    assert decoded == bundle
    assert bundle_to_json(decoded) == encoded


def test_canonical_encoding_is_stable_under_input_permutation() -> None:
    """Canonicalize map order and replay-state insertion order.

    Given: Two equivalent bundles whose input dictionaries have opposite order.
    When: Both bundles are encoded.
    Then: Their canonical JSON strings are identical.
    """
    first = _populated_bundle()
    event_state, _, _ = _real_walk_states()
    event_state.requirements = dict(reversed(tuple(event_state.requirements.items())))
    event_state.last_effective = dict(reversed(tuple(event_state.last_effective.items())))
    event_state.position_qty = dict(reversed(tuple(event_state.position_qty.items())))
    event_state.untrusted_at = dict(reversed(tuple(event_state.untrusted_at.items())))
    for instrument_public_id, pair_requirements in tuple(event_state.requirements.items()):
        event_state.requirements[instrument_public_id] = dict(
            reversed(tuple(pair_requirements.items()))
        )
    second = first.model_copy(
        update={
            "price_ref_spans_by_instrument": dict(
                reversed(tuple(first.price_ref_spans_by_instrument.items()))
            ),
            "event_walk_state": event_walk_state_from_replay(event_state),
        }
    )
    assert bundle_to_json(second) == bundle_to_json(first)


def test_walk_state_converters_round_trip_real_states_through_json() -> None:
    """Restore equal real replay dataclasses without using engine state.

    Given: Real final event and mark states with a boundary-crossing regression shadow.
    When: Their payload models make a JSON round trip and convert back.
    Then: Every carried dataclass field and every shadow compares equal.
    """
    event_state, mark_states, shadows = _real_walk_states()
    decoded = bundle_from_json(bundle_to_json(_populated_bundle()))
    assert event_walk_state_to_replay(decoded.event_walk_state) == event_state
    assert mark_walk_state_to_replay(decoded.mark_walk_state) == (mark_states, shadows)
    assert event_state.position_qty
    assert event_state.basis_unknown
    assert event_state.untrusted_at
    assert mark_states[_INSTRUMENT].event_index == 3
    assert not mark_states[_INSTRUMENT].mark_eligible


def test_empty_collections_round_trip() -> None:
    """Support an empty prefix without manufacturing map entries.

    Given: Empty walk states, spans, lineage, rate sources, and candle planes.
    When: The empty bundle makes a JSON round trip.
    Then: Every collection remains empty and canonical encoding is stable.
    """
    event_state = _EventFxReplayState(
        requirements={},
        last_effective={},
        position_qty={},
        basis_unknown=set(),
        untrusted_at={},
    )
    bundle = PnlCheckpointStateBundle(
        bundle_version=4,
        identity=_identity_payload(),
        engine_state=_engine_state(),
        price_ref_spans_by_instrument={},
        display_activity_spans_by_instrument={},
        rate_sources_prefix=(),
        prefix_lineage_order_ids=(),
        event_walk_state=event_walk_state_from_replay(event_state),
        mark_walk_state=mark_walk_state_from_replay({}, ()),
        event_fx_candles_by_plane=(),
        prefix_fx_elections=(),
        prefix_ref_bindings=(),
        prefix_fx_coverage=(),
    )
    encoded = bundle_to_json(bundle)
    decoded = bundle_from_json(encoded)
    assert decoded == bundle
    assert bundle_to_json(decoded) == encoded
    assert decoded.event_walk_state.requirements == ()
    assert decoded.mark_walk_state.instruments == ()


def test_rejects_extra_field() -> None:
    """Reject unknown envelope fields.

    Given: Otherwise valid bundle JSON with an additional top-level member.
    When: Strict decoding runs.
    Then: ``ValueError`` is raised.
    """
    payload = _json_object(bundle_to_json(_populated_bundle()))
    payload["unexpected"] = True
    with pytest.raises(ValueError, match="unexpected"):
        bundle_from_json(json.dumps(payload))


@pytest.mark.parametrize("version", [1, 3, 5])
def test_rejects_wrong_bundle_version(version: int) -> None:
    """Reject legacy and unknown envelope versions.

    Given: Otherwise valid bundle JSON with ``bundle_version`` set to 1, 3, or 5.
    When: Strict decoding runs.
    Then: ``ValueError`` is raised.

    Args:
        version: Unsupported bundle version number to plant.
    """
    payload = _json_object(bundle_to_json(_populated_bundle()))
    payload["bundle_version"] = version
    assert is_unsupported_bundle_version(json.dumps(payload)) is True
    with pytest.raises(ValueError, match="bundle_version"):
        bundle_from_json(json.dumps(payload))


def test_rejects_invalid_bundle_identity_fields() -> None:
    """Reject unsupported mode and non-positive source_generation in bundle identity.

    Given: Serialized bundles with an invalid mode or zero ``source_generation``.
    When: Strict decoding runs.
    Then: ``ValueError`` rejects each malformed identity field.
    """
    bad_mode = _json_object(bundle_to_json(_populated_bundle()))
    _nested_object(bad_mode, "identity")["mode"] = "sim"
    with pytest.raises(ValueError, match="mode"):
        bundle_from_json(json.dumps(bad_mode))
    bad_generation = _json_object(bundle_to_json(_populated_bundle()))
    _nested_object(bad_generation, "identity")["source_generation"] = 0
    with pytest.raises(ValueError, match="source_generation"):
        bundle_from_json(json.dumps(bad_generation))


def test_rejects_bool_as_int() -> None:
    """Reject booleans in exact integer aggregate fields.

    Given: Otherwise valid JSON whose candle aggregate count is ``true``.
    When: Strict decoding runs.
    Then: ``ValueError`` is raised instead of coercing the boolean to one.
    """
    payload = _json_object(bundle_to_json(_populated_bundle()))
    planes = cast(list[object], payload["event_fx_candles_by_plane"])
    plane = cast(dict[str, object], planes[0])
    aggregate = _nested_object(plane, "aggregate")
    aggregate["count"] = True
    with pytest.raises(ValueError, match="count"):
        bundle_from_json(json.dumps(payload))


def test_rejects_noncanonical_hex_float() -> None:
    """Reject a hexadecimal float with noncanonical spelling.

    Given: A carried pool quantity with uppercase hexadecimal markers.
    When: Strict decoding runs.
    Then: ``ValueError`` requires canonical ``float.hex()`` output.
    """
    payload = _json_object(bundle_to_json(_populated_bundle()))
    event_state = _nested_object(payload, "event_walk_state")
    quantities = cast(list[object], event_state["position_qty"])
    quantity = cast(dict[str, object], quantities[0])
    quantity["position_qty"] = "0X1.0P+0"
    with pytest.raises(ValueError, match="canonical"):
        bundle_from_json(json.dumps(payload))


def test_rejects_naive_datetime() -> None:
    """Reject timestamps without an explicit UTC offset.

    Given: A price-reference span whose minimum lacks its UTC suffix.
    When: Strict decoding runs.
    Then: ``ValueError`` identifies the datetime validation failure.
    """
    payload = _json_object(bundle_to_json(_populated_bundle()))
    spans = _nested_object(payload, "price_ref_spans_by_instrument")
    span = cast(list[object], spans[_INSTRUMENT])
    span[0] = _m(0).replace(tzinfo=None).isoformat()
    with pytest.raises(ValueError, match="datetime"):
        bundle_from_json(json.dumps(payload))


def test_rejects_duplicate_json_keys() -> None:
    """Reject duplicate keys instead of silently keeping the last value.

    Given: Duplicate-key, malformed, and version-free JSON documents.
    When: Strict decoding and the non-raising version probe run.
    Then: ``ValueError`` names the duplicate key and both probe inputs return ``False``.
    """
    encoded = bundle_to_json(_populated_bundle())
    duplicated = encoded.replace(
        '"bundle_version":4',
        '"bundle_version":4,"bundle_version":4',
        1,
    )
    with pytest.raises(ValueError, match="duplicate JSON key"):
        bundle_from_json(duplicated)
    assert is_unsupported_bundle_version("{") is False
    assert is_unsupported_bundle_version("{}") is False


@pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity"])
def test_rejects_nonfinite_json_constants(constant: str) -> None:
    """Reject non-standard nonfinite JSON constants.

    Given: A JSON document containing one nonfinite constant.
    When: The strict JSON parser runs.
    Then: ``ValueError`` names the invalid constant.

    Args:
        constant: Non-standard numeric constant to plant.
    """
    with pytest.raises(ValueError, match="non-finite"):
        bundle_from_json(f'{{"bundle_version":{constant}}}')


@pytest.mark.parametrize("malformed", ["03", "not-a-number"])
def test_rejects_malformed_id_sum(malformed: str) -> None:
    """Reject noncanonical exact decimal identity sums.

    Given: A candle aggregate whose ``id_sum`` is malformed or noncanonical.
    When: Strict decoding runs.
    Then: ``ValueError`` requires a canonical decimal integer string.

    Args:
        malformed: Invalid exact decimal identity sum to plant.
    """
    payload = _json_object(bundle_to_json(_populated_bundle()))
    planes = cast(list[object], payload["event_fx_candles_by_plane"])
    plane = cast(dict[str, object], planes[0])
    aggregate = _nested_object(plane, "aggregate")
    aggregate["id_sum"] = malformed
    with pytest.raises(ValueError, match="canonical decimal"):
        bundle_from_json(json.dumps(payload))


def test_rejects_unsorted_lineage_order_ids() -> None:
    """Require sorted unique prefix lineage identifiers.

    Given: Otherwise valid JSON with lineage identifiers in descending order.
    When: Strict decoding runs.
    Then: ``ValueError`` rejects the noncanonical tuple.
    """
    payload = _json_object(bundle_to_json(_populated_bundle()))
    payload["prefix_lineage_order_ids"] = ["order-2", "order-1"]
    with pytest.raises(ValueError, match="sorted and unique"):
        bundle_from_json(json.dumps(payload))


def test_walk_states_round_trip_int_quantities_with_tagged_int_codec() -> None:
    """Round-trip exact Python int and float walk quantities without AttributeError.

    Given: Event and mark replay states carrying exact ``int`` and ``float``
        pool quantities.
    When: Both walk states are serialized inside a bundle to JSON and decoded back.
    Then: The JSON uses tagged-int payloads, round-trips bit-identically, and
        restores exact ``int`` and ``float`` types and values.
    """
    event_state = _EventFxReplayState(
        requirements={},
        last_effective={},
        position_qty={_POOL: 3, ("I2", "shard-I2"): 1.5},
        basis_unknown=set(),
        untrusted_at={},
    )
    mark_states = {
        _INSTRUMENT: _MarkReplayState(
            event_index=1,
            pool_quantities={"shard-I1": 2, "shard-I1-b": -0.0},
            mark_eligible=True,
        ),
    }
    bundle = _populated_bundle().model_copy(
        update={
            "event_walk_state": event_walk_state_from_replay(event_state),
            "mark_walk_state": mark_walk_state_from_replay(mark_states, ()),
        }
    )
    encoded = bundle_to_json(bundle)
    assert '{"int":"3"}' in encoded
    assert '{"int":"2"}' in encoded
    decoded = bundle_from_json(encoded)
    assert decoded == bundle
    assert bundle_to_json(decoded) == encoded
    restored_event = event_walk_state_to_replay(decoded.event_walk_state)
    restored_marks, restored_shadows = mark_walk_state_to_replay(decoded.mark_walk_state)
    assert restored_shadows == ()
    assert type(restored_event.position_qty[_POOL]) is int
    assert restored_event.position_qty[_POOL] == 3
    assert type(restored_event.position_qty[("I2", "shard-I2")]) is float
    assert restored_event.position_qty[("I2", "shard-I2")] == 1.5
    assert type(restored_marks[_INSTRUMENT].pool_quantities["shard-I1"]) is int
    assert restored_marks[_INSTRUMENT].pool_quantities["shard-I1"] == 2
    assert type(restored_marks[_INSTRUMENT].pool_quantities["shard-I1-b"]) is float
    assert restored_marks[_INSTRUMENT].pool_quantities["shard-I1-b"].hex() == (-0.0).hex()


@pytest.mark.parametrize(
    "target",
    [
        "event_requirements_instrument",
        "event_requirements_pair",
        "event_requirements_minute",
        "event_last_effective",
        "event_position_qty",
        "event_basis_unknown",
        "event_untrusted_at",
        "mark_instruments",
        "mark_pool_quantities",
    ],
)
def test_rejects_duplicate_walk_state_keys(target: str) -> None:
    """Reject duplicate keys in every event-walk and mark-walk state collection.

    Given: A serialized bundle with one duplicated key in a walk-state list.
    When: ``bundle_from_json`` validates the document.
    Then: ``ValueError`` rejects the duplicate key.

    Args:
        target: Walk-state collection to duplicate an entry in.
    """
    payload = _json_object(bundle_to_json(_populated_bundle()))
    event_walk = _nested_object(payload, "event_walk_state")
    mark_walk = _nested_object(payload, "mark_walk_state")
    if target == "event_requirements_instrument":
        items = cast(list[object], event_walk["requirements"])
        event_walk["requirements"] = [items[0], items[0]]
    elif target == "event_requirements_pair":
        items = cast(list[object], event_walk["requirements"])
        first = cast(dict[str, object], items[0])
        pairs = cast(list[object], first["requirements"])
        first["requirements"] = [pairs[0], pairs[0]]
    elif target == "event_requirements_minute":
        items = cast(list[object], event_walk["requirements"])
        first = cast(dict[str, object], items[0])
        pairs = cast(list[object], first["requirements"])
        pair_entry = cast(dict[str, object], pairs[0])
        minutes = cast(list[object], pair_entry["minutes"])
        pair_entry["minutes"] = [minutes[0], minutes[0]]
    elif target == "event_last_effective":
        entries = cast(list[object], event_walk["last_effective"])
        event_walk["last_effective"] = [entries[0], entries[0]]
    elif target == "event_position_qty":
        entries = cast(list[object], event_walk["position_qty"])
        event_walk["position_qty"] = [entries[0], entries[0]]
    elif target == "event_basis_unknown":
        entries = cast(list[object], event_walk["basis_unknown"])
        event_walk["basis_unknown"] = [entries[0], entries[0]]
    elif target == "event_untrusted_at":
        entries = cast(list[object], event_walk["untrusted_at"])
        event_walk["untrusted_at"] = [entries[0], entries[0]]
    elif target == "mark_instruments":
        entries = cast(list[object], mark_walk["instruments"])
        mark_walk["instruments"] = [entries[0], entries[0]]
    else:
        entries = cast(list[object], mark_walk["instruments"])
        first = cast(dict[str, object], entries[0])
        pools = cast(list[object], first["pool_quantities"])
        first["pool_quantities"] = [pools[0], pools[0]]
    with pytest.raises(ValueError, match="sorted and unique"):
        bundle_from_json(json.dumps(payload))


def test_rejects_negative_mark_event_index() -> None:
    """Reject negative mark-replay cursors in strict bundle decoding.

    Given: A serialized bundle whose mark instrument state has ``event_index=-1``.
    When: ``bundle_from_json`` validates the document.
    Then: ``ValueError`` rejects the negative index.
    """
    payload = _json_object(bundle_to_json(_populated_bundle()))
    mark_walk = _nested_object(payload, "mark_walk_state")
    instruments = cast(list[object], mark_walk["instruments"])
    first = cast(dict[str, object], instruments[0])
    first["event_index"] = -1
    with pytest.raises(ValueError, match="event_index"):
        bundle_from_json(json.dumps(payload))


@pytest.mark.parametrize(
    ("event_offset", "prev_offset"),
    [(5, 5), (6, 5)],
)
def test_rejects_inverted_regression_shadow(event_offset: int, prev_offset: int) -> None:
    """Reject empty or inverted half-open regression shadow intervals.

    Given: A regression shadow with ``event_time >= previous_effective``.
    When: ``bundle_from_json`` validates the document.
    Then: ``ValueError`` rejects the non-increasing interval.

    Args:
        event_offset: Minute offset for ``event_time``.
        prev_offset: Minute offset for ``previous_effective``.
    """
    payload = _json_object(bundle_to_json(_populated_bundle()))
    mark_walk = _nested_object(payload, "mark_walk_state")
    mark_walk["regression_shadows"] = [
        {
            "event_time": _m(event_offset).isoformat(),
            "previous_effective": _m(prev_offset).isoformat(),
        }
    ]
    with pytest.raises(ValueError, match="precede"):
        bundle_from_json(json.dumps(payload))


@pytest.mark.parametrize(
    "span_field",
    ["price_ref_spans_by_instrument", "display_activity_spans_by_instrument"],
)
def test_rejects_inverted_activity_spans(span_field: str) -> None:
    """Reject price-reference or display-activity spans where start exceeds end.

    Given: A serialized bundle with ``start > end`` in one span map.
    When: ``bundle_from_json`` validates the document.
    Then: ``ValueError`` rejects the inverted span.

    Args:
        span_field: Span map field name on ``PnlCheckpointStateBundle``.
    """
    payload = _json_object(bundle_to_json(_populated_bundle()))
    spans = _nested_object(payload, span_field)
    spans[_INSTRUMENT] = [_m(6).isoformat(), _m(2).isoformat()]
    with pytest.raises(ValueError, match="span"):
        bundle_from_json(json.dumps(payload))


@pytest.mark.parametrize("pair", [["USD", "EUR"], ["EUR", "EUR"]])
def test_rejects_unordered_or_equal_event_fx_pair(pair: list[str]) -> None:
    """Reject non-canonical event-FX currency pairs where pair[0] >= pair[1].

    Given: A serialized bundle whose event-FX requirement pair is reversed or equal.
    When: ``bundle_from_json`` validates the document.
    Then: ``ValueError`` rejects the non-canonical pair.

    Args:
        pair: Non-canonical two-leg currency pair to plant.
    """
    payload = _json_object(bundle_to_json(_populated_bundle()))
    event_walk = _nested_object(payload, "event_walk_state")
    instruments = cast(list[object], event_walk["requirements"])
    first = cast(dict[str, object], instruments[0])
    pairs = cast(list[object], first["requirements"])
    pair_entry = cast(dict[str, object], pairs[0])
    pair_entry["pair"] = pair
    with pytest.raises(ValueError, match="pair"):
        bundle_from_json(json.dumps(payload))


@pytest.mark.parametrize("legacy_version", [2, 3])
def test_rejects_legacy_bundle_versions_2_and_3(legacy_version: int) -> None:
    """Reject legacy bundle_version 2 and 3 envelopes after bumping BUNDLE_VERSION.

    Given: A serialized envelope whose ``bundle_version`` is downgraded to ``2`` or ``3``.
    When: Strict decoding runs via ``bundle_from_json``.
    Then: ``ValueError`` rejects the legacy bundle version.

    Args:
        legacy_version: Downgraded bundle schema version.
    """
    payload = _json_object(bundle_to_json(_populated_bundle()))
    payload["bundle_version"] = legacy_version
    with pytest.raises(ValueError, match="bundle_version"):
        bundle_from_json(json.dumps(payload))


def test_prefix_fx_election_and_coverage_validators() -> None:
    """Reject mismatched planes, unordered pairs, and duplicate election/coverage keys.

    Given: Populated election stability payloads with mismatched plane currencies,
        duplicate election keys, duplicate ref-binding keys, or unsorted coverage minutes.
    When: ``PnlPrefixFxElectionPayload`` or ``bundle_from_json`` validates them.
    Then: ``ValueError`` rejects every malformed or non-canonical entry.
    """
    with pytest.raises(ValueError, match="does not match pair"):
        PnlPrefixFxElectionPayload(
            instrument_public_id=_INSTRUMENT,
            pair=("EUR", "USD"),
            minute=_m(3),
            elected_plane=("GBP", "USD", "kraken"),
        )
    missing_plane = PnlPrefixFxElectionPayload(
        instrument_public_id=_INSTRUMENT,
        pair=("EUR", "USD"),
        minute=_m(3),
        elected_plane=None,
    )
    assert missing_plane.elected_plane is None
    base_json = bundle_to_json(_populated_bundle())
    dup_elections = _json_object(base_json)
    election_entries = cast(list[object], dup_elections["prefix_fx_elections"])
    dup_elections["prefix_fx_elections"] = [election_entries[0], election_entries[0]]
    with pytest.raises(ValueError, match="sorted and unique"):
        bundle_from_json(json.dumps(dup_elections))


def test_conversion_requirements_round_trip_and_reject_unsorted() -> None:
    """Round-trip prefix conversion-only minutes and enforce canonical ordering.

    Given: An ``_EventFxReplayState`` carrying non-empty ``conversion_requirements``.
    When: The bundle makes a canonical JSON round-trip and its conversion keys
        are permuted or duplicated.
    Then: Restored ``conversion_requirements`` match exactly, input permutation
        produces identical JSON, and duplicate conversion keys are rejected.
    """
    event_state, mark_states, shadows = _real_walk_states()
    conversion_reqs = {
        "I2": {("EUR", "USD"): {_m(1), _m(3)}},
        _INSTRUMENT: {("EUR", "USD"): {_m(2)}},
    }
    state_with_conversions = _EventFxReplayState(
        requirements=event_state.requirements,
        last_effective=event_state.last_effective,
        position_qty=event_state.position_qty,
        basis_unknown=event_state.basis_unknown,
        untrusted_at=event_state.untrusted_at,
        conversion_requirements=conversion_reqs,
    )
    bundle = _populated_bundle().model_copy(
        update={
            "event_walk_state": event_walk_state_from_replay(state_with_conversions),
            "mark_walk_state": mark_walk_state_from_replay(mark_states, shadows),
        }
    )
    encoded = bundle_to_json(bundle)
    decoded = bundle_from_json(encoded)
    assert event_walk_state_to_replay(decoded.event_walk_state) == state_with_conversions
    reversed_state = _EventFxReplayState(
        requirements=event_state.requirements,
        last_effective=event_state.last_effective,
        position_qty=event_state.position_qty,
        basis_unknown=event_state.basis_unknown,
        untrusted_at=event_state.untrusted_at,
        conversion_requirements=dict(reversed(tuple(conversion_reqs.items()))),
    )
    permuted = bundle.model_copy(
        update={"event_walk_state": event_walk_state_from_replay(reversed_state)}
    )
    assert bundle_to_json(permuted) == encoded
    raw = _json_object(encoded)
    event_walk = _nested_object(raw, "event_walk_state")
    entries = cast(list[object], event_walk["conversion_requirements"])
    event_walk["conversion_requirements"] = [entries[0], entries[0]]
    with pytest.raises(ValueError, match="sorted and unique"):
        bundle_from_json(json.dumps(raw))
