"""Native balance candidates require retained full snapshots and gap-free physical source chains."""

import base64
import json
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from typing import Literal
from uuid import UUID

import pytest

from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.infrastructure.exchanges.contracts import ExecutionObservationContext
from snapper.infrastructure.exchanges.contracts import PrivateExecutionObservation
from snapper.infrastructure.exchanges.kraken_futures_balance_stream import (
    CapturedNativeBalanceFrame,
)
from snapper.infrastructure.exchanges.kraken_futures_balance_stream import (
    KrakenFuturesBalanceStream,
)
from snapper.infrastructure.exchanges.kraken_futures_balance_stream import _quantity
from snapper.infrastructure.exchanges.kraken_futures_balance_stream import futures_balance_channel

_NOW = datetime(2026, 10, 23, 10, tzinfo=UTC)
_EPOCH = str(UUID(int=7))


def _selection() -> ExecutionClientSelection:
    """Select a concrete isolated demo source without an inferred margin account."""
    return ExecutionClientSelection(
        wallet_public_id=str(UUID(int=1)),
        exchange="kraken_futures",
        mode="live",
        environment="demo",
        credential_public_id=str(UUID(int=2)),
        credential_timestamp=_NOW,
        public_api_origin="https://demo-futures.kraken.com",
    )


def _frame(
    payload: dict[str, object], sequence: int = 0, receipt: int = 10
) -> CapturedNativeBalanceFrame:
    """Build whole original source bytes, independently retaining context and actual selection."""
    feed: Literal["balances", "balances_snapshot"] = (
        "balances_snapshot" if sequence == 0 else "balances"
    )
    message: dict[str, object] = {
        "feed": feed,
        "account": "venue-account-1",
        "seq": sequence,
        "timestamp": 1792749600000 + sequence,
    }
    message.update(payload)
    wire = json.dumps(message, separators=(",", ":")).encode()
    return CapturedNativeBalanceFrame(
        observation=PrivateExecutionObservation(
            context=ExecutionObservationContext(
                kind="account_cash",
                channel=futures_balance_channel(feed, _EPOCH),
                observed_at=_NOW + timedelta(milliseconds=sequence),
                venue_account_id="venue-account-1",
            ),
            representation="wire_bytes_base64",
            material=base64.b64encode(wire).decode("ascii"),
        ),
        receipt_public_id=str(UUID(int=receipt)),
        client_selection=_selection(),
        connection_epoch_public_id=_EPOCH,
    )


def _snapshot() -> CapturedNativeBalanceFrame:
    """Keep same-currency cash in three independent actual native source profiles."""
    return _frame(
        {
            "holding": {"USD": 5.25, "XBT": 0.1},
            "futures": {
                "F-XBT:USD": {
                    "name": "F-XBT:USD",
                    "pair": "XBT/USD",
                    "unit": "XBT",
                    "balance": 0.5,
                }
            },
            "flex_futures": {
                "currencies": {
                    "USD": {"quantity": 10.5, "value": 99999},
                    "XBT": {"quantity": 0.25, "value": 88888},
                },
                "balance_value": 77777,
                "collateral_value": 66666,
            },
        }
    )


def test_snapshot_and_partial_delta_preserve_profiles_native_units_and_actual_clock() -> None:
    """Source-native quantities remain distinct from valuations and unmodified profile balances.

    Given: A full three-profile snapshot followed by a partial native balance delta,
    When: The cache folds only reported quantity replacements,
    Then: Omitted assets persist, explicit zero persists and reading does not refresh source age.
    """
    stream = KrakenFuturesBalanceStream()
    assert stream.profiles() == ()
    stream.observe(_snapshot())
    stream.observe(
        _frame(
            {
                "holding": {"USD": 0},
                "flex_futures": {
                    "currencies": {"USD": {"value": 123456}, "XBT": {"quantity": 0.3}}
                },
                "futures": {"F-XBT:USD": {"balance": 0.75}},
            },
            sequence=1,
            receipt=11,
        )
    )
    profiles = stream.profiles()
    amounts = {
        item.profile.kind: {quantity.currency: quantity.quantity for quantity in item.quantities}
        for item in profiles
    }
    assert amounts == {
        "holding": {"USD": Decimal("0"), "XBT": Decimal("0.1")},
        "single_collateral": {"XBT": Decimal("0.75")},
        "multi_collateral": {"USD": Decimal("10.5"), "XBT": Decimal("0.3")},
    }
    assert profiles[0].source_proof.observed_at == _NOW + timedelta(milliseconds=1)
    assert stream.profiles() == profiles
    assert profiles[0].source_proof.receipt_public_ids == (str(UUID(int=10)), str(UUID(int=11)))


def test_new_full_snapshot_replaces_inventory_and_starts_an_independent_retained_prefix() -> None:
    """A new complete source snapshot replaces old profiles rather than carrying hidden balances.

    Given: An already observed three-profile source state,
    When: A new full source snapshot contains only an empty holding profile,
    Then: Old profiles disappear and the new receipt is the only replay-prefix anchor.
    """
    stream = KrakenFuturesBalanceStream()
    stream.observe(_snapshot())
    stream.observe(_frame({"holding": {}}, receipt=20))
    profiles = stream.profiles()
    assert len(profiles) == 1
    assert profiles[0].profile.kind == "holding"
    assert profiles[0].quantities == ()
    assert profiles[0].source_proof.receipt_public_ids == (str(UUID(int=20)),)


def test_valuation_only_delta_preserves_exact_inventory_without_fabricating_cash_changes() -> None:
    """Valuation fields cannot replace native quantities even when no quantity map is present.

    Given: Valid native source inventory,
    When: A contiguous delta reports only aggregate or single-profile valuation changes,
    Then: Exact quantities remain unchanged while the actual source tip advances.
    """
    stream = KrakenFuturesBalanceStream()
    stream.observe(_snapshot())
    before = tuple(item.quantities for item in stream.profiles())
    stream.observe(
        _frame(
            {
                "flex_futures": {"balance_value": 123},
                "futures": {"F-XBT:USD": {"portfolio_value": 456}},
            },
            sequence=1,
            receipt=11,
        )
    )
    assert tuple(item.quantities for item in stream.profiles()) == before
    assert stream.profiles()[0].source_proof.tip_sequence == 1


def test_incremental_delta_cannot_initialize_native_inventory_without_a_snapshot() -> None:
    """A latest delta alone cannot certify the complete native profile state.

    Given: A fresh cache with no full source snapshot,
    When: An incremental balance message is observed,
    Then: It refuses and exposes no profile candidate.
    """
    stream = KrakenFuturesBalanceStream()
    with pytest.raises(ValueError, match="snapshot_required"):
        stream.observe(_frame({"holding": {"USD": 1}}, sequence=1))
    assert stream.profiles() == ()


@pytest.mark.parametrize(
    "payload",
    (
        {"seq": 2},
        {"seq": 0},
        {"account": "different-account"},
        {"timestamp": 1},
        {"holding": {"USD": True}},
        {"holding": {"USD": "1.0"}},
        {"holding": []},
        {"holding": {"USD": None}},
        {"flex_futures": {"currencies": {"NEW": {"value": 1000}}}},
        {"futures": {"F-XBT:USD": {"unit": "USD", "balance": 1}}},
        {"futures": {"F-ETH:USD": {"name": "F-ETH:USD", "unit": "ETH", "balance": 1}}},
    ),
)
def test_failed_delta_invalidates_whole_candidate_instead_of_exposing_partial_state(
    payload: dict[str, object],
) -> None:
    """Incomplete or contradictory deltas keep original raw evidence outside the candidate cache.

    Given: A valid native source snapshot,
    When: A later delta has a gap, identity change or malformed native quantity,
    Then: The entire candidate is invalidated and no partially updated inventory is exposed.
    """
    stream = KrakenFuturesBalanceStream()
    stream.observe(_snapshot())
    source = _frame(payload, sequence=1, receipt=11)
    material = source.observation.material
    with pytest.raises(ValueError):
        stream.observe(source)
    assert stream.profiles() == ()
    assert source.observation.material == material


@pytest.mark.parametrize("changed", ("epoch", "selection", "receipt", "clock"))
def test_transport_identity_and_local_provenance_cannot_cross_source_chain(changed: str) -> None:
    """Correct-looking provider sequence numbers cannot stitch another physical or account source.

    Given: A retained full snapshot and a numerically contiguous delta,
    When: Its physical epoch, selected client, receipt identity or local clock contradicts the prefix,
    Then: The candidate refuses the stitched chain.
    """
    stream = KrakenFuturesBalanceStream()
    stream.observe(_snapshot())
    source = _frame({"holding": {"USD": 1}}, sequence=1, receipt=11)
    if changed == "epoch":
        epoch = str(UUID(int=8))
        context = replace(
            source.observation.context, channel=futures_balance_channel("balances", epoch)
        )
        source = replace(
            source,
            connection_epoch_public_id=epoch,
            observation=replace(source.observation, context=context),
        )
    elif changed == "selection":
        source = replace(
            source,
            client_selection=source.client_selection.model_copy(
                update={"wallet_public_id": str(UUID(int=9))}
            ),
        )
    elif changed == "receipt":
        source = replace(source, receipt_public_id=str(UUID(int=10)))
    else:
        context = replace(source.observation.context, observed_at=_NOW - timedelta(seconds=1))
        source = replace(source, observation=replace(source.observation, context=context))
    with pytest.raises(ValueError, match="source_chain_broken|source_clock_regressed"):
        stream.observe(source)
    assert stream.profiles() == ()


@pytest.mark.parametrize(
    "payload",
    (
        {},
        {"futures": {}},
        {"flex_futures": {}},
        {"futures": {"F-XBT:USD": {"name": "F-XBT:USD"}}},
        {"holding": {"": 1}},
        {"holding": {"USD": 1}, "seq": True},
        {"holding": {"USD": 1}, "seq": -1},
        {"holding": {"USD": 1}, "account": " "},
        {"holding": {"USD": 1}, "timestamp": 9999999999999999999999},
        {"holding": {"USD": 1}, "feed": "fills"},
        {"holding": {"USD": 1}, "event": "subscribed"},
    ),
)
def test_malformed_snapshot_never_becomes_native_profile_authority(
    payload: dict[str, object],
) -> None:
    """A feed name and retained bytes alone cannot create complete native profile evidence.

    Given: A whole-frame raw receipt containing malformed or missing snapshot semantics,
    When: Source interpretation is attempted,
    Then: No native profile candidate is exposed.
    """
    stream = KrakenFuturesBalanceStream()
    with pytest.raises(ValueError):
        stream.observe(_frame(payload))
    assert stream.profiles() == ()


@pytest.mark.parametrize("maximum", (0, 4097, True))
def test_cache_cannot_disable_or_expand_the_source_policy_chain_bound(maximum: int) -> None:
    """A runtime cache cannot silently bypass the versioned evidence policy's bounded replay.

    Given: An invalid source-chain budget including a boolean integer alias,
    When: A caller constructs its native source cache,
    Then: The cache refuses before retaining a misleading partial source prefix.
    """
    with pytest.raises(ValueError, match="chain_bound_invalid"):
        KrakenFuturesBalanceStream(max_receipts=maximum)


def test_chain_budget_requires_a_new_snapshot_instead_of_dropping_old_receipts() -> None:
    """A bounded replay chain cannot turn an unverified tail into a complete inventory proof.

    Given: A valid snapshot and delta filling an explicitly small source receipt budget,
    When: Another delta arrives and then a new full snapshot is retained,
    Then: The excess delta invalidates the proof and only the new snapshot restores it.
    """
    stream = KrakenFuturesBalanceStream(max_receipts=2)
    stream.observe(_snapshot())
    stream.observe(_frame({"holding": {"USD": 11}}, sequence=1, receipt=11))
    with pytest.raises(ValueError, match="source_chain_broken"):
        stream.observe(_frame({"holding": {"USD": 12}}, sequence=2, receipt=12))
    assert stream.profiles() == ()
    stream.observe(_frame({"holding": {"USD": 8}}, receipt=30))
    assert stream.profiles()[0].source_proof.receipt_public_ids == (str(UUID(int=30)),)


@pytest.mark.parametrize("changed", ("kind", "channel", "pointer", "representation"))
def test_reconstructed_or_wrongly_selected_raw_context_cannot_certify_native_inventory(
    changed: str,
) -> None:
    """Source bytes must remain whole-frame balance evidence from the actual physical channel.

    Given: Otherwise valid retained source bytes with contradictory context or weaker provenance,
    When: A cache attempts source interpretation,
    Then: It refuses without promoting the bytes to native balance authority.
    """
    source = _snapshot()
    context = source.observation.context
    if changed == "kind":
        context = replace(context, kind="unknown")
    elif changed == "channel":
        context = replace(context, channel="kraken_futures.ws.unknown")
    elif changed == "pointer":
        context = replace(context, target_json_pointer="/holding/USD")
    if changed == "representation":
        observation = replace(source.observation, representation="parsed_fields_json")
    else:
        observation = replace(source.observation, context=context)
    source = replace(source, observation=observation)
    stream = KrakenFuturesBalanceStream()
    with pytest.raises(ValueError, match="original_wire_required|receipt_context_mismatch"):
        stream.observe(source)
    assert stream.profiles() == ()


@pytest.mark.parametrize("epoch", ("not-a-uuid", "A987FBC9-4BED-4078-8F07-9141BA07C9F3"))
def test_noncanonical_physical_epoch_cannot_label_a_balance_source(epoch: str) -> None:
    """Source channel identity requires a canonical local UUID rather than a caller alias.

    Given: An invalid or alternate-spelled physical source identity,
    When: The balance observation channel is constructed,
    Then: It refuses the noncanonical epoch.
    """
    with pytest.raises(ValueError, match="epoch_noncanonical"):
        futures_balance_channel("balances", epoch)


@pytest.mark.parametrize("value", (Decimal("NaN"), Decimal("Infinity")))
def test_native_quantity_boundary_refuses_nonfinite_decimal_operands(value: Decimal) -> None:
    """Exact numeric representation alone cannot make a nonfinite value financial evidence.

    Given: A nonfinite Decimal at the native quantity boundary,
    When: Its exact amount is requested independently of upstream JSON validation,
    Then: The quantity boundary refuses it.
    """
    with pytest.raises(ValueError, match="quantity_nonfinite"):
        _quantity(value)


@pytest.mark.parametrize("missing_profile", ("holding", "multi_collateral"))
def test_absent_profile_cannot_be_initialized_from_a_partial_native_delta(
    missing_profile: str,
) -> None:
    """A complete source snapshot does not prove an omitted native profile's inventory.

    Given: A full source snapshot containing one explicitly observed profile kind,
    When: A later delta introduces a different profile without its own full snapshot,
    Then: The cache refuses instead of calling a partial map complete native inventory.
    """
    stream = KrakenFuturesBalanceStream()
    initial: dict[str, object]
    delta: dict[str, object]
    if missing_profile == "holding":
        initial = {"flex_futures": {"currencies": {"USD": {"quantity": 10}}}}
        delta = {"holding": {"USD": 2}}
    else:
        initial = {"holding": {"USD": 10}}
        delta = {"flex_futures": {"currencies": {"USD": {"quantity": 2}}}}
    stream.observe(_frame(initial))
    with pytest.raises(ValueError, match="profile_requires_snapshot"):
        stream.observe(
            _frame(
                delta,
                sequence=1,
                receipt=11,
            )
        )
    assert stream.profiles() == ()


def test_delayed_old_full_snapshot_cannot_replace_more_recently_observed_native_state() -> None:
    """A late old callback cannot move current cache state backward across a source reconnect.

    Given: A valid snapshot followed by a more recent contiguous balance delta,
    When: An older full snapshot finishes raw capture after the newer frame,
    Then: The cache invalidates rather than replacing the later state with old quantities.
    """
    stream = KrakenFuturesBalanceStream()
    stream.observe(_snapshot())
    stream.observe(_frame({"holding": {"USD": 12}}, sequence=1, receipt=11))
    with pytest.raises(ValueError, match="source_clock_regressed"):
        stream.observe(_frame({"holding": {"USD": 10}}, receipt=20))
    assert stream.profiles() == ()


def test_original_json_numeric_quantity_never_rounds_through_a_float_mirror() -> None:
    """Native source replay preserves the actual wire numeric token beyond binary float precision.

    Given: An original JSON number with more significant digits than a float can retain,
    When: The native source cache decodes and exposes its exact profile quantity,
    Then: Every reported decimal digit survives.
    """
    source = _frame({"holding": {"USD": "0.100000000000000001"}})
    wire = base64.b64decode(source.observation.material).replace(
        b'"0.100000000000000001"', b"0.100000000000000001"
    )
    source = replace(
        source,
        observation=replace(source.observation, material=base64.b64encode(wire).decode()),
    )
    stream = KrakenFuturesBalanceStream()
    stream.observe(source)
    assert stream.profiles()[0].quantities[0].quantity == Decimal("0.100000000000000001")
