"""Current admission proof graph preserves original units and real source identity."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_unit_admission import EconomicUnitContext
from snapper.core.execution_unit_admission import ExecutionRouteReference
from snapper.core.execution_unit_admission import InstrumentSourceObservation
from snapper.core.execution_unit_admission import SubmissionUnitAdmissionPayload
from snapper.core.execution_unit_admission import canonical_unit_admission_payload
from snapper.core.execution_unit_admission import economic_context_digest
from snapper.core.execution_unit_admission import economic_unit_context
from snapper.core.execution_unit_admission import unit_admission_content_digest
from snapper.core.execution_unit_snapshot import SnapshotCatalog
from snapper.core.execution_unit_snapshot import SnapshotCommandMaterial
from snapper.core.execution_unit_snapshot import SnapshotDimensions
from snapper.core.execution_unit_snapshot import SnapshotRebaseContext
from snapper.core.execution_unit_snapshot import SnapshotScope
from tests.helpers.unit_admission import admission_id
from tests.helpers.unit_admission import admission_identity

_NOW = datetime(2026, 10, 23, 6, tzinfo=UTC)
_SPEC_AT = _NOW - timedelta(hours=1)


@pytest.fixture
def context() -> EconomicUnitContext:
    """Provide one complete explicit USD linear-contract economic profile."""
    return EconomicUnitContext(
        base_currency="BTC",
        quote_currency="USD",
        native_order_quantity_unit="base_asset",
        internal_quantity_unit="contract_count",
        quantity_quantum=Decimal("0.0001"),
        price_quantum=Decimal("0.5"),
        price_currency="USD",
        price_quantity_convention="quote_per_base",
        catalog_contract_multiplier=Decimal(1),
        payoff_model="linear",
        contract_multiplier=Decimal(1),
        contract_value_currency="BTC",
        settlement_currency="USD",
        quantity_convention="not_applicable",
        instrument_kind="perpetual",
        expiry_at=None,
    )


@pytest.fixture
def payload(context: EconomicUnitContext) -> SubmissionUnitAdmissionPayload:
    """Provide a whole explicit account, source, current catalog and original snapshot graph."""
    catalog = SnapshotCatalog(
        spec_public_id=admission_id(4),
        spec_timestamp=_SPEC_AT,
        source="kraken_futures:rest.get_instruments",
        version="s2a-v3:fixture",
        observed_at=_SPEC_AT,
    )
    source = InstrumentSourceObservation(
        exchange="kraken_futures",
        instrument_public_id=admission_id(2),
        instrument_timestamp=_SPEC_AT,
        symbol_public_id=admission_id(1),
        symbol_timestamp=_SPEC_AT,
        catalog=catalog,
    )
    return SubmissionUnitAdmissionPayload(
        attempt_public_id=admission_id(96),
        route=ExecutionRouteReference(
            authority_public_id=admission_id(93),
            authority_content_digest="c" * 64,
            scope=ExecutionAccountScope(
                wallet_public_id=admission_id(3),
                exchange="kraken_futures",
                mode="live",
                environment="production",
                margin_account="futures.flex",
            ),
            route_revision="b" * 64,
        ),
        scope=SnapshotScope(
            wallet_public_id=admission_id(3),
            exchange="kraken_futures",
            mode="live",
            instrument_public_id=admission_id(2),
        ),
        command_snapshot_public_id=admission_id(2096),
        command_snapshot_content_digest="d" * 64,
        request_material=SnapshotCommandMaterial(
            command_public_id=admission_id(5),
            wallet_public_id=admission_id(3),
            exchange="kraken_futures",
            mode="live",
            native_symbol="BTC-USD",
            client_order_id="client-1",
            side="buy",
            order_type="limit",
            quantity=Decimal(2),
            price=Decimal(100),
            stop_price=None,
            leverage=None,
            post_only=True,
            reduce_only=False,
        ),
        current_catalog=catalog,
        current_instrument_timestamp=_SPEC_AT,
        current_symbol_public_id=admission_id(1),
        current_symbol_timestamp=_SPEC_AT,
        command_source=source,
        current_source=source,
        economic_context=context,
        interpreted_at=_NOW,
    )


def test_benign_catalog_refresh_preserves_units_but_records_the_actual_revision(
    payload: SubmissionUnitAdmissionPayload,
) -> None:
    """Observation refreshes do not permanently disable an unchanged economic intent.

    Given: Original and current contexts have the same exact economic dimensions,
    When: Only the actual current catalog revision/provider clock changes,
    Then: The witness changes while its economic fingerprint remains identical.
    """
    data = payload.model_dump(mode="python")
    current = payload.current_catalog.model_dump(mode="python")
    current.update(
        spec_timestamp=_NOW - timedelta(seconds=1),
        version="s2a-v3:refresh",
        observed_at=_NOW - timedelta(seconds=1),
    )
    data["current_catalog"] = current
    data["current_source"]["catalog"] = current
    refreshed = SubmissionUnitAdmissionPayload.model_validate(data)

    assert economic_context_digest(refreshed.economic_context) == economic_context_digest(
        payload.economic_context
    )
    assert unit_admission_content_digest(refreshed) != unit_admission_content_digest(payload)
    assert refreshed.current_catalog.version == "s2a-v3:refresh"
    assert refreshed.command_source.catalog.version == "s2a-v3:fixture"


def test_admission_roundtrip_preserves_whole_graph(
    payload: SubmissionUnitAdmissionPayload,
) -> None:
    """Current placement evidence never overwrites or re-hashes its original v1 record.

    Given: A complete version-two proof graph,
    When: It is canonically serialized and parsed,
    Then: All source/account/snapshot references and exact dimensions survive.
    """
    restored = SubmissionUnitAdmissionPayload.model_validate_json(
        canonical_unit_admission_payload(payload)
    )
    assert restored == payload
    assert restored.command_snapshot_content_digest == "d" * 64
    assert unit_admission_content_digest(restored) == unit_admission_content_digest(payload)


@pytest.mark.parametrize(
    "field",
    (
        "quantity_quantum",
        "price_quantum",
        "native_order_quantity_unit",
        "internal_quantity_unit",
        "instrument_kind",
        "expiry_at",
        "quantity_convention",
    ),
)
def test_actual_unit_or_contract_changes_change_the_economic_fingerprint(
    context: EconomicUnitContext, field: str
) -> None:
    """A numerically similar revision cannot silently replace contract or quantity semantics.

    Given: One explicit known economic interpretation,
    When: A quantity/price quantum, unit label, expiry or convention changes,
    Then: Its economic identity changes and must be compared before admission.
    """
    changes: dict[str, object] = {
        "quantity_quantum": Decimal("0.001"),
        "price_quantum": Decimal("1"),
        "native_order_quantity_unit": "contract_count",
        "internal_quantity_unit": "base_asset",
        "instrument_kind": "future",
        "expiry_at": _NOW + timedelta(days=1),
        "quantity_convention": "spv_token",
    }
    data = context.model_dump(mode="python")
    data[field] = changes[field]
    changed = EconomicUnitContext.model_validate(data)
    assert economic_context_digest(changed) != economic_context_digest(context)


def test_inverse_payoff_is_distinct_and_keeps_quote_multiplier_base_settlement(
    context: EconomicUnitContext,
) -> None:
    """Payoff identity includes denomination rather than just the word inverse.

    Given: A complete linear contract,
    When: A real inverse payoff is explicitly described,
    Then: Its fingerprint changes and denomination is preserved.
    """
    data = context.model_dump(mode="python")
    data.update(payoff_model="inverse", contract_value_currency="USD", settlement_currency="BTC")
    inverse = EconomicUnitContext.model_validate(data)
    assert inverse.settlement_currency == "BTC"
    assert economic_context_digest(inverse) != economic_context_digest(context)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("settlement_currency", None),
        ("contract_multiplier", Decimal(2)),
        ("price_currency", "EUR"),
        ("settlement_currency", "BTC"),
        ("contract_value_currency", "USD"),
    ),
)
def test_partial_or_crossed_economic_dimensions_are_refused(
    context: EconomicUnitContext, field: str, value: object
) -> None:
    """Equality cannot certify contradictory currency or multiplier dimensions.

    Given: A partial payoff or inconsistent denomination,
    When: The economic context is revalidated,
    Then: No fingerprint is accepted for that interpretation.
    """
    data = context.model_dump(mode="python")
    data[field] = value
    with pytest.raises(ValidationError):
        EconomicUnitContext.model_validate(data)


def test_inverse_wrong_settlement_is_refused(context: EconomicUnitContext) -> None:
    """An inverse label cannot retain linear-settlement cash semantics.

    Given: A linear context relabeled inverse without changing currencies,
    When: The context is validated,
    Then: The impossible denomination tuple is refused.
    """
    data = context.model_dump(mode="python")
    data["payoff_model"] = "inverse"
    with pytest.raises(ValidationError, match="Inverse payoff"):
        EconomicUnitContext.model_validate(data)


def test_context_projection_refuses_unknown_quantity_price_and_rebase(
    context: EconomicUnitContext,
) -> None:
    """Unknown labels matching each other never become financial compatibility proof.

    Given: An honest v1 snapshot with unknown native units/price/rebase convention,
    When: A complete financial-admission context is requested,
    Then: The unknown interpretation is refused.
    """
    dimensions = SnapshotDimensions(
        base_currency="BTC",
        quote_currency="USD",
        native_order_quantity_unit=None,
        internal_quantity_unit=None,
        signed_quantity_unit=None,
        side="buy",
        signed_quantity=Decimal(2),
        quantity_quantum=None,
        price=Decimal(100),
        price_quantum=None,
        price_currency=None,
        price_quantity_convention="unknown",
        catalog_contract_multiplier=None,
        payoff_model=None,
        contract_multiplier=None,
        contract_value_currency=None,
        settlement_currency=None,
    )
    with pytest.raises(ValidationError):
        economic_unit_context(
            dimensions, SnapshotRebaseContext(quantity_convention="unknown"), "perpetual", None
        )
    known = dimensions.model_dump(mode="python")
    known.update(
        native_order_quantity_unit="base_asset",
        internal_quantity_unit="contract_count",
        signed_quantity_unit="base_asset",
        price_currency="USD",
        price_quantity_convention="quote_per_base",
        catalog_contract_multiplier=context.catalog_contract_multiplier,
        payoff_model=context.payoff_model,
        contract_multiplier=context.contract_multiplier,
        contract_value_currency=context.contract_value_currency,
        settlement_currency=context.settlement_currency,
        quantity_quantum=context.quantity_quantum,
        price_quantum=context.price_quantum,
    )
    projected = economic_unit_context(
        SnapshotDimensions.model_validate(known),
        SnapshotRebaseContext(quantity_convention="not_applicable"),
        "perpetual",
        None,
    )
    assert projected == context


@pytest.mark.parametrize("section", ("scope", "request_material", "route"))
def test_request_target_and_selected_account_must_share_actual_wallet(
    payload: SubmissionUnitAdmissionPayload, section: str
) -> None:
    """A correctly shaped reference cannot authorize another wallet.

    Given: A proof graph crossing its actual account or target wallet,
    When: The current admission is validated,
    Then: The crossed financial scope is refused.
    """
    data = payload.model_dump(mode="python")
    if section == "route":
        data[section]["scope"]["wallet_public_id"] = admission_id(77)
    else:
        data[section]["wallet_public_id"] = admission_id(77)
    with pytest.raises(ValidationError, match="scope must agree"):
        SubmissionUnitAdmissionPayload.model_validate(data)


@pytest.mark.parametrize("field", ("exchange", "instrument_public_id", "symbol_public_id"))
def test_source_identity_change_is_not_hidden_by_same_units_or_symbol_spelling(
    payload: SubmissionUnitAdmissionPayload, field: str
) -> None:
    """Paper/source drift cannot reuse the same visible target as a different market.

    Given: The source has equal-looking economic dimensions,
    When: Its venue, instrument UUID or symbol UUID differs from the original,
    Then: The current attempt refuses the changed source identity.
    """
    data = payload.model_dump(mode="python")
    data["current_source"][field] = "kraken" if field == "exchange" else admission_id(77)
    with pytest.raises(ValidationError, match="source identity changed"):
        SubmissionUnitAdmissionPayload.model_validate(data)


def test_paper_target_requires_distinct_real_source_and_live_target_cannot_cross_it(
    payload: SubmissionUnitAdmissionPayload,
) -> None:
    """Source and execution UUIDs are separate only for an explicitly simulated target.

    Given: A known real source and a Paper target,
    When: Their scopes are coherently updated or the real source is relabeled Paper,
    Then: Only the explicit Paper-to-real-source graph validates.
    """
    data = payload.model_dump(mode="python")
    data["scope"].update(exchange="paper", mode="paper", instrument_public_id=admission_id(140))
    data["request_material"].update(exchange="paper", mode="paper")
    data["route"]["scope"].update(exchange="paper", mode="paper", environment="paper")
    paper = SubmissionUnitAdmissionPayload.model_validate(data)
    assert paper.current_source.instrument_public_id == admission_id(2)
    assert paper.scope.instrument_public_id == admission_id(140)
    for section in ("command_source", "current_source"):
        data[section]["exchange"] = "paper"
    with pytest.raises(ValidationError, match="explicit real source"):
        SubmissionUnitAdmissionPayload.model_validate(data)
    live = payload.model_dump(mode="python")
    live["scope"]["instrument_public_id"] = admission_id(77)
    with pytest.raises(ValidationError, match="actual execution instrument"):
        SubmissionUnitAdmissionPayload.model_validate(live)


@pytest.mark.parametrize("version", (True, 2.0, "2"))
def test_v2_admission_requires_exact_integer_schema(
    payload: SubmissionUnitAdmissionPayload, version: object
) -> None:
    """A coercible version cannot silently alter the admission hash domain.

    Given: A non-integer scalar equal-looking to the admission version,
    When: The graph is parsed,
    Then: The malformed version is refused.
    """
    data = payload.model_dump(mode="python")
    data["schema_version"] = version
    with pytest.raises(ValidationError, match="strict integer"):
        SubmissionUnitAdmissionPayload.model_validate(data)


def test_future_catalog_evidence_and_reused_record_identities_are_refused(
    payload: SubmissionUnitAdmissionPayload,
) -> None:
    """Actual clock order and independent immutable identities remain part of the proof.

    Given: Evidence observed after interpretation or duplicated record identities,
    When: The producer graph is validated,
    Then: Neither claim is silently accepted.
    """
    data = payload.model_dump(mode="python")
    data["current_catalog"]["observed_at"] = _NOW + timedelta(seconds=1)
    with pytest.raises(ValidationError, match="after interpretation"):
        SubmissionUnitAdmissionPayload.model_validate(data)
    identity = admission_identity().model_dump(mode="python")
    identity["admission"]["public_id"] = identity["command_snapshot"]["public_id"]
    with pytest.raises(ValidationError, match="distinct record UUIDs"):
        type(admission_identity()).model_validate(identity)


def test_hash_boundary_revalidates_unchecked_admission_copy(
    payload: SubmissionUnitAdmissionPayload,
) -> None:
    """A frozen-model copy cannot bypass full source/scope validation.

    Given: A copied witness with interpretation earlier than every actual observation,
    When: Its canonical content or digest is requested,
    Then: It is refused at the evidence boundary.
    """
    altered = payload.model_copy(update={"interpreted_at": _SPEC_AT - timedelta(seconds=1)})
    with pytest.raises(ValidationError):
        canonical_unit_admission_payload(altered)
    with pytest.raises(ValidationError):
        unit_admission_content_digest(altered)
