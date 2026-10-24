"""Explicit immutable unit fixtures shared by versioned admission-envelope tests."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal

from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_unit_admission import EconomicUnitContext
from snapper.core.execution_unit_admission import ExecutionRouteReference
from snapper.core.execution_unit_admission import InstrumentSourceObservation
from snapper.core.execution_unit_admission import SubmissionUnitAdmissionPayload
from snapper.core.execution_unit_snapshot import SnapshotCatalog
from snapper.core.execution_unit_snapshot import SnapshotCommandMaterial
from snapper.core.execution_unit_snapshot import SnapshotScope

_NOW = datetime(2026, 10, 23, 6, tzinfo=UTC)
_SPEC_AT = _NOW - timedelta(hours=1)


def _id(number: int) -> str:
    """Return a deterministic isolated source identity without loading database helpers."""
    return f"00000000-0000-7000-8000-{number:012d}"


def linear_unit_context() -> EconomicUnitContext:
    """Provide one complete explicit USD linear-contract economic profile.

    Returns:
        A fixed economic profile with independent quantity and price dimensions.
    """
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


def unit_admission_payload() -> SubmissionUnitAdmissionPayload:
    """Provide a whole explicit account, source, current catalog and original snapshot graph.

    Returns:
        An illustrative immutable graph without asserting that its source rows exist.
    """
    catalog = SnapshotCatalog(
        spec_public_id=_id(4),
        spec_timestamp=_SPEC_AT,
        source="kraken_futures:rest.get_instruments",
        version="s2a-v3:fixture",
        observed_at=_SPEC_AT,
    )
    source = InstrumentSourceObservation(
        exchange="kraken_futures",
        instrument_public_id=_id(2),
        instrument_timestamp=_SPEC_AT,
        symbol_public_id=_id(1),
        symbol_timestamp=_SPEC_AT,
        catalog=catalog,
    )
    return SubmissionUnitAdmissionPayload(
        attempt_public_id=_id(96),
        route=ExecutionRouteReference(
            authority_public_id=_id(93),
            authority_content_digest="c" * 64,
            scope=ExecutionAccountScope(
                wallet_public_id=_id(3),
                exchange="kraken_futures",
                mode="live",
                environment="production",
                margin_account="futures.flex",
            ),
            route_revision="b" * 64,
        ),
        scope=SnapshotScope(
            wallet_public_id=_id(3),
            exchange="kraken_futures",
            mode="live",
            instrument_public_id=_id(2),
        ),
        command_snapshot_public_id=_id(2096),
        command_snapshot_content_digest="d" * 64,
        request_material=SnapshotCommandMaterial(
            command_public_id=_id(5),
            wallet_public_id=_id(3),
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
        current_symbol_public_id=_id(1),
        current_symbol_timestamp=_SPEC_AT,
        command_source=source,
        current_source=source,
        economic_context=linear_unit_context(),
        interpreted_at=_NOW,
    )
