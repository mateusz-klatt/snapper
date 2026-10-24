"""Keep native cash source anchors and postings separate from unproved account completeness."""

import base64
import json
from datetime import UTC
from datetime import datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from snapper.application.portfolio.futures_account_log import FuturesAccountLogEntry
from snapper.application.portfolio.futures_account_log import parse_futures_account_log
from snapper.application.portfolio.futures_cash_ledger import FuturesNativeCashAnchor
from snapper.application.portfolio.futures_cash_ledger import FuturesNativeCashEvent
from snapper.application.portfolio.futures_cash_ledger import account_log_cash_source_identity
from snapper.application.portfolio.futures_cash_ledger import canonical_native_cash_anchor
from snapper.application.portfolio.futures_cash_ledger import canonical_native_cash_event
from snapper.application.portfolio.futures_cash_ledger import native_cash_anchor_digest
from snapper.application.portfolio.futures_cash_ledger import native_cash_event_digest
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.execution_route_authority import ExecutionAccountScope


def _id(number: int) -> str:
    """Return isolated canonical durable evidence identities."""
    return f"00000000-0000-7000-8000-{number:012d}"


def _entry() -> FuturesAccountLogEntry:
    """Provide exact source operands whose fees are already part of the balance change."""
    wire = json.dumps(
        {
            "feed": "account_log",
            "new_entry": {
                "id": 17,
                "date": "2026-10-23T07:00:00Z",
                "asset": "bch",
                "collateral": "bch",
                "margin_account": "f-bch:usd",
                "booking_uid": "Booking-1",
                "info": "funding rate change",
                "old_balance": "1.25",
                "new_balance": "1.5",
                "fee": "0.05",
            },
        }
    ).encode()
    observation = RawAccountingObservation.model_validate(
        {
            "scope": {
                "wallet_public_id": _id(1),
                "exchange": "kraken_futures",
                "mode": "live",
                "environment": "production",
            },
            "route_revision": "actual-client-selection",
            "kind": "account_cash",
            "channel": "account_log",
            "representation": "wire_bytes_base64",
            "material": base64.b64encode(wire).decode(),
            "target_json_pointer": "/new_entry",
            "observed_at": datetime(2026, 10, 23, 8, tzinfo=UTC),
        }
    )
    return parse_futures_account_log(observation)


def _anchor() -> FuturesNativeCashAnchor:
    """Anchor one native currency without a fake local execution watermark."""
    return FuturesNativeCashAnchor(
        scope=ExecutionAccountScope(
            wallet_public_id=_id(1),
            exchange="kraken_futures",
            mode="live",
            environment="production",
            margin_account="f-bch:usd",
        ),
        route_authority_public_id=_id(2),
        source_receipt_public_id=_id(3),
        native_currency="bch",
        balance=Decimal("1.25"),
        source_entry=_entry(),
    )


def _event() -> FuturesNativeCashEvent:
    """Describe one observed balance transition rather than modeled gross trade principal."""
    anchor = _anchor()
    return FuturesNativeCashEvent(
        scope=anchor.scope,
        route_authority_public_id=_id(2),
        anchor_public_id=_id(4),
        raw_receipt_public_id=_id(3),
        ledger_sequence=1,
        source_identity="kraken_futures:account_log:17",
        native_currency="bch",
        cash_before=Decimal("1.25"),
        cash_after=Decimal("1.5"),
        cash_delta=Decimal("0.25"),
        source_entry=anchor.source_entry,
    )


def test_native_cash_payloads_round_trip_all_source_and_unknown_alignment_fields() -> None:
    """Persist deterministic source operands without losing native denomination or absence.

    Given: A real source old balance and its one observed native cash transition.
    When: Complete immutable payloads are serialized for durable replay.
    Then: Revalidation restores exact Decimal source facts and explicitly unknown local alignment.
    """
    anchor = _anchor()
    event = _event()
    anchor_json = canonical_native_cash_anchor(anchor)
    event_json = canonical_native_cash_event(event)
    assert FuturesNativeCashAnchor.model_validate_json(anchor_json) == anchor
    assert FuturesNativeCashEvent.model_validate_json(event_json) == event
    assert json.loads(anchor_json)["source_execution_scope_sequence"] is None
    assert json.loads(anchor_json)["source_execution_prefix_status"] == "unavailable"
    assert json.loads(event_json)["cash_delta"] == "25e-2"
    assert len(native_cash_anchor_digest(anchor)) == 64
    assert len(native_cash_event_digest(event)) == 64
    assert native_cash_event_digest(event) != native_cash_anchor_digest(anchor)
    assert account_log_cash_source_identity(event.source_entry) == "kraken_futures:account_log:17"


@pytest.mark.parametrize(
    "changes",
    (
        {"source_execution_scope_sequence": 0},
        {"source_execution_scope_sequence": 123},
        {"source_execution_prefix_status": "sealed"},
        {"anchor_kind": "venue_account_snapshot"},
        {"balance": Decimal(10000)},
        {"native_currency": "USD"},
        {
            "scope": {
                "wallet_public_id": _id(1),
                "exchange": "kraken_futures",
                "mode": "live",
                "environment": "production",
                "margin_account": "other",
            }
        },
        {
            "scope": {
                "wallet_public_id": _id(1),
                "exchange": "kraken",
                "mode": "live",
                "environment": "production",
                "margin_account": "f-bch:usd",
            }
        },
    ),
)
def test_source_old_balance_cannot_claim_local_alignment_or_another_cash_account(
    changes: dict[str, object],
) -> None:
    """Refuse false zero/max watermarks and projections unsupported by the native source.

    Given: An anchor derived only from one source cash log's old balance.
    When: A caller supplies a sealed prefix, fabricated balance or crossed account/currency.
    Then: The immutable anchor refuses the unsupported claim.
    """
    with pytest.raises(ValidationError):
        FuturesNativeCashAnchor.model_validate({**_anchor().model_dump(), **changes})


@pytest.mark.parametrize(
    "changes",
    (
        {"native_currency": "USD"},
        {"cash_before": Decimal(10000)},
        {"cash_after": Decimal("1.55")},
        {"cash_delta": Decimal("0.3")},
        {"source_identity": "Booking-1"},
        {"ledger_sequence": True},
        {"ledger_sequence": 0},
        {
            "scope": {
                "wallet_public_id": _id(1),
                "exchange": "kraken_futures",
                "mode": "live",
                "environment": "production",
                "margin_account": "other",
            }
        },
    ),
)
def test_native_cash_event_must_conserve_exact_source_balances_and_units(
    changes: dict[str, object],
) -> None:
    """Keep source booking groups, exact log identity and native currency dimensions distinct.

    Given: A valid immutable balance event and its exact original source entry.
    When: Projected units, balances, identity or sequence contradict that source.
    Then: Validation refuses rather than posting guessed or double-counted cash.
    """
    with pytest.raises(ValidationError):
        FuturesNativeCashEvent.model_validate({**_event().model_dump(), **changes})


@pytest.mark.parametrize("version", (True, 1.0, "1", 2))
def test_immutable_cash_schema_discriminators_cannot_be_coerced(version: object) -> None:
    """Prevent one immutable schema from being confused with another through scalar aliases.

    Given: A complete anchor or event with an ambiguous or unsupported version.
    When: Durable cash payload validation occurs.
    Then: Both payload families reject the noncanonical discriminator.
    """
    with pytest.raises(ValidationError):
        FuturesNativeCashAnchor.model_validate(
            {**_anchor().model_dump(), "schema_version": version}
        )
    with pytest.raises(ValidationError):
        FuturesNativeCashEvent.model_validate({**_event().model_dump(), "schema_version": version})
