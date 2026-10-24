"""Keep native entry ownership independent of transport and account subprofiles."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from snapper.application.portfolio.accounting_native_source import (
    AccountingNativeSourceClaimPayload,
)
from snapper.application.portfolio.accounting_native_source import canonical_native_source_claim
from snapper.application.portfolio.accounting_native_source import native_source_claim_digest
from snapper.application.portfolio.accounting_native_source import native_source_family_digest
from snapper.application.portfolio.accounting_native_source import native_source_identity_digest
from snapper.application.portfolio.accounting_native_source import native_source_log_id
from snapper.application.portfolio.accounting_native_source import native_source_semantic_digest
from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_route_authority import ExecutionRouteAuthorityPayload
from snapper.core.execution_route_authority import route_selection_digest
from tests.application.portfolio.test_futures_cash_ledger import _entry
from tests.application.portfolio.test_futures_cash_ledger import _id

_NOW = datetime(2026, 10, 23, 8, tzinfo=UTC)


def _claim() -> AccountingNativeSourceClaimPayload:
    """Provide explicit immutable source and actual-account dimensions for pure validation."""
    scope = ExecutionAccountScope(
        wallet_public_id=_id(1),
        exchange="kraken_futures",
        mode="live",
        environment="production",
        margin_account="f-bch:usd",
    )
    credential_at = _NOW - timedelta(hours=1)
    origin = "https://futures.kraken.com"
    authority = ExecutionRouteAuthorityPayload(
        scope=scope,
        credential_public_id=_id(90),
        credential_timestamp=credential_at,
        public_api_origin=origin,
        account_observation_public_id=_id(91),
        account_observation_timestamp=_NOW,
        balance_observed_at=_NOW,
        balance_receipt_json='[{"currency":"bch","total_decimal":"1.25"}]',
        observed_at=_NOW,
        valid_until=_NOW + timedelta(minutes=1),
        route_revision=route_selection_digest(scope, _id(90), credential_at, origin),
    )
    return AccountingNativeSourceClaimPayload(
        scope=scope,
        accounting_event_public_id=_id(4),
        accounting_event_content_digest="a" * 64,
        raw_receipt_public_id=_id(3),
        route_authority_public_id=_id(2),
        kind="native_cash",
        authority=authority,
        source_entry=_entry(),
    )


def test_native_source_claim_roundtrip_preserves_original_account_and_event_evidence() -> None:
    """Keep original native and route operands even though uniqueness uses a coarser family.

    Given: An actual source entry and explicit credential, account and event references.
    When: The complete claim is canonically serialized and restored.
    Then: All evidence survives with stable full and native body digests.
    """
    claim = _claim()
    restored = AccountingNativeSourceClaimPayload.model_validate_json(
        canonical_native_source_claim(claim)
    )
    assert restored == claim
    assert native_source_claim_digest(restored) == native_source_claim_digest(claim)
    assert len(native_source_claim_digest(claim)) == 64
    assert native_source_log_id(claim.source_entry) == "17"
    assert native_source_identity_digest(claim.source_entry) != native_source_semantic_digest(
        claim.source_entry
    )


def test_margin_and_instrument_changes_cannot_create_another_native_identity() -> None:
    """Exclude margin and asset namespaces from ownership while detecting their changed content.

    Given: One log ID already names a specific native entry in the actual account family.
    When: A contradictory entry changes its margin, asset and contract label.
    Then: Its identity collides while the complete source-body semantics differ.
    """
    claim = _claim()
    entry = claim.source_entry.model_copy(
        update={"margin_account": "other-margin", "asset": "PF_OTHERUSD", "contract": "PF_OTHERUSD"}
    )
    other_scope = claim.scope.model_copy(update={"margin_account": "other-margin"})
    assert native_source_identity_digest(entry) == native_source_identity_digest(claim.source_entry)
    assert native_source_family_digest(other_scope) == native_source_family_digest(claim.scope)
    assert native_source_semantic_digest(entry) != native_source_semantic_digest(claim.source_entry)


@pytest.mark.parametrize(
    "changes",
    (
        {"wallet_public_id": _id(8)},
        {"environment": "demo"},
    ),
)
def test_actual_wallet_and_environment_remain_separate_native_source_families(
    changes: dict[str, object],
) -> None:
    """Retain actual execution isolation without inventing cross-wallet venue ownership.

    Given: A production source family with a known local wallet.
    When: The actual wallet or selected environment changes.
    Then: Native source family digests differ while the native source ID itself remains exact.
    """
    scope = _claim().scope
    changed = ExecutionAccountScope.model_validate({**scope.model_dump(), **changes})
    assert native_source_family_digest(changed) != native_source_family_digest(scope)


def test_transport_pointer_and_optional_account_header_do_not_change_native_body_identity() -> None:
    """Separate selected entry semantics from snapshot position and optional envelope metadata.

    Given: The same source entry is retained from a live frame and a later snapshot.
    When: Its selector changes and a consistent account header becomes available.
    Then: Native body identity and semantics remain the same; the full claim still retains metadata.
    """
    entry = _entry()
    moved = entry.model_copy(update={"target_json_pointer": "/logs/3", "venue_account_id": "A"})
    assert native_source_identity_digest(moved) == native_source_identity_digest(entry)
    assert native_source_semantic_digest(moved) == native_source_semantic_digest(entry)


def test_native_log_identifier_above_database_integer_width_is_retained_exactly() -> None:
    """Avoid a fabricated signed64-bit restriction on documented positive log identifiers.

    Given: A source log ID is larger than the database integer range.
    When: Its native identity is prepared for durable text storage.
    Then: Every decimal digit survives and its bounded digest remains stable.
    """
    entry = _entry().model_copy(update={"source_log_id": 2**128 + 17})
    assert native_source_log_id(entry) == "340282366920938463463374607431768211473"
    assert len(native_source_identity_digest(entry)) == 64


@pytest.mark.parametrize(
    "changes",
    (
        {"schema_version": True},
        {"schema_version": 1.0},
        {"schema_version": 2},
        {"source_domain": "another_source"},
        {"kind": "unproven"},
        {"accounting_event_content_digest": "a" * 63},
        {"source_entry": {**_entry().model_dump(), "margin_account": "other"}},
    ),
)
def test_unbound_or_ambiguous_native_claim_material_is_rejected(changes: dict[str, object]) -> None:
    """Refuse alternate schema aliases and account/source mismatches before durable writes.

    Given: A structurally valid native source claim.
    When: Its immutable discriminator or actual account proof becomes contradictory.
    Then: Strict payload validation refuses the changed material.
    """
    with pytest.raises(ValidationError):
        AccountingNativeSourceClaimPayload.model_validate({**_claim().model_dump(), **changes})


def test_claim_requires_consistent_known_source_and_authority_account_ids() -> None:
    """Retain explicit unknown identities but refuse conflicting actual account evidence.

    Given: The bound authority names a known external account.
    When: The original frame presents that account or a different known account.
    Then: Only consistent known evidence can form a claim.
    """
    claim = _claim()
    authority = claim.authority.model_copy(update={"venue_account_id": "A"})
    valid = AccountingNativeSourceClaimPayload.model_validate(
        {
            **claim.model_dump(),
            "authority": authority,
            "source_entry": claim.source_entry.model_copy(update={"venue_account_id": "A"}),
        }
    )
    assert valid.source_entry.venue_account_id == "A"
    with pytest.raises(ValidationError, match="actual account"):
        AccountingNativeSourceClaimPayload.model_validate(
            {
                **valid.model_dump(),
                "source_entry": valid.source_entry.model_copy(
                    update={"venue_account_id": "B", "fee": Decimal("0.1")}
                ),
            }
        )
