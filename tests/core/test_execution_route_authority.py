"""Account namespace, actual route selection and bounded observation evidence."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest
from pydantic import ValidationError

from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.execution_route_authority import ExecutionRouteAuthorityPayload
from snapper.core.execution_route_authority import ExecutionRouteSelection
from snapper.core.execution_route_authority import account_scope_digest
from snapper.core.execution_route_authority import canonical_route_authority_payload
from snapper.core.execution_route_authority import client_selection_digest
from snapper.core.execution_route_authority import client_selection_from_authority
from snapper.core.execution_route_authority import route_authority_content_digest
from snapper.core.execution_route_authority import route_selection_digest

_NOW = datetime(2026, 10, 23, 6, tzinfo=UTC)


def _id(number: int) -> str:
    """Keep isolated semantic fixture identities explicit and canonical."""
    return f"00000000-0000-7000-8000-{number:012d}"


@pytest.fixture
def scope() -> ExecutionAccountScope:
    """Provide an explicit production cash account without instrument identity."""
    return ExecutionAccountScope(
        wallet_public_id=_id(3),
        exchange="kraken",
        mode="live",
        environment="production",
        margin_account="spot.cash",
    )


@pytest.fixture
def authority(scope: ExecutionAccountScope) -> ExecutionRouteAuthorityPayload:
    """Provide exact selected public metadata and bounded decoded account evidence."""
    credential_at = _NOW - timedelta(hours=1)
    origin = "https://api.kraken.com"
    return ExecutionRouteAuthorityPayload(
        scope=scope,
        credential_public_id=_id(90),
        credential_timestamp=credential_at,
        public_api_origin=origin,
        account_observation_public_id=_id(91),
        account_observation_timestamp=_NOW - timedelta(seconds=1),
        balance_observed_at=_NOW - timedelta(seconds=2),
        balance_receipt_json='[ { "currency": "USD", "total_decimal": "100.00" } ]',
        observed_at=_NOW,
        valid_until=_NOW + timedelta(seconds=60),
        route_revision=route_selection_digest(scope, _id(90), credential_at, origin),
    )


def test_canonical_roundtrip_preserves_decoded_receipt_verbatim(
    authority: ExecutionRouteAuthorityPayload,
) -> None:
    """Decoded JSON fidelity never becomes an invented original-wire claim.

    Given: Exact decoded balance material with meaningful decimal spelling,
    When: The whole authority is canonically serialized and parsed,
    Then: Scope and every byte of decoded material survive with a stable digest.
    """
    canonical = canonical_route_authority_payload(authority)
    restored = ExecutionRouteAuthorityPayload.model_validate_json(canonical)

    assert restored == authority
    assert restored.balance_receipt_json == authority.balance_receipt_json
    assert route_authority_content_digest(restored) == route_authority_content_digest(authority)


def test_rotating_credentials_changes_route_not_the_shared_cash_namespace(
    authority: ExecutionRouteAuthorityPayload,
) -> None:
    """A changed credential cannot create a second spendable account by changing its digest.

    Given: One logical wallet/venue/environment/margin account,
    When: A different credential revision is explicitly selected,
    Then: Its route/authority differ while the logical account identity stays identical.
    """
    data = authority.model_dump(mode="python")
    data.update(
        credential_public_id=_id(190),
        route_revision=route_selection_digest(
            authority.scope,
            _id(190),
            authority.credential_timestamp,
            authority.public_api_origin,
        ),
    )
    rotated = ExecutionRouteAuthorityPayload.model_validate(data)

    assert account_scope_digest(rotated.scope) == account_scope_digest(authority.scope)
    assert rotated.route_revision != authority.route_revision
    assert route_authority_content_digest(rotated) != route_authority_content_digest(authority)


def test_raw_client_selection_is_shared_before_margin_account_interpretation(
    authority: ExecutionRouteAuthorityPayload,
) -> None:
    """Raw receipts can prove the selected client without inventing an unknown cash account.

    Given: One authenticated client exposing two actual margin accounts,
    When: Their account authorities are projected onto the selected client,
    Then: The client fingerprint agrees while their cash-account scopes differ.
    """
    account = authority.scope.model_dump(mode="python")
    account["margin_account"] = "separate-margin-account"
    other_scope = ExecutionAccountScope.model_validate(account)
    selection = ExecutionRouteSelection(
        scope=other_scope,
        credential_public_id=authority.credential_public_id,
        credential_timestamp=authority.credential_timestamp,
        public_api_origin=authority.public_api_origin,
    )
    original_client = client_selection_from_authority(authority)
    other_client = client_selection_from_authority(selection)

    assert original_client == other_client
    assert client_selection_digest(original_client) == client_selection_digest(other_client)
    assert account_scope_digest(other_scope) != account_scope_digest(authority.scope)
    assert "margin_account" not in original_client.model_dump()
    assert "instrument_public_id" not in original_client.model_dump()
    assert (
        ExecutionClientSelection.model_validate_json(original_client.model_dump_json())
        == original_client
    )


def test_raw_client_identity_is_revalidated_before_hashing(
    authority: ExecutionRouteAuthorityPayload,
) -> None:
    """A forged raw-client environment cannot be blessed through an unchecked model copy.

    Given: An actual live client with a copied Paper environment,
    When: Its public client fingerprint is requested,
    Then: The contradictory selection is refused.
    """
    original = client_selection_from_authority(authority)
    altered = original.model_copy(update={"environment": "paper"})
    with pytest.raises(ValidationError, match="must agree"):
        client_selection_digest(altered)


@pytest.mark.parametrize("field", ("wallet_public_id", "exchange", "environment", "margin_account"))
def test_account_dimension_changes_never_alias_the_old_cash_scope(
    scope: ExecutionAccountScope, field: str
) -> None:
    """Distinct custody environments and margin accounts never share a ledger namespace.

    Given: A valid explicit actual account scope,
    When: One real account dimension changes,
    Then: Its namespace digest changes independently of credentials and instruments.
    """
    replacements = {
        "wallet_public_id": _id(4),
        "exchange": "kraken_futures",
        "environment": "demo",
        "margin_account": "futures.flex",
    }
    updated = scope.model_dump(mode="python")
    updated[field] = replacements[field]

    assert account_scope_digest(
        ExecutionAccountScope.model_validate(updated)
    ) != account_scope_digest(scope)


@pytest.mark.parametrize(
    ("exchange", "mode", "environment"),
    (
        ("paper", "live", "production"),
        ("paper", "paper", "demo"),
        ("kraken", "paper", "paper"),
        ("kraken", "live", "paper"),
    ),
)
def test_simulated_and_authenticated_scopes_cannot_be_mixed(
    exchange: str, mode: str, environment: str
) -> None:
    """A simulated source cannot acquire live account authority by relabeling one field.

    Given: Contradictory exchange, execution mode and environment,
    When: An account scope is validated,
    Then: The contradiction is refused before hashing or storage.
    """
    with pytest.raises(ValidationError, match="must agree"):
        ExecutionAccountScope.model_validate(
            {
                "wallet_public_id": _id(3),
                "exchange": exchange,
                "mode": mode,
                "environment": environment,
                "margin_account": "cash",
            }
        )


@pytest.mark.parametrize(
    "origin",
    (
        "http://api.kraken.com",
        "https://fixture-user:fixture-password@api.kraken.com",
        "https://api.kraken.com?api_key=fixture-secret",
        "https://api.kraken.com/private/orders",
        "https://api.kraken.com#fixture-fragment",
        "https://api.kraken.com/",
        "https:///",
        "https://api.kraken.com:invalid",
    ),
)
def test_public_origin_refuses_authentication_parameters_and_request_payloads(
    scope: ExecutionAccountScope, origin: str
) -> None:
    """Route fingerprints accept public routing metadata only.

    Given: A URL containing a credential, parameter, path or malformed endpoint,
    When: Selected-route evidence is constructed,
    Then: It is refused without including the supplied secret in the error text.
    """
    with pytest.raises(ValidationError) as raised:
        ExecutionRouteSelection(
            scope=scope,
            credential_public_id=_id(90),
            credential_timestamp=_NOW - timedelta(hours=1),
            public_api_origin=origin,
        )
    assert "fixture-password" not in str(raised.value)
    assert "fixture-secret" not in str(raised.value)


@pytest.mark.parametrize(
    ("credential_id", "credential_at", "origin"),
    (
        (None, _NOW, "https://api.kraken.com"),
        (_id(90), None, "https://api.kraken.com"),
        (None, None, "https://api.kraken.com"),
        (_id(90), _NOW, None),
    ),
)
def test_live_selection_requires_complete_actual_credential_and_origin(
    scope: ExecutionAccountScope,
    credential_id: str | None,
    credential_at: datetime | None,
    origin: str | None,
) -> None:
    """Missing route data is explicit refusal rather than a production default.

    Given: Partial selected-credential or actual client-origin evidence,
    When: An authenticated route is validated,
    Then: No route fingerprint or authority is fabricated.
    """
    with pytest.raises(ValidationError):
        ExecutionRouteSelection(
            scope=scope,
            credential_public_id=credential_id,
            credential_timestamp=credential_at,
            public_api_origin=origin,
        )


def test_paper_selection_is_explicit_and_cannot_claim_a_private_api_origin() -> None:
    """Paper credentials can be absent honestly, while its venue remains simulated.

    Given: An explicitly simulated Paper account,
    When: A local route is selected or an authenticated origin is added,
    Then: The local route validates and the authenticated-origin claim is refused.
    """
    scope = ExecutionAccountScope(
        wallet_public_id=_id(3),
        exchange="paper",
        mode="paper",
        environment="paper",
        margin_account="paper.cash",
    )
    selection = ExecutionRouteSelection(
        scope=scope, credential_public_id=None, credential_timestamp=None, public_api_origin=None
    )
    assert selection.scope.environment == "paper"
    with pytest.raises(ValidationError, match="cannot claim"):
        ExecutionRouteSelection(
            scope=scope,
            credential_public_id=None,
            credential_timestamp=None,
            public_api_origin="https://api.kraken.com",
        )


@pytest.mark.parametrize("version", (True, 1.0, "1"))
def test_authority_version_is_a_strict_integer(
    authority: ExecutionRouteAuthorityPayload, version: object
) -> None:
    """Equal-looking version scalars cannot silently select the authority hash domain.

    Given: A valid authority and a coercible non-integer version,
    When: It is validated at the storage boundary,
    Then: The disguised version is refused.
    """
    data = authority.model_dump(mode="python")
    data["schema_version"] = version
    with pytest.raises(ValidationError, match="strict integer"):
        ExecutionRouteAuthorityPayload.model_validate(data)


@pytest.mark.parametrize("material", ('{"a":1,"a":2}', "[NaN]", "{}", "null", "5"))
def test_balances_require_unambiguous_decoded_array_evidence(
    authority: ExecutionRouteAuthorityPayload, material: str
) -> None:
    """Ambiguous JSON and non-balance shapes never become account evidence.

    Given: Invalid or non-array decoded balance material,
    When: The authority is parsed,
    Then: It is refused before a digest can bless the payload.
    """
    data = authority.model_dump(mode="python")
    data["balance_receipt_json"] = material
    with pytest.raises(ValidationError):
        ExecutionRouteAuthorityPayload.model_validate(data)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("balance_observed_at", _NOW + timedelta(seconds=1)),
        ("account_observation_timestamp", _NOW + timedelta(seconds=1)),
        ("observed_at", _NOW - timedelta(seconds=10)),
        ("valid_until", _NOW),
        ("credential_timestamp", _NOW),
        ("route_revision", "f" * 64),
    ),
)
def test_authority_clocks_and_selected_route_are_not_independent_claims(
    authority: ExecutionRouteAuthorityPayload, field: str, value: object
) -> None:
    """A fresh label cannot replace actual response ordering or selected-route identity.

    Given: One changed clock or route fingerprint,
    When: The authority is revalidated,
    Then: Contradictory evidence is refused.
    """
    data = authority.model_dump(mode="python")
    data[field] = value
    with pytest.raises(ValidationError):
        ExecutionRouteAuthorityPayload.model_validate(data)


def test_unchecked_model_copy_is_revalidated_at_hash_boundaries(
    authority: ExecutionRouteAuthorityPayload,
) -> None:
    """Frozen models do not excuse unchecked copy updates at a trust boundary.

    Given: A model-copy update that bypassed the constructor,
    When: The authority is hashed or canonically serialized,
    Then: The invalid bounded-observation claim is revalidated and refused.
    """
    altered = authority.model_copy(update={"valid_until": authority.observed_at})
    with pytest.raises(ValidationError):
        route_authority_content_digest(altered)
    with pytest.raises(ValidationError):
        canonical_route_authority_payload(altered)


def test_producer_identity_rejects_boolean_sequence_and_blank_uuid() -> None:
    """Insertion provenance remains strict even before any database transaction starts.

    Given: Malformed producer identities,
    When: They are validated,
    Then: Neither a boolean counter nor an absent UUID is accepted.
    """
    with pytest.raises(ValidationError):
        ExecutionEvidenceIdentity(public_id=_id(1), session_id=_id(99), sequence_id=True)
    with pytest.raises(ValidationError):
        ExecutionEvidenceIdentity(public_id="", session_id=_id(99), sequence_id=1)
