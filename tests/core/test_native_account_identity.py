"""Strict public key identity, actual read-attempt binding and retained owner contradictions."""

import hashlib
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest
from pydantic import ValidationError

from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.native_account_identity import NativeAccountIdentityProofPayload
from snapper.core.native_account_identity import NativeAccountIdentityRootReference
from snapper.core.native_account_identity import NativeAccountPublicIdentity
from snapper.core.native_account_identity import canonical_native_account_identity
from snapper.core.native_account_identity import native_account_identity_digest
from snapper.core.native_account_read import NativeAccountReadAttemptPayload
from snapper.core.native_account_read import native_account_read_attempt_digest

_NOW = datetime(2026, 10, 23, 13, tzinfo=UTC)


def _id(number: int) -> str:
    """Keep synthetic public client, request, external account and root proof identities separate."""
    return f"00000000-0000-7000-8000-{number:012d}"


def _selection(**updates: object) -> ExecutionClientSelection:
    """Represent actual supported public routing without any API key or secret."""
    return ExecutionClientSelection.model_validate(
        {
            "wallet_public_id": _id(1),
            "exchange": "kraken_futures",
            "mode": "live",
            "environment": "production",
            "credential_public_id": _id(2),
            "credential_timestamp": _NOW - timedelta(days=1),
            "public_api_origin": "https://futures.kraken.com",
            **updates,
        }
    )


def _observation(**updates: object) -> NativeAccountPublicIdentity:
    """Provide only whitelisted public observations after an actual producer key-match check."""
    return NativeAccountPublicIdentity.model_validate(
        {
            "client_selection": _selection(),
            "account_uid": _id(90),
            "general_access": "READ_ONLY",
            "transfer_access": "NO_ACCESS",
            "request_started_at": _NOW + timedelta(seconds=1),
            "observed_at": _NOW + timedelta(seconds=2),
            **updates,
        }
    )


def _proof(**updates: object) -> NativeAccountIdentityProofPayload:
    """Bind public identity to one original committed request without a fabricated earlier owner."""
    attempt = NativeAccountReadAttemptPayload(
        client_selection=_selection(), family_sequence=1, started_at=_NOW
    )
    return NativeAccountIdentityProofPayload.model_validate(
        {
            "attempt_public_id": _id(10),
            "attempt_content_digest": native_account_read_attempt_digest(attempt),
            "attempt": attempt,
            "observation": _observation(),
            "recorded_at": _NOW + timedelta(seconds=3),
            "owner_root": None,
            **updates,
        }
    )


def test_canonical_public_identity_roundtrips_without_any_sensitive_response_field() -> None:
    """Public provenance has a stable separate hash without copying the provider's sensitive body.

    Given: One actual selected account-read attempt and whitelisted identity observation,
    When: The immutable proof is serialized, hashed and independently reconstructed,
    Then: Public facts survive exactly and the request/raw financial hash domains remain distinct.
    """
    proof = _proof()
    canonical = canonical_native_account_identity(proof)
    restored = NativeAccountIdentityProofPayload.model_validate_json(canonical)
    assert restored == proof
    assert restored.matches_owner is True
    assert (
        native_account_identity_digest(proof)
        == hashlib.sha256(
            b"snapper:native-account-public-identity:v1\x00" + canonical.encode()
        ).hexdigest()
    )
    assert native_account_identity_digest(proof) != native_account_read_attempt_digest(
        proof.attempt
    )
    for field in ("apiKey", "api_key", "signature", "iiban", "body"):
        assert field not in canonical


@pytest.mark.parametrize("account_uid,expected", ((_id(90), True), (_id(91), False)))
def test_owner_conflict_remains_retained_evidence_instead_of_replacing_the_original_account(
    account_uid: str,
    expected: bool,
) -> None:
    """A changed external account is observable without becoming the wallet's new owner.

    Given: An independently rebound original owner reference,
    When: The same or a different account UID is observed during a later read,
    Then: The exact observation survives and the match decision remains explicit.
    """
    root = NativeAccountIdentityRootReference(
        public_id=_id(50), content_digest="a" * 64, account_uid=_id(90)
    )
    proof = _proof(owner_root=root, observation=_observation(account_uid=account_uid))
    assert proof.matches_owner is expected
    assert proof.observation.account_uid == account_uid
    assert proof.owner_root == root


@pytest.mark.parametrize("access", ("NO_ACCESS", "READ_ONLY", "FULL_ACCESS"))
def test_native_permission_levels_are_source_values_separate_from_identity_ownership(
    access: str,
) -> None:
    """Restricted credentials can still provide honest identity observations.

    Given: Each documented general-access level,
    When: A strict public identity proof is retained,
    Then: Its permission remains an explicit source value without changing account ownership.
    """
    proof = _proof(observation=_observation(general_access=access))
    assert proof.observation.general_access == access
    assert proof.matches_owner is True


@pytest.mark.parametrize(
    "field,value",
    (
        ("apiKey", "synthetic-sensitive-value"),
        ("iiban", "synthetic-sensitive-value"),
        ("body", "synthetic-sensitive-value"),
        ("signature", "synthetic-sensitive-value"),
        ("general_access", True),
        ("transfer_access", "full_access"),
        ("account_uid", "not-an-external-uuid"),
        ("source_policy", "generic_parsed_metadata"),
    ),
)
def test_public_identity_rejects_extra_sensitive_fields_and_coerced_source_labels(
    field: str, value: object
) -> None:
    """The stored public shape cannot accidentally retain an original key-check response.

    Given: A secret-bearing extra field or a noncanonical source value,
    When: The public observation boundary validates it,
    Then: It refuses without printing the supplied sensitive input in its error text.
    """
    with pytest.raises(ValidationError) as error:
        _observation(**{field: value})
    assert "synthetic-sensitive-value" not in str(error.value)


@pytest.mark.parametrize(
    "selection",
    (
        {"exchange": "kraken"},
        {"public_api_origin": "https://other.invalid"},
        {"environment": "demo"},
        {
            "exchange": "paper",
            "mode": "paper",
            "environment": "paper",
            "credential_public_id": None,
            "credential_timestamp": None,
            "public_api_origin": None,
        },
    ),
)
def test_native_key_identity_requires_the_actual_supported_client_route(
    selection: dict[str, object],
) -> None:
    """A public account identifier is not proof that the correct authenticated source returned it.

    Given: A syntactically valid client with a crossed exchange, mode or endpoint,
    When: It is bound to native key identity,
    Then: The incompatible source is refused.
    """
    with pytest.raises(ValidationError, match="source route"):
        _observation(client_selection=_selection(**selection))


@pytest.mark.parametrize("change", ("credential", "request", "attempt", "recording"))
def test_original_identity_clocks_cannot_precede_their_actual_source_or_follow_retention(
    change: str,
) -> None:
    """Separate local clocks are retained instead of being rewritten to fit a caller's interval.

    Given: A future credential or a backwards request, attempt or recording boundary,
    When: The original public identity is bound,
    Then: The inconsistent chronology is refused.
    """
    with pytest.raises(ValidationError, match="chronology|knowledge interval"):
        if change == "credential":
            _observation(
                client_selection=_selection(credential_timestamp=_NOW + timedelta(seconds=2))
            )
        elif change == "request":
            _observation(request_started_at=_NOW + timedelta(seconds=3))
        elif change == "attempt":
            _proof(observation=_observation(request_started_at=_NOW - timedelta(seconds=1)))
        else:
            _proof(recorded_at=_NOW + timedelta(seconds=1))


@pytest.mark.parametrize("version", (True, 1.0, 2))
def test_proof_version_has_no_boolean_float_or_future_alias(version: object) -> None:
    """Version identity remains explicit across canonical serialization.

    Given: A coercible alias or an unsupported proof version,
    When: The immutable payload is validated,
    Then: It is refused instead of acquiring version-one semantics.
    """
    with pytest.raises(ValidationError):
        _proof(schema_version=version)


@pytest.mark.parametrize("change", ("digest", "selection"))
def test_public_identity_must_belong_to_the_original_actual_request(change: str) -> None:
    """Matching account labels cannot replace the independently committed attempt.

    Given: A changed attempt digest or a different selected wallet,
    When: The identity proof is assembled,
    Then: It refuses the substituted request binding.
    """
    with pytest.raises(ValidationError, match="attempt digest|different selected client"):
        if change == "digest":
            _proof(attempt_content_digest="b" * 64)
        else:
            _proof(observation=_observation(client_selection=_selection(wallet_public_id=_id(3))))


def test_unchecked_instance_mutation_is_revalidated_before_canonical_hashing() -> None:
    """Frozen-model escape hatches cannot silently alter proof version semantics.

    Given: An otherwise valid proof changed through a deliberately unchecked model copy,
    When: It reaches the canonical persistence boundary,
    Then: Full instance validation refuses the forged version.
    """
    unsafe = _proof().model_copy(update={"schema_version": True})
    with pytest.raises(ValidationError):
        native_account_identity_digest(unsafe)


def test_actual_demo_identity_is_explicit_without_becoming_a_production_account() -> None:
    """Authenticated demo routing remains separate from both production and Paper.

    Given: An actual selected demo origin and credential,
    When: Its native identity observation is retained,
    Then: The exact demo environment remains visible without a production alias.
    """
    selected = _selection(environment="demo", public_api_origin="https://demo-futures.kraken.com")
    observed = _observation(client_selection=selected)
    assert observed.client_selection.environment == "demo"
    assert observed.client_selection.mode == "live"
