"""Raw financial receipts preserve evidence before unit or account interpretation."""

import base64
import json
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast

import pytest
from pydantic import ValidationError

from snapper.core.contract_accounting_observation import AccountingObservationScope
from snapper.core.contract_accounting_observation import AccountingWriteProvenance
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.contract_accounting_observation import accounting_hold_scope_digest
from snapper.core.contract_accounting_observation import accounting_observation_digest
from snapper.core.contract_accounting_observation import canonical_accounting_observation

_WALLET = "aabbccdd-eeff-4011-8233-445566778899"
_OTHER = "11223344-5566-4788-99aa-bbccddeeff00"
_OBSERVED = datetime(2026, 10, 23, 6, 0, tzinfo=UTC)


def _scope_data() -> dict[str, object]:
    """Build the actual selected route without asserting known account balances.

    Returns:
        One production futures account family and known margin profile.
    """
    return {
        "wallet_public_id": _WALLET,
        "exchange": "kraken_futures",
        "mode": "live",
        "environment": "production",
        "margin_account": "flex",
    }


def _data() -> dict[str, object]:
    """Construct a parsed observation with honestly unavailable financial authority.

    Returns:
        Fresh raw-envelope fields whose provider material is intentionally opaque.
    """
    return {
        "scope": _scope_data(),
        "route_revision": "selected-public-route-v1",
        "kind": "execution",
        "channel": "futures.websocket.fills",
        "representation": "decoded_json_text",
        "material": '{ "size": "1.000", "price": "100.00" }',
        "observed_at": _OBSERVED,
    }


def test_private_receipt_material_is_retained_without_repr_or_validation_error_leakage() -> None:
    """Keep sensitive source bytes durable while ordinary diagnostics omit their contents.

    Given: A private source payload and a later envelope-level provenance contradiction.
    When: The receipt is represented or its invalid envelope raises a validation error.
    Then: Neither diagnostic exposes source material, while canonical storage retains it.
    """
    material = '{"private_receipt":"AccountSensitive-DoNotLog"}'
    data = {**_data(), "material": material}
    observation = RawAccountingObservation.model_validate(data)
    assert "AccountSensitive-DoNotLog" not in repr(observation)
    assert json.loads(canonical_accounting_observation(observation))["material"] == material
    with pytest.raises(ValidationError) as raised:
        RawAccountingObservation.model_validate({**data, "credential_public_id": _OTHER})
    diagnostic = str(raised.value)
    assert "Credential identity and exact revision" in diagnostic
    assert "AccountSensitive-DoNotLog" not in diagnostic
    assert "input_value" not in diagnostic


@pytest.mark.parametrize("material", ("", " ", "NaN", '{"x":1,"x":2}', "not-json", "\x00"))
@pytest.mark.parametrize(
    "representation", ("decoded_json_text", "parsed_fields_json", "opaque_text")
)
def test_financially_malformed_material_is_retained_verbatim(
    material: str, representation: str
) -> None:
    """Capture malformed data before financial interpretation can reject it.

    Given: Empty, malformed, non-finite or duplicate-key provider material.
    When: An interpretation-free accounting envelope is created.
    Then: Its exact material survives canonicalization without invented amounts.
    """
    data = _data()
    data.update(material=material, representation=representation)
    observation = RawAccountingObservation.model_validate(data)
    decoded = cast(dict[str, object], json.loads(canonical_accounting_observation(observation)))
    assert decoded["material"] == material
    assert observation.route_authority_public_id is None
    assert observation.source_identity is None
    assert observation.source_instrument_public_id is None


@pytest.mark.parametrize("material", (b"", b"\x00\xff\x01", b'{ "size": NaN }'))
def test_wire_bytes_are_preserved_as_canonical_base64(material: bytes) -> None:
    """Keep malformed transport bytes without claiming a decoded financial payload.

    Given: Empty or non-UTF8 original bytes from the source transport.
    When: They are explicitly tagged with canonical base64 representation.
    Then: Decoding the retained material returns the exact byte sequence.
    """
    data = _data()
    data.update(representation="wire_bytes_base64", material=base64.b64encode(material).decode())
    observation = RawAccountingObservation.model_validate(data)
    assert base64.b64decode(observation.material, validate=True) == material


@pytest.mark.parametrize("material", ("Zg", "Zh==", "***", "Zg==\n"))
def test_invalid_or_noncanonical_base64_is_refused(material: str) -> None:
    """Require honest byte representation rather than silently fixing the tag.

    Given: Invalid or noncanonical base64 text.
    When: It is labeled as original wire bytes.
    Then: The envelope refuses that representation before storage.
    """
    data = _data()
    data.update(representation="wire_bytes_base64", material=material)
    with pytest.raises(ValidationError, match="canonical base64"):
        RawAccountingObservation.model_validate(data)


@pytest.mark.parametrize(
    ("mode", "environment", "accepted"),
    (
        ("live", "production", True),
        ("live", "demo", True),
        ("live", "paper", False),
        ("paper", "production", False),
        ("paper", "demo", False),
        ("paper", "paper", True),
    ),
)
def test_actual_environment_is_not_aliased_to_execution_mode(
    mode: str, environment: str, accepted: bool
) -> None:
    """Keep actual demo routing distinct from Snapper Paper and production.

    Given: Every supported source-mode and environment combination.
    When: A raw observation scope is validated.
    Then: Only coherent actual routing combinations are accepted.
    """
    data = _scope_data()
    data.update(mode=mode, environment=environment)
    if mode == "paper":
        data["exchange"] = "paper"
    if accepted:
        assert AccountingObservationScope.model_validate(data).environment == environment
    else:
        with pytest.raises(ValidationError, match="environment disagree"):
            AccountingObservationScope.model_validate(data)


def test_rotation_and_margin_classification_do_not_fork_the_coarse_hold_scope() -> None:
    """Keep financial refusals visible across credential and interpretation changes.

    Given: One account family before and after a margin profile is classified.
    When: Coarse hold identities are calculated.
    Then: Unknown and known margin labels retain the same account-family key.
    """
    data = _scope_data()
    known = AccountingObservationScope.model_validate(data)
    data["margin_account"] = None
    unknown = AccountingObservationScope.model_validate(data)
    data["margin_account"] = "F-XBTUSD"
    other_margin = AccountingObservationScope.model_validate(data)
    assert accounting_hold_scope_digest(known) == accounting_hold_scope_digest(unknown)
    assert accounting_hold_scope_digest(known) == accounting_hold_scope_digest(other_margin)


@pytest.mark.parametrize(
    "updates",
    (
        {"wallet_public_id": _OTHER},
        {"exchange": "kraken"},
        {"environment": "demo"},
        {"mode": "paper", "environment": "paper", "exchange": "paper"},
    ),
)
def test_distinct_account_families_have_distinct_hold_keys(updates: dict[str, object]) -> None:
    """Retain wallet, venue, actual environment and source-mode isolation.

    Given: A change to one actual account-family identity.
    When: The hold scope is hashed.
    Then: It cannot collide with the unchanged account's refusal key.
    """
    data = _scope_data()
    original = AccountingObservationScope.model_validate(data)
    data.update(updates)
    changed = AccountingObservationScope.model_validate(data)
    assert accounting_hold_scope_digest(original) != accounting_hold_scope_digest(changed)


@pytest.mark.parametrize(
    "updates",
    (
        {"exchange": "paper"},
        {"mode": "paper", "environment": "paper"},
    ),
)
def test_paper_exchange_and_source_mode_must_agree(updates: dict[str, object]) -> None:
    """Keep actual Paper clients distinct from live futures and demo routing.

    Given: A source-mode and actual exchange contradiction.
    When: The raw account-family envelope is validated.
    Then: It refuses the incorrect scope instead of certifying another route.
    """
    data = _scope_data()
    data.update(updates)
    with pytest.raises(ValidationError, match="actual exchange disagree"):
        AccountingObservationScope.model_validate(data)


@pytest.mark.parametrize(
    "updates",
    (
        {"source_identity": "caseSensitiveExecution"},
        {"source_identity_kind": "venue_execution_id"},
        {"credential_public_id": _OTHER},
        {"credential_timestamp": _OBSERVED},
    ),
)
def test_incomplete_source_identity_or_credential_revision_is_refused(
    updates: dict[str, object],
) -> None:
    """Keep provenance labels explicit without treating a partial identity as proof.

    Given: An ID without its tag or a credential without its exact revision.
    When: The raw envelope is validated.
    Then: The contradictory provenance is rejected.
    """
    data = _data()
    data.update(updates)
    with pytest.raises(ValidationError, match="together|provenance tag"):
        RawAccountingObservation.model_validate(data)


def test_complete_public_credential_and_unproven_identity_are_retained() -> None:
    """Record the actual selected source without upgrading a synthetic ID to a venue fact.

    Given: Exact credential metadata and an explicitly unproven parser identity.
    When: A receipt is frozen and hashed.
    Then: The tags survive without establishing account or execution authority.
    """
    data = _data()
    data.update(
        credential_public_id=_OTHER,
        credential_timestamp=_OBSERVED,
        source_identity="recon-synthetic-1",
        source_identity_kind="unproven",
        route_authority_public_id=_WALLET,
        source_instrument_public_id=_OTHER,
        order_public_id=_OTHER,
        native_symbol="PF_XBTUSD",
    )
    observation = RawAccountingObservation.model_validate(data)
    assert observation.source_identity_kind == "unproven"
    assert observation.credential_public_id == _OTHER
    assert observation.credential_timestamp == _OBSERVED
    assert len(accounting_observation_digest(observation)) == 64


def test_case_sensitive_source_identity_and_changed_receipt_are_not_deduplicated() -> None:
    """Separate observation content from a possible venue execution identity.

    Given: Case-sensitive IDs and redelivered observations with changed fees.
    When: Content digests are produced.
    Then: Distinct source evidence retains distinct observation identities.
    """
    data = _data()
    data.update(source_identity="AbC", source_identity_kind="venue_execution_id")
    original = accounting_observation_digest(RawAccountingObservation.model_validate(data))
    data["source_identity"] = "abc"
    assert accounting_observation_digest(RawAccountingObservation.model_validate(data)) != original
    data["source_identity"] = "AbC"
    data["material"] = '{ "fee": "-0.01" }'
    assert accounting_observation_digest(RawAccountingObservation.model_validate(data)) != original


def test_uuid_spellings_normalize_and_insertion_identity_is_separate() -> None:
    """Keep canonical source identity stable across local transaction retries.

    Given: Alternate UUID spellings and distinct local insertion provenance.
    When: The receipt envelope is canonicalized.
    Then: Source content is stable and insertion IDs cannot alter its digest.
    """
    data = _data()
    original = RawAccountingObservation.model_validate(data)
    scope = cast(dict[str, object], data["scope"])
    scope["wallet_public_id"] = _WALLET.upper()
    changed = RawAccountingObservation.model_validate(data)
    assert accounting_observation_digest(original) == accounting_observation_digest(changed)
    first = AccountingWriteProvenance(
        public_id=_WALLET, session_id=_OTHER, sequence_id=1, timestamp=_OBSERVED
    )
    second = first.model_copy(update={"public_id": _OTHER, "sequence_id": 2})
    assert first.public_id != second.public_id
    assert "public_id" not in cast(
        dict[str, object], json.loads(canonical_accounting_observation(original))
    )


def test_new_observation_time_changes_content_without_changing_financial_scope() -> None:
    """Retain redelivery as a new observation rather than silently dropping it.

    Given: An otherwise identical frame observed at a later instant.
    When: Its complete content digest is produced.
    Then: The observation differs while its durable financial scope stays the same.
    """
    data = _data()
    first = RawAccountingObservation.model_validate(data)
    data["observed_at"] = _OBSERVED + timedelta(seconds=1)
    second = RawAccountingObservation.model_validate(data)
    assert accounting_observation_digest(first) != accounting_observation_digest(second)
    assert accounting_hold_scope_digest(first.scope) == accounting_hold_scope_digest(second.scope)


@pytest.mark.parametrize(
    "pointer", (None, "", "/", "/fills/0", "/logs/1", "/new_entry", "/~0/~1", "/~01")
)
def test_raw_child_selector_retains_exact_rfc6901_identity(pointer: str | None) -> None:
    """Bind a child to one exact observation without rewriting the original frame.

    Given: A valid whole-frame or child JSON pointer.
    When: The version-two raw envelope is frozen.
    Then: The exact pointer is retained alongside the unchanged source material.
    """
    data = _data()
    data["target_json_pointer"] = pointer
    observation = RawAccountingObservation.model_validate(data)
    assert observation.target_json_pointer == pointer
    assert observation.schema_version == 2


@pytest.mark.parametrize("pointer", ("fills/0", "#/fills/0", "/bad~", "/bad~2", "/bad~x"))
def test_invalid_raw_selector_syntax_is_refused(pointer: str) -> None:
    """Keep financial child identity unambiguous before a receipt UUID is allocated.

    Given: A URI fragment, missing slash or invalid JSON-pointer escape.
    When: A raw child selection is validated.
    Then: It refuses the invalid selector instead of silently rewriting it.
    """
    data = _data()
    data["target_json_pointer"] = pointer
    with pytest.raises(ValidationError, match="pointer"):
        RawAccountingObservation.model_validate(data)


def test_two_same_id_children_in_one_frame_have_distinct_observation_content() -> None:
    """Keep sibling events separate even when a provider repeats an execution identity.

    Given: Two contradictory fills with the same source execution ID in one raw frame.
    When: Each child receives its own persisted selector.
    Then: Their content identities differ without altering the original whole frame.
    """
    data = _data()
    data.update(
        material='{"fills":[{"id":"same","qty":1},{"id":"same","qty":2}]}',
        source_identity="same",
        source_identity_kind="venue_execution_id",
        target_json_pointer="/fills/0",
    )
    first = RawAccountingObservation.model_validate(data)
    data["target_json_pointer"] = "/fills/1"
    second = RawAccountingObservation.model_validate(data)
    assert first.material == second.material
    assert accounting_observation_digest(first) != accounting_observation_digest(second)


@pytest.mark.parametrize("version", (True, False, 2.0, "2", 1))
def test_observation_schema_version_cannot_silently_reinterpret_old_or_coerced_data(
    version: object,
) -> None:
    """Keep the child-selection schema revision explicit before deployment.

    Given: A legacy or coercible version value.
    When: The version-two receipt envelope is validated.
    Then: It refuses the incompatible schema instead of accepting an alias.
    """
    data = _data()
    data["schema_version"] = version
    with pytest.raises(ValidationError):
        RawAccountingObservation.model_validate(data)


@pytest.mark.parametrize("field", ("material", "route_revision", "observed_at"))
def test_raw_envelope_is_frozen(field: str) -> None:
    """Prevent retry payload changes after its local observation identity is allocated.

    Given: A validated frozen source observation.
    When: One of its fields is reassigned.
    Then: The mutation is rejected before a changed transaction retry can occur.
    """
    observation = RawAccountingObservation.model_validate(_data())
    with pytest.raises(ValidationError, match="frozen"):
        setattr(observation, field, "changed")
