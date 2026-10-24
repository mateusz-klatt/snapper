"""Native identity boundaries preserving explicit source clocks and legacy history."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest
from pydantic import ValidationError

from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_route_authority import ExecutionRouteAuthorityPayload
from snapper.core.execution_route_authority import account_scope_digest
from snapper.core.execution_route_authority import route_selection_digest
from snapper.core.native_account_authority import NativeAccountAuthorityPayload
from snapper.core.native_account_authority import NativeAuthorityOwnerReference
from snapper.core.native_account_authority import NativeAuthorityProfile
from snapper.core.native_account_authority import NativeAuthorityReadReference
from snapper.core.native_account_authority import canonical_native_account_authority
from snapper.core.native_account_authority import client_selection_from_native_authority
from snapper.core.native_account_authority import native_account_authority_digest

_NOW = datetime(2026, 10, 23, 13, tzinfo=UTC)


def _id(number: int) -> str:
    """Keep constructed public fixture identities explicit and separate from actual credentials."""
    return f"00000000-0000-7000-8000-{number:012d}"


@pytest.fixture
def authority() -> NativeAccountAuthorityPayload:
    """Provide internally coherent illustrative references, without claiming real source binding."""
    scope = ExecutionAccountScope(
        wallet_public_id=_id(1),
        exchange="kraken_futures",
        mode="live",
        environment="production",
        margin_account="flex",
    )
    credential_at = _NOW - timedelta(hours=1)
    origin = "https://futures.kraken.com"
    read = NativeAuthorityReadReference(
        attempt_public_id=_id(2),
        attempt_content_digest="a" * 64,
        family_sequence=1,
        outcome_public_id=_id(3),
        outcome_content_digest="b" * 64,
        raw_receipt_public_id=_id(4),
        raw_content_digest="c" * 64,
        request_started_at=_NOW - timedelta(seconds=2),
        source_observed_at=_NOW - timedelta(seconds=1),
        source_recorded_at=_NOW,
        server_time_text="2026-10-23T12:59:59.000Z",
        server_time=_NOW - timedelta(seconds=1),
    )
    return NativeAccountAuthorityPayload(
        scope=scope,
        credential_public_id=_id(5),
        credential_timestamp=credential_at,
        public_api_origin=origin,
        venue_account_id=_id(6),
        owner_proof=NativeAuthorityOwnerReference(
            public_id=_id(7),
            content_digest="d" * 64,
            read_attempt_public_id=read.attempt_public_id,
            account_uid=_id(6),
            observed_at=_NOW - timedelta(seconds=3),
        ),
        native_read=read,
        native_profile=NativeAuthorityProfile(
            native_account_key="flex",
            account_type="multiCollateralMarginAccount",
            settlement_currency=None,
            content_digest="e" * 64,
        ),
        observed_at=read.source_observed_at,
        valid_until=_NOW + timedelta(seconds=10),
        route_revision=route_selection_digest(scope, _id(5), credential_at, origin),
    )


def test_native_roundtrip_does_not_manufacture_legacy_observation_fields(
    authority: NativeAccountAuthorityPayload,
) -> None:
    """Preserve actual native source references without satisfying the old payload by invention.

    Given: A native profile identity with explicit same-attempt source and owner references,
    When: Its versioned payload is serialized and restored,
    Then: References and clocks survive while the legacy observation payload refuses it.
    """
    canonical = canonical_native_account_authority(authority)
    restored = NativeAccountAuthorityPayload.model_validate_json(canonical)

    assert restored == authority
    assert native_account_authority_digest(restored) == native_account_authority_digest(authority)
    assert "account_observation_public_id" not in canonical
    assert "balance_receipt_json" not in canonical
    assert "account_log_raw_receipt_public_id" not in canonical
    with pytest.raises(ValidationError):
        ExecutionRouteAuthorityPayload.model_validate_json(canonical)


def test_empty_history_cash_profile_remains_read_only_identity(
    authority: NativeAccountAuthorityPayload,
) -> None:
    """Do not require a financial history entry before an actual source account can be named.

    Given: A same-attempt owner and accounts response for a literal cash profile,
    When: The profile identity is represented without any account-log reference,
    Then: Its public account scope is representable and no order/collateral permission appears.
    """
    scope = ExecutionAccountScope.model_validate(
        authority.scope.model_dump() | {"margin_account": "cash"}
    )
    material = authority.model_dump() | {
        "scope": scope,
        "native_profile": NativeAuthorityProfile(
            native_account_key="cash",
            account_type="cashAccount",
            settlement_currency=None,
            content_digest="f" * 64,
        ),
        "route_revision": route_selection_digest(
            scope,
            authority.credential_public_id,
            authority.credential_timestamp,
            authority.public_api_origin,
        ),
    }
    observed = NativeAccountAuthorityPayload.model_validate(material)

    assert observed.scope.margin_account == "cash"
    assert observed.native_profile.account_type == "cashAccount"
    assert not {"available_collateral", "order_permission", "sealed_prefix"} & set(
        observed.model_fields_set
    )


@pytest.mark.parametrize("version", [True, False, 1.0, "1"])
def test_version_aliases_cannot_relabel_native_evidence(
    authority: NativeAccountAuthorityPayload, version: object
) -> None:
    """Reject coerced schema versions before any payload can enter a native hash domain.

    Given: Valid native references and a non-integer version alias,
    When: A payload or unchecked copied model is passed to the canonical boundary,
    Then: The evidence is refused rather than canonically relabeled as version one.
    """
    forged = authority.model_copy(update={"schema_version": version})

    with pytest.raises(ValidationError, match="must be an integer"):
        canonical_native_account_authority(forged)


@pytest.mark.parametrize("wallet", ["FLEX", "flex_futures", " flex", "flex ", "f-flex"])
def test_wallet_aliases_cannot_bind_an_actual_rest_profile(
    authority: NativeAccountAuthorityPayload, wallet: str
) -> None:
    """Keep the accepted wallet-name policy narrower than prefix, whitespace or case guesses.

    Given: An exact native REST profile key and a differently spelled requested scope,
    When: A route revision is correctly computed for the changed scope,
    Then: The literal profile mismatch still refuses the native authority.
    """
    scope = ExecutionAccountScope.model_validate(
        authority.scope.model_dump() | {"margin_account": wallet}
    )
    material = authority.model_dump() | {
        "scope": scope,
        "route_revision": route_selection_digest(
            scope,
            authority.credential_public_id,
            authority.credential_timestamp,
            authority.public_api_origin,
        ),
    }

    with pytest.raises(ValidationError, match="exact documented REST wallet name"):
        NativeAccountAuthorityPayload.model_validate(material)


@pytest.mark.parametrize("exchange", ["kraken", "coinbase"])
def test_non_futures_routes_cannot_reuse_native_source_references(
    authority: NativeAccountAuthorityPayload, exchange: str
) -> None:
    """Refuse a different provider even if its public client has an internally valid route.

    Given: Native Futures source references and another supported exchange label,
    When: The account scope is replaced without changing those sources,
    Then: The native authority route contract rejects the provider substitution.
    """
    material = authority.model_dump()
    material["scope"] = authority.scope.model_dump() | {"exchange": exchange}

    with pytest.raises(ValidationError, match="actual supported Futures route"):
        NativeAccountAuthorityPayload.model_validate(material)


def test_demo_scope_cannot_claim_the_production_client_origin(
    authority: NativeAccountAuthorityPayload,
) -> None:
    """Keep environment identity attached to the actual observed client endpoint.

    Given: Production read and owner references with a demo-labeled scope,
    When: The original production origin remains selected,
    Then: Native validation refuses the false environment claim.
    """
    material = authority.model_dump()
    material["scope"] = authority.scope.model_dump() | {"environment": "demo"}

    with pytest.raises(ValidationError, match="actual supported Futures route"):
        NativeAccountAuthorityPayload.model_validate(material)


def test_route_revision_is_not_an_unchecked_account_attestation(
    authority: NativeAccountAuthorityPayload,
) -> None:
    """An immutable route projection must agree with the actual source selection.

    Given: Internally coherent native account references,
    When: The route revision is replaced by an unrelated valid-length digest,
    Then: The selection mismatch refuses the record before canonical insertion.
    """
    material = authority.model_dump() | {"route_revision": "f" * 64}

    with pytest.raises(ValidationError, match="route revision disagrees"):
        NativeAccountAuthorityPayload.model_validate(material)


@pytest.mark.parametrize("field", ["read_attempt_public_id", "account_uid"])
def test_borrowed_owner_reference_cannot_name_another_attempt_or_account(
    authority: NativeAccountAuthorityPayload, field: str
) -> None:
    """Keep current profile identity inseparable from the exact same-attempt observed owner.

    Given: Native accounts evidence for one selected account and read attempt,
    When: Its owner proof instead names a different attempt or provider account UID,
    Then: The payload refuses the crossed identity before store-side source verification.
    """
    material = authority.model_dump()
    material["owner_proof"] = authority.owner_proof.model_dump() | {field: _id(99)}

    with pytest.raises(ValidationError, match="same read attempt"):
        NativeAccountAuthorityPayload.model_validate(material)


@pytest.mark.parametrize("offset", [timedelta(hours=-2), timedelta(seconds=1)])
def test_key_preflight_clock_stays_between_credential_selection_and_accounts_request(
    authority: NativeAccountAuthorityPayload, offset: timedelta
) -> None:
    """Do not retroactively attach a future preflight or one predating its credential revision.

    Given: One selected public credential revision and an actual accounts request clock,
    When: The owner-proof observation falls outside their admissible interval,
    Then: The native source chronology refuses the record.
    """
    material = authority.model_dump()
    material["owner_proof"] = authority.owner_proof.model_dump() | {
        "observed_at": authority.native_read.request_started_at + offset
    }

    with pytest.raises(ValidationError, match="key proof must precede"):
        NativeAccountAuthorityPayload.model_validate(material)


@pytest.mark.parametrize("field", ["observed_at", "valid_until"])
def test_poll_time_or_expired_at_capture_cannot_replace_original_source_clock(
    authority: NativeAccountAuthorityPayload, field: str
) -> None:
    """Authority insertion cannot manufacture observation age or a nonpositive validity window.

    Given: A retained original response observation and bounded expiry,
    When: Poll time replaces the source clock or expiry equals the source clock,
    Then: The immutable payload refuses the rewritten freshness basis.
    """
    value = _NOW if field == "observed_at" else authority.observed_at
    material = authority.model_dump() | {field: value}

    with pytest.raises(ValidationError, match="original source clock"):
        NativeAccountAuthorityPayload.model_validate(material)


@pytest.mark.parametrize("field", ["request_started_at", "source_recorded_at"])
def test_raw_response_clock_order_is_preserved_before_authority_use(
    authority: NativeAccountAuthorityPayload, field: str
) -> None:
    """An HTTP response cannot be observed before request start or after its durable recording.

    Given: A complete native response reference with distinct source clocks,
    When: The request or recording boundary crosses the original observation,
    Then: Source-reference validation rejects that impossible chronology.
    """
    value = _NOW if field == "request_started_at" else _NOW - timedelta(seconds=2)
    material = authority.native_read.model_dump() | {field: value}

    with pytest.raises(ValidationError, match="recording clocks disagree"):
        NativeAuthorityReadReference.model_validate(material)


@pytest.mark.parametrize("single_collateral", [True, False])
def test_profile_settlement_metadata_cannot_change_the_documented_account_type(
    authority: NativeAccountAuthorityPayload, single_collateral: bool
) -> None:
    """Do not invent settlement currency for multi-collateral cash or omit it for a native margin.

    Given: A literal source profile identity,
    When: Its settlement metadata contradicts the documented account type,
    Then: The profile is refused before it can supply account authority.
    """
    material = authority.native_profile.model_dump() | {
        "account_type": "marginAccount" if single_collateral else "cashAccount",
        "settlement_currency": None if single_collateral else "XBT",
    }

    with pytest.raises(ValidationError, match="settlement currency"):
        NativeAuthorityProfile.model_validate(material)


def test_valid_single_collateral_profile_keeps_its_exact_native_wallet_name(
    authority: NativeAccountAuthorityPayload,
) -> None:
    """Represent a native single-collateral wallet without translating it to a WS/log alias.

    Given: A retained REST profile named fi_xbtusd with explicit settlement currency,
    When: Its account scope and route revision use that same literal key,
    Then: The profile is representable without claiming an f-xbt:usd mapping.
    """
    scope = ExecutionAccountScope.model_validate(
        authority.scope.model_dump() | {"margin_account": "fi_xbtusd"}
    )
    material = authority.model_dump() | {
        "scope": scope,
        "native_profile": NativeAuthorityProfile(
            native_account_key="fi_xbtusd",
            account_type="marginAccount",
            settlement_currency="xbt",
            content_digest="f" * 64,
        ),
        "route_revision": route_selection_digest(
            scope,
            authority.credential_public_id,
            authority.credential_timestamp,
            authority.public_api_origin,
        ),
    }
    native = NativeAccountAuthorityPayload.model_validate(material)

    assert native.scope.margin_account == "fi_xbtusd"
    assert native.native_profile.settlement_currency == "xbt"
    assert "f-xbt:usd" not in canonical_native_account_authority(native)


def test_native_source_family_does_not_fork_the_logical_cash_scope(
    authority: NativeAccountAuthorityPayload,
) -> None:
    """Keep the account namespace independent of authority record IDs and read ordinals.

    Given: Two coherent native identity observations of the same actual wallet profile,
    When: Only source proof content changes,
    Then: Authority content changes while the shared account and selected client stay equal.
    """
    material = authority.model_dump()
    material["native_profile"] = authority.native_profile.model_dump() | {
        "content_digest": "f" * 64
    }
    changed = NativeAccountAuthorityPayload.model_validate(material)

    assert native_account_authority_digest(changed) != native_account_authority_digest(authority)
    assert account_scope_digest(changed.scope) == account_scope_digest(authority.scope)
    assert client_selection_from_native_authority(
        changed
    ) == client_selection_from_native_authority(authority)


def test_native_client_projection_revalidates_unchecked_copied_owner_identity(
    authority: NativeAccountAuthorityPayload,
) -> None:
    """A convenient client projection must not legitimize an unchecked copied native owner.

    Given: A valid native payload copied with a different provider account UID,
    When: Its selected public client is requested through the native-family helper,
    Then: Strict native validation rejects the source-owner contradiction before projection.
    """
    forged = authority.model_copy(update={"venue_account_id": _id(99)})

    with pytest.raises(ValidationError, match="same read attempt"):
        client_selection_from_native_authority(forged)
