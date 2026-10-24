"""Genuine signed local HTTP checks for the separate same-response UUID/IIBAN projection."""

import copy
import json
import pickle
from typing import cast

import pytest
from kraken.futures import User
from requests import Session

from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.infrastructure.exchanges.kraken_futures_key_identity import (
    NativeKeyIdentityTransportError,
)
from snapper.infrastructure.exchanges.kraken_futures_key_identity import (
    read_futures_prepared_account_link,
)
from snapper.infrastructure.exchanges.prepared_futures_account_link import (
    FuturesAccountLinkSourceError,
)
from snapper.infrastructure.exchanges.prepared_futures_account_link import (
    PreparedFuturesAccountLink,
)
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyTokenError
from snapper.infrastructure.network.egress_context import egress_identity
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _ACCOUNT_UID
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _KEY
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _PRIVATE_MARKER
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _SECRET
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _Adapter
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _assert_closed
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _assert_safe_error
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _body
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _ChangedFinalAuth
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _client
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _read
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _selection

_INTERNAL_ACCOUNT = "AA00 TEST ACCT 0001"


def _link(
    user: User, selected: ExecutionClientSelection | None = None
) -> PreparedFuturesAccountLink:
    """Use the actual selected SDK and ordinary private egress ownership for one original check."""
    with egress_identity(
        exchange="kraken_futures",
        traffic_class="private",
        owner="test",
        operation="account_link_identity",
    ):
        return read_futures_prepared_account_link(user, selected or _selection())


@pytest.mark.parametrize("demo", (False, True))
def test_actual_original_response_projects_uuid_and_iiban_without_changing_native_v1(
    demo: bool,
) -> None:
    """The new source retains the same original identity and an explicitly separate internal ID.

    Given: A real SDK signed local response containing UUID, IIBAN, permissions and private extras,
    When: Its explicit account-link operation completes and closes the source response,
    Then: Only safe same-response fields leave the operation and the token is still uncommitted.
    """
    adapter = _Adapter(_body(iiban=_INTERNAL_ACCOUNT))
    selected = _selection(demo=demo)
    with _client(adapter, demo=demo) as user:
        source = _link(user, selected)
        try:
            value = source.observation
            assert value.internal_account_id == _INTERNAL_ACCOUNT
            assert value.identity.account_uid == _ACCOUNT_UID
            assert value.identity.client_selection == selected
            assert value.identity.general_access == "READ_ONLY"
            assert value.identity.request_started_at <= value.identity.observed_at
            assert value.identity.source_policy == "kraken_futures_key_check_redacted_v1"
            assert value.source_policy == "kraken_futures_key_check_iiban_v1"
            assert value.operation_public_id != value.identity.account_uid
            assert "iiban" not in value.identity.model_dump_json()
            assert "internal_account" not in value.identity.model_dump_json()
            for private in (_KEY, _SECRET, _PRIVATE_MARKER, "allowedCidrBlocks", "apiKey"):
                assert private not in value.model_dump_json()
            assert _INTERNAL_ACCOUNT not in repr(source)
            assert len(adapter.calls) == 1
            assert adapter.calls[0].headers["APIKey"] == _KEY
            assert adapter.calls[0].headers["Authent"]
            _assert_closed(adapter)
            with pytest.raises(PreparedKeyTokenError, match="proof_not_committed"):
                source.token.require_proof_reference()
        finally:
            source.close()


@pytest.mark.parametrize(
    "internal_id",
    (
        None,
        True,
        1,
        1.25,
        [],
        {},
        "",
        "AA00TESTACCT0001",
        "aa00 TEST ACCT 0001",
        "AA00 TEST ACCT 0001\n",
        "AA00 TEST ACCT 0001\r",
        "AA00\tTEST ACCT 0001",
        "AA00\u00a0TEST ACCT 0001",
        "AA00 TEST ACCT 000\x00",
        " AA00 TEST ACCT 0001",
        "AA00 TEST ACCT 0001 ",
        "AA00 TEST ACCT 000!",
        "AA00 TEST ACCT 000\u00c9",
        "AA00 TEST ACCT 000\ud800",
    ),
)
def test_actual_invalid_iiban_refuses_new_source_but_preserves_existing_identity_contract(
    internal_id: object,
) -> None:
    """New association prerequisites do not add data to, or narrow, historical v1 observations.

    Given: An original matched signed response with an unsupported or null internal identifier,
    When: The explicit link operation is attempted and the legacy identity operation is read separately,
    Then: The link refuses privately while the existing UUID/permission projection remains available.
    """
    adapter = _Adapter(_body(iiban=internal_id))
    with _client(adapter) as user:
        with pytest.raises(
            NativeKeyIdentityTransportError, match="internal_account_unavailable"
        ) as caught:
            _link(user)
        _assert_safe_error(caught.value)
        identity = _read(user)
    assert identity.account_uid == _ACCOUNT_UID
    assert len(adapter.calls) == 2
    _assert_closed(adapter)


def test_missing_original_iiban_does_not_reuse_a_prior_same_uid_observation() -> None:
    """The same account UUID cannot fill an absent identifier from an earlier response.

    Given: One genuine issued capture followed by an original response omitting IIBAN,
    When: The next signed operation observes the same UUID and credentials,
    Then: The next operation refuses without borrowing the prior identifier or operation identity.
    """
    adapter = _Adapter(_body(iiban=_INTERNAL_ACCOUNT))
    with _client(adapter) as user:
        source = _link(user)
        try:
            body = json.loads(_body())
            del body["iiban"]
            adapter.body = json.dumps(body).encode()
            with pytest.raises(
                NativeKeyIdentityTransportError, match="internal_account_unavailable"
            ):
                _link(user)
            assert source.observation.internal_account_id == _INTERNAL_ACCOUNT
        finally:
            source.close()
    assert len(adapter.calls) == 2
    _assert_closed(adapter)


@pytest.mark.parametrize("encoding", ("utf-16-le", "utf-16-be", "utf-32-le", "utf-32-be"))
def test_non_utf8_original_body_cannot_issue_the_new_account_link(encoding: str) -> None:
    """The new response contract refuses decoder autodetection of non-UTF-8 source documents.

    Given: An otherwise valid sensitive original response encoded as UTF-16 or UTF-32,
    When: The actual signed new operation consumes its bounded response bytes,
    Then: No account-link source is issued and the original response is closed.
    """
    adapter = _Adapter(_body(iiban=_INTERNAL_ACCOUNT).decode().encode(encoding))
    with _client(adapter) as user, pytest.raises(NativeKeyIdentityTransportError) as caught:
        _link(user)
    _assert_safe_error(caught.value)
    _assert_closed(adapter)


@pytest.mark.parametrize("alias", ("internal_id", "uid", "selection"))
def test_public_nested_model_alias_cannot_change_original_issued_account_link(alias: str) -> None:
    """Registry-owned copies preserve the actual same-response binding after public reads.

    Given: One genuine issued observation and a detached nested public model copy,
    When: Object-level mutation changes its IIBAN, UUID or selected credential,
    Then: A second read returns the original captured values from the privately owned source.
    """
    adapter = _Adapter(_body(iiban=_INTERNAL_ACCOUNT))
    with _client(adapter) as user:
        source = _link(user)
        try:
            original = source.observation
            alias_value = source.observation
            if alias == "internal_id":
                object.__setattr__(alias_value, "internal_account_id", "AA00 TEST ACCT 9999")
            elif alias == "uid":
                object.__setattr__(alias_value.identity, "account_uid", "changed")
            else:
                object.__setattr__(
                    alias_value.identity.client_selection, "credential_public_id", "changed"
                )
            assert source.observation == original
            assert source.observation != alias_value
        finally:
            source.close()


@pytest.mark.parametrize("access", ("observation", "token"))
def test_public_receipt_construction_cannot_mint_an_account_link(access: str) -> None:
    """A Python object with the public receipt type is not evidence of actual HTTP source capture.

    Given: A caller-constructed receipt which has never traversed the closed original transport,
    When: Its public observation or token is requested,
    Then: The actual issuance lookup refuses and repeated close remains harmless.
    """
    forged = PreparedFuturesAccountLink()
    with pytest.raises(FuturesAccountLinkSourceError, match="not_transport_issued"):
        getattr(forged, access)
    forged.close()
    forged.close()


@pytest.mark.parametrize("close", ("source", "token"))
def test_closing_actual_source_or_token_revokes_both_link_accessors(close: str) -> None:
    """Closed capture cannot later be reopened from its public safe value.

    Given: An actually issued link and its original private token,
    When: Either the source owner or token owner closes its live operation,
    Then: Neither the source nor the token accessor returns an active account-link value.
    """
    adapter = _Adapter(_body(iiban=_INTERNAL_ACCOUNT))
    with _client(adapter) as user:
        source = _link(user)
        if close == "source":
            source.close()
        else:
            source.token.close()
        for access in ("observation", "token"):
            with pytest.raises((FuturesAccountLinkSourceError, PreparedKeyTokenError)):
                getattr(source, access)
        source.close()


@pytest.mark.parametrize("operation", ("pickle", "copy", "deepcopy"))
def test_issued_account_link_cannot_be_copied_or_serialized(operation: str) -> None:
    """An issued source identity is local to its actual response and runtime owner.

    Given: A genuine signed original capture with an active token,
    When: Generic serialization or copying attempts to duplicate its operation identity,
    Then: The opaque boundary refuses before traversing any credential-bearing runtime value.
    """
    adapter = _Adapter(_body(iiban=_INTERNAL_ACCOUNT))
    with _client(adapter) as user:
        source = _link(user)
        try:
            with pytest.raises(TypeError, match="cannot be"):
                if operation == "pickle":
                    pickle.dumps(source)
                elif operation == "copy":
                    copy.copy(source)
                else:
                    copy.deepcopy(source)
        finally:
            source.close()


def test_final_requests_key_mutation_cannot_issue_an_original_account_link(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The added IIBAN projection retains the final signed-request key binding.

    Given: The actual SDK and a Requests auth mutator changing its final prepared signing key,
    When: The new account-link read reaches the closed dispatch guard,
    Then: It refuses before any physical local HTTP call or source issuance.
    """
    adapter = _Adapter(_body(iiban=_INTERNAL_ACCOUNT))
    with _client(adapter) as user:
        session = cast(Session, getattr(user, "_FuturesClient__session"))
        monkeypatch.setattr(session, "auth", _ChangedFinalAuth("APIKey"))
        with pytest.raises(NativeKeyIdentityTransportError) as caught:
            _link(user)
        _assert_safe_error(caught.value)
    assert not adapter.calls


@pytest.mark.parametrize(
    "duplicate",
    (
        b',"iiban":"AA00 TEST ACCT 0001"}',
        b',"iiban":"AA00 TEST ACCT 9999"}',
        b',"ii\\u0062an":"AA00 TEST ACCT 9999"}',
    ),
)
def test_duplicate_original_iiban_fields_never_issue_an_account_link(duplicate: bytes) -> None:
    """One source response cannot offer competing JSON names for the account identifier.

    Given: A real signed response with repeated same, changed or escaped-spelling IIBAN keys,
    When: The explicit account-link operation decodes that original document,
    Then: Unique-key parsing refuses before either value can become an issued observation.
    """
    adapter = _Adapter(_body(iiban=_INTERNAL_ACCOUNT)[:-1] + duplicate)
    with _client(adapter) as user, pytest.raises(
        NativeKeyIdentityTransportError, match="json_invalid"
    ) as caught:
        _link(user)
    _assert_safe_error(caught.value)
    _assert_closed(adapter)


def test_each_original_response_keeps_its_own_operation_identity_and_token() -> None:
    """The same selected SDK does not let a later body overwrite an earlier original capture.

    Given: Two actual signed responses from the same account with different source IIBAN values,
    When: The second operation completes and the first source is closed,
    Then: Each kept its own response fields and operation UUID, and the second token stays open.
    """
    adapter = _Adapter(_body(iiban=_INTERNAL_ACCOUNT))
    with _client(adapter) as user:
        first = _link(user)
        second: PreparedFuturesAccountLink | None = None
        try:
            original = first.observation
            adapter.body = _body(iiban="AA00 TEST ACCT 9999")
            second = _link(user)
            assert first.observation == original
            assert second.observation.internal_account_id == "AA00 TEST ACCT 9999"
            assert second.observation.operation_public_id != original.operation_public_id
            assert second.token is not first.token
            first.close()
            assert second.observation.internal_account_id == "AA00 TEST ACCT 9999"
            with pytest.raises(PreparedKeyTokenError, match="proof_not_committed"):
                second.token.require_proof_reference()
        finally:
            first.close()
            if second is not None:
                second.close()
    assert len(adapter.calls) == 2
    _assert_closed(adapter)
