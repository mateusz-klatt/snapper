"""Actual two-key regressions for an original User whose signing key changes after preflight.

Actual signed HTTP uses a local requests adapter and real SQLite proof/outcome
rows. The archived unchanged-producer RED is preserved separately; these are the matching GREEN regressions.
Both keys and account identities are synthetic; no venue or order is involved.
"""

from pathlib import Path
from typing import Any
from typing import cast

import pytest
from requests import PreparedRequest
from requests import Session
from requests.auth import AuthBase

import snapper.application.trade.native_account_owner_preflight as preflight
import snapper.application.trade.native_account_read as producer
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.native_account_identity import NativeAccountPublicIdentity
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.native_account_identity_store import BoundNativeAccountIdentityProof
from snapper.data.native_account_identity_store import find_native_account_identity
from tests.helpers.native_account_authority import native_authority_scenario
from tests.helpers.native_account_authority import native_authority_uuid

_KEY_A = "synthetic-key"
_KEY_B = "SYNTHETIC_SECOND_EXTERNAL_ACCOUNT_KEY"
_ACCOUNT_B = (
    b'{"flex":{"type":"multiCollateralMarginAccount","currencies":'
    b'{"USD":{"quantity":9999,"value":9999}}}}'
)


@pytest.mark.parametrize("when", ("after_identity_commit", "inside_accounts_prepare"))
async def test_actual_accounts_request_cannot_switch_keys_after_safe_identity_preflight(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, when: str
) -> None:
    """Object/public-route equality cannot bind account bytes if the actual prepared key changed.

    Given: Actual committed UID-A key identity and the same SDK User with two synthetic signing keys,
    When: That object's key changes after proof commit or during actual accounts request preparation,
    Then: The producer refuses before physical accounts dispatch, retaining no UID-A/account-B source.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        user = source.client._user_client
        assert user is not None and user._key == _KEY_A
        actual_retain = preflight.retain_native_account_identity
        actual_prepare = user._prepare_request
        source.adapter.profiles = _ACCOUNT_B

        async def retain_then_mutate(
            repository: AccountingRepository,
            attempt_public_id: str,
            observation: NativeAccountPublicIdentity,
            identity: ExecutionEvidenceIdentity,
        ) -> BoundNativeAccountIdentityProof:
            """Wait for the real safe proof commit before the concurrent mutable-key change."""
            result = await actual_retain(repository, attempt_public_id, observation, identity)
            monkeypatch.setattr(user, "_key", _KEY_B)
            return result

        def mutate_accounts_prepare(**kwargs: Any) -> Any:
            """Run the actual SDK signing function after a just-in-time accounts-only key change."""
            if kwargs.get("uri") == "/derivatives/api/v3/accounts":
                monkeypatch.setattr(user, "_key", _KEY_B)
            return actual_prepare(**kwargs)

        if when == "after_identity_commit":
            monkeypatch.setattr(preflight, "retain_native_account_identity", retain_then_mutate)
        else:
            monkeypatch.setattr(user, "_prepare_request", mutate_accounts_prepare)
        outcome = await producer.read_native_futures_accounts(
            source.repository, source.client, source.tracker
        )
        async with source.repository.session() as session:
            proof = await find_native_account_identity(session, outcome.payload.attempt_public_id)
        assert proof is not None and proof.payload.observation.account_uid == native_authority_uuid(
            80
        )
        account_requests = [
            request
            for request in source.adapter.calls
            if cast(str, request.url).endswith("/derivatives/api/v3/accounts")
        ]
        actual_key_labels = tuple(
            "A" if request.headers.get("APIKey") == _KEY_A else "B" for request in account_requests
        )
        assert outcome.payload.result.kind == "failed", {
            "actual_safe_proof_uid": proof.payload.observation.account_uid,
            "physical_accounts_key_labels": actual_key_labels,
            "result_kind": outcome.payload.result.kind,
            "actual_accounts_native_USD": "9999 from B if request was dispatched",
        }
        assert not account_requests
        assert outcome.payload.source_reference is None


class _AccountsOnlyChangedAuth(AuthBase):
    """Exercise Requests' real post-SDK authentication mutation point without changing key-check auth."""

    def __call__(self, prepared: PreparedRequest) -> PreparedRequest:
        """Rewrite only the actual final accounts request header after SDK signing has finished."""
        if (prepared.url or "").endswith("/derivatives/api/v3/accounts"):
            prepared.headers["APIKey"] = _KEY_B
        return prepared


async def test_final_requests_auth_cannot_change_the_guarded_accounts_key_after_sdk_preparation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SDK-prepared headers are weaker than the final request after Session authentication merging.

    Given: A successful actual key preflight and an accounts-only Requests AuthBase mutator,
    When: Session.prepare_request rewrites APIKey after the SDK prepared its original headers,
    Then: The final guarded source refuses before physical accounts send and retains no false account data.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        user = source.client._user_client
        assert user is not None
        session = getattr(user, "_FuturesClient__session", None)
        assert isinstance(session, Session)
        monkeypatch.setattr(session, "auth", _AccountsOnlyChangedAuth())
        outcome = await producer.read_native_futures_accounts(
            source.repository, source.client, source.tracker
        )
        assert outcome.payload.result.kind == "failed"
        assert outcome.payload.source_reference is None
        assert len(source.adapter.calls) == 1
        assert (source.adapter.calls[0].url or "").endswith("/api/auth/v1/api-keys/v3/check")
        async with source.repository.session() as stored:
            proof = await find_native_account_identity(stored, outcome.payload.attempt_public_id)
        assert proof is not None and proof.payload.matches_owner
        assert _KEY_B not in outcome.payload.model_dump_json()


class _RetainedFinalAuth(AuthBase):
    """Retain only the original actual AuthBase request for a deterministic later mutation."""

    def __init__(self, path: str) -> None:
        """Select the actual endpoint whose original auth reference must not remain dispatchable."""
        self.path = path
        self.original: PreparedRequest | None = None

    def __call__(self, prepared: PreparedRequest) -> PreparedRequest:
        """Keep the actual original object without changing it during initial preparation."""
        if (prepared.url or "").endswith(self.path):
            self.original = prepared
        return prepared


@pytest.mark.parametrize("path", ("/api/auth/v1/api-keys/v3/check", "/derivatives/api/v3/accounts"))
async def test_retained_auth_reference_cannot_reroute_the_private_prepared_dispatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, path: str
) -> None:
    """AuthBase retaining its original request cannot mutate the privately checked dispatch later.

    Given: Real signed SDK reads and AuthBase retaining the actual prepared request object,
    When: Actual environment merging mutates that retained URL, method, body and hook list,
    Then: Only the isolated original GET reaches the local adapter and its same-key source is captured.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        user = source.client._user_client
        assert user is not None
        session = getattr(user, "_FuturesClient__session", None)
        assert isinstance(session, Session)
        retained = _RetainedFinalAuth(path)
        monkeypatch.setattr(session, "auth", retained)
        actual_merge = session.merge_environment_settings

        def merge_then_mutate(
            url: str,
            proxies: dict[str, str] | None,
            stream: bool | None,
            verify: bool | str | None,
            cert: str | tuple[str, str] | None,
        ) -> dict[str, object]:
            """Mutate AuthBase's real original alias at the actual post-check environment boundary."""
            settings: dict[str, object] = actual_merge(url, proxies, stream, verify, cert)
            if url.endswith(path):
                original = retained.original
                assert original is not None
                original.url = "https://unexpected.invalid" + path
                original.method = "POST"
                original.body = b"SYNTHETIC_UNEXPECTED_BODY"
                original.hooks["response"].clear()
            return settings

        monkeypatch.setattr(session, "merge_environment_settings", merge_then_mutate)
        outcome = await producer.read_native_futures_accounts(
            source.repository, source.client, source.tracker
        )
        assert retained.original is not None and retained.original.method == "POST"
        assert outcome.payload.result.kind == "captured"
        assert len(source.adapter.calls) == 2
        assert all(
            request.method == "GET" and request.body is None for request in source.adapter.calls
        )
        assert all(
            (request.url or "").startswith("https://futures.kraken.com/")
            for request in source.adapter.calls
        )
        assert all(request.headers.get("APIKey") == _KEY_A for request in source.adapter.calls)
