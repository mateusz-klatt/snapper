"""Actual product producer HTTP/storage integration without venue orders or synthetic source DTOs."""

import asyncio
import base64
from pathlib import Path

import pytest
from requests import PreparedRequest
from requests.auth import AuthBase
from sqlalchemy import func
from sqlalchemy import select

import snapper.application.trade.futures_product_read as producer
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.futures_product_read import FuturesProductReadFailure
from snapper.core.futures_product_read import FuturesProductReadSource
from snapper.core.futures_product_read import ProductReadKeyReference
from snapper.core.futures_product_read import canonical_futures_product_outcome
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.futures_product_capture import FuturesProductWriteResult
from snapper.data.futures_product_read_store import BoundFuturesProductOutcome
from snapper.data.futures_product_read_store import begin_futures_product_read
from snapper.data.futures_product_read_store import get_bound_futures_product_outcome
from snapper.data.futures_product_read_store import get_latest_futures_product_read
from snapper.data.models import AccountingRawReceipt
from snapper.data.models import FuturesProductReadAttempt
from snapper.data.models import FuturesProductReadOutcome
from snapper.data.native_account_read_store import get_latest_native_account_read
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyToken
from snapper.messaging.infrastructure.publisher import SequenceTracker
from tests.helpers.futures_product_read import PRODUCT_PATH
from tests.helpers.futures_product_read import futures_product_scenario
from tests.helpers.native_account_authority import native_authority_uuid


def _identity(number: int) -> ExecutionEvidenceIdentity:
    """Allocate an explicit test-owned product request without changing any genuine source proof."""
    return ExecutionEvidenceIdentity(
        public_id=native_authority_uuid(number),
        session_id=native_authority_uuid(900),
        sequence_id=number,
    )


async def test_actual_http_observes_committed_identity_accounts_and_separate_product_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Prove actual source commit ordering at the final Requests adapter boundary.

    Given: A real isolated database and selected SDK User with closed signed key/accounts/product HTTP.
    When: The canonical product producer completes an actual authenticated product observation.
    Then: Independent SQLite probes see all prior commits and the original entity rebinds exactly.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        outcome = await producer.read_native_futures_products(
            native.repository, native.client, native.tracker
        )
        assert isinstance(outcome.payload.result, FuturesProductReadSource)
        assert base64.b64decode(outcome.payload.result.body) == scenario.adapter._original_body()
        assert scenario.adapter.visible_product_sources == [(1, 1, 1)]
        assert len(scenario.adapter.product_calls) == 1
        async with native.repository.session() as session:
            bound = await get_bound_futures_product_outcome(
                session, outcome.payload.attempt_public_id
            )
            latest = await get_latest_futures_product_read(
                session, native.selection, native.clock.current
            )
            assert bound == outcome
            assert latest is not None and latest.outcome == outcome
            assert (
                await session.scalar(select(func.count()).select_from(FuturesProductReadAttempt))
                == 1
            )
            assert (
                await session.scalar(select(func.count()).select_from(FuturesProductReadOutcome))
                == 1
            )
            assert await session.scalar(select(func.count()).select_from(AccountingRawReceipt)) == 1


@pytest.mark.parametrize(
    ("status", "body", "expected"),
    [
        (403, b'{"apiKey":"synthetic-product-secret"}', "http_error"),
        (200, b'{"extra":[{"APIKEY":"synthetic-product-secret"}]}', "source_unsafe"),
        (200, b'{"extra":"synthetic-product-secret",', "source_invalid"),
    ],
)
async def test_actual_product_failures_retain_only_fixed_metadata_without_response_body_or_hash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    status: int,
    body: bytes,
    expected: str,
) -> None:
    """Keep failed or authentication-bearing actual HTTP responses outside durable product evidence.

    Given: The real product endpoint returns a failed, unsafe or malformed original entity.
    When: The closed producer classifies that response and commits its actual terminal result.
    Then: Only fixed failure metadata remains and the newest product request still blocks fallback.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        scenario.adapter.product_status = status
        scenario.adapter.product_body = body
        outcome = await producer.read_native_futures_products(
            native.repository, native.client, native.tracker
        )
        assert isinstance(outcome.payload.result, FuturesProductReadFailure)
        assert outcome.payload.result.reason == expected
        material = canonical_futures_product_outcome(outcome.payload)
        assert "synthetic-product-secret" not in material
        assert base64.b64encode(body).decode("ascii") not in material
        assert "body" not in outcome.payload.result.model_dump()
        async with native.repository.session() as session:
            latest = await get_latest_futures_product_read(
                session, native.selection, native.clock.current
            )
            assert latest is not None and latest.outcome == outcome


class _ChangeProductRequest(AuthBase):
    """Alter only the final real product request after SDK signing, using synthetic fixture material."""

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        """Leave actual identity/accounts preparation untouched and mutate the later product request."""
        if (request.url or "").endswith(PRODUCT_PATH):
            request.headers["APIKey"] = "different-synthetic-product-key"
        return request


@pytest.mark.parametrize("boundary", ["after_product_commit", "requests_auth"])
async def test_actual_product_request_cannot_change_key_after_successful_account_preparation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, boundary: str
) -> None:
    """Bind product evidence to the key physically sent after all SDK and Requests preparation.

    Given: Actual key/accounts sources and a committed product attempt bound to the original key.
    When: The same SDK key or Session.auth changes only the later product authentication.
    Then: No physical product request occurs and the committed product attempt receives a failure.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        user = native.client._user_client
        assert user is not None
        original_start = producer._start_product_operation

        async def begin_then_change(
            repository: AccountingRepository, token: PreparedKeyToken, tracker: SequenceTracker
        ) -> producer._ProductOperation:
            """Wait for the real attempt commit before mutating the same actual SDK object."""
            operation = await original_start(repository, token, tracker)
            monkeypatch.setattr(user, "_key", "different-synthetic-product-key")
            return operation

        if boundary == "after_product_commit":
            monkeypatch.setattr(producer, "_start_product_operation", begin_then_change)
        else:
            native.sessions[0].auth = _ChangeProductRequest()
        outcome = await producer.read_native_futures_products(
            native.repository, native.client, native.tracker
        )
        assert isinstance(outcome.payload.result, FuturesProductReadFailure)
        assert outcome.payload.result.reason == "transport_error"
        assert scenario.adapter.product_calls == []
        assert outcome.payload.attempt.key_proof.account_uid == native_authority_uuid(80)


async def test_late_success_cannot_replace_newer_pending_product_request_or_change_accounts_ordering(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep original request order authoritative after actual HTTP completes but persistence is delayed.

    Given: A real product response pauses before its outcome commit while a newer request commits.
    When: The older response finally commits its genuine original entity.
    Then: Current product selection remains pending on the newer request and accounts ordering is intact.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        arrived, release = asyncio.Event(), asyncio.Event()
        original_record = producer._record_product_result
        ready: list[producer._ProductOperation] = []

        async def pause_capture(
            repository: AccountingRepository,
            operation: producer._ProductOperation,
            result: FuturesProductWriteResult,
        ) -> BoundFuturesProductOutcome:
            """Delay only durable capture after the real signed product HTTP response has arrived."""
            ready.append(operation)
            arrived.set()
            await release.wait()
            return await original_record(repository, operation, result)

        monkeypatch.setattr(producer, "_record_product_result", pause_capture)
        task = asyncio.create_task(
            producer.read_native_futures_products(native.repository, native.client, native.tracker)
        )
        try:
            await asyncio.wait_for(arrived.wait(), timeout=5)
            reference = ProductReadKeyReference.model_validate(ready[0].attempt.payload.key_proof)
            pending = await begin_futures_product_read(native.repository, reference, _identity(902))
            async with native.repository.session() as session:
                accounts_before = await get_latest_native_account_read(
                    session, native.selection, native.clock.current
                )
            release.set()
            completed = await task
            async with native.repository.session() as session:
                latest = await get_latest_futures_product_read(
                    session, native.selection, native.clock.current
                )
                accounts_after = await get_latest_native_account_read(
                    session, native.selection, native.clock.current
                )
            assert isinstance(completed.payload.result, FuturesProductReadSource)
            assert latest is not None and latest.attempt.public_id == pending.public_id
            assert latest.outcome is None
            assert pending.payload.family_sequence == completed.payload.attempt.family_sequence + 1
            assert accounts_before == accounts_after
        finally:
            release.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
