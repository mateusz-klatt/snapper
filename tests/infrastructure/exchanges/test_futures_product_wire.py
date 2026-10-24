"""Actual guarded product receipt ownership survives producer completion without permitting forgery."""

import base64
import copy
import pickle
from pathlib import Path

import pytest
from kraken.futures import User

import snapper.application.trade.futures_product_read as producer
from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.futures_product_read import FuturesProductReadSource
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.futures_product_capture import FuturesProductWriteResult
from snapper.data.futures_product_key_binding import FuturesProductBindingError
from snapper.data.futures_product_read_store import BoundFuturesProductOutcome
from snapper.data.futures_product_read_store import _append_futures_product_outcome
from snapper.data.futures_product_read_store import begin_futures_product_read
from snapper.data.futures_product_read_store import get_bound_futures_product_outcome
from snapper.data.futures_product_read_store import get_latest_futures_product_read
from snapper.infrastructure.exchanges.futures_trading_instruments_wire import (
    FuturesProductTransportError,
)
from snapper.infrastructure.exchanges.futures_trading_instruments_wire import (
    GuardedFuturesProductReceipt,
)
from snapper.infrastructure.exchanges.futures_trading_instruments_wire import (
    require_guarded_product_receipt,
)
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyToken
from tests.helpers.futures_product_read import FuturesProductScenario
from tests.helpers.futures_product_read import futures_product_scenario
from tests.helpers.native_account_authority import native_authority_uuid


async def _actual_source(
    scenario: FuturesProductScenario, monkeypatch: pytest.MonkeyPatch
) -> tuple[BoundFuturesProductOutcome, GuardedFuturesProductReceipt]:
    """Retain the actual issued object while exercising every genuine producer/source/store boundary."""
    actual_read = producer.read_futures_trading_instruments_wire
    captured: list[GuardedFuturesProductReceipt] = []

    def retain_actual(
        user: User,
        selected: ExecutionClientSelection,
        token: PreparedKeyToken,
        product_attempt_public_id: str,
    ) -> GuardedFuturesProductReceipt:
        """Return the unchanged actual transport response while preserving all guards and commits."""
        result = actual_read(user, selected, token, product_attempt_public_id)
        captured.append(result)
        return result

    monkeypatch.setattr(producer, "read_futures_trading_instruments_wire", retain_actual)
    native = scenario.native
    outcome = await producer.read_native_futures_products(
        native.repository, native.client, native.tracker
    )
    assert len(captured) == 1
    return outcome, captured[0]


async def test_only_actual_issued_product_receipt_can_enter_source_classification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Require actual final-request transport issuance, not a caller-created matching source shape.

    Given: The actual producer completes signed local key, accounts and product HTTP with real commits.
    When: A caller reconstructs the exact product receipt or mutates its original issued material.
    Then: Only the unchanged actual transport object is accepted and all reconstructed shapes fail.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        _, actual = await _actual_source(scenario, monkeypatch)
        source = require_guarded_product_receipt(actual)
        assert source.entity_bytes == scenario.adapter._original_body()
        forged = GuardedFuturesProductReceipt(source)
        for unissued in (object(), source, forged):
            with pytest.raises(FuturesProductTransportError, match="actual_transport_required"):
                require_guarded_product_receipt(unissued)
        with pytest.raises(AttributeError, match="immutable"):
            actual._receipt = source
        assert "original-product-marker" not in repr(actual)
        with pytest.raises(TypeError, match="serialized"):
            pickle.dumps(actual)
        with pytest.raises(TypeError, match="copied"):
            copy.deepcopy(actual)
        changed = source.model_copy(update={"entity_bytes": b"changed-after-transport"})
        object.__setattr__(actual, "_receipt", changed)
        with pytest.raises(FuturesProductTransportError, match="actual_transport_required"):
            require_guarded_product_receipt(actual)


async def test_actual_issued_product_response_cannot_complete_another_committed_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bind the original actual transport invocation to its own request despite equal key and clock copies.

    Given: Actual product HTTP has completed and another request uses the same actual proof and clock.
    When: The original issued response is offered as the later request's first successful outcome.
    Then: The writer refuses its different original attempt identity and leaves the later request pending.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        original, received = await _actual_source(scenario, monkeypatch)
        native = scenario.native
        pending = await begin_futures_product_read(
            native.repository,
            original.payload.attempt.key_proof,
            ExecutionEvidenceIdentity(
                public_id=native_authority_uuid(960),
                session_id=native_authority_uuid(961),
                sequence_id=960,
            ),
        )
        async with native.repository.session() as session:
            await native.repository._begin_execution_insert_transaction(session)
            await native.repository._acquire_execution_fence_lock(
                session, native.selection.wallet_public_id
            )
            with pytest.raises(
                FuturesProductBindingError, match="actual_transport_attempt_mismatch"
            ):
                await _append_futures_product_outcome(
                    session,
                    pending.public_id,
                    received,
                    ExecutionEvidenceIdentity(
                        public_id=native_authority_uuid(962),
                        session_id=native_authority_uuid(961),
                        sequence_id=962,
                    ),
                )
        async with native.repository.session() as session:
            assert await get_bound_futures_product_outcome(session, pending.public_id) is None
            assert (
                await get_bound_futures_product_outcome(session, original.payload.attempt_public_id)
                == original
            )
        assert len(scenario.adapter.product_calls) == 1


@pytest.mark.parametrize("mutation_target", ["retained_receipt", "public_return"])
async def test_actual_product_source_is_isolated_from_unchecked_model_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mutation_target: str
) -> None:
    """Preserve original issued source bytes even when a caller bypasses a public model's frozen setter.

    Given: Genuine product HTTP has issued an opaque response and exposed its validated public view.
    When: Caller code uses unchecked model mutation on the receipt or on the returned source view.
    Then: Changed retained material refuses and a changed public view cannot rewrite the original source.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        _, actual = await _actual_source(scenario, monkeypatch)
        original = require_guarded_product_receipt(actual)
        expected = original.entity_bytes
        changed = actual._receipt if mutation_target == "retained_receipt" else original
        object.__setattr__(changed, "entity_bytes", b"synthetic-unchecked-product-mutation")
        if mutation_target == "retained_receipt":
            with pytest.raises(FuturesProductTransportError, match="actual_transport_required"):
                require_guarded_product_receipt(actual)
        else:
            assert require_guarded_product_receipt(actual).entity_bytes == expected


@pytest.mark.parametrize("mutation_target", ["retained_receipt", "public_return"])
async def test_actual_product_capture_cannot_persist_changed_source_through_a_model_alias(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mutation_target: str
) -> None:
    """Prove source isolation at the actual first-outcome transaction after genuine signed HTTP.

    Given: A real product request completes while its original captured outcome is not yet written.
    When: Caller code mutates the retained receipt or a public source view just before actual capture.
    Then: Changed retained material leaves the request pending and a changed public view cannot alter storage.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        actual_record = producer._record_product_result
        original_bodies: list[bytes] = []

        async def mutate_before_capture(
            repository: AccountingRepository,
            operation: producer._ProductOperation,
            received: FuturesProductWriteResult,
        ) -> BoundFuturesProductOutcome:
            """Change the real issued source at the last caller seam before unchanged actual storage."""
            assert isinstance(received, GuardedFuturesProductReceipt)
            public_view = require_guarded_product_receipt(received)
            original_bodies.append(public_view.entity_bytes)
            changed = received._receipt if mutation_target == "retained_receipt" else public_view
            forged_body = public_view.entity_bytes.replace(
                b"original-product-marker", b"forged-product-marker"
            )
            assert forged_body != public_view.entity_bytes
            object.__setattr__(changed, "entity_bytes", forged_body)
            return await actual_record(repository, operation, received)

        monkeypatch.setattr(producer, "_record_product_result", mutate_before_capture)
        if mutation_target == "retained_receipt":
            with pytest.raises(FuturesProductTransportError, match="actual_transport_required"):
                await producer.read_native_futures_products(
                    native.repository, native.client, native.tracker
                )
        else:
            await producer.read_native_futures_products(
                native.repository, native.client, native.tracker
            )
        assert len(original_bodies) == 1
        async with native.repository.session() as session:
            latest = await get_latest_futures_product_read(
                session, native.selection, native.clock.current
            )
            assert latest is not None
            if mutation_target == "retained_receipt":
                assert latest.outcome is None
            else:
                assert latest.outcome is not None
                result = latest.outcome.payload.result
                assert isinstance(result, FuturesProductReadSource)
                assert base64.b64decode(result.body) == original_bodies[0]
        assert len(scenario.adapter.product_calls) == 1
