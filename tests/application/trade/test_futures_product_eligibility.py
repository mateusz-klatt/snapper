"""Current product prerequisites from genuine signed key/accounts/product sources and actual stores."""

from datetime import timedelta
from pathlib import Path

import pytest

import snapper.application.trade.native_account_authority as authority_capture
import snapper.application.trade.native_order_access as order_access
from snapper.application.trade.futures_product_eligibility import FuturesProductFreshnessPolicy
from snapper.application.trade.futures_product_eligibility import require_current_futures_product
from snapper.application.trade.futures_product_read import read_native_futures_products
from snapper.application.trade.native_account_authority import NativeAuthorityCaptureRequest
from snapper.application.trade.native_account_authority import capture_native_account_authority
from snapper.application.trade.native_order_access import NativeFuturesOrderAccessUnavailableError
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.execution_unit_admission import ExecutionRouteReference
from snapper.data.futures_product_key_binding import FuturesProductBindingError
from snapper.data.futures_product_read_store import begin_futures_product_read
from tests.application.trade.test_futures_trading_instruments import _row
from tests.application.trade.test_futures_trading_instruments import _wire
from tests.helpers.futures_product_read import FuturesProductScenario
from tests.helpers.futures_product_read import futures_product_scenario
from tests.helpers.native_account_authority import capture_native_account_read
from tests.helpers.native_account_authority import native_authority_uuid


def _identity(number: int) -> ExecutionEvidenceIdentity:
    """Keep distinct actual capture/request identities without inventing account or source observations."""
    return ExecutionEvidenceIdentity(
        public_id=native_authority_uuid(number),
        session_id=native_authority_uuid(950),
        sequence_id=number,
    )


async def _actual_route(
    scenario: FuturesProductScenario,
    account_attempt_public_id: str,
    monkeypatch: pytest.MonkeyPatch,
) -> ExecutionRouteReference:
    """Produce actual native profile authority by independently binding the supplied accounts attempt."""
    native = scenario.native
    monkeypatch.setattr(authority_capture, "datetime", native.clock)
    monkeypatch.setattr(order_access, "datetime", native.clock)
    captured = await capture_native_account_authority(
        native.repository,
        NativeAuthorityCaptureRequest(
            read_attempt_public_id=account_attempt_public_id,
            native_account_key="flex",
        ),
        _identity(951),
        native.policy,
    )
    return captured.reference


def _policy(scenario: FuturesProductScenario) -> FuturesProductFreshnessPolicy:
    """Use distinct trusted product-age and accounts-age windows to expose accidental renewal."""
    return FuturesProductFreshnessPolicy(
        native_accounts=scenario.native.policy,
        maximum_product_source_age=timedelta(seconds=5),
        maximum_product_server_clock_skew=timedelta(seconds=1),
    )


@pytest.mark.parametrize(("post_only", "reduction"), [(False, False), (True, False), (False, True)])
async def test_actual_current_account_and_product_read_return_unsatisfied_order_conditions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, post_only: bool, reduction: bool
) -> None:
    """Expose necessary exchange restrictions after independently proving actual full-access account state.

    Given: Real same-key account/product sources and an explicitly full-access key-check response.
    When: The actual current prerequisite is evaluated under the wallet execution fence.
    Then: Exact outgoing symbol restrictions are reported without claiming funds or order permission.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        scenario.adapter.general_access = "FULL_ACCESS"
        scenario.adapter.product_body = _wire(
            [_row({"postOnly": post_only, "restricted": reduction})],
            {"serverTime": native.clock.current.isoformat().replace("+00:00", "Z")},
        )
        product = await read_native_futures_products(
            native.repository, native.client, native.tracker
        )
        route = await _actual_route(
            scenario, product.payload.attempt.key_proof.read_attempt_public_id, monkeypatch
        )
        async with native.repository.session() as session:
            await native.repository._begin_execution_insert_transaction(session)
            await native.repository._acquire_execution_fence_lock(
                session, native.selection.wallet_public_id
            )
            required = await require_current_futures_product(
                session, route, "PF_XBTUSD", _policy(scenario)
            )
        assert required.product_outcome == product
        assert required.requirements.requires_post_only is post_only
        assert required.requirements.requires_position_reduction is reduction
        assert required.access.key_proof.payload.observation.account_uid == native_authority_uuid(
            80
        )


async def test_read_only_account_can_read_products_but_cannot_satisfy_current_order_access(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep readable product metadata distinct from the actual account permission required for orders.

    Given: A real READ_ONLY selected key successfully reads both account and instrument data.
    When: Current product prerequisites are evaluated for a new order under the actual wallet fence.
    Then: The independently loaded key permission refuses before product flags can imply access.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        product = await read_native_futures_products(
            native.repository, native.client, native.tracker
        )
        assert product.payload.result.kind == "captured"
        route = await _actual_route(
            scenario, product.payload.attempt.key_proof.read_attempt_public_id, monkeypatch
        )
        async with native.repository.session() as session:
            await native.repository._begin_execution_insert_transaction(session)
            await native.repository._acquire_execution_fence_lock(
                session, native.selection.wallet_public_id
            )
            with pytest.raises(
                NativeFuturesOrderAccessUnavailableError, match="requires_full_access"
            ):
                await require_current_futures_product(
                    session, route, "PF_XBTUSD", _policy(scenario)
                )


@pytest.mark.parametrize("latest", ["pending", "failed"])
async def test_newer_pending_or_failed_product_attempt_blocks_the_older_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, latest: str
) -> None:
    """Select actual request order before market eligibility instead of searching for a convenient success.

    Given: An actual successful product source is followed by a real pending or unsuccessful request.
    When: Current product prerequisites rebind the newest request against a fresh actual account route.
    Then: The new state refuses even though an older unrestricted product list remains stored.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        scenario.adapter.general_access = "FULL_ACCESS"
        product = await read_native_futures_products(
            native.repository, native.client, native.tracker
        )
        if latest == "pending":
            await begin_futures_product_read(
                native.repository, product.payload.attempt.key_proof, _identity(952)
            )
        else:
            scenario.adapter.product_status = 503
            product = await read_native_futures_products(
                native.repository, native.client, native.tracker
            )
        route = await _actual_route(
            scenario, product.payload.attempt.key_proof.read_attempt_public_id, monkeypatch
        )
        async with native.repository.session() as session:
            await native.repository._begin_execution_insert_transaction(session)
            await native.repository._acquire_execution_fence_lock(
                session, native.selection.wallet_public_id
            )
            with pytest.raises(FuturesProductBindingError, match="latest_read"):
                await require_current_futures_product(
                    session, route, "PF_XBTUSD", _policy(scenario)
                )


async def test_original_product_age_expires_while_actual_account_source_remains_current(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep product source age independent from still-fresh actual account identity and permissions.

    Given: A real product source has a five-second age bound while its actual account has sixty seconds.
    When: The trusted clock reaches the product boundary and current prerequisites run under the fence.
    Then: Product freshness refuses without borrowing the longer account window or a reread timestamp.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        scenario.adapter.general_access = "FULL_ACCESS"
        product = await read_native_futures_products(
            native.repository, native.client, native.tracker
        )
        route = await _actual_route(
            scenario, product.payload.attempt.key_proof.read_attempt_public_id, monkeypatch
        )
        native.clock.current += timedelta(seconds=5)
        async with native.repository.session() as session:
            await native.repository._begin_execution_insert_transaction(session)
            await native.repository._acquire_execution_fence_lock(
                session, native.selection.wallet_public_id
            )
            with pytest.raises(FuturesProductBindingError, match="original_source_stale"):
                await require_current_futures_product(
                    session, route, "PF_XBTUSD", _policy(scenario)
                )


async def test_later_actual_same_owner_key_check_does_not_relabel_or_invalidate_product_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Compare stable account/root/credential identity while preserving independent actual read clocks.

    Given: A real product source is followed by another genuine same-owner key/accounts read.
    When: Current native authority refers to the newer proof under the same selected credential.
    Then: The original still-fresh product remains usable without replacing its source or proof identity.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        scenario.adapter.general_access = "FULL_ACCESS"
        product = await read_native_futures_products(
            native.repository, native.client, native.tracker
        )
        native.clock.current += timedelta(seconds=1)
        accounts = await capture_native_account_read(native)
        route = await _actual_route(scenario, accounts.payload.attempt_public_id, monkeypatch)
        async with native.repository.session() as session:
            await native.repository._begin_execution_insert_transaction(session)
            await native.repository._acquire_execution_fence_lock(
                session, native.selection.wallet_public_id
            )
            required = await require_current_futures_product(
                session, route, "PF_XBTUSD", _policy(scenario)
            )
        assert required.product_outcome == product
        assert required.access.key_proof.public_id != product.payload.attempt.key_proof.public_id
        assert required.access.checked_at > product.payload.completed_at
        assert required.access.key_proof.payload.observation.account_uid == (
            product.payload.attempt.key_proof.account_uid
        )


@pytest.mark.parametrize("offset", [-2, 2])
async def test_actual_product_server_clock_is_independently_bounded_from_fresh_accounts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, offset: int
) -> None:
    """Refuse implausible original product time even when actual account and local receipt age are fresh.

    Given: Actual full-access account sources are fresh and product HTTP carries a skewed server time.
    When: The current product prerequisite compares that original time with actual local observation.
    Then: It refuses the provider-clock discrepancy without borrowing the accounts server clock.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        scenario.adapter.general_access = "FULL_ACCESS"
        provider_time = native.clock.current + timedelta(seconds=offset)
        scenario.adapter.product_body = _wire(
            [_row()], {"serverTime": provider_time.isoformat().replace("+00:00", "Z")}
        )
        product = await read_native_futures_products(
            native.repository, native.client, native.tracker
        )
        route = await _actual_route(
            scenario, product.payload.attempt.key_proof.read_attempt_public_id, monkeypatch
        )
        async with native.repository.session() as session:
            await native.repository._begin_execution_insert_transaction(session)
            await native.repository._acquire_execution_fence_lock(
                session, native.selection.wallet_public_id
            )
            with pytest.raises(FuturesProductBindingError, match="server_clock_outside_policy"):
                await require_current_futures_product(
                    session, route, "PF_XBTUSD", _policy(scenario)
                )


async def test_actual_full_access_accounts_without_product_read_do_not_authorize_market(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Require an independent actual product observation even after complete account preparation.

    Given: The real key/accounts producer and native authority succeed without any product request.
    When: A current product prerequisite is requested under the actual wallet fence.
    Then: It refuses the absent product source and performs no product HTTP as an implicit fallback.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        scenario.adapter.general_access = "FULL_ACCESS"
        accounts = await capture_native_account_read(native)
        route = await _actual_route(scenario, accounts.payload.attempt_public_id, monkeypatch)
        async with native.repository.session() as session:
            await native.repository._begin_execution_insert_transaction(session)
            await native.repository._acquire_execution_fence_lock(
                session, native.selection.wallet_public_id
            )
            with pytest.raises(FuturesProductBindingError, match="latest_read_missing_or_pending"):
                await require_current_futures_product(
                    session, route, "PF_XBTUSD", _policy(scenario)
                )
        assert scenario.adapter.product_calls == []
