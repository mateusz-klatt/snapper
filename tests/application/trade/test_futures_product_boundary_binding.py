"""Actual source, credential rotation and restored-storage boundaries for product eligibility."""

import sqlite3
from contextlib import closing
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import func
from sqlalchemy import select

from snapper.application.trade.futures_product_eligibility import FuturesProductFreshnessPolicy
from snapper.application.trade.futures_product_eligibility import _require_compatible_account
from snapper.application.trade.futures_product_eligibility import require_current_futures_product
from snapper.application.trade.futures_product_read import read_native_futures_products
from snapper.application.trade.native_order_access import (
    require_current_native_futures_order_access,
)
from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.data.futures_product_key_binding import FuturesProductBindingError
from snapper.data.futures_product_key_binding import get_bound_product_read_key
from snapper.data.futures_product_key_binding import product_read_key_reference
from snapper.data.futures_product_read_store import get_bound_futures_product_attempt
from snapper.data.futures_product_read_store import get_bound_futures_product_outcome
from snapper.data.models import FuturesProductReadAttempt as ProductAttemptRow
from snapper.data.native_account_authority_store import NativeAuthorityFreshnessPolicy
from snapper.data.native_account_identity_store import find_native_account_identity
from tests.application.trade.test_futures_product_eligibility import _actual_route
from tests.application.trade.test_futures_product_eligibility import _policy
from tests.helpers.futures_product_read import futures_product_scenario
from tests.helpers.native_account_authority import capture_native_account_read
from tests.helpers.native_account_authority import native_authority_uuid
from tests.helpers.native_account_authority import rotate_native_credential


@pytest.mark.parametrize(("age", "skew"), [(0, 1), (1, -1)])
def test_product_policy_refuses_disabled_age_or_negative_provider_clock_bound(
    age: int, skew: int
) -> None:
    """Reject invalid trusted configuration before any current-source decision.

    Given: A valid independent native-account freshness policy.
    When: Product freshness disables its age bound or reverses its permitted clock-skew interval.
    Then: Configuration refuses instead of implicitly granting an unbounded source lifetime.
    """
    native = NativeAuthorityFreshnessPolicy(timedelta(seconds=60), timedelta(seconds=1))
    with pytest.raises(ValueError, match="Product freshness bounds"):
        FuturesProductFreshnessPolicy(native, timedelta(seconds=age), timedelta(seconds=skew))


@pytest.mark.parametrize("missing", ["selection", "user"])
async def test_actual_product_client_precondition_refuses_before_any_attempt_or_http(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, missing: str
) -> None:
    """Require both the actual selected client and original SDK owner before preparing a product read.

    Given: A real SDK/repository fixture whose client loses its selected identity or SDK User.
    When: The actual product producer is called before starting owner preparation.
    Then: It refuses without committing a product request or sending any HTTP request.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native

        def unavailable_selection() -> ExecutionClientSelection | None:
            """Expose an actual absent public client binding without replacing a receipt or issuer."""
            return None

        with monkeypatch.context() as change:
            if missing == "selection":
                change.setattr(native.client, "execution_client_selection", unavailable_selection)
            else:
                change.setattr(native.client, "_user_client", None)
            with pytest.raises(FuturesProductBindingError, match="actual_client_missing"):
                await read_native_futures_products(native.repository, native.client, native.tracker)
        async with native.repository.session() as session:
            assert await session.scalar(select(func.count()).select_from(ProductAttemptRow)) == 0
        assert scenario.adapter.calls == []


@pytest.mark.parametrize("changed", ["digest", "recorded_at"])
async def test_product_reference_rebinds_actual_proof_instead_of_trusting_a_changed_public_copy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, changed: str
) -> None:
    """Require exact independently stored source identity and chronology on a historical reference.

    Given: Genuine signed key/accounts/product HTTP has committed an original safe proof.
    When: Its public reference changes only the proof digest or durable recording time.
    Then: The actual database loader refuses the copy and the original product evidence still loads.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        original = await read_native_futures_products(
            native.repository, native.client, native.tracker
        )
        reference = original.payload.attempt.key_proof
        altered = reference.model_copy(
            update=(
                {"content_digest": "a" * 64}
                if changed == "digest"
                else {"recorded_at": reference.recorded_at + timedelta(microseconds=1)}
            )
        )
        async with native.repository.session() as session:
            with pytest.raises(FuturesProductBindingError, match="actual_key_proof_mismatch"):
                await get_bound_product_read_key(session, altered)
            assert (
                await get_bound_futures_product_outcome(session, original.payload.attempt_public_id)
                == original
            )


async def test_actual_conflicting_external_owner_cannot_become_a_product_key_reference(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Preserve a genuinely observed owner conflict without upgrading it into product source authority.

    Given: A real original owner and a rotated credential whose actual key response names another UID.
    When: The retained conflicting proof is offered to the product-reference projection.
    Then: Projection refuses the actual owner mismatch and the original product remains unchanged.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        original = await read_native_futures_products(
            native.repository, native.client, native.tracker
        )
        await rotate_native_credential(native)
        scenario.adapter.account_uid = native_authority_uuid(81)
        conflict = await capture_native_account_read(native)
        assert conflict.payload.result.kind == "failed"
        async with native.repository.session() as session:
            proof = await find_native_account_identity(session, conflict.payload.attempt_public_id)
            assert proof is not None and not proof.payload.matches_owner
            with pytest.raises(FuturesProductBindingError, match="original_owner_mismatch"):
                product_read_key_reference(proof)
            assert (
                await get_bound_futures_product_outcome(session, original.payload.attempt_public_id)
                == original
            )


async def test_current_product_refuses_old_credential_source_after_actual_same_owner_rotation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Rebind current credential revision even when the original and current real venue UID agree.

    Given: A successful product source followed by actual credential rotation and a fresh same-owner account read.
    When: The current public prerequisite loads the older product under the real wallet fence.
    Then: Latest-source selection refuses first; the independent account comparison also refuses mixing these real sources.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        scenario.adapter.general_access = "FULL_ACCESS"
        original = await read_native_futures_products(
            native.repository, native.client, native.tracker
        )
        await rotate_native_credential(native)
        accounts = await capture_native_account_read(native)
        route = await _actual_route(scenario, accounts.payload.attempt_public_id, monkeypatch)
        async with native.repository.session() as session:
            await native.repository._begin_execution_insert_transaction(session)
            await native.repository._acquire_execution_fence_lock(
                session, native.selection.wallet_public_id
            )
            with pytest.raises(FuturesProductBindingError, match="latest_client_changed"):
                await require_current_futures_product(
                    session, route, "PF_XBTUSD", _policy(scenario)
                )
            current = await require_current_native_futures_order_access(
                session, route, _policy(scenario).native_accounts
            )
            with pytest.raises(
                FuturesProductBindingError, match="current_account_or_credential_changed"
            ):
                _require_compatible_account(original, current)
            assert (
                await get_bound_futures_product_outcome(session, original.payload.attempt_public_id)
                == original
            )
        assert len(scenario.adapter.product_calls) == 1


def _restore_invalid_canonical_payload(path: Path, table: str, public_id: str) -> None:
    """Model a corrupt restored row in this test's isolated SQLite file while restoring every real guard."""
    assert table in {"futures_product_read_attempts", "futures_product_read_outcomes"}
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute("BEGIN IMMEDIATE")
        triggers = connection.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='trigger' AND tbl_name=?",
            (table,),
        ).fetchall()
        assert triggers
        for name, _ in triggers:
            escaped = name.replace('"', '""')
            connection.execute(f'DROP TRIGGER "{escaped}"')
        changed = connection.execute(
            f"UPDATE {table} SET canonical_payload=? WHERE public_id=?",
            ('{"schema_version":0}', public_id),
        )
        assert changed.rowcount == 1
        for _, definition in triggers:
            connection.execute(definition)


@pytest.mark.parametrize("kind", ["attempt", "outcome"])
async def test_actual_historical_loader_refuses_malformed_canonical_payload_after_isolated_restore(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    """Keep actual historical readers closed when a restored database contains structurally invalid source bytes.

    Given: Actual signed HTTP has created valid source rows in an isolated database.
    When: A test-only restore transaction corrupts one canonical payload and reinstalls its real guards.
    Then: The unchanged actual loader refuses that row without accepting projections or inventing a replacement.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        original = await read_native_futures_products(
            native.repository, native.client, native.tracker
        )
        public_id = original.payload.attempt_public_id if kind == "attempt" else original.public_id
        table = (
            "futures_product_read_attempts"
            if kind == "attempt"
            else "futures_product_read_outcomes"
        )
        _restore_invalid_canonical_payload(native.adapter.path, table, public_id)
        async with native.repository.session() as session:
            with pytest.raises(FuturesProductBindingError, match=f"{kind}_payload_invalid"):
                if kind == "attempt":
                    await get_bound_futures_product_attempt(session, public_id)
                else:
                    await get_bound_futures_product_outcome(
                        session, original.payload.attempt_public_id
                    )
