"""Current flex risk reads rebind genuine signed account sources and never infer order permission."""

from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest

import snapper.application.trade.current_futures_flex_risk as current_risk
import snapper.application.trade.native_account_authority as authority_capture
from snapper.application.trade.futures_flex_risk import FuturesFlexRiskSourceError
from snapper.application.trade.native_account_authority import NativeAuthorityCaptureRequest
from snapper.application.trade.native_account_authority import capture_native_account_authority
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.execution_unit_admission import ExecutionRouteReference
from snapper.core.native_account_read import NativeAccountReadCapturedSource
from snapper.data.execution_route_authority_store import require_current_route_authority
from snapper.data.native_account_read_store import begin_native_account_read
from tests.application.trade.test_futures_flex_risk import _WIRE
from tests.helpers.native_account_authority import NativeAuthorityScenario
from tests.helpers.native_account_authority import capture_native_account_read
from tests.helpers.native_account_authority import native_authority_scenario
from tests.helpers.native_account_authority import native_authority_uuid
from tests.helpers.unit_admission import refresh_admission_account
from tests.helpers.unit_admission import seed_admission_scenario


async def _actual_risk_route(
    scenario: NativeAuthorityScenario, monkeypatch: pytest.MonkeyPatch, account: str = "flex"
) -> ExecutionRouteReference:
    """Commit real key/accounts sources and current profile authority without inventing source DTOs."""
    scenario.adapter.profiles = _WIRE.split(b'"accounts":', 1)[1][:-1]
    monkeypatch.setattr(authority_capture, "datetime", scenario.clock)
    monkeypatch.setattr(current_risk, "datetime", scenario.clock)
    outcome = await capture_native_account_read(scenario)
    assert isinstance(outcome.payload.result, NativeAccountReadCapturedSource)
    authority = await capture_native_account_authority(
        scenario.repository,
        NativeAuthorityCaptureRequest(
            read_attempt_public_id=outcome.payload.attempt_public_id,
            native_account_key=account,
        ),
        ExecutionEvidenceIdentity(
            public_id=native_authority_uuid(1100),
            session_id=native_authority_uuid(1101),
            sequence_id=1100,
        ),
        scenario.policy,
    )
    return authority.reference


async def test_read_only_key_can_observe_actual_current_risk_without_order_permission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Separate authenticated risk observation from the independent permission needed to send orders.

    Given: A real READ_ONLY key completes signed local accounts HTTP with complete original risk fields.
    When: The current risk reader rebinds that source under the wallet execution fence.
    Then: It returns exact original risk observations without another HTTP call or order capability.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as scenario:
        route = await _actual_risk_route(scenario, monkeypatch)
        assert scenario.adapter.general_access == "READ_ONLY"
        count = len(scenario.adapter.calls)
        async with scenario.repository.session() as session:
            await scenario.repository._begin_execution_insert_transaction(session)
            await scenario.repository._acquire_execution_fence_lock(
                session, scenario.selection.wallet_public_id
            )
            result = await current_risk.require_current_futures_flex_risk(
                session, route, scenario.policy
            )
        assert result.authority.public_id == route.authority_public_id
        assert result.snapshot.margin_usd.initial_margin_usd == Decimal(100)
        assert result.snapshot.margin_usd.initial_margin_with_orders_usd == Decimal(125)
        assert result.snapshot.margin_usd.available_margin_usd == Decimal(-1)
        assert result.snapshot.currencies[0].quantity_native == Decimal("0.003000000000000000001")
        assert result.checked_at == scenario.clock.current
        assert len(scenario.adapter.calls) == count


@pytest.mark.parametrize("later_result", ["pending", "failed"])
async def test_later_pending_or_failed_accounts_read_blocks_older_risk_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, later_result: str
) -> None:
    """Preserve coarse accounts request ordering when risk fields remain readable in old raw bytes.

    Given: A genuine current risk source is followed by another actual pending or failed accounts read.
    When: Current risk is requested using the earlier authority under the real wallet fence.
    Then: The newer accounts state refuses without falling back to older favorable amounts.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as scenario:
        route = await _actual_risk_route(scenario, monkeypatch)
        if later_result == "failed":
            scenario.adapter.fail_accounts = True
            await capture_native_account_read(scenario)
        else:
            await begin_native_account_read(
                scenario.repository,
                scenario.selection,
                ExecutionEvidenceIdentity(
                    public_id=native_authority_uuid(1102),
                    session_id=native_authority_uuid(1101),
                    sequence_id=1102,
                ),
            )
        async with scenario.repository.session() as session:
            await scenario.repository._begin_execution_insert_transaction(session)
            await scenario.repository._acquire_execution_fence_lock(
                session, scenario.selection.wallet_public_id
            )
            with pytest.raises(ValueError):
                await current_risk.require_current_futures_flex_risk(
                    session, route, scenario.policy
                )


async def test_actual_source_age_cannot_be_renewed_by_reading_retained_risk_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep original request age authoritative when immutable account risk bytes remain in storage.

    Given: A genuine accounts risk source reaches the trusted native account source-age boundary.
    When: The current risk reader reopens the original source under the wallet fence.
    Then: Actual account freshness refuses instead of renewing risk age at the later read clock.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as scenario:
        route = await _actual_risk_route(scenario, monkeypatch)
        scenario.clock.current += timedelta(seconds=60)
        async with scenario.repository.session() as session:
            await scenario.repository._begin_execution_insert_transaction(session)
            await scenario.repository._acquire_execution_fence_lock(
                session, scenario.selection.wallet_public_id
            )
            with pytest.raises(ValueError):
                await current_risk.require_current_futures_flex_risk(
                    session, route, scenario.policy
                )


async def test_another_actual_native_account_cannot_borrow_flex_risk_from_same_response(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep selected margin account identity attached to the source used by the risk projection.

    Given: Real account authority selects cash while the same original HTTP also contains flex fields.
    When: The flex risk reader is called with that actual cash route under the wallet fence.
    Then: It refuses the account mismatch rather than borrow collateral from a sibling profile.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as scenario:
        route = await _actual_risk_route(scenario, monkeypatch, "cash")
        async with scenario.repository.session() as session:
            await scenario.repository._begin_execution_insert_transaction(session)
            await scenario.repository._acquire_execution_fence_lock(
                session, scenario.selection.wallet_public_id
            )
            with pytest.raises(FuturesFlexRiskSourceError, match="current_native_route_required"):
                await current_risk.require_current_futures_flex_risk(
                    session, route, scenario.policy
                )


async def test_current_risk_requires_caller_transaction_before_sampling_clock_or_reading_sources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Refuse an unfenced caller before current account evidence can be presented as a risk prerequisite.

    Given: Actual current account/risk evidence exists but a new caller has not begun its transaction.
    When: The current risk reader is invoked directly on that session.
    Then: It refuses before source lookup implicitly begins a transaction.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as scenario:
        route = await _actual_risk_route(scenario, monkeypatch)
        async with scenario.repository.session() as session:
            assert not session.in_transaction()
            with pytest.raises(RuntimeError, match="existing wallet-fenced transaction"):
                await current_risk.require_current_futures_flex_risk(
                    session, route, scenario.policy
                )
            assert not session.in_transaction()


async def test_actual_legacy_flex_route_cannot_supply_guarded_native_risk_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Require the native source family even when an actual older route uses the literal flex name.

    Given: A genuine stored legacy Futures route is refreshed with the selected flex margin account.
    When: Its unchanged legacy loader and the new current risk reader inspect the same actual route.
    Then: Historical loading succeeds while the new reader refuses the missing native source family.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as scenario:
        monkeypatch.setattr(current_risk, "datetime", scenario.clock)
        legacy = await seed_admission_scenario(
            scenario.repository, scenario.clock.current, source="kraken_futures"
        )
        scenario.clock.current += timedelta(seconds=1)
        route = await refresh_admission_account(
            legacy,
            {"scope": legacy.authority.scope.model_copy(update={"margin_account": "flex"})},
            scenario.clock.current,
            1200,
        )
        async with scenario.repository.session() as session:
            await scenario.repository._begin_execution_insert_transaction(session)
            await scenario.repository._acquire_execution_fence_lock(
                session, route.scope.wallet_public_id
            )
            actual = await require_current_route_authority(session, route, scenario.clock.current)
            assert actual.scope.margin_account == "flex"
            with pytest.raises(FuturesFlexRiskSourceError, match="current_native_source_required"):
                await current_risk.require_current_futures_flex_risk(
                    session, route, scenario.policy
                )
