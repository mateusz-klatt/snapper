"""Current raw margin observations from genuine signed key/accounts/product sources and actual stores."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid7

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

import snapper.application.trade.native_account_authority as authority_capture
import snapper.application.trade.native_order_access as order_access
from snapper.application.trade.current_futures_margin import CurrentFuturesMarginUnavailableError
from snapper.application.trade.current_futures_margin import require_current_futures_margin_schedule
from snapper.application.trade.futures_margin_schedule import FuturesMarginScheduleSourceError
from snapper.application.trade.futures_product_eligibility import FuturesProductFreshnessPolicy
from snapper.application.trade.futures_product_read import read_native_futures_products
from snapper.application.trade.native_account_authority import NativeAuthorityCaptureRequest
from snapper.application.trade.native_account_authority import capture_native_account_authority
from snapper.application.trade.native_order_access import NativeFuturesOrderAccessUnavailableError
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.execution_unit_admission import ExecutionRouteReference
from snapper.core.futures_product_read import FuturesProductReadSource
from snapper.data.futures_product_key_binding import FuturesProductBindingError
from snapper.data.futures_product_read_store import begin_futures_product_read
from tests.application.trade.test_futures_trading_instruments import _row
from tests.application.trade.test_futures_trading_instruments import _wire
from tests.helpers.futures_product_read import FuturesProductScenario
from tests.helpers.futures_product_read import futures_product_scenario
from tests.helpers.native_account_authority import capture_native_account_read


def _identity(scenario: FuturesProductScenario) -> ExecutionEvidenceIdentity:
    """Allocate actual producer-owned route/retry coordinates without fabricating source rows."""
    tracker = scenario.native.tracker
    return ExecutionEvidenceIdentity(
        public_id=str(uuid7()),
        session_id=tracker.session_id,
        sequence_id=tracker.next_sequence("test.current.authenticated_margin"),
    )


def _body(
    scenario: FuturesProductScenario,
    threshold_field: str = "numNonContractUnits",
    instrument_type: str = "flexible_futures",
) -> bytes:
    """Extend only the actual local HTTP entity with literal numeric tiers and original source time."""
    levels = [
        {threshold_field: 0, "initialMargin": 0.02, "maintenanceMargin": 0.01},
        {threshold_field: 500000, "initialMargin": 0.04, "maintenanceMargin": 0.02},
    ]
    return _wire(
        [_row({"type": instrument_type, "marginLevels": levels})],
        {"serverTime": scenario.native.clock.current.isoformat().replace("+00:00", "Z")},
    )


async def _route(
    scenario: FuturesProductScenario,
    account_attempt_public_id: str,
    monkeypatch: pytest.MonkeyPatch,
    native_account_key: str = "flex",
) -> ExecutionRouteReference:
    """Capture native authority only from the actual original accounts/proof graph and selected profile."""
    native = scenario.native
    monkeypatch.setattr(authority_capture, "datetime", native.clock)
    monkeypatch.setattr(order_access, "datetime", native.clock)
    captured = await capture_native_account_authority(
        native.repository,
        NativeAuthorityCaptureRequest(
            read_attempt_public_id=account_attempt_public_id,
            native_account_key=native_account_key,
        ),
        _identity(scenario),
        native.policy,
    )
    return captured.reference


def _policy(scenario: FuturesProductScenario) -> FuturesProductFreshnessPolicy:
    """Keep margin age equal to its original product source rather than create a second renewal clock."""
    return FuturesProductFreshnessPolicy(
        native_accounts=scenario.native.policy,
        maximum_product_source_age=timedelta(seconds=5),
        maximum_product_server_clock_skew=timedelta(seconds=1),
    )


@asynccontextmanager
async def _locked(scenario: FuturesProductScenario) -> AsyncIterator[AsyncSession]:
    """Own the actual existing wallet fence for the entire current prerequisite read."""
    native = scenario.native
    async with native.repository.session() as session:
        await native.repository._begin_execution_insert_transaction(session)
        await native.repository._acquire_execution_fence_lock(
            session, native.selection.wallet_public_id
        )
        yield session


@pytest.mark.parametrize(
    "instrument_type", ["flexible_futures", "futures_inverse", "futures_vanilla"]
)
async def test_actual_native_profile_controls_raw_margin_field_without_type_unit_inference(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, instrument_type: str
) -> None:
    """The actual collateral class binds the original raw field while instrument text stays observational.

    Given: Genuine full-access key/accounts/product responses retain an actual native flex profile.
    When: The current reader observes numeric margin tiers from that exact authenticated product body.
    Then: It preserves the original threshold field/rates without converting them to order base quantity.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        scenario.adapter.general_access = "FULL_ACCESS"
        scenario.adapter.product_body = _body(scenario, instrument_type=instrument_type)
        original = await read_native_futures_products(
            native.repository, native.client, native.tracker
        )
        assert isinstance(original.payload.result, FuturesProductReadSource)
        route = await _route(
            scenario, original.payload.attempt.key_proof.read_attempt_public_id, monkeypatch
        )
        async with _locked(scenario) as session:
            current = await require_current_futures_margin_schedule(
                session, route, "PF_XBTUSD", _policy(scenario)
            )
        assert current.product.product_outcome == original
        assert current.native_profile.account_type == "multiCollateralMarginAccount"
        assert current.native_profile.native_account_key == "flex"
        assert current.schedule.instrument_type == instrument_type
        assert current.schedule.threshold_field == "numNonContractUnits"
        assert [tier.threshold for tier in current.schedule.tiers] == [Decimal(0), Decimal(500000)]
        assert current.schedule.tiers[0].initial_margin_rate == Decimal("0.02")
        assert current.schedule.tiers[0].maintenance_margin_rate == Decimal("0.01")
        assert current.product.product_outcome.payload.result == original.payload.result
        assert all(request.method == "GET" for request in scenario.adapter.calls)


@pytest.mark.parametrize("native_key", ["cash", "fi_xbtusd", "other_multi"])
async def test_actual_native_cash_single_or_other_wallet_is_not_promoted_to_flex_margin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, native_key: str
) -> None:
    """A familiar product type never manufactures the actual supported collateral wallet.

    Given: Real source bytes select cash, single-collateral or an explicitly different multi wallet.
    When: The current reader rebinds that observed profile beside a genuine authenticated schedule.
    Then: It refuses the unsupported profile instead of assigning flex from instrument type or alias.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        scenario.adapter.general_access = "FULL_ACCESS"
        if native_key == "other_multi":
            scenario.adapter.profiles = scenario.adapter.profiles.replace(
                b'"flex":', b'"other_multi":'
            )
        scenario.adapter.product_body = _body(scenario)
        original = await read_native_futures_products(
            native.repository, native.client, native.tracker
        )
        route = await _route(
            scenario,
            original.payload.attempt.key_proof.read_attempt_public_id,
            monkeypatch,
            native_key,
        )
        async with _locked(scenario) as session:
            with pytest.raises(CurrentFuturesMarginUnavailableError, match="flex_profile_required"):
                await require_current_futures_margin_schedule(
                    session, route, "PF_XBTUSD", _policy(scenario)
                )


async def test_actual_flex_profile_refuses_contract_count_thresholds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Literal single-collateral threshold data does not become multi-collateral notional evidence.

    Given: Actual native flex authority and a retained authenticated row with contracts thresholds.
    When: The current reader compares that raw field with the actual observed collateral class.
    Then: It refuses the mismatch without converting, replacing or applying the source thresholds.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        scenario.adapter.general_access = "FULL_ACCESS"
        scenario.adapter.product_body = _body(scenario, threshold_field="contracts")
        original = await read_native_futures_products(
            native.repository, native.client, native.tracker
        )
        route = await _route(
            scenario, original.payload.attempt.key_proof.read_attempt_public_id, monkeypatch
        )
        async with _locked(scenario) as session:
            with pytest.raises(
                CurrentFuturesMarginUnavailableError, match="threshold_field_mismatch"
            ):
                await require_current_futures_margin_schedule(
                    session, route, "PF_XBTUSD", _policy(scenario)
                )


@pytest.mark.parametrize(
    ("kind", "reason"),
    [
        ("public_only", "schedule_absent"),
        ("empty", "schedule_empty"),
        ("mixed", "tier_unit_mismatch"),
    ],
)
async def test_actual_product_without_one_coherent_account_schedule_does_not_fall_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str, reason: str
) -> None:
    """Authenticated source origin cannot replace absent or contradictory account-resolved tiers.

    Given: Genuine product source contains only retail tiers, an empty schedule or mixed raw fields.
    When: The current margin reader interprets the original retained body.
    Then: It refuses that exact source defect and never falls back to a public classification schedule.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        scenario.adapter.general_access = "FULL_ACCESS"
        if kind == "public_only":
            changes: dict[str, object] = {
                "retailMarginLevels": [
                    {"numNonContractUnits": 0, "initialMargin": 0.5, "maintenanceMargin": 0.25}
                ]
            }
        elif kind == "empty":
            changes = {"marginLevels": []}
        else:
            changes = {
                "marginLevels": [
                    {"numNonContractUnits": 0, "initialMargin": 0.02, "maintenanceMargin": 0.01},
                    {"contracts": 100, "initialMargin": 0.04, "maintenanceMargin": 0.02},
                ]
            }
        scenario.adapter.product_body = _wire(
            [_row(changes)],
            {"serverTime": native.clock.current.isoformat().replace("+00:00", "Z")},
        )
        original = await read_native_futures_products(
            native.repository, native.client, native.tracker
        )
        route = await _route(
            scenario, original.payload.attempt.key_proof.read_attempt_public_id, monkeypatch
        )
        async with _locked(scenario) as session:
            with pytest.raises(FuturesMarginScheduleSourceError, match=reason):
                await require_current_futures_margin_schedule(
                    session, route, "PF_XBTUSD", _policy(scenario)
                )


@pytest.mark.parametrize("latest", ["pending", "failed"])
async def test_actual_newest_product_barrier_prevents_older_margin_schedule_use(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, latest: str
) -> None:
    """A retained older tier schedule cannot bypass the actual latest request state.

    Given: A real captured schedule is followed by a pending or failed product request.
    When: Current margin evidence is requested under its actual wallet fence.
    Then: The inherited current product boundary refuses without searching for the older success.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        scenario.adapter.general_access = "FULL_ACCESS"
        scenario.adapter.product_body = _body(scenario)
        original = await read_native_futures_products(
            native.repository, native.client, native.tracker
        )
        if latest == "pending":
            await begin_futures_product_read(
                native.repository, original.payload.attempt.key_proof, _identity(scenario)
            )
        else:
            scenario.adapter.product_status = 503
            original = await read_native_futures_products(
                native.repository, native.client, native.tracker
            )
        route = await _route(
            scenario, original.payload.attempt.key_proof.read_attempt_public_id, monkeypatch
        )
        async with _locked(scenario) as session:
            with pytest.raises(FuturesProductBindingError, match="latest_read"):
                await require_current_futures_margin_schedule(
                    session, route, "PF_XBTUSD", _policy(scenario)
                )


async def test_actual_read_only_product_source_cannot_supply_order_margin_prerequisite(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Readable account-resolved tiers remain separate from the new-order access prerequisite.

    Given: A real READ_ONLY key successfully captures the account and authenticated schedule source.
    When: The order-oriented current margin boundary reloads actual access.
    Then: It refuses FULL_ACCESS absence while retaining the original readable source unchanged.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        scenario.adapter.product_body = _body(scenario)
        original = await read_native_futures_products(
            native.repository, native.client, native.tracker
        )
        assert isinstance(original.payload.result, FuturesProductReadSource)
        route = await _route(
            scenario, original.payload.attempt.key_proof.read_attempt_public_id, monkeypatch
        )
        async with _locked(scenario) as session:
            with pytest.raises(
                NativeFuturesOrderAccessUnavailableError, match="requires_full_access"
            ):
                await require_current_futures_margin_schedule(
                    session, route, "PF_XBTUSD", _policy(scenario)
                )


async def test_actual_original_schedule_age_cannot_be_renewed_by_current_profile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The same actual product source clocks govern every derived margin observation.

    Given: A genuine schedule is captured under a five-second product age policy.
    When: Its actual native authority remains fresh but the original product request reaches that bound.
    Then: Margin evaluation refuses rather than assign a new observation or recording clock.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        scenario.adapter.general_access = "FULL_ACCESS"
        scenario.adapter.product_body = _body(scenario)
        original = await read_native_futures_products(
            native.repository, native.client, native.tracker
        )
        route = await _route(
            scenario, original.payload.attempt.key_proof.read_attempt_public_id, monkeypatch
        )
        native.clock.current += timedelta(seconds=5)
        async with _locked(scenario) as session:
            with pytest.raises(FuturesProductBindingError, match="original_source_stale"):
                await require_current_futures_margin_schedule(
                    session, route, "PF_XBTUSD", _policy(scenario)
                )


async def test_actual_later_same_owner_proof_preserves_original_schedule_and_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Independent same-owner key checks do not relabel earlier authenticated margin observations.

    Given: A genuine schedule is followed by another actual same-credential key/accounts read.
    When: Current native authority references the later proof under the same UID/root/selection.
    Then: The original still-fresh schedule remains bound without replacing its source or proof UUID.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        scenario.adapter.general_access = "FULL_ACCESS"
        scenario.adapter.product_body = _body(scenario)
        original = await read_native_futures_products(
            native.repository, native.client, native.tracker
        )
        native.clock.current += timedelta(seconds=1)
        accounts = await capture_native_account_read(native)
        route = await _route(scenario, accounts.payload.attempt_public_id, monkeypatch)
        async with _locked(scenario) as session:
            current = await require_current_futures_margin_schedule(
                session, route, "PF_XBTUSD", _policy(scenario)
            )
        assert current.product.product_outcome == original
        assert (
            current.product.access.key_proof.public_id
            != original.payload.attempt.key_proof.public_id
        )
        assert current.schedule.threshold_field == "numNonContractUnits"
        assert [tier.threshold for tier in current.schedule.tiers] == [Decimal(0), Decimal(500000)]
