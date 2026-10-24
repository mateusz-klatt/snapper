"""Real native HTTP source and legacy evidence sharing one isolated catalog for admission tests."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import pytest

import snapper.application.trade.account_bound_submission_admission as account_producer
import snapper.application.trade.unit_snapshot_submission as legacy_producer
from snapper.core.execution_unit_admission import ExecutionRouteReference
from snapper.data.execution_unit_snapshot_store import SnapshotWriteResult
from snapper.data.models import TradeCommand
from snapper.messaging.schemas.data import OrderRequestData
from tests.helpers.native_account_authority import NativeAuthorityScenario
from tests.helpers.native_account_authority import build_native_authority_payload
from tests.helpers.native_account_authority import capture_native_account_read
from tests.helpers.native_account_authority import insert_native_authority
from tests.helpers.native_account_authority import native_authority_reference
from tests.helpers.native_account_authority import native_authority_scenario
from tests.helpers.unit_admission import AdmissionScenario
from tests.helpers.unit_admission import admission_id
from tests.helpers.unit_admission import admission_identity
from tests.helpers.unit_admission import admission_stamp
from tests.helpers.unit_admission import seed_admission_scenario


@dataclass(frozen=True, slots=True)
class AccountAdmissionScenario:
    """Two genuine authority families, distinct wallets and durable outgoing command operands."""

    native: NativeAuthorityScenario
    legacy: AdmissionScenario
    native_order: OrderRequestData
    native_route: ExecutionRouteReference


async def _native_intent(
    native: NativeAuthorityScenario, legacy: AdmissionScenario
) -> OrderRequestData:
    """Persist an independent native-wallet command using the actual shared Futures catalog."""
    intent_at = native.clock.current - timedelta(seconds=30)
    client_id = "native-admission-client-1"
    wallet_id = native.selection.wallet_public_id
    async with native.repository.session() as session:
        session.add(
            TradeCommand(
                **admission_stamp(5000, intent_at).model_dump(mode="python"),
                command_type="create",
                shard_key="kraken_futures.BTC-USD.live",
                wallet_public_id=wallet_id,
                exchange=legacy.order.exchange,
                instrument=legacy.order.instrument,
                mode="live",
                strategy_id="manual",
                client_order_id=client_id,
                venue_client_id=client_id,
                side="buy",
                order_type="limit",
                quantity=2.0,
                price=100.0,
                post_only=True,
                reduce_only=False,
                status="created",
                created_at=intent_at,
                correlation_id=admission_id(5001),
            )
        )
        await session.commit()
    material = legacy.order.model_dump(mode="python")
    material.update(
        public_id=client_id,
        client_order_id=client_id,
        wallet_public_id=wallet_id,
        timestamp=intent_at,
        sequence_id=5000,
    )
    return OrderRequestData.model_validate(material)


@asynccontextmanager
async def account_admission_scenario(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[AccountAdmissionScenario]:
    """Own real source rows and closed HTTP clients without external venue or order access.

    Args:
        tmp_path: Isolated caller-owned directory for the actual SQLite fixture.
        monkeypatch: Controlled source/producer clocks and socket-free requests routing.

    Yields:
        Both real account-source families and independently stored native command material.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as native:
        monkeypatch.setattr(account_producer, "datetime", native.clock)
        monkeypatch.setattr(legacy_producer, "datetime", native.clock)
        legacy = await seed_admission_scenario(
            native.repository, native.clock.current, source="kraken_futures"
        )
        order = await _native_intent(native, legacy)
        outcome = await capture_native_account_read(native)
        payload = await build_native_authority_payload(native, outcome)
        result = await insert_native_authority(native, payload)
        route = native_authority_reference(result, payload)
        yield AccountAdmissionScenario(native, legacy, order, route)


async def capture_account_admission(
    scenario: AccountAdmissionScenario, native: bool = True, number: int = 96
) -> SnapshotWriteResult:
    """Exercise the actual fence-owning producer against one explicitly selected real family.

    Args:
        scenario: Actual isolated accounts, commands and public catalog rows.
        native: Select the genuine native HTTP source or separately retained legacy evidence.
        number: Stable submit-attempt and distinct immutable admission/snapshot identities.

    Returns:
        The committed version-three record without invoking an exchange order method.
    """
    return await account_producer.capture_account_submission_admission(
        scenario.native.repository,
        scenario.native_order if native else scenario.legacy.order,
        scenario.native_route if native else scenario.legacy.route,
        admission_identity(number),
        scenario.native.policy if native else None,
    )
