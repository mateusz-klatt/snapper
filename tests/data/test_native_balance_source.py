"""Native source candidates require actual immutable raw rows from the selected client."""

import base64
import json
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Literal

import pytest
from sqlalchemy import func
from sqlalchemy import select

from snapper.application.trade.native_balance_source import get_bound_native_balance_profiles
from snapper.core.contract_accounting_observation import AccountingWriteProvenance
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.execution_route_authority import client_selection_digest
from snapper.core.execution_route_authority import client_selection_from_authority
from snapper.data.contract_accounting_store import capture_accounting_observation
from snapper.data.models import AccountingHoldResolution
from snapper.data.models import AccountingRawReceipt
from snapper.data.repository import SQLAlchemyRepository
from snapper.infrastructure.exchanges.contracts import ExecutionObservationContext
from snapper.infrastructure.exchanges.contracts import PrivateExecutionObservation
from snapper.infrastructure.exchanges.kraken_futures_balance_stream import (
    CapturedNativeBalanceFrame,
)
from snapper.infrastructure.exchanges.kraken_futures_balance_stream import (
    KrakenFuturesBalanceStream,
)
from snapper.infrastructure.exchanges.kraken_futures_balance_stream import futures_balance_channel
from snapper.infrastructure.exchanges.native_balance_source import NativeBalanceStreamProof
from tests.helpers.unit_admission import AdmissionScenario
from tests.helpers.unit_admission import admission_id
from tests.helpers.unit_admission import admission_stamp
from tests.helpers.unit_admission import seed_admission_scenario

_NOW = datetime(2026, 10, 23, 10, tzinfo=UTC)
_EPOCH = admission_id(700)


@pytest.fixture
async def scenario(tmp_path: Path) -> AsyncIterator[AdmissionScenario]:
    """Use a real isolated file-backed raw receipt store and constructed account fixture."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'native-balance.db'}")
    try:
        await repository.create_all()
        yield await seed_admission_scenario(repository, _NOW, source="kraken_futures")
    finally:
        await repository.engine.dispose()


async def _capture(
    scenario: AdmissionScenario,
    sequence: int,
    raw_updates: dict[str, object] | None = None,
) -> CapturedNativeBalanceFrame:
    """Commit whole genuine test source bytes before building a memory-only source candidate."""
    selection = client_selection_from_authority(scenario.authority)
    feed: Literal["balances", "balances_snapshot"] = (
        "balances_snapshot" if sequence == 0 else "balances"
    )
    material = json.dumps(
        {
            "feed": feed,
            "account": "actual-wire-account",
            "seq": sequence,
            "timestamp": 1792749600000 + sequence,
            "holding": {"USD": 10.25 if sequence == 0 else "0.100000000000000001"},
        },
        separators=(",", ":"),
    ).encode()
    material = material.replace(b'"0.100000000000000001"', b"0.100000000000000001")
    observation = PrivateExecutionObservation(
        context=ExecutionObservationContext(
            kind="account_cash",
            channel=futures_balance_channel(feed, _EPOCH),
            observed_at=_NOW + timedelta(milliseconds=sequence),
        ),
        representation="wire_bytes_base64",
        material=base64.b64encode(material).decode(),
    )
    values: dict[str, object] = {
        "scope": {**scenario.authority.scope.model_dump(), "margin_account": None},
        "route_revision": client_selection_digest(selection),
        "credential_public_id": selection.credential_public_id,
        "credential_timestamp": selection.credential_timestamp,
        "kind": "account_cash",
        "channel": observation.context.channel,
        "representation": observation.representation,
        "material": observation.material,
        "observed_at": observation.context.observed_at,
    }
    values.update(raw_updates or {})
    raw = RawAccountingObservation.model_validate(values)
    provenance = AccountingWriteProvenance.model_validate(
        admission_stamp(800 + sequence, _NOW + timedelta(seconds=2)).model_dump()
    )
    result = await capture_accounting_observation(scenario.repository, raw, provenance)
    return CapturedNativeBalanceFrame(
        observation=observation,
        receipt_public_id=result.public_id,
        client_selection=selection,
        connection_epoch_public_id=_EPOCH,
    )


async def _proof(
    scenario: AdmissionScenario, raw_updates: dict[str, object] | None = None
) -> NativeBalanceStreamProof:
    """Build an actual two-receipt candidate while optionally corrupting only a retained claim."""
    stream = KrakenFuturesBalanceStream()
    stream.observe(await _capture(scenario, 0))
    stream.observe(await _capture(scenario, 1, raw_updates))
    return stream.profiles()[0].source_proof


async def test_bound_native_profiles_replay_exact_raw_amounts_without_resolving_holds(
    scenario: AdmissionScenario,
) -> None:
    """A deterministic immutable replay is evidence rather than financial admission.

    Given: Two committed original balance frames and their unresolved coarse holds,
    When: A caller replays their actual native source proof,
    Then: The exact quantity is returned and neither hold is resolved as a side effect.
    """
    proof = await _proof(scenario)
    async with scenario.repository.session() as session:
        profiles = await get_bound_native_balance_profiles(session, proof)
        raw_count = await session.scalar(select(func.count()).select_from(AccountingRawReceipt))
        resolution_count = await session.scalar(
            select(func.count()).select_from(AccountingHoldResolution)
        )
    assert profiles[0].quantities[0].quantity == Decimal("0.100000000000000001")
    assert profiles[0].source_proof == proof
    assert raw_count == 2
    assert resolution_count == 0


@pytest.mark.parametrize(
    "updates",
    (
        {"route_revision": "different-actual-client"},
        {"credential_public_id": admission_id(999)},
        {"credential_timestamp": _NOW - timedelta(seconds=1)},
        {"credential_public_id": None, "credential_timestamp": None},
        {"kind": "execution"},
    ),
)
async def test_raw_source_selection_cannot_be_replaced_by_a_memory_candidate(
    scenario: AdmissionScenario, updates: dict[str, object]
) -> None:
    """A well-formed memory candidate cannot override immutable retained client provenance.

    Given: A source candidate whose second raw receipt has different actual client metadata,
    When: Durable replay validates its immutable envelope,
    Then: It refuses the source-selection mismatch before promoting any profile quantity.
    """
    proof = await _proof(scenario, updates)
    async with scenario.repository.session() as session:
        with pytest.raises(ValueError, match="raw_client_selection_mismatch"):
            await get_bound_native_balance_profiles(session, proof)


@pytest.mark.parametrize("changed", ("account", "clock", "sequence", "receipt"))
async def test_candidate_tip_projection_cannot_override_actual_retained_source_metadata(
    scenario: AdmissionScenario, changed: str
) -> None:
    """The returned profile proof must be reproduced from retained bytes and source clocks.

    Given: A candidate with a changed account, clock, sequence projection or receipt reference,
    When: Its raw chain is reloaded and replayed,
    Then: The changed candidate is refused rather than trusted as source authority.
    """
    proof = await _proof(scenario)
    if changed == "account":
        proof = proof.model_copy(update={"venue_account_id": "different-account"})
    elif changed == "clock":
        proof = proof.model_copy(update={"observed_at": _NOW + timedelta(seconds=1)})
    elif changed == "sequence":
        proof = proof.model_copy(update={"snapshot_sequence": 1, "tip_sequence": 2})
    else:
        proof = proof.model_copy(
            update={"receipt_public_ids": (admission_id(999), proof.receipt_public_ids[1])}
        )
    async with scenario.repository.session() as session:
        with pytest.raises(ValueError, match="tip_projection_mismatch|receipt_missing"):
            await get_bound_native_balance_profiles(session, proof)


async def test_retained_source_epoch_is_verified_from_the_actual_immutable_channel(
    scenario: AdmissionScenario,
) -> None:
    """A candidate cannot relabel both original receipts as a different physical receive epoch.

    Given: Correct source receipts and a valid-looking replacement epoch UUID,
    When: Their actual immutable channel identities are checked,
    Then: Durable replay refuses the mismatched physical source epoch.
    """
    proof = await _proof(scenario)
    proof = proof.model_copy(update={"connection_epoch_public_id": admission_id(701)})
    async with scenario.repository.session() as session:
        with pytest.raises(ValueError, match="receipt_context_mismatch"):
            await get_bound_native_balance_profiles(session, proof)
