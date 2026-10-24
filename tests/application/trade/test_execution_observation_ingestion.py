"""Actual callback receipts, uncertain commit retries and preserved account provenance."""

import asyncio
import base64
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
from sqlalchemy.exc import OperationalError

import snapper.application.trade.execution_observation_ingestion as ingestion
from snapper.application.trade.unit_snapshot_route import CapturedExecutionRoute
from snapper.core.contract_accounting_observation import AccountingWriteProvenance
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.execution_route_authority import client_selection_digest
from snapper.core.execution_route_authority import client_selection_from_authority
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.contract_accounting_store import RawAccountingWriteResult
from snapper.data.contract_accounting_store import capture_accounting_observation
from snapper.data.execution_unit_snapshot_store import SnapshotWriteResult
from snapper.data.models import AccountingRawReceipt
from snapper.data.models import AccountingScopeHold
from snapper.data.repository import SQLAlchemyRepository
from snapper.infrastructure.exchanges.contracts import ExecutionObservationContext
from snapper.infrastructure.exchanges.contracts import PrivateExecutionObservation
from snapper.messaging.infrastructure.publisher import SequenceTracker
from tests.helpers.unit_admission import admission_id
from tests.helpers.unit_admission import seed_admission_scenario

_NOW = datetime(2026, 10, 23, 9, tzinfo=UTC)
_WIRE = b'{"feed":"fills","fills":[{"fill_id":"opaque-1","price":0.1234567890123456789}]}'


def _selection() -> ExecutionClientSelection:
    """Select an actual public client revision without pretending to know its margin account."""
    return ExecutionClientSelection(
        wallet_public_id=admission_id(1),
        exchange="kraken_futures",
        mode="live",
        environment="production",
        credential_public_id=admission_id(2),
        credential_timestamp=_NOW,
        public_api_origin="https://futures.kraken.com",
    )


def _source(identity: str | None = "opaque-1") -> PrivateExecutionObservation:
    """Retain one selected child of an original financial payload with exact decimal bytes."""
    return PrivateExecutionObservation(
        ExecutionObservationContext(
            kind="execution",
            channel="futures.private.fills",
            observed_at=_NOW,
            target_json_pointer="/fills/0",
            source_identity=identity,
        ),
        "wire_bytes_base64",
        base64.b64encode(_WIRE).decode("ascii"),
    )


@pytest.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Use a separate physical database with the real immutable receipt and hold guards."""
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'source.db'}")
    await repo.create_all()
    try:
        yield repo
    finally:
        await repo.engine.dispose()


async def test_callback_returns_only_after_raw_source_and_coarse_hold_commit(
    repository: SQLAlchemyRepository,
) -> None:
    """Source bytes and a hold commit first.

    Given: unknown units,
    When: the callback runs,
    Then: source bytes and a hold commit first.
    """
    selection = _selection()
    sink = ingestion.DurableExecutionObservationSink(
        repository, lambda: selection, lambda: None, SequenceTracker()
    )
    receipt_id = await sink(_source())
    async with repository.session() as session:
        receipt = (await session.execute(select(AccountingRawReceipt))).scalar_one()
        hold = (await session.execute(select(AccountingScopeHold))).scalar_one()
    retained = RawAccountingObservation.model_validate_json(receipt.canonical_payload)
    assert receipt.public_id == receipt_id == hold.raw_receipt_public_id
    assert base64.b64decode(retained.material) == _WIRE
    assert retained.target_json_pointer == "/fills/0"
    assert retained.route_revision == client_selection_digest(selection)
    assert retained.scope.margin_account is None
    assert retained.route_authority_public_id is None
    assert retained.credential_public_id == selection.credential_public_id
    assert retained.source_identity_kind == "unproven"


async def test_unknown_commit_ack_retries_one_exact_receipt_identity(
    repository: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One original hold survives.

    Given: a lost commit response,
    When: storage retries,
    Then: one original hold survives.
    """
    attempted: list[AccountingWriteProvenance] = []

    async def capture(
        repo: AccountingRepository,
        source: RawAccountingObservation,
        stamp: AccountingWriteProvenance,
    ) -> RawAccountingWriteResult:
        """Lose only the acknowledgement after a successful real database commit."""
        attempted.append(stamp)
        result = await capture_accounting_observation(repo, source, stamp)
        if len(attempted) == 1:
            raise OperationalError("commit", {}, RuntimeError("response lost"))
        return result

    monkeypatch.setattr(ingestion, "capture_accounting_observation", capture)
    sink = ingestion.DurableExecutionObservationSink(
        repository, _selection, lambda: None, SequenceTracker()
    )
    receipt_id = await sink(_source())
    assert len(attempted) == 2
    assert attempted[0] is attempted[1]
    async with repository.session() as session:
        receipts = list((await session.execute(select(AccountingRawReceipt))).scalars())
        holds = list((await session.execute(select(AccountingScopeHold))).scalars())
    assert [row.public_id for row in receipts] == [receipt_id]
    assert len(holds) == 1


async def test_cancellation_of_failed_storage_never_acknowledges_the_callback(
    repository: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No success escapes.

    Given: unavailable storage,
    When: shutdown cancels the callback,
    Then: no success escapes.
    """
    failed = asyncio.Event()

    async def capture(
        repo: AccountingRepository,
        source: RawAccountingObservation,
        stamp: AccountingWriteProvenance,
    ) -> RawAccountingWriteResult:
        """Signal a real callback attempt before reporting unavailable storage."""
        failed.set()
        raise OperationalError("begin", {}, RuntimeError("unavailable"))

    monkeypatch.setattr(ingestion, "capture_accounting_observation", capture)
    sink = ingestion.DurableExecutionObservationSink(
        repository, _selection, lambda: None, SequenceTracker()
    )
    task = asyncio.create_task(sink(_source()))
    await failed.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    async with repository.session() as session:
        assert (await session.execute(select(AccountingRawReceipt))).first() is None


async def test_missing_actual_client_identity_never_reaches_storage(
    repository: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No account scope is invented.

    Given: unproven routing,
    When: a private frame arrives,
    Then: no account scope is invented.
    """
    writer = AsyncMock()
    monkeypatch.setattr(ingestion, "capture_accounting_observation", writer)
    sink = ingestion.DurableExecutionObservationSink(
        repository, lambda: None, lambda: None, SequenceTracker()
    )
    with pytest.raises(RuntimeError, match="identity_unproven"):
        await sink(_source())
    writer.assert_not_awaited()


def test_fallback_fields_and_missing_source_identity_remain_explicitly_unproven() -> None:
    """Their weaker provenance remains visible.

    Given: reconstructed fields,
    When: captured,
    Then: their weaker provenance remains visible.
    """
    source = PrivateExecutionObservation(_source(None).context, "parsed_fields_json", "{}")
    result = ingestion.build_accounting_observation(source, _selection(), None)
    assert result.representation == "parsed_fields_json"
    assert result.source_identity is None
    assert result.source_identity_kind is None


@pytest.mark.parametrize("same_client", (True, False))
async def test_only_same_selected_client_can_supply_historical_authority(
    repository: SQLAlchemyRepository, same_client: bool
) -> None:
    """Its authority is not adopted.

    Given: a historical response,
    When: credentials differ,
    Then: its authority is not adopted.
    """
    scenario = await seed_admission_scenario(repository, _NOW, source="kraken_futures")
    authority = CapturedExecutionRoute(
        SnapshotWriteResult(1, scenario.route.authority_public_id, "a" * 64, False),
        scenario.authority,
        scenario.route,
    )
    selection = client_selection_from_authority(scenario.authority)
    if not same_client:
        selection = ExecutionClientSelection.model_validate(
            {**selection.model_dump(), "credential_public_id": admission_id(321)}
        )
    result = ingestion.build_accounting_observation(_source(), selection, authority)
    expected = authority.result.public_id if same_client else None
    assert result.route_authority_public_id == expected
    assert result.scope.margin_account is None
