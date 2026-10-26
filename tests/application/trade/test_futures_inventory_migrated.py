"""Exercise the actual inventory vertical on genuinely blank-to0066 migrated databases.

SQLite always owns a new file. PostgreSQL accepts only the established explicit
owner-only Unix-socket cluster receipt, creates a fresh disposable database per
case and validates actual server identity before creation and removal. The
installed SDK signs real PreparedRequests; a local Requests adapter supplies
original bytes while another actual SQL connection observes committed sources.
No parser, proof loader, store, issued receipt or migration is replaced.
"""

import asyncio
import base64
import hashlib
import json
from collections.abc import AsyncIterator
from contextlib import suppress
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid7

import pytest
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.exc import DBAPIError

from snapper.application.trade.current_futures_inventory import FuturesInventoryFreshnessPolicy
from snapper.application.trade.current_futures_inventory import read_current_futures_inventory
from snapper.application.trade.futures_inventory_read import CurrentFuturesInventoryRead
from snapper.application.trade.futures_inventory_read import capture_prepared_futures_inventory
from snapper.application.trade.futures_inventory_read import refresh_current_futures_inventory
from snapper.application.trade.futures_open_inventory import FuturesOpenOrdersSnapshot
from snapper.application.trade.futures_open_inventory import FuturesOpenPositionsSnapshot
from snapper.application.trade.native_private_owner_preparation import prepare_native_private_owner
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.futures_inventory_read import FuturesInventoryReadFailure
from snapper.core.futures_inventory_read import FuturesInventoryReadSource
from snapper.core.futures_inventory_read import InventoryReadPurpose
from snapper.data.futures_inventory_read_store import begin_futures_inventory_read
from snapper.data.models import FuturesInventoryReadAttempt as InventoryAttemptRow
from snapper.data.models import FuturesInventoryReadOutcome as InventoryOutcomeRow
from tests.application.trade.test_native_position_recording_v3_migrated import _postgres_database
from tests.helpers.futures_inventory_migrated import INVENTORY_PATHS
from tests.helpers.futures_inventory_migrated import InventorySourceFault
from tests.helpers.futures_inventory_migrated import MigratedInventoryDatabase
from tests.helpers.futures_inventory_migrated import MigratedInventoryScenario
from tests.helpers.futures_inventory_migrated import lose_inventory_commit_acknowledgement
from tests.helpers.futures_inventory_migrated import migrated_inventory_database
from tests.helpers.futures_inventory_migrated import migrated_inventory_scenario
from tests.helpers.native_account_authority import native_authority_uuid
from tests.helpers.native_account_authority import rotate_native_credential

pytestmark = pytest.mark.timeout(120)

_ORDERS: InventoryReadPurpose = "kraken_futures.open_orders"
_POSITIONS: InventoryReadPurpose = "kraken_futures.open_positions"
_POLICY = FuturesInventoryFreshnessPolicy(timedelta(seconds=30), timedelta(seconds=5))


@pytest.fixture(params=("sqlite", "postgresql"))
async def inventory_database(
    request: pytest.FixtureRequest, tmp_path: Path
) -> AsyncIterator[MigratedInventoryDatabase]:
    """Provide actual complete0066 migrations independently of the ORM template fixture."""
    assert request.param in {"sqlite", "postgresql"}
    if request.param == "sqlite":
        url = URL.create("sqlite+aiosqlite", database=str(tmp_path / "migrated-inventory.db"))
        async with migrated_inventory_database(url) as database:
            yield database
        return
    async with _postgres_database(tmp_path) as url, migrated_inventory_database(url) as database:
        yield database


async def _refresh(
    scenario: MigratedInventoryScenario, purpose: InventoryReadPurpose = _ORDERS
) -> CurrentFuturesInventoryRead:
    """Call the genuine owned asynchronous producer and its fenced current-reader result."""
    native = scenario.native
    return await refresh_current_futures_inventory(
        native.repository, native.client, purpose, native.tracker
    )


async def _bootstrap(scenario: MigratedInventoryScenario) -> None:
    """Reuse the actual successful key/accounts token for both committed initial inventories."""
    native = scenario.native
    prepared = await prepare_native_private_owner(native.repository, native.client, native.tracker)
    try:
        assert prepared.outcome.payload.result.kind == "captured"
        token = prepared.require_token()
        assert native.client._private_ws_client is None
        await capture_prepared_futures_inventory(
            native.repository, native.client, prepared, native.tracker
        )
        assert prepared.require_token() is token
    finally:
        prepared.close()


@pytest.mark.parametrize("purpose", (_ORDERS, _POSITIONS))
async def test_migrated_read_only_sdk_commits_attempt_before_actual_inventory_get(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    inventory_database: MigratedInventoryDatabase,
    purpose: InventoryReadPurpose,
) -> None:
    """Prove durable original source binding without requiring trade or native-profile authority.

    Given: An actual blank-to0066 database, installed SDK and READ_ONLY key/accounts preparation,
    When: The producer issues its real fixed endpoint GET through the local HTTP adapter,
    Then: A second SQL connection sees its committed proof-bound attempt before any outcome.
    """
    async with migrated_inventory_scenario(tmp_path, monkeypatch, inventory_database) as scenario:
        current = await _refresh(scenario, purpose)
        assert current.available and current.reason == "available" and current.outcome is not None
        source = current.outcome.result
        assert isinstance(source, FuturesInventoryReadSource)
        assert base64.b64decode(source.body, validate=True) == scenario.adapter.original_bodies[0]
        observed = inventory_database.observer.seen
        assert (
            len(observed) == 1
            and observed[0].attempt_public_id == current.outcome.attempt_public_id
        )
        assert observed[0].proof.observation.general_access == "READ_ONLY"
        assert observed[0].proof.observation.transfer_access == "NO_ACCESS"
        assert current.outcome.attempt.key_proof.public_id == observed[0].proof_public_id
        assert current.outcome.attempt.family_sequence == 1
        assert scenario.adapter.inventory_calls[0].url == (
            "https://futures.kraken.com" + INVENTORY_PATHS[purpose]
        )
        if isinstance(current.snapshot, FuturesOpenOrdersSnapshot):
            assert len(current.snapshot.orders) == 1
            assert current.snapshot.orders[0].filled_size == Decimal("0.125000000000000000001")
            assert current.snapshot.orders[0].limit_price.value == Decimal(
                "12345.000000000000000000001"
            )
        else:
            assert isinstance(current.snapshot, FuturesOpenPositionsSnapshot)
            assert len(current.snapshot.positions) == 1
            assert current.snapshot.positions[0].wire_symbol == "FF_XBTUSD_260925"
            assert current.snapshot.positions[0].size == Decimal("1.250000000000000000001")


async def test_migrated_bootstrap_reuses_original_owner_and_both_independent_endpoint_ordinals(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    inventory_database: MigratedInventoryDatabase,
) -> None:
    """Initial recovery reads consume actual stored inventories without repeating key/accounts GETs.

    Given: One original successful native owner preparation before any private socket handoff,
    When: Both initial inventory endpoints commit and actual consumer refreshes ask for each,
    Then: Four total SDK GETs suffice and each purpose independently starts at ordinal one.
    """
    async with migrated_inventory_scenario(tmp_path, monkeypatch, inventory_database) as scenario:
        await _bootstrap(scenario)
        calls_before = len(scenario.adapter.calls)
        assert calls_before == 4
        for purpose in (_ORDERS, _POSITIONS):
            current = await _refresh(scenario, purpose)
            assert current.available and current.outcome is not None
            assert current.outcome.attempt.family_sequence == 1
        assert len(scenario.adapter.calls) == calls_before
        assert [item.attempt.purpose for item in inventory_database.observer.seen] == [
            _ORDERS,
            _POSITIONS,
        ]


async def test_migrated_latest_pending_then_failed_orders_masks_old_success_only_for_orders(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    inventory_database: MigratedInventoryDatabase,
) -> None:
    """Newest request barriers survive real commits while the other endpoint remains independent.

    Given: Actual successful orders/positions plus a newer separately committed orders attempt,
    When: The current reader sees pending and a genuine failed orders refresh follows,
    Then: Pending/failure never return old success or empty orders and positions stay available.
    """
    async with migrated_inventory_scenario(tmp_path, monkeypatch, inventory_database) as scenario:
        await _bootstrap(scenario)
        original = await _refresh(scenario)
        assert original.outcome is not None
        native = scenario.native
        native.clock.current += timedelta(seconds=1)
        identity = ExecutionEvidenceIdentity(
            public_id=str(uuid7()),
            session_id=native.tracker.session_id,
            sequence_id=900,
        )
        pending = await begin_futures_inventory_read(
            native.repository, original.outcome.attempt.key_proof, _ORDERS, identity
        )
        assert pending.payload.family_sequence == 2
        current = await read_current_futures_inventory(
            native.repository, native.selection, _ORDERS, _POLICY
        )
        assert current.reason == "pending" and current.snapshot is None and current.outcome is None
        scenario.adapter.fail_purpose = _ORDERS
        failed = await _refresh(scenario)
        assert not failed.available and failed.snapshot is None and failed.reason == "failed"
        assert failed.outcome is not None
        assert failed.outcome.attempt.family_sequence == 3
        assert failed.outcome.result == FuturesInventoryReadFailure(reason="transport_error")
        positions = await _refresh(scenario, _POSITIONS)
        assert positions.available and positions.outcome is not None
        assert positions.outcome.attempt.family_sequence == 1


@pytest.mark.parametrize(
    ("fault", "reason"),
    (
        ("key", "source_unsafe"),
        ("signature", "source_unsafe"),
        ("nonce", "source_unsafe"),
        ("duplicate", "source_invalid"),
        ("malformed", "source_invalid"),
    ),
)
async def test_migrated_source_refusal_retains_only_fixed_failure_without_body_or_body_digest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    inventory_database: MigratedInventoryDatabase,
    fault: InventorySourceFault,
    reason: str,
) -> None:
    """Actual source privacy and full semantic parsing precede captured-body persistence.

    Given: A real signed inventory GET echoing its current auth material or malformed source,
    When: The unchanged guarded transport, source parser and store process original response bytes,
    Then: Only safe failure metadata commits and no raw body or body digest is retained.
    """
    async with migrated_inventory_scenario(tmp_path, monkeypatch, inventory_database) as scenario:
        scenario.adapter.source_fault = fault
        current = await _refresh(scenario)
        assert not current.available and current.snapshot is None and current.reason == "failed"
        assert current.outcome is not None
        result = current.outcome.result
        assert isinstance(result, FuturesInventoryReadFailure) and result.reason == reason
        async with scenario.native.repository.session() as session:
            row = (await session.scalars(select(InventoryOutcomeRow))).one()
            attempt = (await session.scalars(select(InventoryAttemptRow))).one()
            payload = json.loads(row.canonical_payload)
            assert payload["result"] == {"kind": "failed", "reason": reason}
            body = scenario.adapter.original_bodies[0]
            assert base64.b64encode(body).decode("ascii") not in row.canonical_payload
            assert hashlib.sha256(body).hexdigest() not in row.canonical_payload
            assert row.attempt_public_id == attempt.public_id


async def test_migrated_genuine_empty_inventory_is_available_until_exact_original_key_age_ceiling(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    inventory_database: MigratedInventoryDatabase,
) -> None:
    """An observed empty book differs from unavailable and later reads cannot renew its age.

    Given: A complete genuine empty source backed by actual committed key and inventory evidence,
    When: The trusted current-reader clock reaches the exact original key GET age ceiling,
    Then: Empty was available before expiry and stale now has no synthetic replacement snapshot.
    """
    async with migrated_inventory_scenario(tmp_path, monkeypatch, inventory_database) as scenario:
        scenario.adapter.empty = True
        current = await _refresh(scenario)
        assert current.available and isinstance(current.snapshot, FuturesOpenOrdersSnapshot)
        assert current.snapshot.orders == ()
        assert current.outcome is not None
        calls = len(scenario.adapter.calls)
        native = scenario.native
        native.clock.current += timedelta(seconds=30)
        stale = await read_current_futures_inventory(
            native.repository, native.selection, _ORDERS, _POLICY
        )
        assert stale.reason == "stale" and stale.snapshot is None
        assert stale.outcome is not None and stale.outcome.payload == current.outcome
        assert len(scenario.adapter.calls) == calls


async def test_migrated_credential_revision_preserves_account_root_and_inventory_family_sequence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    inventory_database: MigratedInventoryDatabase,
) -> None:
    """Actual source rebinding across credential revisions keeps one endpoint ordering family.

    Given: One current READ_ONLY source and a genuine new SCD2 credential revision for the same UID,
    When: The new actual SDK client refreshes inventory under that original account owner root,
    Then: The new credential and proof bind while ordinal two supersedes the earlier request.
    """
    async with migrated_inventory_scenario(tmp_path, monkeypatch, inventory_database) as scenario:
        first = await _refresh(scenario)
        assert first.available and first.outcome is not None
        await rotate_native_credential(scenario.native)
        rotated = await _refresh(scenario)
        assert rotated.available and rotated.outcome is not None
        before, after = first.outcome.attempt, rotated.outcome.attempt
        assert after.family_sequence == 2
        assert after.client_selection != before.client_selection
        assert after.key_proof.public_id != before.key_proof.public_id
        assert after.key_proof.key_owner_root_public_id == before.key_proof.key_owner_root_public_id
        assert after.key_proof.account_uid == before.key_proof.account_uid


async def test_migrated_wrong_uid_never_relabels_old_inventory_or_reads_with_conflicting_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    inventory_database: MigratedInventoryDatabase,
) -> None:
    """A later real key-check UID conflict cannot adopt an old book or mint an inventory namespace.

    Given: Actual successful inventory and a subsequent changed account UID from the original SDK key,
    When: The source expires and refresh performs the genuine key-ownership check,
    Then: Inventory stays unavailable without a second inventory GET or an invented empty source.
    """
    async with migrated_inventory_scenario(tmp_path, monkeypatch, inventory_database) as scenario:
        first = await _refresh(scenario)
        assert first.available
        scenario.adapter.account_uid = native_authority_uuid(81)
        scenario.native.clock.current += timedelta(seconds=31)
        refused = await _refresh(scenario)
        assert not refused.available and refused.snapshot is None
        assert len(scenario.adapter.inventory_calls) == 1
        async with scenario.native.repository.session() as session:
            attempts = (await session.scalars(select(InventoryAttemptRow))).all()
            assert len(attempts) == 1


async def test_migrated_pending_http_and_repeated_cancellation_join_actual_worker_before_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    inventory_database: MigratedInventoryDatabase,
) -> None:
    """A blocked real SDK HTTP worker cannot outlive cancellation and original token cleanup.

    Given: A committed inventory request whose actual local transport callback is still blocked,
    When: Independent current reads observe pending and the caller cancels repeatedly,
    Then: The owner waits for HTTP release and records safe cancellation without an empty book.
    """
    async with migrated_inventory_scenario(tmp_path, monkeypatch, inventory_database) as scenario:
        scenario.adapter.block_purpose = _ORDERS
        task = asyncio.create_task(_refresh(scenario))
        try:
            assert await asyncio.to_thread(scenario.adapter.inventory_entered.wait, 3)
            native = scenario.native
            pending = await read_current_futures_inventory(
                native.repository, native.selection, _ORDERS, _POLICY
            )
            assert pending.reason == "pending" and pending.snapshot is None
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
            task.cancel()
        finally:
            scenario.adapter.inventory_release.set()
            with suppress(asyncio.CancelledError):
                await task
        assert task.cancelled()
        cancelled = await read_current_futures_inventory(
            native.repository, native.selection, _ORDERS, _POLICY
        )
        assert cancelled.reason == "failed" and cancelled.snapshot is None
        assert cancelled.outcome is not None
        assert cancelled.outcome.payload.result == FuturesInventoryReadFailure(reason="cancelled")


async def test_migrated_lost_attempt_commit_reply_leaves_durable_pending_without_inventory_get(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    inventory_database: MigratedInventoryDatabase,
) -> None:
    """Uncertain request commitment never becomes permission to issue an unowned inventory GET.

    Given: Actual successful original-key preparation and one genuine inventory attempt commit,
    When: The transport loses only that real database commit's acknowledgement,
    Then: The committed request remains pending and no endpoint GET or failed outcome is invented.
    """
    async with migrated_inventory_scenario(tmp_path, monkeypatch, inventory_database) as scenario:
        async with lose_inventory_commit_acknowledgement(monkeypatch, "attempt") as acknowledgement:
            uncertain = await _refresh(scenario)
        assert acknowledgement.lost and not uncertain.available
        assert uncertain.reason == "unverifiable" and uncertain.outcome is None
        assert scenario.adapter.inventory_calls == []
        native = scenario.native
        pending = await read_current_futures_inventory(
            native.repository, native.selection, _ORDERS, _POLICY
        )
        assert pending.reason == "pending" and pending.snapshot is None and pending.outcome is None
        assert pending.attempt is not None and pending.attempt.payload.family_sequence == 1
        async with native.repository.session() as session:
            assert len((await session.scalars(select(InventoryAttemptRow))).all()) == 1
            assert (await session.scalars(select(InventoryOutcomeRow))).all() == []
        resumed = await _refresh(scenario)
        assert resumed.available and resumed.outcome is not None
        assert resumed.outcome.attempt.family_sequence == 2
        assert len(scenario.adapter.inventory_calls) == 1


async def test_migrated_lost_capture_commit_reply_preserves_success_without_false_failure_or_resend(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    inventory_database: MigratedInventoryDatabase,
) -> None:
    """A lost terminal commit reply cannot overwrite an actual captured result with failure.

    Given: A genuine SDK inventory source appended and committed through the actual outcome writer,
    When: Only that real commit's acknowledgement is lost before the producer returns,
    Then: The initial result is unavailable and a later refresh reuses the one committed success.
    """
    async with migrated_inventory_scenario(tmp_path, monkeypatch, inventory_database) as scenario:
        async with lose_inventory_commit_acknowledgement(monkeypatch, "outcome") as acknowledgement:
            uncertain = await _refresh(scenario)
        assert acknowledgement.lost and not uncertain.available
        assert uncertain.reason == "unverifiable" and uncertain.outcome is None
        calls = len(scenario.adapter.calls)
        async with scenario.native.repository.session() as session:
            original = (await session.scalars(select(InventoryOutcomeRow))).one()
            original_payload = original.canonical_payload
            original_public_id = original.public_id
            assert original.result_kind == "captured"
        resumed = await _refresh(scenario)
        assert resumed.available and resumed.outcome is not None
        assert isinstance(resumed.outcome.result, FuturesInventoryReadSource)
        assert resumed.outcome.attempt.family_sequence == 1
        assert len(scenario.adapter.calls) == calls and len(scenario.adapter.inventory_calls) == 1
        async with scenario.native.repository.session() as session:
            retained = (await session.scalars(select(InventoryOutcomeRow))).one()
            assert retained.public_id == original_public_id
            assert (
                retained.canonical_payload == original_payload
                and retained.result_kind == "captured"
            )


@pytest.mark.parametrize("model", (InventoryAttemptRow, InventoryOutcomeRow))
@pytest.mark.parametrize("operation", ("UPDATE", "DELETE"))
async def test_migrated_actual_inventory_rows_refuse_mutation_and_survive_transaction_rollback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    inventory_database: MigratedInventoryDatabase,
    model: type[InventoryAttemptRow] | type[InventoryOutcomeRow],
    operation: str,
) -> None:
    """Actual migrated SQLite/PostgreSQL guards preserve original SDK evidence after refused DML.

    Given: A real committed source and its original inventory request and captured outcome rows,
    When: Direct SQL attempts UPDATE or DELETE and the failed caller transaction rolls back,
    Then: Physical triggers refuse mutation and the same exact qualified source remains available.
    """
    async with migrated_inventory_scenario(tmp_path, monkeypatch, inventory_database) as scenario:
        current = await _refresh(scenario)
        assert current.available and current.outcome is not None
        async with scenario.native.repository.session() as session:
            original = (await session.scalars(select(model))).one()
            original_payload, original_id = original.canonical_payload, original.public_id
            table = model.__tablename__
            statement = (
                f"UPDATE {table} SET canonical_payload='changed'"
                if operation == "UPDATE"
                else f"DELETE FROM {table}"
            )
            with pytest.raises(DBAPIError, match="append-only"):
                await session.execute(text(statement))
            await session.rollback()
            retained = (await session.scalars(select(model))).one()
            assert (
                retained.public_id == original_id and retained.canonical_payload == original_payload
            )
        reused = await _refresh(scenario)
        assert reused.available and reused.outcome == current.outcome
        assert len(scenario.adapter.inventory_calls) == 1
