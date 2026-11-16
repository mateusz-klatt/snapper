"""Durability, binding and local-path refusals for sanitized refresh evidence."""

import json
import os
import stat
import time
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from uuid import UUID

import pytest

from snapper.core.market_snapshot_refresh_budget import RefreshBudget
from snapper.core.market_snapshot_refresh_contracts import RefreshAliasProof
from snapper.core.market_snapshot_refresh_contracts import RefreshCommitReceipt
from snapper.core.market_snapshot_refresh_contracts import RefreshSnapshotReceipt
from snapper.core.market_snapshot_refresh_contracts import snapshot_batch_digest
from snapper.core.market_snapshot_refresh_intent import RefreshIntentSnapshot
from snapper.core.market_snapshot_refresh_intent import RefreshWriteIntent
from snapper.core.market_snapshot_refresh_scope import RefreshTarget
from snapper.core.market_snapshot_refresh_scope import freeze_refresh_scope
from snapper.infrastructure.market_data import refresh_receipts as evidence
from snapper.infrastructure.market_data.refresh_receipts import RefreshReceiptError
from snapper.infrastructure.market_data.refresh_receipts import RefreshReceiptJournal

_HOST = str(UUID(int=1))
_RUN = str(UUID(int=2))
_INSTRUMENT = str(UUID(int=3))
_NOW = datetime(2090, 1, 2, 3, 4, 5, tzinfo=UTC)


def _budget() -> RefreshBudget:
    """Return a short unchanged absolute deadline for local evidence operations."""
    return RefreshBudget(time.monotonic() + 10)


def _intent() -> RefreshWriteIntent:
    """Construct synthetic price-free intent with one explicit alias and operand."""
    target = RefreshTarget(
        instrument_public_id=_INSTRUMENT,
        native_symbol="BTC/USD",
        exchange="kraken",
        base="BTC",
        quote="USD",
        asset_type="crypto",
    )
    scope = freeze_refresh_scope([_INSTRUMENT], ["kraken"], [target.metadata()])
    aliases = (
        RefreshAliasProof(
            instrument_public_id=_INSTRUMENT,
            symbol_public_id=str(UUID(int=4)),
            alias_public_id=str(UUID(int=5)),
            alias_row_id=6,
            exchange="kraken",
            channel="ws",
            exchange_symbol="XBT/USD",
        ),
    )
    return RefreshWriteIntent(
        host_invocation_id=_HOST,
        run_id=_RUN,
        scope_digest=scope.scope_digest,
        batch_digest=snapshot_batch_digest(
            scope.scope_digest, _NOW, aliases, [(_INSTRUMENT, "c" * 64)]
        ),
        collection_started_at=_NOW,
        requested_instrument_public_ids=scope.requested_instrument_public_ids,
        configured_fx_exchanges=scope.configured_fx_exchanges,
        required=scope.required,
        excluded=scope.excluded,
        alias_proofs=aliases,
        snapshots=(RefreshIntentSnapshot(_INSTRUMENT, _RUN, 1, _NOW, "c" * 64),),
    )


def _receipt() -> RefreshCommitReceipt:
    """Bind one synthetic committed physical version to the test intent."""
    intent = _intent()
    return RefreshCommitReceipt(
        run_id=intent.run_id,
        scope_digest=intent.scope_digest,
        batch_digest=intent.batch_digest,
        collection_started_at=intent.collection_started_at,
        recorded_at=_NOW,
        alias_proofs=intent.alias_proofs,
        snapshots=(
            RefreshSnapshotReceipt(
                instrument_public_id=_INSTRUMENT,
                row_id=9,
                public_id=str(UUID(int=10)),
                payload_digest="c" * 64,
                session_id=_RUN,
                sequence_id=1,
                timestamp=_NOW,
            ),
        ),
    )


@pytest.fixture
def journal(tmp_path: Path) -> RefreshReceiptJournal:
    """Create a private parent and exclusively owned invocation under it.

    Args:
        tmp_path: Isolated filesystem root supplied by pytest.

    Returns:
        A newly created private synthetic invocation journal.
    """
    tmp_path.chmod(0o700)
    return RefreshReceiptJournal.create(tmp_path, _HOST, _budget())


@pytest.mark.asyncio
async def test_durable_round_trip_preserves_provenance_without_prices(
    journal: RefreshReceiptJournal, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Publish intent and acknowledgment, keeping final job success a separate decision.

    Given: A private journal and one complete synthetic batch identity.
    When: Both artifacts are fsynced and read through their typed interfaces.
    Then: Original provenance survives, no prices exist, and file-before-directory sync holds.
    """
    sync_modes: list[int] = []
    original = evidence.os.fsync

    def record_sync(descriptor: int) -> None:
        """Retain real durability while recording the kind of descriptor synced.

        Args:
            descriptor: Actual file or directory being synced.
        """
        sync_modes.append(stat.S_IFMT(os.fstat(descriptor).st_mode))
        original(descriptor)

    monkeypatch.setattr(evidence.os, "fsync", record_sync)
    await journal.write_intent(_intent(), _budget())
    await journal.record_commit(_receipt(), _budget())
    assert journal.read_intent(_budget()) == _intent()
    assert journal.read_commit(_budget()) == _receipt()
    assert sync_modes == [stat.S_IFREG, stat.S_IFDIR, stat.S_IFREG, stat.S_IFDIR]
    assert stat.S_IMODE(journal.directory.stat().st_mode) == 0o700
    for path in journal.directory.iterdir():
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        content = path.read_text()
        assert "last_price" not in content
        assert "freshness_ready" not in content
        assert "success" not in content


@pytest.mark.asyncio
async def test_existing_evidence_is_never_overwritten(journal: RefreshReceiptJournal) -> None:
    """Refuse a second publication rather than treating it as retry or recovery.

    Given: An already durable intent.
    When: A caller tries to write it again.
    Then: The original bytes survive and the duplicate attempt refuses.
    """
    await journal.write_intent(_intent(), _budget())
    previous = (journal.directory / "intent.json").read_bytes()
    with pytest.raises(RefreshReceiptError, match="receipt_publication_failed"):
        await journal.write_intent(_intent(), _budget())
    assert (journal.directory / "intent.json").read_bytes() == previous
    assert journal.read_intent(_budget()) == _intent()


@pytest.mark.asyncio
async def test_prepublication_sync_failure_leaves_no_visible_intent(
    journal: RefreshReceiptJournal, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Require actual fsync acknowledgment before publishing a worker's write intent.

    Given: A filesystem that fails to sync the pending evidence file.
    When: Intent publication is attempted before database persistence.
    Then: The error is sanitized and no final intent is visible.
    """

    def broken_sync(descriptor: int) -> None:
        """Inject an I/O failure without exposing its sensitive diagnostic payload.

        Args:
            descriptor: Real descriptor whose sync is deliberately rejected.
        """
        raise OSError(f"private-path-and-value-{descriptor}")

    monkeypatch.setattr(evidence.os, "fsync", broken_sync)
    with pytest.raises(RefreshReceiptError, match="^receipt_publication_failed$"):
        await journal.write_intent(_intent(), _budget())
    assert not (journal.directory / "intent.json").exists()
    assert (journal.directory / "intent.json.pending").is_file()


@pytest.mark.asyncio
async def test_expired_budget_never_publishes_intent(journal: RefreshReceiptJournal) -> None:
    """Keep the filesystem phase inside the inherited work budget.

    Given: An already expired absolute deadline.
    When: The worker asks for durable intent.
    Then: The operation refuses without creating an artifact.
    """
    with pytest.raises(TimeoutError):
        await journal.write_intent(_intent(), RefreshBudget(0))
    assert list(journal.directory.iterdir()) == []


@pytest.mark.asyncio
async def test_intent_is_bound_to_host_invocation(journal: RefreshReceiptJournal) -> None:
    """Distinguish a host invocation from a different worker's producer identity.

    Given: An intent naming a different host invocation.
    When: It is submitted to this journal.
    Then: It is rejected before durable publication.
    """
    with pytest.raises(RefreshReceiptError, match="intent_invocation_mismatch"):
        await journal.write_intent(replace(_intent(), host_invocation_id=_RUN), _budget())
    assert list(journal.directory.iterdir()) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["run_id", "scope_digest", "batch_digest"])
async def test_commit_cannot_rebind_intent(journal: RefreshReceiptJournal, field: str) -> None:
    """Do not certify another invocation from a successful but unrelated receipt.

    Given: A durable intent and an acknowledgment with one altered binding.
    When: The acknowledgment is published.
    Then: No commit file is created.
    """
    await journal.write_intent(_intent(), _budget())
    changed = _receipt().model_copy(update={field: "d" * 64})
    with pytest.raises(RefreshReceiptError, match="commit_intent_mismatch"):
        journal.write_commit(changed, _budget())
    assert not (journal.directory / "commit.json").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["missing", "duplicate", "payload", "alias"])
async def test_exact_batch_identity_required(journal: RefreshReceiptJournal, change: str) -> None:
    """Reject incomplete, duplicated or changed operand evidence.

    Given: A valid intent and a structurally altered committed batch.
    When: The receipt is checked against the durable intent.
    Then: The exact batch identity requirement refuses publication.
    """
    await journal.write_intent(_intent(), _budget())
    original = _receipt()
    changed = {
        "missing": original.model_copy(update={"snapshots": ()}),
        "duplicate": original.model_copy(update={"snapshots": original.snapshots * 2}),
        "payload": original.model_copy(
            update={
                "snapshots": (
                    original.snapshots[0].model_copy(update={"payload_digest": "d" * 64}),
                )
            }
        ),
        "alias": original.model_copy(update={"alias_proofs": ()}),
    }[change]
    with pytest.raises(RefreshReceiptError, match="commit_intent_mismatch"):
        journal.write_commit(changed, _budget())


@pytest.mark.parametrize("kind", ["symlink", "public", "fifo", "hardlink", "oversize"])
def test_unsafe_evidence_is_refused(
    journal: RefreshReceiptJournal, tmp_path: Path, kind: str
) -> None:
    """Do not follow an unsafe evidence path or block on a named pipe.

    Given: An artifact with an inadmissible filesystem type or permissions.
    When: Evidence is read.
    Then: A sanitized error is returned without consuming arbitrary input.
    """
    path = journal.directory / "intent.json"
    outside = tmp_path / "outside"
    outside.write_text("untrusted")
    outside.chmod(0o600)
    if kind == "symlink":
        path.symlink_to(outside)
    elif kind == "fifo":
        os.mkfifo(path, 0o600)
    elif kind == "hardlink":
        os.link(outside, path)
    else:
        path.write_bytes(b"x" * (evidence.MAX_RECEIPT_BYTES + 1) if kind == "oversize" else b"x")
        path.chmod(0o644 if kind == "public" else 0o600)
    with pytest.raises(RefreshReceiptError):
        journal.read_intent(_budget())


@pytest.mark.asyncio
@pytest.mark.parametrize("value", ["malformed", "unknown_field", "wrong_invocation"])
async def test_corrupt_intent_cannot_be_accepted(
    journal: RefreshReceiptJournal, value: str
) -> None:
    """Strictly decode evidence and retain the host invocation boundary on readback.

    Given: An initially valid intent whose stored bytes are corrupted.
    When: The host reads the artifact.
    Then: Malformed JSON, unexpected fields and another invocation are refused.
    """
    await journal.write_intent(_intent(), _budget())
    path = journal.directory / "intent.json"
    data = json.loads(path.read_text())
    if value == "unknown_field":
        data["unexpected"] = True
    elif value == "wrong_invocation":
        data["host_invocation_id"] = _RUN
    path.write_text("not json" if value == "malformed" else json.dumps(data))
    with pytest.raises(RefreshReceiptError):
        journal.read_intent(_budget())


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["session", "sequence", "time", "scope", "batch", "ids"])
async def test_inconsistent_intent_refuses_before_publication(
    journal: RefreshReceiptJournal, change: str
) -> None:
    """Do not acknowledge syntactically typed but internally inconsistent provenance.

    Given: An intent with one invalid producer, scope, time or operand binding.
    When: The worker asks to durably publish it before persistence.
    Then: Publication refuses and no final intent exists.
    """
    intent = _intent()
    snapshot = intent.snapshots[0]
    alternatives = {
        "session": replace(intent, snapshots=(replace(snapshot, session_id=_HOST),)),
        "sequence": replace(intent, snapshots=(replace(snapshot, sequence_id=0),)),
        "time": replace(
            intent, snapshots=(replace(snapshot, received_at=_NOW - timedelta(seconds=1)),)
        ),
        "scope": replace(intent, scope_digest="e" * 64),
        "batch": replace(intent, batch_digest="e" * 64),
        "ids": replace(intent, snapshots=()),
    }
    with pytest.raises(RefreshReceiptError):
        await journal.write_intent(alternatives[change], _budget())
    assert not (journal.directory / "intent.json").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["sequence", "time"])
async def test_receipt_binds_provenance_beyond_a_retained_payload_hash(
    journal: RefreshReceiptJournal, change: str
) -> None:
    """Compare explicit original provenance even when an altered intent retains its hashes.

    Given: A durable acknowledgment and a later plausible corruption of intent provenance.
    When: The host reads the acknowledgment against its intent.
    Then: A different positive sequence or receive time cannot be silently accepted.
    """
    await journal.write_intent(_intent(), _budget())
    await journal.record_commit(_receipt(), _budget())
    path = journal.directory / "intent.json"
    data = json.loads(path.read_text())
    if change == "sequence":
        data["snapshots"][0]["sequence_id"] = 2
    else:
        data["snapshots"][0]["received_at"] = (_NOW + timedelta(seconds=1)).isoformat()
    path.write_text(json.dumps(data))
    with pytest.raises(RefreshReceiptError, match="commit_intent_mismatch"):
        journal.read_commit(_budget())


@pytest.mark.parametrize("invocation", ["bad", "../escape", " " + _HOST])
def test_invocation_path_is_never_caller_controlled(tmp_path: Path, invocation: str) -> None:
    """Require canonical UUIDs before using identity as a directory component.

    Given: A malformed or path-bearing invocation string.
    When: A journal is created.
    Then: Creation refuses without touching the filesystem.
    """
    with pytest.raises(RefreshReceiptError, match="invalid_invocation_identity"):
        RefreshReceiptJournal.create(tmp_path, invocation, _budget())
    assert list(tmp_path.iterdir()) == []


def test_existing_invocation_directory_refuses(journal: RefreshReceiptJournal) -> None:
    """Never reuse a previous invocation's directory implicitly.

    Given: An existing private invocation directory.
    When: A second journal tries to create the same identity.
    Then: The attempt refuses without replacing any existing evidence.
    """
    with pytest.raises(RefreshReceiptError, match="receipt_directory_creation_failed"):
        RefreshReceiptJournal.create(journal.directory.parent, _HOST, _budget())


def test_public_parent_refuses(tmp_path: Path) -> None:
    """Prevent publication under a parent readable or writable by another user.

    Given: A world-readable output parent.
    When: A private invocation is requested beneath it.
    Then: Admission refuses before creating the invocation directory.
    """
    tmp_path.chmod(0o755)
    with pytest.raises(RefreshReceiptError, match="unsafe_receipt_directory"):
        RefreshReceiptJournal.create(tmp_path, _HOST, _budget())
    assert list(tmp_path.iterdir()) == []
