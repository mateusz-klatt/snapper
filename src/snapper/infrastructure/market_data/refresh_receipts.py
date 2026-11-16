"""Private durable refresh evidence, separate from readiness and job success.

Only sanitized operands and exact committed-version identities are published.
A commit receipt does not assert final freshness or container cleanup. Publication
never replaces an existing artifact and does not replay an uncertain write.
Local filesystem calls can block in the kernel; budget checks do not establish
an unconditional filesystem or remote-database wall-clock guarantee.
"""

import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

from pydantic import TypeAdapter
from pydantic import ValidationError

from snapper.core.market_snapshot_refresh_budget import RefreshBudget
from snapper.core.market_snapshot_refresh_contracts import RefreshCommitReceipt
from snapper.core.market_snapshot_refresh_contracts import snapshot_batch_digest
from snapper.core.market_snapshot_refresh_intent import RefreshWriteIntent
from snapper.core.market_snapshot_refresh_scope import freeze_refresh_scope

MAX_RECEIPT_BYTES = 1_048_576
_INTENT_ADAPTER = TypeAdapter(RefreshWriteIntent)


class RefreshReceiptError(RuntimeError):
    """A fixed local evidence failure code without paths, operands or credentials."""


def _canonical_uuid(value: str) -> bool:
    """Check an exact UUID spelling without coercing malformed invocation identity."""
    try:
        return str(UUID(value)) == value
    except ValueError:
        return False


def _private_directory(path: Path) -> int:
    """Open an owned private directory without following its final symlink.

    Args:
        path: Absolute trusted-parent path selected by the host admission.

    Returns:
        An owned directory descriptor that the caller must close.
    """
    if os.name != "posix" or not path.is_absolute():
        raise RefreshReceiptError("unsupported_receipt_directory")
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        metadata = os.fstat(descriptor)
        if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) & 0o077:
            raise RefreshReceiptError("unsafe_receipt_directory")
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def _write_bytes(directory: int, name: str, payload: bytes) -> None:
    """Fsync private evidence before publishing its exclusive final hard link.

    Args:
        directory: Validated owned private directory descriptor.
        name: Internal fixed artifact name, never a caller-supplied path.
        payload: Bounded encoded evidence with no clear financial values.
    """
    temporary = name + ".pending"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
    descriptor = os.open(temporary, flags, 0o600, dir_fd=directory)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    os.link(temporary, name, src_dir_fd=directory, dst_dir_fd=directory, follow_symlinks=False)
    os.unlink(temporary, dir_fd=directory)
    os.fsync(directory)


def _publish(path: Path, name: str, payload: bytes, budget: RefreshBudget) -> None:
    """Refuse oversized evidence and preserve uncertain artifacts after I/O failures.

    Args:
        path: Private invocation directory.
        name: Internal fixed artifact name.
        payload: Sanitized serialized intent or committed-version receipt.
        budget: The original supervisor deadline, never renewed here.
    """
    budget.remaining()
    if len(payload) > MAX_RECEIPT_BYTES:
        raise RefreshReceiptError("receipt_too_large")
    try:
        directory = _private_directory(path)
        try:
            _write_bytes(directory, name, payload)
        finally:
            os.close(directory)
    except OSError:
        raise RefreshReceiptError("receipt_publication_failed") from None
    budget.remaining()


def _read_file(directory: int, name: str) -> bytes:
    """Read a bounded regular owned artifact while rejecting symlinks and hard links.

    Args:
        directory: Validated private directory descriptor.
        name: Internal fixed artifact name.

    Returns:
        Exact bounded bytes, without parsing or reporting their contents.
    """
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC
    descriptor = os.open(name, flags, dir_fd=directory)
    with os.fdopen(descriptor, "rb") as stream:
        metadata = os.fstat(stream.fileno())
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_nlink != 1
            or metadata.st_size > MAX_RECEIPT_BYTES
        ):
            raise RefreshReceiptError("unsafe_receipt_file")
        payload = stream.read(MAX_RECEIPT_BYTES + 1)
        if len(payload) > MAX_RECEIPT_BYTES:
            raise RefreshReceiptError("receipt_too_large")
        return payload


def _read(path: Path, name: str, budget: RefreshBudget) -> bytes:
    """Read evidence through descriptor-relative paths within the original deadline.

    Args:
        path: Private invocation directory.
        name: Internal fixed artifact name.
        budget: The original supervisor deadline.

    Returns:
        Exact bounded artifact bytes.
    """
    budget.remaining()
    try:
        directory = _private_directory(path)
        try:
            payload = _read_file(directory, name)
        finally:
            os.close(directory)
    except OSError:
        raise RefreshReceiptError("receipt_read_failed") from None
    budget.remaining()
    return payload


def _matches_intent(intent: RefreshWriteIntent, receipt: RefreshCommitReceipt) -> bool:
    """Bind a commit acknowledgment to the exact pre-write producer and payloads."""
    expected = {
        row.instrument_public_id: (
            row.payload_digest,
            row.session_id,
            row.sequence_id,
            row.received_at,
        )
        for row in intent.snapshots
    }
    observed = {
        row.instrument_public_id: (
            row.payload_digest,
            row.session_id,
            row.sequence_id,
            row.timestamp,
        )
        for row in receipt.snapshots
    }
    return (
        receipt.run_id == intent.run_id
        and receipt.scope_digest == intent.scope_digest
        and receipt.batch_digest == intent.batch_digest
        and receipt.collection_started_at == intent.collection_started_at
        and receipt.alias_proofs == intent.alias_proofs
        and len(receipt.snapshots) == len(intent.snapshots) == len(expected) == len(observed)
        and expected == observed
    )


def _validate_intent_scope(intent: RefreshWriteIntent) -> None:
    """Recompute the frozen scope, including required and excluded metadata buckets."""
    try:
        scope = freeze_refresh_scope(
            intent.requested_instrument_public_ids,
            intent.configured_fx_exchanges,
            [row.metadata() for row in (*intent.required, *intent.excluded)],
        )
    except ValueError:
        raise RefreshReceiptError("invalid_intent_scope") from None
    if (
        scope.scope_digest != intent.scope_digest
        or scope.required != intent.required
        or scope.excluded != intent.excluded
        or scope.unresolved_fx_currencies
    ):
        raise RefreshReceiptError("invalid_intent_scope")
    snapshots = tuple(sorted(row.instrument_public_id for row in intent.snapshots))
    aliases = tuple(sorted(proof.instrument_public_id for proof in intent.alias_proofs))
    required_aliases = tuple(
        sorted(row.instrument_public_id for row in scope.required if row.exchange == "kraken")
    )
    if snapshots != scope.required_instrument_ids or aliases != required_aliases:
        raise RefreshReceiptError("incomplete_intent_scope")


def _validate_intent_provenance(intent: RefreshWriteIntent) -> None:
    """Validate bounded producer provenance without pretending hashes reveal raw prices."""
    if not _canonical_uuid(intent.run_id) or intent.collection_started_at.utcoffset() is None:
        raise RefreshReceiptError("invalid_intent_provenance")
    sequences = [row.sequence_id for row in intent.snapshots]
    if len(set(sequences)) != len(sequences) or any(
        row.session_id != intent.run_id
        or row.sequence_id <= 0
        or row.received_at.utcoffset() is None
        or row.received_at < intent.collection_started_at
        or re.fullmatch(r"[0-9a-f]{64}", row.payload_digest) is None
        for row in intent.snapshots
    ):
        raise RefreshReceiptError("invalid_intent_provenance")
    digest = snapshot_batch_digest(
        intent.scope_digest,
        intent.collection_started_at,
        intent.alias_proofs,
        [(row.instrument_public_id, row.payload_digest) for row in intent.snapshots],
    )
    if intent.batch_digest != digest:
        raise RefreshReceiptError("invalid_intent_batch")


def _validate_intent(intent: RefreshWriteIntent, invocation_id: str) -> None:
    """Reject inconsistent local evidence before it can authorize or describe a write."""
    if intent.host_invocation_id != invocation_id or not _canonical_uuid(invocation_id):
        raise RefreshReceiptError("intent_invocation_mismatch")
    _validate_intent_scope(intent)
    _validate_intent_provenance(intent)


@dataclass(frozen=True, slots=True)
class RefreshReceiptJournal:
    """Invocation-bound durable evidence with no automatic recovery or success flag.

    Attributes:
        directory: Exclusive private invocation directory under an admitted parent.
        invocation_id: Canonical host identity, separate from the producer run UUID.
    """

    directory: Path
    invocation_id: str

    @classmethod
    def create(
        cls, parent: Path, invocation_id: str, budget: RefreshBudget
    ) -> RefreshReceiptJournal:
        """Create a new private invocation directory and durably publish its entry.

        Args:
            parent: Existing owned private directory admitted by the host.
            invocation_id: A canonical UUID that must not have an existing directory.
            budget: The original supervisor deadline.

        Returns:
            The new invocation-bound evidence journal.
        """
        budget.remaining()
        if not _canonical_uuid(invocation_id):
            raise RefreshReceiptError("invalid_invocation_identity")
        try:
            descriptor = _private_directory(parent)
            try:
                os.mkdir(invocation_id, mode=0o700, dir_fd=descriptor)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        except OSError:
            raise RefreshReceiptError("receipt_directory_creation_failed") from None
        budget.remaining()
        return cls(parent / invocation_id, invocation_id)

    async def write_intent(self, intent: RefreshWriteIntent, budget: RefreshBudget) -> None:
        """Publish sanitized intent before the worker is allowed to begin persistence.

        Args:
            intent: Immutable pre-write identity and digests without prices.
            budget: The original supervisor deadline, including local I/O time.
        """
        _validate_intent(intent, self.invocation_id)
        payload = _INTENT_ADAPTER.dump_json(intent)
        _publish(self.directory, "intent.json", payload, budget)

    def read_intent(self, budget: RefreshBudget) -> RefreshWriteIntent:
        """Read an exact typed intent without treating its presence as a commit.

        Args:
            budget: The original supervisor deadline.

        Returns:
            The strictly parsed intent for this invocation only.
        """
        try:
            intent = _INTENT_ADAPTER.validate_json(
                _read(self.directory, "intent.json", budget), strict=True
            )
        except ValidationError:
            raise RefreshReceiptError("invalid_intent_receipt") from None
        _validate_intent(intent, self.invocation_id)
        return intent

    def write_commit(self, receipt: RefreshCommitReceipt, budget: RefreshBudget) -> None:
        """Record an acknowledged exact batch, independently of final job readiness.

        Args:
            receipt: Exact committed versions returned by the atomic store.
            budget: The original supervisor deadline.
        """
        if not _matches_intent(self.read_intent(budget), receipt):
            raise RefreshReceiptError("commit_intent_mismatch")
        _publish(self.directory, "commit.json", receipt.model_dump_json().encode(), budget)

    async def record_commit(self, receipt: RefreshCommitReceipt, budget: RefreshBudget) -> None:
        """Provide the worker's asynchronous acknowledgment port without detached I/O.

        Args:
            receipt: Validated store acknowledgment to preserve before verification.
            budget: The original supervisor deadline.
        """
        self.write_commit(receipt, budget)

    def read_commit(self, budget: RefreshBudget) -> RefreshCommitReceipt:
        """Read an intent-bound acknowledgment for independent exact store verification.

        Args:
            budget: The original supervisor deadline.

        Returns:
            The exact commit receipt, never a synthesized success from current freshness.
        """
        try:
            receipt = RefreshCommitReceipt.model_validate_json(
                _read(self.directory, "commit.json", budget), strict=True
            )
        except ValidationError:
            raise RefreshReceiptError("invalid_commit_receipt") from None
        if not _matches_intent(self.read_intent(budget), receipt):
            raise RefreshReceiptError("commit_intent_mismatch")
        return receipt
