"""Guarded void of the active candles of retired Kraken instruments (catalog item C5).

``snapper void-candles`` closes every active candle of some instruments, in every
timeframe, at one database instant. The default run is a dry run: it writes
``preview.csv``, ``summary.json`` and ``approval.json`` and returns the digest to
vote on. A write takes the voted directory and its digest, rebuilds the plan and
refuses unless the rebuilt approval and preview equal the voted ones byte for
byte. The repository then closes exactly the voted versions under the candle
writer fence.

Every write records ``intent.json`` before it starts and ``prepared.json`` with the
database's ``void_at`` before any row is closed. A lost commit acknowledgement is
settled by a reconciliation that holds the same fence while it decides and
writes ``reconciliation.json``; a run that reconciliation has concluded can no
longer prepare. Nothing here prints the database URL or an error's text.

Spec: proprietary ``plans/spec_2026_11_11_frankencoin_candle_void.md``.
"""

import asyncio
import csv
import io
import json
import os
import sys
from collections import Counter
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from types import ModuleType
from typing import Final
from typing import cast
from uuid import uuid7

from snapper.application.updaters.historical import candle_repair
from snapper.application.updaters.historical.candle_repair import canonical_json
from snapper.application.updaters.historical.candle_repair import classify_failure
from snapper.application.updaters.historical.candle_repair import error_fields
from snapper.application.updaters.historical.candle_repair import sha256_hex
from snapper.application.updaters.historical.candle_repair import write_exclusive
from snapper.core.json_types import JsonObject
from snapper.core.json_types import JsonValue
from snapper.data import candle_void_types
from snapper.data import models
from snapper.data import repository as repository_module
from snapper.data.candle_void_types import CandleVersionKey
from snapper.data.candle_void_types import CandleVersionReading
from snapper.data.candle_void_types import CandleVoidReferences
from snapper.data.candle_void_types import CandleVoidRefusedError
from snapper.data.candle_void_types import CandleVoidRequest
from snapper.data.candle_void_types import CandleVoidTarget
from snapper.data.candle_void_types import CatalogExpectation
from snapper.data.repository import Repository
from snapper.data.repository_types import CandleVersionRow

SCHEMA: Final[str] = "candle-void/1"
"""Version of the approval's layout."""

OPERATION: Final[str] = "void"
"""The one operation this command performs."""

EXCHANGE: Final[str] = "kraken"
"""The only exchange whose instruments the command accepts."""

MIN_AUTHOR_AGE: Final[timedelta] = timedelta(minutes=60)
"""The newest active candle of the cohort must be at least this old."""

REQUIRED_EVIDENCE: Final[str] = "writer_cessation"
"""Evidence name every run must bind."""

LOCK_ATTEMPTS: Final[int] = 3
"""Most attempts at the fence, for a write and for a reconciliation."""

LOCK_BACKOFF_SECONDS: Final[tuple[float, ...]] = (0.5, 1.0)
"""Pause after a lost fence attempt, outside any transaction."""

PREVIEW_FILE: Final[str] = "preview.csv"
SUMMARY_FILE: Final[str] = "summary.json"
APPROVAL_FILE: Final[str] = "approval.json"
INTENT_FILE: Final[str] = "intent.json"
PREPARED_FILE: Final[str] = "prepared.json"
RECEIPT_FILE: Final[str] = "receipt.json"
REFUSAL_FILE: Final[str] = "refusal.json"
FAILURE_FILE: Final[str] = "failure.json"
RECONCILIATION_FILE: Final[str] = "reconciliation.json"

PREVIEW_COLUMNS: Final[tuple[str, ...]] = (
    "instrument_public_id",
    "native_symbol",
    "timeframe",
    "open_at",
    "id",
    "public_id",
    "session_id",
    "sequence_id",
    "timestamp",
    "source",
    "complete",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "trades",
    "price_basis",
)
"""Columns of ``preview.csv``, in order."""

_RETRYABLE_BEFORE_PREPARE: Final[frozenset[str]] = frozenset(
    {"lock_timeout", "serialization_failure", "deadlock"}
)
"""Failures that prove nothing was committed and allow another attempt."""

SUCCESS_STATUSES: Final[frozenset[str]] = frozenset({"acknowledged", "reconciled_applied"})
"""Outcomes after which the voted versions are closed."""


class VoidRefusedError(Exception):
    """A refusal before anything was written to the database."""


def _utc_now() -> datetime:
    """Return the current UTC time.

    Returns:
        Timezone-aware current time.
    """
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class EvidenceFile:
    """One evidence file bound into the approval.

    Attributes:
        name: Evidence name, for example ``writer_cessation``.
        path: File to hash.
    """

    name: str
    path: Path


@dataclass(frozen=True, slots=True)
class VoidRequest:
    """A validated request.

    Attributes:
        instrument_public_ids: Instruments to void, unique and sorted.
        evidence: Evidence files to bind.
        output_dir: New or empty directory for the run's files.
    """

    instrument_public_ids: tuple[str, ...]
    evidence: tuple[EvidenceFile, ...]
    output_dir: Path


@dataclass(frozen=True, slots=True)
class VoteBinding:
    """The vote a write executes.

    Attributes:
        voted_dir: Output directory of the voted dry run.
        expect_digest: sha256 of its ``approval.json``.
    """

    voted_dir: Path
    expect_digest: str


@dataclass(frozen=True, slots=True)
class VoidContext:
    """The repository, the run's bound identity, its clock and its pause.

    Attributes:
        repository: Repository of the target database.
        database: Credential-free endpoint of the database.
        code_revision: Hashes from :func:`void_code_revision`.
        clock: Current UTC time.
        sleep: Asynchronous pause between fence attempts.
    """

    repository: Repository
    database: JsonObject
    code_revision: JsonObject
    clock: Callable[[], datetime] = _utc_now
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep


@dataclass(frozen=True, slots=True)
class VoidPlan:
    """Everything a dry run binds, rebuilt identically before a write.

    Attributes:
        targets: Resolved instruments, in request order.
        keys: Keys of every active candle of the cohort.
        preview: ``preview.csv`` bytes.
        summary: ``summary.json`` document.
        approval: ``approval.json`` document.
        digest: sha256 of the canonical approval.
    """

    targets: tuple[CandleVoidTarget, ...]
    keys: tuple[CandleVersionKey, ...]
    preview: bytes
    summary: JsonObject
    approval: JsonObject
    digest: str


@dataclass(frozen=True, slots=True)
class VoidOutcome:
    """How a write or a reconciliation ended.

    Attributes:
        status: ``acknowledged``, ``refused``, ``failed``, ``uncertain``,
            ``reconciled_applied``, ``not_applied`` or ``inconsistent``.
        record: The document written for it, or the unwritten description of an
            ``uncertain`` outcome.
    """

    status: str
    record: JsonObject


def void_code_revision(extra_modules: Sequence[ModuleType]) -> JsonObject:
    """Hash the installed source of every module that decides what is closed.

    Args:
        extra_modules: Further imported modules to hash, such as the command.

    Returns:
        sha256 of each module's file, keyed by its path below the package root.
    """
    modules = (
        repository_module,
        models,
        candle_void_types,
        candle_repair,
        sys.modules[__name__],
        *extra_modules,
    )
    return {
        f"{module.__name__.replace('.', '/')}.py": sha256_hex(
            Path(cast(str, module.__file__)).read_bytes()
        )
        for module in modules
    }


def write_durable(path: Path, data: bytes) -> None:
    """Write a new file with mode 0600 and flush it to disk before returning.

    Args:
        path: File to create; an existing path is never replaced.
        data: Complete content.
    """
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())


def evidence_entries(evidence: Sequence[EvidenceFile]) -> list[JsonValue]:
    """Hash the evidence files in name order.

    Args:
        evidence: Evidence files.

    Returns:
        One ``{name, path, sha256}`` entry per file.

    Raises:
        VoidRefusedError: When ``writer_cessation`` is missing, a name repeats or
            a file cannot be read.
    """
    names = [item.name for item in evidence]
    if REQUIRED_EVIDENCE not in names:
        raise VoidRefusedError(f"--evidence {REQUIRED_EVIDENCE}=<file> is required")
    if len(set(names)) != len(names):
        raise VoidRefusedError("an --evidence name is given twice")
    entries: list[JsonValue] = []
    for item in sorted(evidence, key=lambda entry: entry.name):
        try:
            data = item.path.read_bytes()
        except OSError:
            raise VoidRefusedError(f"--evidence {item.name} cannot be read") from None
        entries.append({"name": item.name, "path": str(item.path), "sha256": sha256_hex(data)})
    return entries


def check_targets(request: VoidRequest, targets: Sequence[CandleVoidTarget]) -> None:
    """Refuse an instrument the void must not touch.

    Args:
        request: Validated request.
        targets: Resolved instruments.

    Raises:
        VoidRefusedError: For an unknown instrument, another exchange, an active
            alias on the exchange, or a capability with market data or trading on.
    """
    if len(targets) != len(request.instrument_public_ids):
        raise VoidRefusedError("an --instrument has no active version")
    for target in targets:
        if target.exchange != EXCHANGE:
            raise VoidRefusedError(f"instrument {target.instrument_public_id} is not on {EXCHANGE}")
        if target.catalog.active_alias_count:
            raise VoidRefusedError(f"{target.native_symbol} still has an active {EXCHANGE} alias")
        if target.catalog.can_market_data or target.catalog.can_trade:
            raise VoidRefusedError(
                f"{target.native_symbol} has an active {EXCHANGE} capability with market data "
                "or trading on"
            )


def version_key(row: CandleVersionRow) -> CandleVersionKey:
    """Build a version key from a stored row.

    Args:
        row: Stored candle row.

    Returns:
        Its version key.
    """
    return CandleVersionKey(
        instrument_public_id=row["instrument_public_id"],
        timeframe=row["timeframe"],
        open_at=row["open_at"],
        id=row["id"],
        public_id=row["public_id"],
        session_id=row["session_id"],
        sequence_id=row["sequence_id"],
        timestamp=row["timestamp"],
    )


def key_json(key: CandleVersionKey) -> JsonObject:
    """Write a version key as JSON.

    Args:
        key: Version key.

    Returns:
        Its fields, instants as ISO 8601 text.
    """
    return {
        "instrument_public_id": key.instrument_public_id,
        "timeframe": key.timeframe,
        "open_at": key.open_at.isoformat(),
        "id": key.id,
        "public_id": key.public_id,
        "session_id": key.session_id,
        "sequence_id": key.sequence_id,
        "timestamp": key.timestamp.isoformat(),
    }


def key_from_json(document: Mapping[str, JsonValue]) -> CandleVersionKey:
    """Read a version key written by :func:`key_json`.

    Args:
        document: Key document.

    Returns:
        The version key.
    """
    return CandleVersionKey(
        instrument_public_id=str(document["instrument_public_id"]),
        timeframe=str(document["timeframe"]),
        open_at=datetime.fromisoformat(str(document["open_at"])),
        id=int(cast(int, document["id"])),
        public_id=str(document["public_id"]),
        session_id=str(document["session_id"]),
        sequence_id=int(cast(int, document["sequence_id"])),
        timestamp=datetime.fromisoformat(str(document["timestamp"])),
    )


def _cell(value: object) -> str:
    """Render one preview value deterministically.

    Args:
        value: Stored value.

    Returns:
        ISO 8601 for instants, ``true``/``false`` for flags, ``repr`` for floats,
        an empty string for None, and ``str`` otherwise.
    """
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return repr(value)
    return str(value)


def preview_bytes(rows: Sequence[CandleVersionRow], symbols: Mapping[str, str]) -> bytes:
    """Write one preview line per active candle.

    Args:
        rows: Stored rows, sorted by instrument, timeframe and open time.
        symbols: Native symbol per instrument.

    Returns:
        CSV bytes with the :data:`PREVIEW_COLUMNS` header.
    """
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(PREVIEW_COLUMNS)
    for row in rows:
        values = cast(Mapping[str, object], row)
        writer.writerow(
            [
                _cell(
                    symbols[row["instrument_public_id"]]
                    if name == "native_symbol"
                    else values[name]
                )
                for name in PREVIEW_COLUMNS
            ]
        )
    return buffer.getvalue().encode()


def _catalog_json(catalog: CatalogExpectation) -> JsonObject:
    """Write one catalog expectation as JSON.

    Args:
        catalog: Catalog state.

    Returns:
        Its fields, the instant as ISO 8601 text or None.
    """
    timestamp = catalog.capability_timestamp
    return {
        "instrument_public_id": catalog.instrument_public_id,
        "capability_public_id": catalog.capability_public_id,
        "capability_timestamp": None if timestamp is None else timestamp.isoformat(),
        "can_market_data": catalog.can_market_data,
        "can_trade": catalog.can_trade,
        "active_alias_count": catalog.active_alias_count,
    }


def summary_document(
    rows: Sequence[CandleVersionRow],
    targets: Sequence[CandleVoidTarget],
    references: CandleVoidReferences,
) -> JsonObject:
    """Summarize the cohort and everything the vote should see.

    Args:
        rows: Stored rows of the cohort, not empty.
        targets: Resolved instruments.
        references: FX artifact and shadow candle counts.

    Returns:
        Counts per instrument, timeframe, source and UTC day, the newest
        candle-author time, the catalog states and the reference counts.
    """
    counts = Counter(
        f"{row['instrument_public_id']}|{row['timeframe']}|{row['source']}|"
        f"{row['open_at'].astimezone(UTC).date().isoformat()}"
        for row in rows
    )
    return {
        "candles": len(rows),
        "counts": {key: counts[key] for key in sorted(counts)},
        "newest_author_timestamp": max(row["timestamp"] for row in rows).isoformat(),
        "catalog": [_catalog_json(target.catalog) for target in targets],
        "fx_references": {
            "elections_active": references.elections_active,
            "elections_historical": references.elections_historical,
            "proofs_active": references.proofs_active,
            "proofs_historical": references.proofs_historical,
        },
        "shadow_candles": {
            key: references.shadow_candles[key] for key in sorted(references.shadow_candles)
        },
    }


def approval_document(
    context: VoidContext,
    targets: Sequence[CandleVoidTarget],
    preview: bytes,
    summary: JsonObject,
    evidence: list[JsonValue],
) -> JsonObject:
    """Bind everything the write depends on.

    Args:
        context: Run context.
        targets: Resolved instruments.
        preview: Preview bytes.
        summary: Summary document.
        evidence: Evidence entries.

    Returns:
        The approval document; its canonical sha256 is the digest.
    """
    return {
        "schema": SCHEMA,
        "operation": OPERATION,
        "database": context.database,
        "instruments": [
            {
                "instrument_public_id": target.instrument_public_id,
                "native_symbol": target.native_symbol,
                "exchange": target.exchange,
            }
            for target in targets
        ],
        "code_revision": context.code_revision,
        "preview_sha256": sha256_hex(preview),
        "summary_sha256": sha256_hex(canonical_json(summary)),
        "evidence": evidence,
    }


async def build_plan(context: VoidContext, request: VoidRequest) -> VoidPlan:
    """Read the cohort and build the dry run's documents.

    Args:
        context: Run context.
        request: Validated request.

    Returns:
        The plan.

    Raises:
        VoidRefusedError: For a refused instrument, missing evidence, an empty
            cohort, or a cohort whose newest candle is younger than
            :data:`MIN_AUTHOR_AGE`.
    """
    repository = context.repository
    targets = tuple(await repository.get_candle_void_targets(request.instrument_public_ids))
    check_targets(request, targets)
    evidence = evidence_entries(request.evidence)
    rows = await repository.get_active_candle_versions(request.instrument_public_ids)
    if not rows:
        raise VoidRefusedError("the instruments have no active candle")
    newest = max(row["timestamp"] for row in rows)
    if context.clock() - newest < MIN_AUTHOR_AGE:
        raise VoidRefusedError(
            f"minimum candle-author age: the newest candle was written at {newest.isoformat()}, "
            f"less than {int(MIN_AUTHOR_AGE.total_seconds() // 60)} minutes ago"
        )
    references = await repository.get_candle_void_references(request.instrument_public_ids)
    symbols = {target.instrument_public_id: target.native_symbol for target in targets}
    preview = preview_bytes(rows, symbols)
    summary = summary_document(rows, targets, references)
    approval = approval_document(context, targets, preview, summary, evidence)
    return VoidPlan(
        targets=targets,
        keys=tuple(version_key(row) for row in rows),
        preview=preview,
        summary=summary,
        approval=approval,
        digest=sha256_hex(canonical_json(approval)),
    )


async def plan_void(context: VoidContext, request: VoidRequest) -> VoidPlan:
    """Run the dry run and write its three files.

    Args:
        context: Run context.
        request: Validated request; its output directory exists and is empty.

    Returns:
        The plan, whose digest is voted on.
    """
    plan = await build_plan(context, request)
    write_exclusive(request.output_dir / PREVIEW_FILE, plan.preview)
    write_exclusive(request.output_dir / SUMMARY_FILE, canonical_json(plan.summary))
    write_exclusive(request.output_dir / APPROVAL_FILE, canonical_json(plan.approval))
    return plan


def load_vote(binding: VoteBinding) -> tuple[bytes, bytes]:
    """Read the voted approval and preview, and check the approval's digest.

    Args:
        binding: The vote.

    Returns:
        The voted ``approval.json`` and ``preview.csv`` bytes.

    Raises:
        VoidRefusedError: When a file cannot be read or the digest differs.
    """
    try:
        approval = (binding.voted_dir / APPROVAL_FILE).read_bytes()
        preview = (binding.voted_dir / PREVIEW_FILE).read_bytes()
    except OSError:
        raise VoidRefusedError("the voted directory cannot be read") from None
    if sha256_hex(approval) != binding.expect_digest:
        raise VoidRefusedError("the voted approval.json does not match --expect-digest")
    return approval, preview


def _settle(directory: Path, name: str, status: str, record: JsonObject) -> VoidOutcome:
    """Write an outcome file and return the outcome.

    Args:
        directory: Run directory.
        name: Outcome file name.
        status: Outcome status.
        record: Outcome fields.

    Returns:
        The outcome.
    """
    document: JsonObject = {"status": status, **record}
    write_exclusive(directory / name, canonical_json(document))
    return VoidOutcome(status=status, record=document)


def _preparation_hook(directory: Path, run: JsonObject) -> Callable[[datetime], None]:
    """Build the hook V1 calls under its fence before closing any row.

    Args:
        directory: Run directory.
        run: The run's identity fields.

    Returns:
        A hook that refuses a concluded run and records ``void_at`` durably.
    """

    def on_prepared(void_at: datetime) -> None:
        if (directory / RECONCILIATION_FILE).exists():
            raise CandleVoidRefusedError("reconciled")
        write_durable(
            directory / PREPARED_FILE, canonical_json({**run, "void_at": void_at.isoformat()})
        )

    return on_prepared


async def write_void(
    context: VoidContext, request: VoidRequest, binding: VoteBinding
) -> VoidOutcome:
    """Execute a vote: rebuild, compare, record the intent, close, settle.

    Args:
        context: Run context.
        request: The voted run's request; its output directory exists and is
            empty and becomes the write's run directory.
        binding: The vote.

    Returns:
        The outcome. Only ``acknowledged`` and ``reconciled_applied`` mean the
        voted versions are closed.

    Raises:
        VoidRefusedError: When the rebuilt plan differs from the vote.
    """
    voted_approval, voted_preview = load_vote(binding)
    plan = await build_plan(context, request)
    if canonical_json(plan.approval) != voted_approval or plan.preview != voted_preview:
        raise VoidRefusedError("the rebuilt plan differs from the voted one")
    directory = request.output_dir
    run: JsonObject = {"run_id": str(uuid7()), "voted_digest": binding.expect_digest}
    write_exclusive(
        directory / INTENT_FILE,
        canonical_json({**run, "keys": [key_json(key) for key in plan.keys]}),
    )
    void_request = CandleVoidRequest(
        instrument_public_ids=request.instrument_public_ids,
        expected=plan.keys,
        catalog=tuple(target.catalog for target in plan.targets),
        on_prepared=_preparation_hook(directory, run),
    )
    attempt = 0
    while True:
        attempt += 1
        try:
            result = await context.repository.void_candle_versions(void_request)
        except CandleVoidRefusedError as refusal:
            return _settle(directory, REFUSAL_FILE, "refused", {**run, "reason": refusal.code})
        except Exception as error:
            outcome = classify_failure(error)
            prepared = (directory / PREPARED_FILE).exists()
            if outcome in _RETRYABLE_BEFORE_PREPARE and not prepared and attempt < LOCK_ATTEMPTS:
                await context.sleep(LOCK_BACKOFF_SECONDS[attempt - 1])
                continue
            if outcome == "uncertain":
                return await reconcile_void(context, directory)
            return _settle(
                directory,
                FAILURE_FILE,
                "failed",
                {**run, "outcome": outcome, **error_fields(error)},
            )
        return _settle(
            directory,
            RECEIPT_FILE,
            "acknowledged",
            {
                **run,
                "void_at": result.void_at.isoformat(),
                "closed": len(result.closed),
                "keys": [key_json(key) for key in result.closed],
            },
        )


def conclude(
    readings: Mapping[CandleVersionKey, CandleVersionReading], void_at: datetime | None
) -> str:
    """Decide what a run did from a fenced reading of its expected versions.

    Args:
        readings: Each expected key's reading.
        void_at: The run's prepared instant, or None without ``prepared.json``.

    Returns:
        ``not_applied`` without a preparation or with every version still
        active; ``reconciled_applied`` when every version ends at the prepared
        instant; ``inconsistent`` otherwise.
    """
    if void_at is None:
        return "not_applied"
    values = list(readings.values())
    if all(reading.known_to == void_at for reading in values):
        return "reconciled_applied"
    if all(reading.active for reading in values):
        return "not_applied"
    return "inconsistent"


def _read_document(path: Path) -> JsonObject:
    """Read a JSON document the run wrote.

    Args:
        path: File to read.

    Returns:
        The document.
    """
    return cast(JsonObject, json.loads(path.read_bytes()))


async def reconcile_void(context: VoidContext, directory: Path) -> VoidOutcome:
    """Settle a run whose outcome is not known, serialized through the candle writer fence.

    Args:
        context: Run context.
        directory: Run directory holding ``intent.json``.

    Returns:
        The concluded outcome, or ``uncertain`` when the fence could not be
        taken. A run already concluded reports its first conclusion.
    """
    intent = _read_document(directory / INTENT_FILE)
    run: JsonObject = {"run_id": intent["run_id"], "voted_digest": intent["voted_digest"]}
    keys = [
        key_from_json(cast(Mapping[str, JsonValue], item))
        for item in cast(list[JsonValue], intent["keys"])
    ]
    concluded: list[VoidOutcome] = []

    def decide(readings: dict[CandleVersionKey, CandleVersionReading]) -> None:
        existing = directory / RECONCILIATION_FILE
        if existing.exists():
            previous = _read_document(existing)
            concluded.append(VoidOutcome(status=str(previous["status"]), record=previous))
            return
        prepared = directory / PREPARED_FILE
        void_at = (
            datetime.fromisoformat(str(_read_document(prepared)["void_at"]))
            if prepared.exists()
            else None
        )
        status = conclude(readings, void_at)
        document: JsonObject = {
            "status": status,
            **run,
            "void_at": None if void_at is None else void_at.isoformat(),
            "expected": len(readings),
            "active": sum(1 for reading in readings.values() if reading.active),
        }
        write_durable(existing, canonical_json(document))
        concluded.append(VoidOutcome(status=status, record=document))

    attempt = 0
    while True:
        attempt += 1
        try:
            await context.repository.observe_candle_void(keys, decide)
        except Exception as error:
            if classify_failure(error) in _RETRYABLE_BEFORE_PREPARE and attempt < LOCK_ATTEMPTS:
                await context.sleep(LOCK_BACKOFF_SECONDS[attempt - 1])
                continue
            return VoidOutcome(status="uncertain", record={**run, **error_fields(error)})
        return concluded[0]
