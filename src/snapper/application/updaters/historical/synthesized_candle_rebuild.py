"""Rebuild higher Kraken spot candles from the minutes established by recorded repairs.

Bind the write to the voted output predecessor, minute fingerprint and fold.
Verification reads each output and its inputs from one snapshot and reports
the complete derived scope.
"""

import asyncio
import csv
import io
import json
import math
import re
import sys
from collections import Counter
from collections import defaultdict
from collections.abc import Collection
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from ipaddress import ip_interface
from itertools import batched
from pathlib import Path
from types import ModuleType
from typing import Annotated
from typing import Literal
from typing import cast

from pydantic import AfterValidator
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import StringConstraints
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy import tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.updaters.historical import candle_repair
from snapper.application.updaters.historical import guarded_candle_write
from snapper.application.updaters.historical.candle_repair import APPROVAL_FILE
from snapper.application.updaters.historical.candle_repair import LIVE_EDGE
from snapper.application.updaters.historical.candle_repair import PREVIEW_FILE
from snapper.application.updaters.historical.candle_repair import RECEIPT_FILE
from snapper.application.updaters.historical.candle_repair import RECEIPT_LINES_FILE
from snapper.application.updaters.historical.candle_repair import SUMMARY_FILE
from snapper.application.updaters.historical.candle_repair import MinuteKey
from snapper.application.updaters.historical.candle_repair import RepairRefusedError
from snapper.application.updaters.historical.candle_repair import RepairWindow
from snapper.application.updaters.historical.candle_repair import RunContext
from snapper.application.updaters.historical.candle_repair import WriteReport
from snapper.application.updaters.historical.candle_repair import sha256_hex
from snapper.application.updaters.historical.guarded_candle_write import CandleKey
from snapper.application.updaters.historical.guarded_candle_write import Chunk
from snapper.application.updaters.historical.guarded_candle_write import ChunkAcknowledgement
from snapper.application.updaters.historical.guarded_candle_write import ChunkAdapter
from snapper.application.updaters.historical.guarded_candle_write import ChunkWriter
from snapper.application.updaters.historical.guarded_candle_write import ReceiptLog
from snapper.application.updaters.historical.guarded_candle_write import WriteOptions
from snapper.application.updaters.historical.guarded_candle_write import candle_key
from snapper.application.updaters.historical.guarded_candle_write import canonical_json
from snapper.application.updaters.historical.guarded_candle_write import reached
from snapper.application.updaters.historical.guarded_candle_write import reconcile_versions
from snapper.application.updaters.historical.guarded_candle_write import totals
from snapper.application.updaters.historical.guarded_candle_write import write_exclusive
from snapper.cli import operator_support
from snapper.core.candle_identity import deterministic_candle_public_id
from snapper.core.json_types import JsonObject
from snapper.core.json_types import JsonValue
from snapper.data import repository as repository_module
from snapper.data.models import Candle
from snapper.data.models import Instrument
from snapper.data.models import ProcessRun
from snapper.data.models import Symbol
from snapper.data.models import is_open_ended_version
from snapper.data.repository import Repository
from snapper.data.repository import sample_other_client_backends
from snapper.data.repository import synthesis_inputs_sha256
from snapper.data.repository import synthesized_observation_loss
from snapper.data.repository import where_active
from snapper.data.repository_types import CandlePredecessor
from snapper.data.repository_types import CandleRow
from snapper.data.repository_types import CandleUpsertRow
from snapper.data.repository_types import CandleVersionRow
from snapper.data.repository_types import ClientSample
from snapper.data.repository_types import SynthesizedRebuildExpectation
from snapper.messaging.publishers import base
from snapper.messaging.publishers import candle_aggregator
from snapper.messaging.publishers import native_candle_finalizer
from snapper.messaging.publishers.base import MarketDataPublisherService
from snapper.messaging.publishers.candle_aggregator import CandleAggregator
from snapper.messaging.publishers.native_candle_finalizer import is_complete_historical_minute

COMMAND = "rebuild-synthesized-candles"
TIMEFRAMES = ("5m", "15m", "30m", "1h", "4h", "1d")
_SECONDS = dict(zip(TIMEFRAMES, (300, 900, 1800, 3600, 14400, 86400), strict=True))
CHUNK_ROWS = 100
CHUNK_MINUTES = 1440
_READ_INSTRUMENTS = 20
_DAY = timedelta(days=1)
_TIMEOUT = "SET LOCAL statement_timeout = '20s'"
_EXCHANGE = "kraken"
_CANDLE_WRITERS = (
    "synthesized_candle_backfill",
    "trade_candle_backfill",
    "kraken_equities_aggregates_backfill",
    "kraken_futures_aggregates_backfill",
    "polygon_grouped_candle_load",
    "polygon_csv_load",
)
_BUCKETS = ("applied", "skipped_by_guard", "applied_superseded", "uncertain", "not_applied")
_SELECTIONS = ("not_voted", "already_applied", "changed")
_VALUES = ("open", "high", "low", "close", "volume", "vwap", "trades")
_BUSINESS = (*_VALUES, "source", "complete", "price_basis")
_WRITE_ACTIONS = {"insert", "rebuild"}

type _Action = Literal[
    "insert",
    "rebuild",
    "unchanged",
    "protected",
    "history_without_current",
    "invalid_input",
    "no_input",
    "after_end",
    "would_lose_observations",
    "refused_by_guard",
]
type _Stop = Literal[
    "completed",
    "max_rows",
    "uncertain_acknowledgement",
    "lock_timeouts",
    "attempts_exhausted",
    "unique_violation",
    "error",
    "cancelled",
]
type _Digest = Annotated[str, StringConstraints(pattern=r"^[0-9A-Fa-f]{64}$")]
type _Uuid = Annotated[
    str,
    StringConstraints(pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"),
]
type _RunId = Annotated[
    str,
    StringConstraints(
        pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    ),
]
type _Nonnegative = Annotated[int, Field(ge=0)]
type _Positive = Annotated[int, Field(gt=0)]


def _valid_instant(value: str) -> str:
    """Validate the calendar components of an RFC 3339 instant."""
    datetime.fromisoformat(value)
    return value


type _Instant = Annotated[
    str,
    StringConstraints(
        pattern=r"^\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(\.\d+)?([Zz]|[+-]\d{2}:\d{2})$"
    ),
    AfterValidator(_valid_instant),
]


def _valid_address(value: str) -> str:
    """Validate a Docker address or an inet address as printed by PostgreSQL."""
    ip_interface(value)
    return value


type _Address = Annotated[str, AfterValidator(_valid_address)]


class _Evidence(BaseModel):
    """Strictly parse operator evidence before using any of its values."""

    model_config = ConfigDict(strict=True, extra="forbid")


class _FileBinding(_Evidence):
    """Coordinator's hashes and terminal identity for a completed invocation."""

    run_id: _RunId
    voted_digest: _Digest
    stop_reason: _Stop
    approval_sha256: _Digest
    receipt_sha256: _Digest
    receipt_lines_sha256: _Digest


class _Container(_Evidence):
    """Container identity, lifetime and observed network addresses."""

    name: Annotated[str, StringConstraints(min_length=1)]
    image_id: Annotated[str, StringConstraints(pattern=r"^sha256:[0-9A-Fa-f]{64}$")]
    started_at: _Instant
    finished_at: _Instant
    addresses: list[_Address]


class _Observer(_Evidence):
    """Fields in the observer's first settlement statement."""

    pid: _Positive
    current_user: str
    session_user: str
    now: _Instant
    clock_timestamp: _Instant
    server_addr: _Address | None
    server_port: _Positive | None
    database: str
    postmaster_start: _Instant


class _Backend(_Evidence):
    """Fields of one backend, preserving hidden values as null."""

    pid: _Positive
    usename: str | None
    datname: str | None
    client_addr: _Address | None
    client_port: int | None
    application_name: str | None
    backend_type: str | None
    backend_start: _Instant | None
    xact_start: _Instant | None
    state: str | None


class _Settlement(_Evidence):
    """One candidate settlement observation of the approved endpoint."""

    endpoint: JsonObject
    observer: _Observer
    backends: list[_Backend]
    role: Annotated[str, StringConstraints(min_length=1)]


class _RunRecord(_FileBinding):
    """Recorded repair invocation and its independently collected evidence."""

    application_name: str | None
    observed_addresses: list[_Address]
    container: _Container
    settlements: list[_Settlement]


class _RunRecords(_Evidence):
    """Manifest of recorded repair runs."""

    runs: Annotated[list[_RunRecord], Field(min_length=1)]


class _ReceiptIdentity(_Evidence):
    """Shared fields that bind a write receipt to its coordinator record."""

    run_id: _RunId
    voted_digest: _Digest
    stop_reason: _Stop
    write_time: _Instant
    started_at: _Instant
    ended_at: _Instant
    selected: _Nonnegative
    scheduled: _Nonnegative
    reached: _Nonnegative
    totals: dict[str, _Nonnegative]


class _Attempt(_Evidence):
    """A chunk attempt's sanitized result."""

    attempt: _Positive
    outcome: Literal[
        "acknowledged",
        "cancelled",
        "lock_timeout",
        "serialization_failure",
        "deadlock",
        "natural_key_conflict",
        "unique_violation",
        "uncertain",
    ]
    error: str | None = None
    driver_error: str | None = None
    sqlstate: str | None = None


class _ErrorFields(_Evidence):
    """Error metadata safe for an operator receipt."""

    error: str
    driver_error: str | None
    sqlstate: str | None


class _ChunkTimes(_Evidence):
    """Metadata common to acknowledged, interrupted and refused chunks."""

    chunk: _Positive
    rows: _Positive
    started_at: _Instant
    ended_at: _Instant
    attempt_count: _Nonnegative
    attempts: list[_Attempt]
    reconciliation_error: _ErrorFields | None


class _MinuteRef(_Evidence):
    """The key of a referenced repair minute."""

    instrument_public_id: _Uuid
    minute_utc: _Instant

    @property
    def key(self) -> MinuteKey:
        """Decode and check the aligned minute."""
        return self.instrument_public_id, _minute(self.minute_utc)


class _RepairWritten(_MinuteRef):
    """Reference to a scheduled one-minute repair."""

    sequence_id: _Positive


class _RepairSelected(_MinuteRef):
    """Reference to a row reported in the repair selection."""

    action: Literal["insert", "correct"]


class _RepairChunk(_ChunkTimes):
    """The repair chunk's five disjoint row-outcome lists."""

    action: Literal["insert", "correct"]
    applied: list[_RepairWritten]
    skipped_by_guard: list[_RepairWritten]
    applied_superseded: list[_RepairWritten]
    uncertain: list[_RepairWritten]
    not_applied: list[_RepairWritten]


class _RepairReceipt(_ReceiptIdentity):
    """The immutable receipt written by one repair invocation."""

    not_voted: list[_RepairSelected]
    already_applied: list[_RepairSelected]
    changed: list[_RepairSelected]
    chunks: list[_RepairChunk]


class _InstrumentEvidence(_Evidence):
    """An approved exchange instrument and its native symbol."""

    instrument_public_id: _Uuid
    symbol: Annotated[str, StringConstraints(min_length=1)]


class _WindowEvidence(_Evidence):
    """The approved interval for enumerating repaired minutes."""

    start: _Instant
    end: _Instant


class _RepairApproval(_Evidence):
    """Repair approval fields needed to enumerate committed writes."""

    model_config = ConfigDict(strict=True, extra="ignore")

    database: JsonObject
    exchange: Literal["kraken"]
    timeframe: Literal["1m"]
    source: Literal["snapshot", "trades"]
    window: _WindowEvidence
    instruments: list[_InstrumentEvidence]
    preview_sha256: _Digest


@dataclass(frozen=True, slots=True)
class _RepairRun:
    """Validated files and lookup tables of one recorded repair run."""

    record: _RunRecord
    receipt: _RepairReceipt
    approval: _RepairApproval
    window: RepairWindow
    instruments: Mapping[str, str]
    preview: Mapping[MinuteKey, candle_repair.PreviewLine]
    rows: Mapping[MinuteKey, tuple[_RepairWritten, _RepairChunk, str]]


@dataclass(frozen=True, slots=True)
class _RecordedRuns:
    """Immutable inputs of a synthesized rebuild plan."""

    digest: str
    records: JsonObject
    runs: tuple[_RepairRun, ...]
    instruments: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class VerificationFiles:
    """Run record and rebuild write directory to verify."""

    directory: Path
    record: Path


@dataclass(frozen=True, slots=True)
class RebuildRequest:
    """Recorded repairs and the bounds of this invocation's output."""

    repair_runs: tuple[Path, ...]
    run_records: Path
    end: datetime
    output_dir: Path
    verification: VerificationFiles | None = None


def code_revision(cli_module: ModuleType) -> JsonObject:
    """Hash the installed source bound by the rebuild approval."""
    modules = (
        repository_module,
        candle_aggregator,
        base,
        native_candle_finalizer,
        guarded_candle_write,
        candle_repair,
        operator_support,
        sys.modules[__name__],
        cli_module,
    )
    return {
        f"{module.__name__.replace('.', '/')}.py": sha256_hex(
            Path(cast(str, module.__file__)).read_bytes()
        )
        for module in modules
    }


def _minute(value: str) -> datetime:
    """Parse an aligned UTC minute in a file reference."""
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise RepairRefusedError("invalid minute in candle evidence") from error
    if parsed.utcoffset() != timedelta(0) or parsed.second or parsed.microsecond:
        raise RepairRefusedError("unaligned UTC minute in candle evidence")
    return parsed


def validate_end(end: datetime, run_time: datetime) -> None:
    """Enforce the UTC-minute and 180-minute live-edge bounds."""
    if end.utcoffset() != timedelta(0) or end.second or end.microsecond:
        raise RepairRefusedError("--end must be a UTC minute boundary")
    if end > run_time - LIVE_EDGE:
        raise RepairRefusedError("--end must be at least 180 minutes before the run")


def _unique_object(pairs: list[tuple[str, JsonValue]]) -> JsonObject:
    """Reject duplicate keys in JSON evidence."""
    result = dict(pairs)
    if len(result) != len(pairs):
        raise ValueError("duplicate JSON keys")
    return result


def _invalid_constant(value: str) -> JsonValue:
    """Reject JSON non-finite number extensions."""
    del value
    raise ValueError("non-finite JSON number")


def _json(data: bytes, label: str) -> JsonValue:
    """Parse a JSON document while keeping untrusted text out of refusals."""
    try:
        return cast(
            JsonValue,
            json.loads(data, object_pairs_hook=_unique_object, parse_constant=_invalid_constant),
        )
    except (ValueError, UnicodeError) as error:
        raise RepairRefusedError(f"{label} does not parse") from error


def _parse[EvidenceT: _Evidence](
    model: type[EvidenceT], document: JsonValue, label: str
) -> EvidenceT:
    """Decode evidence to a typed object with a sanitized format error."""
    try:
        return model.model_validate(document)
    except ValidationError as error:
        raise RepairRefusedError(f"{label} has an invalid format") from error


def _read(path: Path, label: str) -> bytes:
    """Read one evidence file with a safe name for output."""
    try:
        return path.read_bytes()
    except OSError as error:
        raise RepairRefusedError(f"cannot read {label}") from error


def _bind_files(
    binding: _FileBinding, receipt: _ReceiptIdentity, files: Mapping[str, bytes]
) -> None:
    """Compare the exact bytes and terminal identity against the supplied record."""
    for name, digest in (
        (APPROVAL_FILE, binding.approval_sha256),
        (RECEIPT_FILE, binding.receipt_sha256),
        (RECEIPT_LINES_FILE, binding.receipt_lines_sha256),
    ):
        if sha256_hex(files[name]) != digest.lower():
            raise RepairRefusedError(f"{name} differs from the run record")
    if (receipt.run_id, receipt.voted_digest, receipt.stop_reason) != (
        binding.run_id,
        binding.voted_digest.lower(),
        binding.stop_reason,
    ):
        raise RepairRefusedError("receipt identity differs from the run record")


def _read_write_files(directory: Path) -> dict[str, bytes]:
    """Load each file once so that the bytes parsed are also the bytes hashed."""
    return {
        name: _read(directory / name, name)
        for name in (APPROVAL_FILE, PREVIEW_FILE, RECEIPT_FILE, RECEIPT_LINES_FILE)
    }


def _check_receipt_counts(
    receipt: _ReceiptIdentity, chunks: Sequence[_ChunkTimes], counts: Counter[str]
) -> None:
    """Validate the exact totals and count relationships of a write receipt."""
    if receipt.totals != {bucket: counts[bucket] for bucket in _BUCKETS}:
        raise RepairRefusedError("receipt totals do not add up")
    if receipt.reached != sum(chunk.rows for chunk in chunks) or receipt.reached != counts.total():
        raise RepairRefusedError("receipt reached count does not add up")
    if not 0 <= receipt.reached <= receipt.scheduled <= receipt.selected:
        raise RepairRefusedError("receipt scheduled or selected count is inconsistent")
    if receipt.stop_reason in {"completed", "max_rows"} and receipt.reached != receipt.scheduled:
        raise RepairRefusedError("complete receipt did not reach all scheduled rows")
    if receipt.stop_reason == "completed" and receipt.scheduled != receipt.selected:
        raise RepairRefusedError("completed receipt omitted selected rows")
    if receipt.stop_reason == "max_rows" and receipt.scheduled == receipt.selected:
        raise RepairRefusedError("max_rows receipt has no remaining rows")


def _check_chunk_metadata(chunk: _ChunkTimes, number: int) -> None:
    """Check chunk numbering, attempt counts and chronological read bounds."""
    if chunk.chunk != number or chunk.attempt_count != len(chunk.attempts):
        raise RepairRefusedError("receipt chunk numbering or attempt count is inconsistent")
    if [attempt.attempt for attempt in chunk.attempts] != list(range(1, chunk.attempt_count + 1)):
        raise RepairRefusedError("receipt attempts are out of order")
    if datetime.fromisoformat(chunk.started_at) > datetime.fromisoformat(chunk.ended_at):
        raise RepairRefusedError("receipt chunk timestamps are out of order")
    for attempt in chunk.attempts:
        _check_attempt(attempt)


def _check_attempt(attempt: _Attempt) -> None:
    """Require the exact shape emitted for acknowledged and failed attempts."""
    fields = {"attempt", "outcome"}
    if attempt.outcome != "acknowledged":
        fields.update({"error", "driver_error", "sqlstate"})
        if attempt.error is None:
            raise RepairRefusedError("failed receipt attempt is missing its error type")
    if attempt.model_fields_set != fields:
        raise RepairRefusedError("receipt attempt fields do not match its outcome")


def _check_lines[ChunkT: _ChunkTimes](
    model: type[ChunkT], data: bytes, chunks: Sequence[ChunkT]
) -> None:
    """Require JSONL to encode exactly the receipt's chunk list."""
    lines = [
        _parse(model, _json(line, RECEIPT_LINES_FILE), RECEIPT_LINES_FILE)
        for line in data.splitlines()
    ]
    if [line.model_dump(exclude_unset=True) for line in lines] != [
        chunk.model_dump(exclude_unset=True) for chunk in chunks
    ]:
        raise RepairRefusedError("receipt.jsonl differs from receipt.json chunks")


def _check_capture(capture: _Settlement, binding: _RunRecord, database: JsonObject) -> None:
    """Require a valid collector endpoint, role, time and unique observer self-row."""
    observer = capture.observer
    now = datetime.fromisoformat(observer.now)
    if now <= datetime.fromisoformat(binding.container.finished_at):
        raise RepairRefusedError("settlement observer predates container exit")
    if canonical_json(capture.endpoint) != canonical_json(
        database
    ) or observer.database != database.get("database"):
        raise RepairRefusedError("settlement endpoint differs from the repair approval")
    if (observer.current_user, observer.session_user) != (capture.role, capture.role):
        raise RepairRefusedError("settlement observer role does not match the record")
    backends = {backend.pid: backend for backend in capture.backends}
    if len(backends) != len(capture.backends) or observer.pid not in backends:
        raise RepairRefusedError("settlement lacks a unique observer backend")
    own = backends[observer.pid]
    if (own.usename, own.backend_type, own.state) != (capture.role, "client backend", "active"):
        raise RepairRefusedError("settlement observer backend identity differs")
    if own.xact_start is None or datetime.fromisoformat(own.xact_start) != now:
        raise RepairRefusedError("settlement observer transaction start differs")
    if own.backend_start is None or datetime.fromisoformat(own.backend_start) > now:
        raise RepairRefusedError("settlement observer backend start is invalid")


def _check_record(binding: _RunRecord, database: JsonObject) -> None:
    """Validate structural settlement evidence even when it will not be needed."""
    container = binding.container
    if datetime.fromisoformat(container.finished_at) <= datetime.fromisoformat(
        container.started_at
    ):
        raise RepairRefusedError("run record container timestamps are out of order")
    if binding.application_name not in {None, f"snapper:repair-candles:{binding.run_id}"}:
        raise RepairRefusedError("run record has an invalid repair connection name")
    for capture in binding.settlements:
        _check_capture(capture, binding, database)
    observed = [datetime.fromisoformat(item.observer.now) for item in binding.settlements]
    if observed and any(earlier >= observed[-1] for earlier in observed[:-1]):
        raise RepairRefusedError("newest settlement must follow every earlier capture")


def _possible_repair_backend(
    backend: _Backend, binding: _RunRecord, addresses: Collection[str]
) -> bool:
    """Match a candidate by exact name, or by the conservative legacy evidence."""
    if binding.application_name is not None:
        return backend.application_name in {None, binding.application_name}
    if backend.client_addr is None or backend.backend_start is None:
        return True
    started = datetime.fromisoformat(backend.backend_start)
    container = binding.container
    return str(ip_interface(backend.client_addr).ip) in addresses or (
        datetime.fromisoformat(container.started_at)
        <= started
        <= datetime.fromisoformat(container.finished_at)
    )


def _settled(binding: _RunRecord) -> bool:
    """Only the newest structurally validated capture can establish settlement."""
    if not binding.settlements:
        return False
    capture = binding.settlements[-1]
    addresses = {
        str(ip_interface(value).ip)
        for value in (*binding.container.addresses, *binding.observed_addresses)
    }
    candidates = (
        row
        for row in capture.backends
        if row.pid != capture.observer.pid
        and row.backend_type in {None, "client backend"}
        and (row.usename is not None or row.datname is not None)
    )
    return not any(_possible_repair_backend(row, binding, addresses) for row in candidates)


def _inside(key: MinuteKey, window: RepairWindow, instruments: Mapping[str, str]) -> bool:
    """Recognize an approved minute of an approved instrument."""
    return key[0] in instruments and window.start <= key[1] < window.end


def _repair_rows(
    receipt: _RepairReceipt,
    window: RepairWindow,
    instruments: Mapping[str, str],
    preview: Mapping[MinuteKey, candle_repair.PreviewLine],
) -> dict[MinuteKey, tuple[_RepairWritten, _RepairChunk, str]]:
    """Index the repair chunk's disjoint references within the approved scope."""
    rows: dict[MinuteKey, tuple[_RepairWritten, _RepairChunk, str]] = {}
    sequences: set[int] = set()
    counts: Counter[str] = Counter()
    for number, chunk in enumerate(receipt.chunks, 1):
        _check_chunk_metadata(chunk, number)
        before = len(rows)
        for bucket in _BUCKETS:
            for reference in cast(list[_RepairWritten], getattr(chunk, bucket)):
                key = reference.key
                line = preview.get(key)
                if (
                    not _inside(key, window, instruments)
                    or line is None
                    or line.action != chunk.action
                ):
                    raise RepairRefusedError("repair chunk reference is outside its write preview")
                if key in rows or reference.sequence_id in sequences:
                    raise RepairRefusedError("repair receipt repeats a minute or sequence")
                rows[key] = reference, chunk, bucket
                sequences.add(reference.sequence_id)
                counts[bucket] += 1
        if len(rows) - before != chunk.rows:
            raise RepairRefusedError("repair chunk row count does not add up")
    _check_receipt_counts(receipt, receipt.chunks, counts)
    return rows


def _check_repair_selections(
    receipt: _RepairReceipt, window: RepairWindow, instruments: Mapping[str, str]
) -> None:
    """Check selection lists separately and permit the documented not-voted overlap."""
    seen = [
        {reference.key for reference in cast(list[_RepairSelected], getattr(receipt, name))}
        for name in _SELECTIONS
    ]
    for name, keys in zip(_SELECTIONS, seen, strict=True):
        if len(keys) != len(getattr(receipt, name)) or any(
            not _inside(key, window, instruments) for key in keys
        ):
            raise RepairRefusedError("repair receipt has an invalid selection list")
    if seen[1] & seen[2]:
        raise RepairRefusedError("repair receipt overlaps already_applied and changed")


def _load_repair(
    files: Mapping[str, bytes],
    record: _RunRecord,
    receipt: _RepairReceipt,
    database: JsonObject,
) -> _RepairRun:
    """Bind a directory to its record and validate its references before reading the database."""
    _bind_files(record, receipt, files)
    _check_lines(_RepairChunk, files[RECEIPT_LINES_FILE], receipt.chunks)
    approval = _parse(_RepairApproval, _json(files[APPROVAL_FILE], APPROVAL_FILE), APPROVAL_FILE)
    if canonical_json(approval.database) != canonical_json(database):
        raise RepairRefusedError("repair approval targets another database")
    if sha256_hex(files[PREVIEW_FILE]) != approval.preview_sha256.lower():
        raise RepairRefusedError("repair preview.csv does not match its approval")
    instruments = {item.instrument_public_id: item.symbol for item in approval.instruments}
    if len(instruments) != len(approval.instruments):
        raise RepairRefusedError("repair approval repeats an instrument")
    window = RepairWindow(_minute(approval.window.start), _minute(approval.window.end))
    lines = candle_repair.parse_preview(files[PREVIEW_FILE])
    preview = {(line.instrument_public_id, _minute(line.minute_utc)): line for line in lines}
    if len(preview) != len(lines):
        raise RepairRefusedError("repair preview repeats a logical minute")
    if any(
        not _inside(key, window, instruments) or line.symbol != instruments[key[0]]
        for key, line in preview.items()
    ):
        raise RepairRefusedError("repair preview contains an unapproved minute or symbol")
    _check_record(record, database)
    _check_repair_selections(receipt, window, instruments)
    rows = _repair_rows(receipt, window, instruments, preview)
    return _RepairRun(record, receipt, approval, window, instruments, preview, rows)


def _load_runs(request: RebuildRequest, database: JsonObject) -> _RecordedRuns:
    """Match the exact set of run records and directories and bind their instrument names."""
    data = _read(request.run_records, "run records")
    document = _json(data, "run records")
    records = _parse(_RunRecords, document, "run records")
    unmatched = {record.run_id: record for record in records.runs}
    if len(unmatched) != len(records.runs):
        raise RepairRefusedError("run records repeat a run id")
    runs: list[_RepairRun] = []
    instruments: dict[str, str] = {}
    for directory in request.repair_runs:
        files = _read_write_files(directory)
        receipt = _parse(_RepairReceipt, _json(files[RECEIPT_FILE], RECEIPT_FILE), RECEIPT_FILE)
        record = unmatched.pop(receipt.run_id, None)
        if record is None:
            raise RepairRefusedError("repair directory has no distinct run record")
        run = _load_repair(files, record, receipt, database)
        for instrument, symbol in run.instruments.items():
            if instruments.setdefault(instrument, symbol) != symbol:
                raise RepairRefusedError("repair approvals disagree on an instrument symbol")
        runs.append(run)
    if unmatched:
        raise RepairRefusedError("run records include a run without a repair directory")
    return _RecordedRuns(
        sha256_hex(data),
        cast(JsonObject, document),
        tuple(sorted(runs, key=lambda run: run.record.run_id)),
        instruments,
    )


@dataclass(frozen=True, slots=True)
class _ResolvedRun:
    """Committed one-minute repairs and the frozen outcomes of originally uncertain rows."""

    run: _RepairRun
    found: Mapping[MinuteKey, CandleVersionRow]
    resolution: Mapping[MinuteKey, JsonObject]
    settlement: bool | None

    def frozen(self) -> JsonObject:
        """Encode the resolution without depending on iteration order or file paths."""
        return {
            "run_id": self.run.record.run_id,
            "rows": [
                {"instrument_public_id": key[0], "minute_utc": key[1].isoformat(), **item}
                for key, item in sorted(self.resolution.items())
            ],
        }


@dataclass(frozen=True, slots=True)
class _Scope:
    """Validated source evidence and the distinct minutes all runs actually wrote."""

    recorded: _RecordedRuns
    resolved: tuple[_ResolvedRun, ...]
    process_runs: list[JsonObject]

    @property
    def applied(self) -> set[MinuteKey]:
        """Union of the enumerated and payload-checked writes."""
        return {key for run in self.resolved for key in run.found}


async def _check_instruments(
    repository: Repository, instruments: Mapping[str, str], at: datetime
) -> None:
    """Verify that every approved name is the active Kraken spot instrument."""
    async with repository.session() as session:
        for group in batched(sorted(instruments), _READ_INSTRUMENTS, strict=False):
            result = await session.execute(
                select(Instrument.public_id, Symbol.native_symbol)
                .join(Symbol, Symbol.public_id == Instrument.symbol_public_id)
                .where(
                    Instrument.public_id.in_(group),
                    Instrument.exchange == _EXCHANGE,
                    *where_active(Instrument, at),
                    *where_active(Symbol, at),
                )
            )
            observed = {row.public_id: row.native_symbol for row in result}
            if observed != {key: instruments[key] for key in group}:
                raise RepairRefusedError("approved instrument is not the active Kraken spot symbol")


async def _process_guard(repository: Repository, at: datetime) -> list[JsonObject]:
    """Read current process-run records and reject a running candle writer after the release."""
    async with repository.session() as session:
        rows = (
            (
                await session.execute(
                    select(ProcessRun)
                    .where(*where_active(ProcessRun, at))
                    .order_by(ProcessRun.started_at.desc(), ProcessRun.id.desc())
                )
            )
            .scalars()
            .all()
        )
    publisher = next(
        (row for row in rows if {"market-data", "publisher"} <= set(row.tags or ())), None
    )
    writers: dict[str, ProcessRun] = {}
    for row in rows:
        if row.process_name in _CANDLE_WRITERS:
            writers.setdefault(row.process_name, row)
    if any(row.status == "running" for row in writers.values()) and publisher is None:
        raise RepairRefusedError("running candle writer has no publisher start to compare")
    if publisher is not None and any(
        row.status == "running" and row.started_at > publisher.started_at
        for row in writers.values()
    ):
        raise RepairRefusedError("a registered candle writer is running")
    relevant = list(writers.values())
    if publisher is not None:
        relevant.insert(0, publisher)
    return [
        {
            "process_name": row.process_name,
            "public_id": row.public_id,
            "status": row.status,
            "started_at": row.started_at.isoformat(),
            "tags": None if row.tags is None else list(row.tags),
        }
        for row in relevant
    ]


async def _enumerate_repair(repository: Repository, run: _RepairRun) -> list[CandleVersionRow]:
    """Read the indexed approval scope for one repair session in its own transaction."""
    async with repository.session() as session:
        if repository.dialect_name == "postgresql":
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
            await session.execute(text(_TIMEOUT))
        else:
            await session.execute(text("BEGIN IMMEDIATE"))
        versions: list[CandleVersionRow] = []
        for lower, upper in run.window.days():
            for group in batched(sorted(run.instruments), _READ_INSTRUMENTS, strict=False):
                result = await session.execute(
                    select(Candle.__table__).where(
                        Candle.instrument_public_id.in_(group),
                        Candle.timeframe == "1m",
                        Candle.open_at >= lower,
                        Candle.open_at < upper,
                        Candle.session_id == run.record.run_id,
                    )
                )
                versions.extend(cast(CandleVersionRow, dict(row)) for row in result.mappings())
        return versions


def _business_numbers(values: Sequence[str]) -> tuple[float | int, ...]:
    """Parse the seven repr-formatted fields of a voted candle."""
    try:
        floats = tuple(float(value) for value in values[:-1])
        trades = int(values[-1])
    except ValueError as error:
        raise RepairRefusedError("preview contains invalid candle statistics") from error
    if not all(math.isfinite(value) for value in floats):
        raise RepairRefusedError("preview contains non-finite new candle statistics")
    return (*floats, trades)


def _check_repair_time(
    run: _RepairRun, version: CandleVersionRow, chunk: _RepairChunk, line: candle_repair.PreviewLine
) -> None:
    """Check the repair action's acquisition time and the correction's public ID."""
    authored = version["timestamp"]
    if chunk.action == "insert":
        if (
            not datetime.fromisoformat(chunk.started_at)
            <= authored
            <= datetime.fromisoformat(chunk.ended_at)
        ):
            raise RepairRefusedError("repair insertion was not authored inside its receipt chunk")
        return
    known_at = line.new_known_at if run.approval.source == "snapshot" else run.receipt.write_time
    try:
        expected_time = _valid_instant(known_at)
    except ValueError as error:
        raise RepairRefusedError("repair correction has an invalid preview timestamp") from error
    if (
        authored != datetime.fromisoformat(expected_time)
        or version["public_id"] != line.current_public_id
    ):
        raise RepairRefusedError("repair correction identity or timestamp differs from its preview")


def _check_repair_version(run: _RepairRun, version: CandleVersionRow, key: MinuteKey) -> None:
    """Match an enumerated version to its recorded row and the intended payload."""
    record = run.rows.get(key)
    if record is None or record[2] not in {"applied", "applied_superseded", "uncertain"}:
        raise RepairRefusedError("database has a repair version outside the receipt")
    reference, chunk, _ = record
    if version["sequence_id"] != reference.sequence_id:
        raise RepairRefusedError("repair version has a different sequence id")
    line = run.preview[key]
    voted = _business_numbers(
        (
            line.new_open,
            line.new_high,
            line.new_low,
            line.new_close,
            line.new_volume,
            line.new_vwap,
            line.new_trades,
        )
    )
    stored: Mapping[str, object] = version
    if tuple(stored[field] for field in _VALUES) != voted or (
        version["source"],
        version["complete"],
        version["price_basis"],
    ) != (line.new_source, True, None):
        raise RepairRefusedError("repair version payload differs from its preview")
    _check_repair_time(run, version, chunk, line)


def _version_identity(version: CandleVersionRow) -> JsonObject:
    """Render the physical and logical identity of one observed version."""
    return {
        "id": version["id"],
        "public_id": version["public_id"],
        "timestamp": version["timestamp"].isoformat(),
    }


def _resolve_repair(
    run: _RepairRun, versions: Sequence[CandleVersionRow], settlement_clear: bool
) -> _ResolvedRun:
    """Cross-check each version and resolve absence only with a clear newest capture."""
    found: dict[MinuteKey, CandleVersionRow] = {}
    for version in versions:
        key = (version["instrument_public_id"], version["open_at"])
        if key in found:
            raise RepairRefusedError("database repeats a recorded repair version")
        _check_repair_version(run, version, key)
        found[key] = version
    ready = {
        key
        for key, (_, _, bucket) in run.rows.items()
        if bucket in {"applied", "applied_superseded"}
    }
    if ready - found.keys():
        raise RepairRefusedError("database is missing a receipt's applied repair version")
    uncertain = {key for key, (_, _, bucket) in run.rows.items() if bucket == "uncertain"}
    settled = settlement_clear if uncertain - found.keys() else None
    if settled is False:
        raise RepairRefusedError("absent uncertain repair row lacks settlement evidence")
    return _ResolvedRun(
        run, found, {key: _resolved_row(found.get(key)) for key in uncertain}, settled
    )


def _resolved_row(version: CandleVersionRow | None) -> JsonObject:
    """Freeze an uncertain row as applied, superseded, or proven absent."""
    if version is None:
        return {"outcome": "not_applied", "version": None}
    outcome = "applied" if is_open_ended_version(version["known_to"]) else "applied_superseded"
    return {"outcome": outcome, "version": _version_identity(version)}


async def _derive_scope(context: RunContext, recorded: _RecordedRuns, at: datetime) -> _Scope:
    """Verify catalog and process guard before enumerating each independently recorded write."""
    await _check_instruments(context.repository, recorded.instruments, at)
    guard = await _process_guard(context.repository, at)
    runs = []
    for run in recorded.runs:
        settlement_clear = _settled(run.record)
        versions = await _enumerate_repair(context.repository, run)
        runs.append(_resolve_repair(run, versions, settlement_clear))
    return _Scope(recorded, tuple(runs), guard)


def _day(value: datetime) -> datetime:
    """Return midnight of the instant's UTC day."""
    return value.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0)


def _floor(value: datetime, timeframe: str) -> datetime:
    """Floor an instant to its higher-timeframe grid in UTC."""
    seconds = _SECONDS[timeframe]
    offset = int((value - _day(value)).total_seconds())
    return _day(value) + timedelta(seconds=offset // seconds * seconds)


def _end(key: CandleKey) -> datetime:
    """Return the exclusive upper bound of a window's minute grid."""
    return key[2] + timedelta(seconds=_SECONDS[key[1]])


def _sort_window(key: CandleKey) -> tuple[str, int, datetime]:
    """Sort by instrument, timeframe width and start."""
    return key[0], _SECONDS[key[1]], key[2]


def _count_windows(applied: Collection[MinuteKey]) -> Counter[CandleKey]:
    """Count applied minutes in each of their six containing windows."""
    return Counter(
        (instrument, timeframe, _floor(minute, timeframe))
        for instrument, minute in applied
        for timeframe in TIMEFRAMES
    )


@dataclass(frozen=True, slots=True)
class _ReadUnit:
    """Windows from one UTC day and a bounded instrument list."""

    day: datetime
    instruments: tuple[str, ...]
    keys: tuple[CandleKey, ...]


def _units(keys: Collection[CandleKey]) -> list[_ReadUnit]:
    """Group windows by day, reading at most 20 instruments at a time."""
    days: dict[datetime, dict[str, list[CandleKey]]] = defaultdict(lambda: defaultdict(list))
    for key in sorted(keys, key=_sort_window):
        days[_day(key[2])][key[0]].append(key)
    return [
        _ReadUnit(day, group, tuple(key for instrument in group for key in instruments[instrument]))
        for day, instruments in sorted(days.items())
        for group in batched(sorted(instruments), _READ_INSTRUMENTS, strict=False)
    ]


async def _read_output_versions(
    session: AsyncSession, unit: _ReadUnit
) -> dict[CandleKey, list[CandleVersionRow]]:
    """Read all higher-timeframe versions of a bounded batch without ORM identity collapse."""
    result = await session.execute(
        select(Candle.__table__).where(
            Candle.instrument_public_id.in_(unit.instruments),
            Candle.open_at >= unit.day,
            Candle.open_at < unit.day + _DAY,
            tuple_(Candle.instrument_public_id, Candle.timeframe, Candle.open_at).in_(unit.keys),
        )
    )
    versions: dict[CandleKey, list[CandleVersionRow]] = defaultdict(list)
    for raw in result.mappings():
        row = cast(CandleVersionRow, dict(raw))
        versions[candle_key(row)].append(row)
    return versions


async def _read_input_versions(
    session: AsyncSession, unit: _ReadUnit, at: datetime
) -> dict[str, list[CandleVersionRow]]:
    """Read the day's current minute versions of any source, in opening order."""
    result = await session.execute(
        select(Candle.__table__)
        .where(
            Candle.instrument_public_id.in_(unit.instruments),
            Candle.open_at >= unit.day,
            Candle.open_at < unit.day + _DAY,
            Candle.timeframe == "1m",
            Candle.known_to > at,
        )
        .order_by(Candle.open_at)
    )
    versions: dict[str, list[CandleVersionRow]] = defaultdict(list)
    for raw in result.mappings():
        row = cast(CandleVersionRow, dict(raw))
        if is_open_ended_version(row["known_to"]):
            versions[row["instrument_public_id"]].append(row)
    return versions


@dataclass(frozen=True, slots=True)
class _Window:
    """One classified window with enough evidence to reproduce a preview or a guarded call."""

    key: CandleKey
    symbol: str
    applied: int
    action: _Action
    reason: str = ""
    present: int | None = None
    absent: int | None = None
    fingerprint: str = ""
    current: CandleVersionRow | None = None
    row: CandleUpsertRow | None = None


def _minute_error(row: CandleVersionRow, at: datetime) -> str:
    """Return the first failed input rule for a present minute."""
    if not is_complete_historical_minute(cast(CandleRow, row), at):
        return "invalid_minute"
    return "future_minute" if row["timestamp"] > at else ""


def _fold(window: _Window, inputs: Sequence[CandleVersionRow]) -> CandleUpsertRow:
    """Use the ordinary publisher repair's aggregator and exact minute projection."""
    instrument, timeframe, start = window.key
    aggregator = CandleAggregator([timeframe], live_epoch=start, forward_fill=True)
    for version in inputs:
        aggregator.fold(
            MarketDataPublisherService._candle_update_from_row(
                cast(CandleRow, version), window.symbol
            )
        )
    for emitted_timeframe, candle in aggregator.flush(_end(window.key)):
        if emitted_timeframe == timeframe and candle.interval_begin == start:
            return {
                "instrument_public_id": instrument,
                "open_at": start,
                "timeframe": timeframe,
                "open": candle.open,
                "high": candle.high,
                "low": candle.low,
                "close": candle.close,
                "volume": candle.volume,
                "vwap": candle.vwap,
                "trades": candle.trades,
                "complete": candle.complete,
                "source": "synthesized",
                "price_basis": None,
            }
    raise RepairRefusedError("live repair fold did not emit the requested window")


def _business_equal(left: CandleUpsertRow, right: CandleUpsertRow) -> bool:
    """Use the existing synthesized writer's ten compared fields."""
    first: Mapping[str, object] = left
    second: Mapping[str, object] = right
    return all(first[field] == second[field] for field in _BUSINESS)


def _output_action(window: _Window, versions: Sequence[CandleVersionRow]) -> _Window:
    """Classify ownership, business equality and observation loss after a successful fold."""
    current = window.current
    if current is None:
        return replace(window, action="history_without_current" if versions else "insert")
    if current["source"] != "synthesized":
        return replace(window, action="protected", reason=current["source"])
    row = cast(CandleUpsertRow, window.row)
    if _business_equal(current, row):
        return replace(window, action="unchanged")
    loss = synthesized_observation_loss(current, row)
    if loss is not None:
        return replace(window, action="would_lose_observations", reason=loss)
    return replace(window, action="rebuild")


def _classify(
    window: _Window,
    versions: Sequence[CandleVersionRow],
    inputs: Sequence[CandleVersionRow],
    at: datetime,
) -> _Window:
    """Validate the present grid minutes, then derive and classify an eligible output."""
    current = [version for version in versions if is_open_ended_version(version["known_to"])]
    if len(current) > 1:
        raise RepairRefusedError("window has multiple current higher-timeframe versions")
    errors = [(row, reason) for row in inputs if (reason := _minute_error(row, at))]
    window = replace(
        window,
        current=current[0] if current else None,
        present=len(inputs) - len(errors),
        absent=_SECONDS[window.key[1]] // 60 - len(inputs),
    )
    if errors:
        version, reason = errors[0]
        return replace(
            window, action="invalid_input", reason=f"{reason}@{version['open_at'].isoformat()}"
        )
    if not inputs:
        return replace(window, action="no_input")
    folded = replace(window, fingerprint=synthesis_inputs_sha256(inputs), row=_fold(window, inputs))
    return _output_action(folded, versions)


def _inside_grid(key: CandleKey, versions: Sequence[CandleVersionRow]) -> list[CandleVersionRow]:
    """Select the half-open minute interval of one window."""
    end = _end(key)
    return [row for row in versions if key[2] <= row["open_at"] < end]


async def _plan_unit(
    context: RunContext, unit: _ReadUnit, windows: Mapping[CandleKey, _Window]
) -> dict[CandleKey, _Window]:
    """Read the plan's ordinary READ COMMITTED unit with a bounded timeout."""
    repository = context.repository
    async with repository.session() as session:
        if repository.dialect_name == "postgresql":
            await session.execute(text(_TIMEOUT))
        at = context.clock()
        outputs = await _read_output_versions(session, unit)
        inputs = await _read_input_versions(session, unit, at)
    return {
        key: _classify(
            windows[key], outputs.get(key, ()), _inside_grid(key, inputs.get(key[0], ())), at
        )
        for key in unit.keys
    }


def _initial_windows(scope: _Scope, end: datetime) -> dict[CandleKey, _Window]:
    """Build the whole derived scope and mark windows that exceed the end bound."""
    counts = _count_windows(scope.applied)
    return {
        key: _Window(
            key,
            scope.recorded.instruments[key[0]],
            counts[key],
            "after_end" if _end(key) > end else "no_input",
        )
        for key in sorted(counts, key=_sort_window)
    }


@dataclass(frozen=True, slots=True)
class _RebuildChunk(Chunk):
    """Prepared writer metadata and the exact voted expectations."""

    expected: Mapping[CandleKey, SynthesizedRebuildExpectation]


def _expectation(window: _Window) -> SynthesizedRebuildExpectation:
    """Bind a proposed window to the voted current identity and input fingerprint."""
    current = window.current
    predecessor = (
        None
        if current is None
        else CandlePredecessor(
            id=current["id"], public_id=current["public_id"], timestamp=current["timestamp"]
        )
    )
    return SynthesizedRebuildExpectation(predecessor=predecessor, inputs_sha256=window.fingerprint)


def _write_row(window: _Window, run_id: str, at: datetime, sequence: int) -> CandleUpsertRow:
    """Fix writer provenance once and give each insert its canonical public identity."""
    row = cast(CandleUpsertRow, window.row).copy()
    row.update({"timestamp": at, "session_id": run_id, "sequence_id": sequence})
    if window.action == "insert":
        row["public_id"] = deterministic_candle_public_id(*window.key)
    return row


def _chunks(windows: Sequence[_Window], run_id: str, at: datetime) -> list[_RebuildChunk]:
    """Schedule in plan order within the row and grid ceilings."""
    chunks: list[_RebuildChunk] = []
    rows: list[CandleUpsertRow] = []
    expected: dict[CandleKey, SynthesizedRebuildExpectation] = {}
    minutes = 0
    for sequence, window in enumerate(windows, 1):
        grid = _SECONDS[window.key[1]] // 60
        if len(rows) == CHUNK_ROWS or minutes + grid > CHUNK_MINUTES:
            chunks.append(_RebuildChunk(len(chunks) + 1, "rebuild", tuple(rows), expected))
            rows, expected, minutes = [], {}, 0
        rows.append(_write_row(window, run_id, at, sequence))
        expected[window.key] = _expectation(window)
        minutes += grid
    if rows:
        chunks.append(_RebuildChunk(len(chunks) + 1, "rebuild", tuple(rows), expected))
    return chunks


async def _plan(
    context: RunContext, scope: _Scope, request: RebuildRequest, at: datetime
) -> dict[CandleKey, _Window]:
    """Classify the derived scope and ask the real guard about the proposed writes."""
    windows = _initial_windows(scope, request.end)
    active = [key for key, window in windows.items() if window.action != "after_end"]
    for unit in _units(active):
        windows.update(await _plan_unit(context, unit, windows))
    candidates = [window for window in windows.values() if window.action in _WRITE_ACTIONS]
    for chunk in _chunks(candidates, context.run_id, at):
        result = await context.repository.rebuild_synthesized_candles(
            list(chunk.rows), expected=chunk.expected, dry_run=True
        )
        for key, outcome in result.outcomes.items():
            if outcome.status not in {"insert", "replace"}:
                windows[key] = replace(
                    windows[key], action="refused_by_guard", reason=outcome.status
                )
    return windows


_PREVIEW_COLUMNS = (
    "symbol",
    "instrument_public_id",
    "timeframe",
    "window_start_utc",
    "window_end_utc",
    "action",
    "reason",
    "applied_minutes",
    "input_minutes",
    "absent_minutes",
    "inputs_sha256",
    "current_id",
    "current_public_id",
    "current_source",
    "current_known_at",
    "current_complete",
    "current_price_basis",
    *(f"current_{field}" for field in _VALUES),
    *(f"new_{field}" for field in _VALUES),
    "new_complete",
    "new_price_basis",
)
_ACTIONS = _WRITE_ACTIONS | {
    "unchanged",
    "protected",
    "history_without_current",
    "invalid_input",
    "no_input",
    "after_end",
    "would_lose_observations",
    "refused_by_guard",
}
_EXCLUDED = _ACTIONS - _WRITE_ACTIONS - {"unchanged", "refused_by_guard"}


def _cell(value: object) -> str:
    """Render an optional CSV cell, preserving the exact float representation."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, datetime):
        return value.isoformat()
    return repr(value) if isinstance(value, float) else str(value)


def _window_reference(key: CandleKey, action: str) -> JsonObject:
    """Identify one window consistently in previews and receipts."""
    return {
        "instrument_public_id": key[0],
        "timeframe": key[1],
        "window_start_utc": key[2].isoformat(),
        "action": action,
    }


def _preview_line(window: _Window) -> dict[str, str]:
    """Render the initial and rebuilt values with their separate coverage evidence."""
    current: Mapping[str, object] = window.current or {}
    new: Mapping[str, object] = window.row or {}
    payload = {
        **_window_reference(window.key, window.action),
        "symbol": window.symbol,
        "window_end_utc": _end(window.key),
        "reason": window.reason,
        "applied_minutes": window.applied,
        "input_minutes": window.present,
        "absent_minutes": window.absent,
        "inputs_sha256": window.fingerprint,
        "current_known_at": current.get("timestamp"),
    }
    for name in ("id", "public_id", "source", "complete", "price_basis", *_VALUES):
        payload[f"current_{name}"] = current.get(name)
    for name in (*_VALUES, "complete", "price_basis"):
        payload[f"new_{name}"] = new.get(name)
    return {name: _cell(payload[name]) for name in _PREVIEW_COLUMNS}


def _preview_bytes(windows: Collection[_Window]) -> bytes:
    """Write the fixed schema in natural plan order."""
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=_PREVIEW_COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(_preview_line(window) for window in windows)
    return buffer.getvalue().encode()


def _window_key(instrument: str, timeframe: str, value: str) -> CandleKey:
    """Parse a supported and aligned higher-timeframe key."""
    if timeframe not in _SECONDS:
        raise RepairRefusedError("rebuild evidence contains an unknown timeframe")
    start = _minute(value)
    if start != _floor(start, timeframe):
        raise RepairRefusedError("rebuild evidence contains an unaligned window")
    return instrument, timeframe, start


def _check_preview_values(line: Mapping[str, str]) -> None:
    """Validate cells before using them as a vote or committed payload."""
    for column in (
        "applied_minutes",
        "input_minutes",
        "absent_minutes",
        "current_id",
        "current_trades",
    ):
        if line[column]:
            int(line[column])
    for column in _VALUES[:-1]:
        if line[f"current_{column}"]:
            float(line[f"current_{column}"])
    if line["current_known_at"]:
        _valid_instant(line["current_known_at"])
    if any(
        line[column] not in {"", "true", "false"} for column in ("current_complete", "new_complete")
    ):
        raise RepairRefusedError("rebuild preview contains an invalid complete flag")
    new = tuple(line[f"new_{name}"] for name in _VALUES)
    if line["new_complete"] or any(new):
        _business_numbers(new)
    if line["action"] in _WRITE_ACTIONS:
        _check_write_line(line)


def _check_write_line(line: Mapping[str, str]) -> None:
    """Require a valid rebuilt payload and a full predecessor for a replacement."""
    if line["new_complete"] != "true" or line["new_price_basis"]:
        raise RepairRefusedError("rebuild vote has an invalid synthesized payload")
    if re.fullmatch(r"[0-9a-f]{64}", line["inputs_sha256"]) is None:
        raise RepairRefusedError("rebuild vote has an invalid input fingerprint")
    if line["action"] == "rebuild" and not all(
        line[name] for name in ("current_id", "current_public_id", "current_known_at")
    ):
        raise RepairRefusedError("rebuild vote lacks the output predecessor")


def _parse_preview(data: bytes) -> dict[CandleKey, dict[str, str]]:
    """Load a rebuild preview, validating its exact header and unique window keys."""
    try:
        rows = list(csv.reader(io.StringIO(data.decode(), newline=""), strict=True))
        if not rows or tuple(rows[0]) != _PREVIEW_COLUMNS:
            raise RepairRefusedError("rebuild preview.csv has an unexpected header")
        lines: dict[CandleKey, dict[str, str]] = {}
        for row in rows[1:]:
            line = dict(zip(_PREVIEW_COLUMNS, row, strict=True))
            key = _window_key(
                line["instrument_public_id"], line["timeframe"], line["window_start_utc"]
            )
            if (
                key in lines
                or line["action"] not in _ACTIONS
                or _minute(line["window_end_utc"]) != _end(key)
            ):
                raise RepairRefusedError("rebuild preview repeats or misstates a window")
            _check_preview_values(line)
            lines[key] = line
        return lines
    except (ValueError, UnicodeError, csv.Error) as error:
        raise RepairRefusedError("rebuild preview.csv does not parse") from error


class _FrozenVersion(_Evidence):
    """Identity of one found originally uncertain minute."""

    id: _Positive
    public_id: _Uuid
    timestamp: _Instant


class _FrozenRow(_MinuteRef):
    """Frozen uncertain-row resolution."""

    outcome: Literal["applied", "applied_superseded", "not_applied"]
    version: _FrozenVersion | None


class _FrozenRun(_Evidence):
    """Uncertain rows resolved for one recorded repair."""

    run_id: _RunId
    rows: list[_FrozenRow]


class _RunBinding(_Evidence):
    """Hash and parsed run records, plus the independently resolved uncertain rows."""

    sha256: _Digest
    runs: list[_RunRecord]
    resolutions: list[_FrozenRun]


class _Approval(_Evidence):
    """The exact rebuild approval document."""

    command: Literal["rebuild-synthesized-candles"]
    database: JsonObject
    timeframes: list[str]
    end: _Instant
    run_records: _RunBinding
    instruments: list[_InstrumentEvidence]
    code_revision: dict[str, _Digest]
    preview_sha256: _Digest


@dataclass(frozen=True, slots=True)
class _BoundPreview:
    """A parsed preview and its approved metadata."""

    approval: _Approval
    lines: Mapping[CandleKey, dict[str, str]]


def _approved_preview(approval: bytes, preview: bytes) -> _BoundPreview:
    """Check a preview against its approval hash before parsing its lines."""
    metadata = _parse(_Approval, _json(approval, APPROVAL_FILE), APPROVAL_FILE)
    if sha256_hex(preview) != metadata.preview_sha256.lower():
        raise RepairRefusedError("rebuild preview.csv does not match its approval")
    return _BoundPreview(metadata, _parse_preview(preview))


def _voted_preview(options: WriteOptions) -> _BoundPreview:
    """Verify the vote's digest and preview before starting any database read."""
    approval = _read(options.voted_preview / APPROVAL_FILE, "voted approval.json")
    if sha256_hex(approval) != options.expect_digest:
        raise RepairRefusedError("voted approval.json does not match --expect-digest")
    return _approved_preview(
        approval, _read(options.voted_preview / PREVIEW_FILE, "voted preview.csv")
    )


def _instruments_json(instruments: Mapping[str, str]) -> list[JsonObject]:
    """Render approved instrument names in stable order."""
    return [
        {"instrument_public_id": key, "symbol": name} for key, name in sorted(instruments.items())
    ]


def _check_approval(
    approval: _Approval,
    context: RunContext,
    request: RebuildRequest,
    recorded: _RecordedRuns,
    *,
    verification: bool = False,
) -> None:
    """Bind everything known before the enumeration; verification permits a code change."""
    if canonical_json(approval.database) != canonical_json(
        context.database
    ) or approval.timeframes != list(TIMEFRAMES):
        raise RepairRefusedError("rebuild approval targets another database or timeframe set")
    if approval.end != request.end.isoformat():
        raise RepairRefusedError("rebuild approval --end differs")
    binding = approval.run_records
    if (
        binding.sha256 != recorded.digest
        or [item.model_dump() for item in binding.runs] != recorded.records["runs"]
    ):
        raise RepairRefusedError("rebuild approval binds different run records")
    if [item.model_dump() for item in approval.instruments] != _instruments_json(
        recorded.instruments
    ):
        raise RepairRefusedError("rebuild approval binds different instruments")
    if not verification and approval.code_revision != context.code_revision:
        raise RepairRefusedError("rebuild approval binds another code revision")


def _check_resolution(
    frozen: Sequence[_FrozenRun], scope: _Scope, *, verification: bool = False
) -> None:
    """Check the frozen DB evidence; verification accepts later supersession of the same row."""
    indexed = {run.run_id: run.rows for run in frozen}
    if len(indexed) != len(frozen) or set(indexed) != {
        run.run.record.run_id for run in scope.resolved
    }:
        raise RepairRefusedError("rebuild approval resolves a different set of runs")
    for run in scope.resolved:
        rows = indexed[run.run.record.run_id]
        expected = {
            row.key: {
                "outcome": row.outcome,
                "version": row.version.model_dump() if row.version else None,
            }
            for row in rows
        }
        if len(expected) != len(rows) or set(expected) != run.resolution.keys():
            raise RepairRefusedError("rebuild approval resolves a different set of uncertain rows")
        for key, old in expected.items():
            actual = run.resolution[key]
            if old == actual:
                continue
            if (
                verification
                and old == {"outcome": "applied", "version": actual["version"]}
                and actual["outcome"] == "applied_superseded"
            ):
                continue
            raise RepairRefusedError("rebuild approval has a different uncertain-row resolution")


def _approval(
    context: RunContext, request: RebuildRequest, scope: _Scope, preview_sha256: str
) -> JsonObject:
    """Create the repeatable vote header from the authenticated files and enumeration."""
    return {
        "command": COMMAND,
        "database": context.database,
        "timeframes": list(TIMEFRAMES),
        "end": request.end.isoformat(),
        "run_records": {
            "sha256": scope.recorded.digest,
            "runs": scope.recorded.records["runs"],
            "resolutions": [run.frozen() for run in scope.resolved],
        },
        "instruments": list(_instruments_json(scope.recorded.instruments)),
        "code_revision": context.code_revision,
        "preview_sha256": preview_sha256,
    }


def _group_summary(windows: Collection[_Window]) -> JsonObject:
    """Count actions, reasons and input coverage for a summary group."""
    return {
        "actions": dict(Counter(window.action for window in windows)),
        "reasons": dict(Counter(window.reason for window in windows if window.reason)),
        "input_minutes": sum(window.present or 0 for window in windows),
        "absent_minutes": sum(window.absent or 0 for window in windows),
    }


def _source_summary(scope: _Scope) -> list[JsonObject]:
    """Report resolved receipts and which already-applied lines another run proves."""
    counts = Counter(key for run in scope.resolved for key in run.found)
    summaries: list[JsonObject] = []
    for item in scope.resolved:
        run = item.run
        proven = sum(
            counts[row.key] > int(row.key in item.found) for row in run.receipt.already_applied
        )
        found_applied = sum(is_open_ended_version(row["known_to"]) for row in item.found.values())
        summaries.append(
            {
                **run.record.model_dump(),
                "applied": found_applied,
                "applied_superseded": len(item.found) - found_applied,
                "resolved_uncertain": len(item.resolution),
                "already_applied": proven,
                "already_applied_unproven": len(run.receipt.already_applied) - proven,
                "settlement_clear": item.settlement,
            }
        )
    return summaries


def _summary(windows: Collection[_Window], scope: _Scope) -> JsonObject:
    """Summarize the whole derived window scope and the contributing repair writes."""
    by_timeframe: dict[str, list[_Window]] = defaultdict(list)
    by_day: dict[str, list[_Window]] = defaultdict(list)
    by_symbol_day: dict[tuple[str, str, str], list[_Window]] = defaultdict(list)
    for window in windows:
        _, timeframe, start = window.key
        day = start.date().isoformat()
        by_timeframe[timeframe].append(window)
        by_day[day].append(window)
        by_symbol_day[(window.symbol, timeframe, day)].append(window)
    applied = Counter(
        (scope.recorded.instruments[instrument], minute.date().isoformat())
        for instrument, minute in scope.applied
    )
    latest = max((_end(window.key) for window in windows), default=None)
    return {
        **_group_summary(windows),
        "by_timeframe": {group: _group_summary(rows) for group, rows in by_timeframe.items()},
        "by_day": {group: _group_summary(rows) for group, rows in by_day.items()},
        "by_symbol_timeframe_day": [
            {"symbol": symbol, "timeframe": timeframe, "day": day, **_group_summary(rows)}
            for (symbol, timeframe, day), rows in sorted(by_symbol_day.items())
        ],
        "applied_minutes": [
            {"symbol": symbol, "day": day, "count": count}
            for (symbol, day), count in sorted(applied.items())
        ],
        "runs": list(_source_summary(scope)),
        "process_runs": list(scope.process_runs),
        "latest_window_end": latest.isoformat() if latest is not None else None,
    }


@dataclass(frozen=True, slots=True)
class PlanArtifacts:
    """Identity and summary of the three emitted plan files."""

    digest: str
    summary: JsonObject


def _write_plan(
    context: RunContext,
    request: RebuildRequest,
    scope: _Scope,
    windows: Collection[_Window],
    verification: JsonObject | None = None,
) -> PlanArtifacts:
    """Write a complete preview, approval and summary with owner-only modes."""
    preview = _preview_bytes(windows)
    preview_hash = sha256_hex(preview)
    approval = canonical_json(_approval(context, request, scope, preview_hash))
    digest = sha256_hex(approval)
    summary = {**_summary(windows, scope), "digest": digest, "preview_sha256": preview_hash}
    if verification is not None:
        summary["verification"] = verification
    write_exclusive(request.output_dir / PREVIEW_FILE, preview)
    write_exclusive(request.output_dir / APPROVAL_FILE, approval)
    write_exclusive(
        request.output_dir / SUMMARY_FILE,
        json.dumps(summary, indent=2, sort_keys=True).encode() + b"\n",
    )
    return PlanArtifacts(digest, summary)


@dataclass(frozen=True, slots=True)
class _Selection:
    """The voted subset and each unmatched preview reference."""

    selected: list[_Window]
    not_voted: list[JsonObject]
    already_applied: list[JsonObject]
    changed: list[JsonObject]


def _matches_vote(planned: Mapping[str, str], voted: Mapping[str, str]) -> bool:
    """Compare a write's approved action, fingerprint, values and predecessor."""
    fields = (
        "action",
        "inputs_sha256",
        *(f"new_{name}" for name in (*_VALUES, "complete", "price_basis")),
    )
    if planned["action"] == "rebuild":
        fields += ("current_id", "current_public_id", "current_known_at")
    return all(planned[name] == voted[name] for name in fields)


def _select(
    windows: Mapping[CandleKey, _Window], preview: Mapping[CandleKey, dict[str, str]]
) -> _Selection:
    """Pick only still matching votes and retain the repair command's selection outcomes."""
    votes = {key: line for key, line in preview.items() if line["action"] in _WRITE_ACTIONS}
    selection = _Selection([], [], [], [])
    for key, window in windows.items():
        if window.action not in _WRITE_ACTIONS:
            continue
        voted = votes.get(key)
        if voted is None or not _matches_vote(_preview_line(window), voted):
            selection.not_voted.append(_window_reference(key, window.action))
        else:
            selection.selected.append(window)
            votes.pop(key)
    for key, voted in votes.items():
        window = windows[key]
        ref = _window_reference(key, voted["action"])
        if window.action == "unchanged" and _same_preview_values(_preview_line(window), voted):
            selection.already_applied.append(ref)
        else:
            selection.changed.append(ref)
    return selection


def _same_preview_values(first: Mapping[str, str], second: Mapping[str, str]) -> bool:
    """Compare the rendered payload of an already-equal window with the vote."""
    return all(
        first[f"new_{name}"] == second[f"new_{name}"]
        for name in (*_VALUES, "complete", "price_basis")
    )


def _sample_json(sample: ClientSample | None) -> JsonObject | None:
    """Record the count and time of the actual instantaneous observation."""
    if sample is None:
        return None
    return {
        "other_client_backends": sample.other_client_backends,
        "observed_at": sample.observed_at.isoformat(),
    }


@dataclass(frozen=True, slots=True)
class _RebuildAdapter:
    """Connect rebuild repository results and receipts to the shared writer."""

    context: RunContext
    actions: Mapping[CandleKey, str]

    async def write(self, chunk: _RebuildChunk) -> ChunkAcknowledgement:
        """Look up and call the guarded write once; the chunk stays fixed on retries."""
        result = await self.context.repository.rebuild_synthesized_candles(
            list(chunk.rows), expected=chunk.expected
        )
        applied: list[CandleUpsertRow] = []
        skipped: dict[CandleKey, JsonObject] = {}
        for row in chunk.rows:
            key = candle_key(row)
            outcome = result.outcomes[key]
            if outcome.status in {"inserted", "replaced"}:
                applied.append(row)
            else:
                skipped[key] = {"status": outcome.status, "reason": outcome.reason}
        return ChunkAcknowledgement(applied, skipped, result.sample)

    async def reconcile(self, rows: Sequence[CandleUpsertRow]) -> dict[CandleKey, str]:
        """Reconcile rebuild rows by full natural key and fixed timestamp in a plain read."""
        return await reconcile_versions(
            self.context.repository,
            {candle_key(row): row for row in rows},
            self.context.run_id,
            extra_fields=("timestamp", "price_basis"),
        )

    def row_reference(self, row: CandleUpsertRow) -> JsonObject:
        """Keep each insert or rebuild action on the row even in a mixed chunk."""
        key = candle_key(row)
        return {**_window_reference(key, self.actions[key]), "sequence_id": row["sequence_id"]}

    def receipt_fields(self, ack: ChunkAcknowledgement | None, stop: str | None) -> JsonObject:
        """Report acknowledged samples only, and keep uncertainty visible."""
        sample = ack.sample if ack is not None else None
        sole: JsonValue = "not_observed"
        if self.context.repository.dialect_name == "sqlite":
            sole = "not_applicable"
        elif stop in {"uncertain_acknowledgement", "cancelled"}:
            sole = False
        elif sample is not None:
            sole = sample.other_client_backends == 0
        return {"sole_client": sole, "client_sample": _sample_json(sample)}


def _sole_client_write(
    dialect: str, start: ClientSample | None, records: Sequence[JsonObject]
) -> JsonValue:
    """Aggregate the instantaneous observations into the receipt's run-level state."""
    if dialect == "sqlite":
        return "not_applicable"
    observed = [chunk["sole_client"] for chunk in records if chunk["sole_client"] != "not_observed"]
    if not observed:
        return "no_writes"
    return (
        start is not None
        and start.other_client_backends == 0
        and all(item is True for item in observed)
    )


@dataclass(frozen=True, slots=True)
class _WriteRun:
    """The fixed metadata of one rebuild write and its selected windows."""

    run_id: str
    voted_digest: str
    write_time: datetime
    selection: _Selection
    scheduled: int

    def receipt(
        self,
        stop: str,
        records: Sequence[JsonObject],
        ended: datetime,
        dialect: str,
        start_sample: ClientSample | None,
    ) -> JsonObject:
        """Render the durable write result with its sole-client observations."""
        return {
            "run_id": self.run_id,
            "voted_digest": self.voted_digest,
            "stop_reason": stop,
            "write_time": self.write_time.isoformat(),
            "started_at": self.write_time.isoformat(),
            "ended_at": ended.isoformat(),
            "selected": len(self.selection.selected),
            "scheduled": self.scheduled,
            "reached": reached(records),
            "chunks": list(records),
            "not_voted": list(self.selection.not_voted),
            "already_applied": list(self.selection.already_applied),
            "changed": list(self.selection.changed),
            "totals": totals(records),
            "start_sample": _sample_json(start_sample),
            "sole_client_write": _sole_client_write(dialect, start_sample, records),
        }


async def _apply(
    context: RunContext, request: RebuildRequest, options: WriteOptions, selection: _Selection
) -> tuple[str, JsonObject]:
    """Take the start sample, apply only selected chunks and leave a final receipt."""
    scheduled = selection.selected[: options.max_rows]
    run = _WriteRun(
        context.run_id, options.expect_digest, context.clock(), selection, len(scheduled)
    )
    receipt = ReceiptLog(request.output_dir / RECEIPT_LINES_FILE)
    adapter = _RebuildAdapter(context, {window.key: window.action for window in scheduled})
    writer = ChunkWriter(
        options,
        receipt,
        ChunkAdapter(
            adapter.write, adapter.reconcile, adapter.row_reference, adapter.receipt_fields
        ),
        context.clock,
        context.sleep,
    )
    stop = "error"
    start = None
    try:
        async with context.repository.session() as session:
            start = await sample_other_client_backends(session)
        stop = await writer.apply(_chunks(scheduled, context.run_id, run.write_time))
        if stop == "completed" and len(scheduled) < len(selection.selected):
            stop = "max_rows"
    except asyncio.CancelledError, KeyboardInterrupt:
        stop = "cancelled"
        raise
    finally:
        receipt.close()
        result = run.receipt(
            stop, writer.records, context.clock(), context.repository.dialect_name, start
        )
        write_exclusive(
            request.output_dir / RECEIPT_FILE,
            json.dumps(result, indent=2, sort_keys=True).encode() + b"\n",
        )
    return stop, totals(writer.records)


class _WindowRef(_Evidence):
    """A higher-timeframe reference from a rebuild receipt."""

    instrument_public_id: _Uuid
    timeframe: str
    window_start_utc: _Instant
    action: Literal["insert", "rebuild"]

    @property
    def key(self) -> CandleKey:
        """Decode the exact window natural key."""
        return _window_key(self.instrument_public_id, self.timeframe, self.window_start_utc)


class _WindowWritten(_WindowRef):
    """A scheduled rebuild row."""

    sequence_id: _Positive


class _GuardOutcome(_Evidence):
    """The fenced guard's reason for withholding a scheduled row."""

    status: Literal[
        "predecessor_changed",
        "protected_source",
        "history_without_current",
        "not_newer",
        "unchanged",
        "would_lose_observations",
        "inputs_changed",
    ]
    reason: str | None


class _WindowSkipped(_WindowWritten):
    """A scheduled row the guard withheld."""

    guard_outcome: _GuardOutcome


class _Sample(_Evidence):
    """One observation made immediately before a commit or at write start."""

    other_client_backends: _Nonnegative
    observed_at: _Instant


class _RebuildReceiptChunk(_ChunkTimes):
    """A numbered guarded rebuild chunk, including its sample when acknowledged."""

    action: Literal["rebuild"]
    applied: list[_WindowWritten]
    skipped_by_guard: list[_WindowSkipped]
    applied_superseded: list[_WindowWritten]
    uncertain: list[_WindowWritten]
    not_applied: list[_WindowWritten]
    sole_client: bool | Literal["not_observed", "not_applicable"]
    client_sample: _Sample | None


class _RebuildReceipt(_ReceiptIdentity):
    """Rebuild write evidence parsed by verification."""

    not_voted: list[_WindowRef]
    already_applied: list[_WindowRef]
    changed: list[_WindowRef]
    chunks: list[_RebuildReceiptChunk]
    start_sample: _Sample | None
    sole_client_write: bool | Literal["not_applicable", "no_writes"]


@dataclass(frozen=True, slots=True)
class _VerificationEvidence:
    """A rebuild write authenticated before any verification query."""

    binding: _FileBinding
    record_hash: str
    preview: _BoundPreview
    receipt: _RebuildReceipt
    rows: Mapping[CandleKey, tuple[_WindowWritten, str]]
    selections: Mapping[str, set[CandleKey]]


def _verification_rows(
    receipt: _RebuildReceipt, preview: Mapping[CandleKey, dict[str, str]]
) -> dict[CandleKey, tuple[_WindowWritten, str]]:
    """Validate receipt membership, action, sequence uniqueness and bucket counts."""
    rows: dict[CandleKey, tuple[_WindowWritten, str]] = {}
    sequences: set[int] = set()
    counts: Counter[str] = Counter()
    for number, chunk in enumerate(receipt.chunks, 1):
        _check_chunk_metadata(chunk, number)
        before = len(rows)
        for bucket in _BUCKETS:
            for reference in cast(list[_WindowWritten], getattr(chunk, bucket)):
                key = reference.key
                if key not in preview or preview[key]["action"] != reference.action:
                    raise RepairRefusedError(
                        "write receipt references a missing or different preview action"
                    )
                if key in rows or reference.sequence_id in sequences:
                    raise RepairRefusedError("write receipt repeats a window or sequence")
                rows[key] = reference, bucket
                sequences.add(reference.sequence_id)
                counts[bucket] += 1
        if len(rows) - before != chunk.rows:
            raise RepairRefusedError("write chunk row count does not add up")
    _check_receipt_counts(receipt, receipt.chunks, counts)
    return rows


def _verification_selections(
    receipt: _RebuildReceipt, preview: Mapping[CandleKey, dict[str, str]]
) -> dict[str, set[CandleKey]]:
    """Verify every selection reference belongs to the write scope."""
    selections = {}
    for name in _SELECTIONS:
        references = cast(list[_WindowRef], getattr(receipt, name))
        keys = {ref.key for ref in references}
        if len(keys) != len(references) or not keys <= preview.keys():
            raise RepairRefusedError("write receipt has an invalid selection list")
        selections[name] = keys
    return selections


def _load_verification(files: VerificationFiles) -> _VerificationEvidence:
    """Validate immutable hashes, receipt JSONL, preview and references without using the DB."""
    record = _read(files.record, "verification record")
    binding = _parse(_FileBinding, _json(record, "verification record"), "verification record")
    artifacts = _read_write_files(files.directory)
    receipt = _parse(_RebuildReceipt, _json(artifacts[RECEIPT_FILE], RECEIPT_FILE), RECEIPT_FILE)
    _bind_files(binding, receipt, artifacts)
    _check_lines(_RebuildReceiptChunk, artifacts[RECEIPT_LINES_FILE], receipt.chunks)
    preview = _approved_preview(artifacts[APPROVAL_FILE], artifacts[PREVIEW_FILE])
    rows = _verification_rows(receipt, preview.lines)
    selections = _verification_selections(receipt, preview.lines)
    return _VerificationEvidence(binding, sha256_hex(record), preview, receipt, rows, selections)


def _verify_committed(
    window: _Window, versions: Sequence[CandleVersionRow], evidence: _VerificationEvidence
) -> bool:
    """Distinguish a known write from a missing or invalid immutable payload."""
    receipt = evidence.receipt
    found = [row for row in versions if row["session_id"] == receipt.run_id]
    entry = evidence.rows.get(window.key)
    if not found:
        if entry is not None and entry[1] in {"applied", "applied_superseded"}:
            raise RepairRefusedError("verification is missing a committed write version")
        return False
    if (
        len(found) != 1
        or entry is None
        or entry[1] not in {"applied", "applied_superseded", "uncertain"}
    ):
        raise RepairRefusedError("verification found an unreferenced write version")
    row = found[0]
    if row["sequence_id"] != entry[0].sequence_id or row["timestamp"] != datetime.fromisoformat(
        receipt.write_time
    ):
        raise RepairRefusedError("verification version sequence or timestamp differs")
    expected = evidence.preview.lines[window.key]
    actual: Mapping[str, object] = row
    if tuple(actual[field] for field in _VALUES) != _business_numbers(
        tuple(expected[f"new_{field}"] for field in _VALUES)
    ) or (row["source"], row["complete"], row["price_basis"]) != ("synthesized", True, None):
        raise RepairRefusedError("verification version payload differs from the write preview")
    return True


def _outstanding(
    key: CandleKey,
    entry: tuple[_WindowWritten, str] | None,
    action: str,
    evidence: _VerificationEvidence,
) -> str:
    """Assign the first applicable reason to a window the write did not reach."""
    if entry is not None and entry[1] in {"skipped_by_guard", "not_applied"}:
        return entry[1]
    for label in ("changed", "not_voted"):
        if key in evidence.selections[label]:
            return label
    if action == "refused_by_guard":
        return action
    return "not_reached"


def _verification_class(
    window: _Window, found: bool, evidence: _VerificationEvidence
) -> tuple[str, str]:
    """Place the whole scope into disjoint write-time classes and snapshot results."""
    if found:
        return (
            ("verified", "")
            if window.action == "unchanged"
            else ("regressed", window.reason or window.action)
        )
    entry = evidence.rows.get(window.key)
    if entry is not None and entry[1] == "uncertain":
        return "uncertain", ""
    planned = evidence.preview.lines[window.key]
    action = planned["action"]
    if action == "unchanged":
        return "unchanged_at_write", ""
    if action in _EXCLUDED:
        return "excluded", action
    return "outstanding", _outstanding(window.key, entry, action, evidence)


@dataclass(frozen=True, slots=True)
class _Snapshot:
    """The clocks and all versions observed in one verification transaction."""

    before: datetime
    after: datetime
    outputs: Mapping[CandleKey, list[CandleVersionRow]]
    inputs: Mapping[str, list[CandleVersionRow]]


async def _snapshot(context: RunContext, unit: _ReadUnit) -> _Snapshot:
    """Observe both sides of a window in one repeatable, plain read transaction."""
    repository = context.repository
    async with repository.session() as session:
        if repository.dialect_name == "postgresql":
            await session.execute(
                text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            )
            before, after = (await session.execute(text("SELECT now(), clock_timestamp()"))).one()
            await session.execute(text(_TIMEOUT))
            outputs = await _read_output_versions(session, unit)
            inputs = await _read_input_versions(session, unit, after)
            return _Snapshot(before, after, outputs, inputs)
        before = context.clock()
        await session.execute(text("BEGIN"))
        outputs = await _read_output_versions(session, unit)
        inputs = await _read_input_versions(session, unit, before)
        return _Snapshot(before, context.clock(), outputs, inputs)


def _verify_window(
    initial: _Window, seen: _Snapshot, evidence: _VerificationEvidence, number: int, at: datetime
) -> JsonObject:
    """Check the immutable write then independently classify its snapshot."""
    key = initial.key
    versions = seen.outputs.get(key, [])
    inputs = _inside_grid(key, seen.inputs.get(key[0], []))
    window = _classify(initial, versions, inputs, at)
    found = _verify_committed(window, versions, evidence)
    category, reason = _verification_class(window, found, evidence)
    current = window.current
    identity = (
        None
        if current is None
        else {
            **_version_identity(current),
            "session_id": current["session_id"],
        }
    )
    return {
        **_window_reference(key, window.action),
        "class": category,
        "reason": reason,
        "snapshot_reason": window.reason,
        "current": identity,
        "unit": number,
        "still_written": found
        and current is not None
        and current["session_id"] == evidence.receipt.run_id,
    }


async def _verify(
    context: RunContext, initial: Mapping[CandleKey, _Window], evidence: _VerificationEvidence
) -> JsonObject:
    """Verify every derived window using the verification transactions only."""
    if initial.keys() != evidence.preview.lines.keys():
        raise RepairRefusedError("verification scope differs from the write preview")
    units: list[JsonObject] = []
    results: list[JsonObject] = []
    for number, unit in enumerate(_units(initial.keys()), 1):
        seen = await _snapshot(context, unit)
        at = seen.after if context.repository.dialect_name == "postgresql" else seen.before
        units.append(
            {
                "unit": number,
                "day": unit.day.date().isoformat(),
                "instruments": list(unit.instruments),
                "now": seen.before.isoformat(),
                "clock_timestamp": seen.after.isoformat(),
            }
        )
        results.extend(
            _verify_window(initial[key], seen, evidence, number, at) for key in unit.keys
        )
    counts = Counter(cast(str, row["class"]) for row in results)
    return {
        evidence.binding.run_id: {
            "record": evidence.binding.model_dump(),
            "record_sha256": evidence.record_hash,
            "preview_sha256": evidence.preview.approval.preview_sha256,
            "write_code_revision": dict(evidence.preview.approval.code_revision),
            "verification_code_revision": context.code_revision,
            "units": list(units),
            "windows": list(results),
            "counts": {
                name: counts[name]
                for name in (
                    "verified",
                    "regressed",
                    "uncertain",
                    "outstanding",
                    "unchanged_at_write",
                    "excluded",
                )
            },
        },
    }


async def plan_rebuild(context: RunContext, request: RebuildRequest) -> PlanArtifacts:
    """Read the recorded inputs and write a dry-run preview and optional verification."""
    at = context.clock()
    validate_end(request.end, at)
    recorded = _load_runs(request, context.database)
    evidence = (
        _load_verification(request.verification) if request.verification is not None else None
    )
    if evidence is not None:
        _check_approval(evidence.preview.approval, context, request, recorded, verification=True)
    scope = await _derive_scope(context, recorded, at)
    verification = None
    if evidence is not None:
        _check_resolution(
            evidence.preview.approval.run_records.resolutions, scope, verification=True
        )
    windows = await _plan(context, scope, request, at)
    if evidence is not None:
        verification = await _verify(context, _initial_windows(scope, request.end), evidence)
    return _write_plan(context, request, scope, windows.values(), verification)


async def write_rebuild(
    context: RunContext, request: RebuildRequest, options: WriteOptions
) -> WriteReport:
    """Bind a confirmed write to the voted subset, then execute exactly the surviving rows."""
    if request.verification is not None:
        raise RepairRefusedError("verification options are only available in a dry run")
    voted = _voted_preview(options)
    at = context.clock()
    validate_end(request.end, at)
    recorded = _load_runs(request, context.database)
    _check_approval(voted.approval, context, request, recorded)
    scope = await _derive_scope(context, recorded, at)
    _check_resolution(voted.approval.run_records.resolutions, scope)
    windows = await _plan(context, scope, request, at)
    if not voted.lines.keys() <= windows.keys():
        raise RepairRefusedError("voted preview contains a window outside the derived scope")
    selection = _select(windows, voted.lines)
    artifacts = _write_plan(context, request, scope, windows.values())
    stop, counts = await _apply(context, request, options, selection)
    return WriteReport(options.expect_digest, artifacts.digest, stop, counts)
