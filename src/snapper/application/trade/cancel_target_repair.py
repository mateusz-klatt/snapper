"""Repair the evidence of stale cancel commands (G3-C).

The plan is ``plans/plan_2026_11_15_g3c_cancel_target_repair.md`` in the
private repository (revision 3a). A cancel command whose target order the
venue cancelled, but whose ``order_terminal`` evidence was never recorded,
gets that evidence through ``insert_order_terminal_once``; the reconciler's
fold and G2's retirement pass then advance both commands. A cancel whose
target was refused by the live-trading interlock before any venue call
becomes ``failed`` with ``target_never_submitted``. Nothing else is written:
no order, no fill quantity, no venue call.

Each cancel is evaluated in stages, first match wins: the cancel (A), its
target (B), existing terminal evidence (C), an earlier failure (D), the
cancel's status (E), then a branch on the target's exchange (F) into the
Walutomat checks (G) or the Kraken spot checks (H). The dry run writes a
deterministic preview and summary whose digest the voters approve; a write
recomputes both and refuses unless they match the vote byte for byte.
"""

import json
import math
import sys
from collections.abc import Callable
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from types import ModuleType
from typing import Final
from typing import cast
from uuid import uuid7

from snapper.application.engine import service as engine_service
from snapper.application.engine.service import compute_shard_key
from snapper.application.trade import cancel_target as cancel_target_module
from snapper.application.trade import reconciler as reconciler_module
from snapper.application.trade.command_request import parse_shard_key
from snapper.application.trade.reconciler import _CANCEL_RETIREMENT_SOURCE_STATUSES
from snapper.application.trade.reconciler import _FOLD_SOURCE_STATUSES
from snapper.application.trade.reconciler import cancel_retirement
from snapper.application.trade.reconciler import fold_lifecycle_advance
from snapper.application.updaters.historical.candle_repair import canonical_json
from snapper.application.updaters.historical.candle_repair import sha256_hex
from snapper.core.json_types import JsonObject
from snapper.core.json_types import JsonValue
from snapper.core.types import ExchangeEnum
from snapper.core.types import ExecutionModeEnum
from snapper.core.types import TradeCommandStatusEnum
from snapper.data import cancel_target_repair_reads as reads
from snapper.data import repository as repository_module
from snapper.data import repository_types as repository_types_module
from snapper.data.models import is_open_ended_version
from snapper.data.repository import _ORDER_LIFECYCLE_EVENT_TYPES
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import ExecutionEvidenceRow
from snapper.data.repository_types import OrderVersionRow
from snapper.data.repository_types import TradeCommandRow
from snapper.data.repository_types import TradeProjectionCheckpointRow
from snapper.data.repository_types import VenueEventInsertRow
from snapper.data.repository_types import VenueEventRow
from snapper.messaging.executors import base as executor_base

INTERLOCK_CODE: Final[str] = "order_interlock_blocked"
"""The executor's exact ``last_error`` for an order the live-trading interlock refused."""

NEVER_SUBMITTED: Final[str] = "target_never_submitted"
"""The ``last_error`` a repaired Walutomat cancel carries."""

CANCEL_TERMINAL_STATUSES: Final[tuple[str, ...]] = ("canceled", "cancelled")
"""Terminal statuses that record a cancel, compared lowercased."""

SUBMIT_EVIDENCE_TYPES: Final[tuple[str, ...]] = (
    "order_accepted",
    "fill_observed",
    "order_terminal",
    "order_submit_unknown",
)
"""Venue event types that only a submitted order produces."""

_CANCELED_ORDER_STATUS: Final[str] = "canceled"
_CANCEL_COMMAND_TYPE: Final[str] = "cancel"
_PREDICTED_EVENT_PUBLIC_ID: Final[str] = "planned-order-terminal"


class CancelTargetRepairRefusedError(Exception):
    """A condition under which the whole run refuses; its text names no credential."""


class RepairAction(StrEnum):
    """The one action a cancel line gets."""

    RECORD_TERMINAL = "record_terminal"
    FAIL_CANCEL = "fail_cancel"
    ALREADY_RECORDED = "already_recorded"
    ALREADY_FAILED = "already_failed"
    REFUSE = "refuse"


class ExecutionClass(StrEnum):
    """Binding condition 1's class of one ``executions`` row."""

    ANOMALOUS = "anomalous"
    COUNTED = "counted"
    EMPTY = "empty"


@dataclass(frozen=True, slots=True)
class CancelEvidence:
    """Everything read for one cancel, in one pass.

    Attributes:
        cancel_public_id: Public id the operator named.
        cancel: The cancel's active row, or ``None``.
        target: The target create or submit command, or ``None``.
        target_ambiguous: Whether several active targets match.
        target_versions: Every version of the target command.
        order_versions: Every version of every order with the client order id.
        executions: Every execution of those orders.
        events: Every venue event of the client order id.
        open_commands: The target shard's open commands.
        checkpoint: The target shard's active checkpoint.
        shard_wallets: Every wallet seen on the target shard.
    """

    cancel_public_id: str
    cancel: TradeCommandRow | None
    target: TradeCommandRow | None
    target_ambiguous: bool
    target_versions: list[TradeCommandRow]
    order_versions: list[OrderVersionRow]
    executions: list[ExecutionEvidenceRow]
    events: list[VenueEventRow]
    open_commands: list[TradeCommandRow]
    checkpoint: TradeProjectionCheckpointRow | None
    shard_wallets: list[str]


@dataclass(frozen=True, slots=True)
class KrakenCase:
    """A Kraken target whose order resolved to exactly one ``public_id``.

    Attributes:
        evidence: The evidence read for the cancel.
        cancel: The cancel's active row.
        target: The target command.
        versions: Every version of the resolved order, by ``id``.
    """

    evidence: CancelEvidence
    cancel: TradeCommandRow
    target: TradeCommandRow
    versions: list[OrderVersionRow]

    @property
    def canceled(self) -> OrderVersionRow | None:
        """Return the canceled version of the resolved order.

        Returns:
            The start of the trailing ``canceled`` run, or ``None``.
        """
        return canceled_version(self.versions)


@dataclass(frozen=True, slots=True)
class Decision:
    """The action of one cancel line and the reason when it refuses.

    Attributes:
        action: The line's action.
        reason: The refusal reason, or ``None``.
        kraken: The Kraken case behind a ``record_terminal`` line.
    """

    action: RepairAction
    reason: str | None = None
    kraken: KrakenCase | None = None


@dataclass(frozen=True, slots=True)
class PlanLine:
    """One planned cancel line.

    Attributes:
        evidence: What was read for the cancel.
        decision: The evaluated action.
        preview: The line's preview document.
    """

    evidence: CancelEvidence
    decision: Decision
    preview: JsonObject


@dataclass(frozen=True, slots=True)
class RepairPlan:
    """A complete plan, as voted or as recomputed.

    Attributes:
        lines: One line per requested cancel, in request order.
        preview: The preview document.
        summary: The summary document.
        digest: sha256 of the canonical JSON of both documents.
    """

    lines: list[PlanLine]
    preview: list[JsonValue]
    summary: JsonObject
    digest: str


def _non_blank(value: str | None) -> bool:
    """Return whether a value is set and not blank."""
    return value is not None and bool(value.strip())


def classify_execution(row: ExecutionEvidenceRow) -> ExecutionClass:
    """Classify one execution row under binding condition 1, first match wins.

    Args:
        row: The execution read without joins.

    Returns:
        ``anomalous`` for a non-finite or negative size or fee, or a zero size
        with a fee; ``counted`` for a positive size or an exec or trade id that
        is not NULL, as DEC-16a decided; ``empty`` for a zero size, a zero fee
        and both ids NULL.
    """
    size, fee = row["size"], row["fee"]
    if not (math.isfinite(size) and math.isfinite(fee)) or size < 0 or fee < 0:
        return ExecutionClass.ANOMALOUS
    if not size and fee:
        return ExecutionClass.ANOMALOUS
    if size or row["exec_id"] is not None or row["trade_id"] is not None:
        return ExecutionClass.COUNTED
    return ExecutionClass.EMPTY


def canceled_version(versions: Sequence[OrderVersionRow]) -> OrderVersionRow | None:
    """Return the first version of the run of ``canceled`` versions that ends the history.

    Args:
        versions: Every version of one order, ordered by ``id``.

    Returns:
        The earliest version of the trailing contiguous ``canceled`` run, or
        ``None`` when the last version is not ``canceled``.
    """
    first: OrderVersionRow | None = None
    for version in reversed(versions):
        if version["status"] != _CANCELED_ORDER_STATUS:
            break
        first = version
    return first


def _refuse(reason: str) -> Decision:
    """Build a refusal decision."""
    return Decision(RepairAction.REFUSE, reason)


def _existing_terminal(events: Sequence[VenueEventRow]) -> Decision | None:
    """Stage C: decide from an existing ``order_terminal``, non-cancel statuses first."""
    terminals = [event for event in events if event["event_type"] == "order_terminal"]
    statuses = [(event["status"] or "").lower() for event in terminals]
    for status in statuses:
        if status not in CANCEL_TERMINAL_STATUSES:
            return _refuse(f"terminal_exists_{status or 'none'}")
    if statuses:
        return Decision(RepairAction.ALREADY_RECORDED)
    return None


def _walutomat_decision(evidence: CancelEvidence, target: TradeCommandRow) -> Decision:
    """Stage G: the local no-submit proof of a Walutomat target."""
    if target["status"] != TradeCommandStatusEnum.FAILED or target["last_error"] != INTERLOCK_CODE:
        return _refuse("target_not_interlock_blocked")
    if any(version["exchange_order_id"] is not None for version in evidence.order_versions):
        return _refuse("target_has_exchange_order_id")
    if any(event["event_type"] in SUBMIT_EVIDENCE_TYPES for event in evidence.events):
        return _refuse("target_has_submit_evidence")
    return Decision(RepairAction.FAIL_CANCEL)


def _resolve_kraken_order(
    evidence: CancelEvidence, cancel: TradeCommandRow, target: TradeCommandRow
) -> KrakenCase | Decision:
    """Resolve the target's one order from the versions with its wallet and mode."""
    public_ids = {
        version["public_id"]
        for version in evidence.order_versions
        if version["wallet_public_id"] == target["wallet_public_id"]
        and version["mode"] == target["mode"]
    }
    if not public_ids:
        return _refuse("order_missing")
    if len(public_ids) > 1:
        return _refuse("order_ambiguous")
    public_id = next(iter(public_ids))
    versions = [version for version in evidence.order_versions if version["public_id"] == public_id]
    return KrakenCase(evidence, cancel, target, versions)


def _check_order_history(case: KrakenCase) -> str | None:
    """Check the order is spot, its history is consistent and it ended ``canceled``."""
    if any(version["leverage"] is not None for version in case.versions):
        return "not_kraken_spot"
    active = [version for version in case.versions if is_open_ended_version(version["known_to"])]
    if len(active) != 1 or active[0] is not case.versions[-1]:
        return "order_versions_inconsistent"
    if active[0]["status"] != _CANCELED_ORDER_STATUS:
        return "order_not_canceled"
    canceled = case.canceled
    if canceled is None or canceled["timestamp"] <= case.cancel["created_at"]:
        return "canceled_not_after_cancel"
    return None


def _check_executions(case: KrakenCase) -> str | None:
    """Apply binding condition 1 to the order's executions."""
    classes = {classify_execution(row) for row in case.evidence.executions}
    if ExecutionClass.ANOMALOUS in classes:
        return "anomalous_execution"
    if ExecutionClass.COUNTED in classes:
        return "has_execution"
    return None


def _check_lineage(case: KrakenCase) -> str | None:
    """Check the wallet, the exchange order id and the shard key reproduce exactly."""
    target = case.target
    wallet = target["wallet_public_id"]
    canceled = case.canceled
    parsed = parse_shard_key(target["shard_key"])
    if (
        not wallet
        or canceled is None
        or not _non_blank(canceled["exchange_order_id"])
        or not parsed
    ):
        return "lineage_mismatch"
    recomputed = compute_shard_key(
        instrument=target["instrument"],
        exchange=ExchangeEnum.KRAKEN,
        mode=ExecutionModeEnum.LIVE,
        wallet_public_id=wallet,
        strategy_tag=parsed[4],
    )
    if recomputed != target["shard_key"]:
        return "lineage_mismatch"
    if any(
        version["wallet_public_id"] != wallet or version["mode"] != target["mode"]
        for version in case.versions
    ):
        return "lineage_mismatch"
    return None


def checkpoint_open_command_ids(checkpoint: TradeProjectionCheckpointRow | None) -> list[str]:
    """Return the open command ids that a shard checkpoint holds.

    Args:
        checkpoint: The shard's active checkpoint, or ``None``.

    Returns:
        The public ids stored as JSON in ``open_command_ids``.
    """
    if checkpoint is None:
        return []
    raw = cast(list[object], json.loads(checkpoint["open_command_ids"] or "[]"))
    return [str(item) for item in raw]


def _check_shard(case: KrakenCase) -> str | None:
    """Check the shard holds no other open command and names no other wallet."""
    target_id = case.target["public_id"]
    if any(command["public_id"] != target_id for command in case.evidence.open_commands):
        return "shard_has_other_open_command"
    if any(item != target_id for item in checkpoint_open_command_ids(case.evidence.checkpoint)):
        return "shard_checkpoint_open_command"
    if any(wallet != case.target["wallet_public_id"] for wallet in case.evidence.shard_wallets):
        return "shard_wallet_mismatch"
    return None


_KRAKEN_CHECKS: Final[tuple[Callable[[KrakenCase], str | None], ...]] = (
    _check_order_history,
    _check_executions,
    _check_lineage,
    _check_shard,
)
"""Stage H's checks after the order is resolved, in plan order."""


def _kraken_decision(
    evidence: CancelEvidence, cancel: TradeCommandRow, target: TradeCommandRow
) -> Decision:
    """Stage H: the record-terminal checks of a Kraken spot target."""
    if target["mode"] != ExecutionModeEnum.LIVE or target["leverage"] is not None:
        return _refuse("not_kraken_spot")
    if target["status"] not in _FOLD_SOURCE_STATUSES:
        return _refuse("target_not_foldable")
    resolved = _resolve_kraken_order(evidence, cancel, target)
    if isinstance(resolved, Decision):
        return resolved
    for check in _KRAKEN_CHECKS:
        reason = check(resolved)
        if reason is not None:
            return _refuse(reason)
    return Decision(RepairAction.RECORD_TERMINAL, kraken=resolved)


def _identity_refusal(evidence: CancelEvidence) -> Decision | None:
    """Stages A and B: refuse a missing or foreign cancel and a missing or ambiguous target."""
    cancel = evidence.cancel
    if cancel is None:
        return _refuse("cancel_missing")
    if cancel["command_type"] != _CANCEL_COMMAND_TYPE:
        return _refuse("not_a_cancel")
    if evidence.target_ambiguous:
        return _refuse("target_ambiguous")
    if evidence.target is None:
        return _refuse("target_missing")
    return None


def _cancel_state_decision(cancel: TradeCommandRow) -> Decision | None:
    """Stages D and E: an earlier failure, or a cancel G2 would not retire."""
    if (
        cancel["status"] == TradeCommandStatusEnum.FAILED
        and cancel["last_error"] == NEVER_SUBMITTED
    ):
        return Decision(RepairAction.ALREADY_FAILED)
    if cancel["status"] not in _CANCEL_RETIREMENT_SOURCE_STATUSES:
        return _refuse("cancel_not_retirable")
    return None


def evaluate(evidence: CancelEvidence) -> Decision:
    """Decide one cancel's action, stages A to H, first match wins.

    Args:
        evidence: Everything read for the cancel.

    Returns:
        The line's decision.
    """
    early = _identity_refusal(evidence) or _existing_terminal(evidence.events)
    if early is not None:
        return early
    cancel = cast(TradeCommandRow, evidence.cancel)
    target = cast(TradeCommandRow, evidence.target)
    state = _cancel_state_decision(cancel)
    if state is not None:
        return state
    if target["exchange"] == ExchangeEnum.WALUTOMAT:
        return _walutomat_decision(evidence, target)
    if target["exchange"] == ExchangeEnum.KRAKEN:
        return _kraken_decision(evidence, cancel, target)
    return _refuse("unsupported_exchange")


async def gather_evidence(
    repository: SQLAlchemyRepository, cancel_public_id: str
) -> CancelEvidence:
    """Read everything the evaluation of one cancel needs.

    Args:
        repository: Repository to read from.
        cancel_public_id: Public id of the cancel command.

    Returns:
        The evidence; fields that need a missing cancel or target are empty.

    Raises:
        CancelTargetRepairRefusedError: When the cancel or its shard's
            checkpoint has more than one active version.
    """
    try:
        cancel = await reads.get_active_trade_command(repository, cancel_public_id)
    except RuntimeError as error:
        raise CancelTargetRepairRefusedError(
            f"cancel {cancel_public_id} has several active versions"
        ) from error
    if cancel is None:
        return CancelEvidence(cancel_public_id, None, None, False, [], [], [], [], [], None, [])
    client_order_id = cancel["client_order_id"]
    target: TradeCommandRow | None = None
    ambiguous = False
    try:
        target = await repository.get_active_create_command_by_client_order_id(
            client_order_id, cancel["exchange"]
        )
    except RuntimeError:
        ambiguous = True
    events = await reads.get_venue_events_for_client_order_id(repository, client_order_id)
    versions = await reads.get_order_versions_by_client_order_id(repository, client_order_id)
    public_ids = sorted({version["public_id"] for version in versions})
    executions = await reads.get_executions_by_order_public_ids(repository, public_ids)
    if target is None:
        return CancelEvidence(
            cancel_public_id,
            cancel,
            None,
            ambiguous,
            [],
            versions,
            executions,
            events,
            [],
            None,
            [],
        )
    shard_key = target["shard_key"]
    return CancelEvidence(
        cancel_public_id=cancel_public_id,
        cancel=cancel,
        target=target,
        target_ambiguous=ambiguous,
        target_versions=await reads.get_trade_command_versions(repository, target["public_id"]),
        order_versions=versions,
        executions=executions,
        events=events,
        open_commands=await reads.get_open_commands_on_shard(repository, shard_key),
        checkpoint=await _active_checkpoint(repository, shard_key),
        shard_wallets=await reads.get_shard_wallets(repository, shard_key),
    )


async def _active_checkpoint(
    repository: SQLAlchemyRepository, shard_key: str
) -> TradeProjectionCheckpointRow | None:
    """Read a shard's active checkpoint, refusing the run when it is not unique."""
    try:
        return await reads.get_active_checkpoint(repository, shard_key)
    except RuntimeError as error:
        raise CancelTargetRepairRefusedError(
            f"shard {shard_key} has several active checkpoints"
        ) from error


def terminal_fields(case: KrakenCase) -> VenueEventInsertRow:
    """Build the deterministic fields of the terminal to record, as the executor would.

    The run adds ``timestamp``, ``session_id`` and ``sequence_id`` when it
    writes; they are left out here so the preview stays deterministic.

    Args:
        case: A Kraken case that passed every stage H check.

    Returns:
        The row without its write-time fields.
    """
    target = case.target
    canceled = cast(OrderVersionRow, case.canceled)
    parsed = cast(tuple[str, str, str, str, str | None], parse_shard_key(target["shard_key"]))
    return {
        "event_type": "order_terminal",
        "shard_key": compute_shard_key(
            instrument=target["instrument"],
            exchange=ExchangeEnum.KRAKEN,
            mode=ExecutionModeEnum.LIVE,
            wallet_public_id=cast(str, target["wallet_public_id"]),
            strategy_tag=parsed[4],
        ),
        "wallet_public_id": cast(str, target["wallet_public_id"]),
        "exchange": ExchangeEnum.KRAKEN.value,
        "instrument": target["instrument"],
        "mode": ExecutionModeEnum.LIVE.value,
        "received_at": canceled["timestamp"],
        "exchange_order_id": canceled["exchange_order_id"],
        "client_order_id": case.cancel["client_order_id"],
        "side": None,
        "status": _CANCELED_ORDER_STATUS,
        "fill_price": None,
        "fill_size": None,
        "cum_fill_size": None,
        "fee": None,
        "fee_asset": None,
        "exec_id": None,
        "trade_id": None,
        "error": None,
        "venue_timestamp": None,
        "payload_json": None,
        "liquidity_role": "unknown",
    }


def _planned_event(case: KrakenCase, fields: VenueEventInsertRow) -> VenueEventRow:
    """Shape the planned terminal as the fold reads it, with an id above every existing one."""
    next_id = max((event["id"] for event in case.evidence.events), default=0) + 1
    return {
        "id": next_id,
        "public_id": _PREDICTED_EVENT_PUBLIC_ID,
        "timestamp": fields["received_at"],
        "session_id": _PREDICTED_EVENT_PUBLIC_ID,
        "sequence_id": 0,
        "event_type": fields["event_type"],
        "shard_key": fields["shard_key"],
        "wallet_public_id": fields["wallet_public_id"],
        "command_public_id": None,
        "exchange": fields["exchange"],
        "instrument": fields["instrument"],
        "mode": fields["mode"],
        "exchange_order_id": fields["exchange_order_id"],
        "client_order_id": fields["client_order_id"],
        "venue_client_id": None,
        "side": None,
        "status": fields["status"],
        "fill_price": None,
        "fill_size": None,
        "cum_fill_size": None,
        "fee": None,
        "fee_asset": None,
        "exec_id": None,
        "trade_id": None,
        "error": None,
        "venue_timestamp": None,
        "received_at": fields["received_at"],
    }


def predict(case: KrakenCase) -> JsonObject:
    """Predict the fold's and G2's advances with the reconciler's own functions.

    Args:
        case: A Kraken case that passed every stage H check.

    Returns:
        The target's and the cancel's predicted transitions.
    """
    fields = terminal_fields(case)
    lifecycle = [
        event
        for event in case.evidence.events
        if event["event_type"] in _ORDER_LIFECYCLE_EVENT_TYPES
    ]
    advance = fold_lifecycle_advance(case.target, [*lifecycle, _planned_event(case, fields)])
    target_status = str(advance.status) if advance is not None else case.target["status"]
    cancel_status, last_error = cancel_retirement(
        target_status, fields["received_at"], case.cancel["created_at"]
    )
    return {
        "target": {"from": case.target["status"], "to": target_status},
        "cancel": {
            "from": case.cancel["status"],
            "to": str(cancel_status),
            "last_error": last_error,
        },
    }


def _iso(value: datetime | None) -> str | None:
    """Render a time as ISO 8601, or ``None``."""
    return None if value is None else value.isoformat()


def _execution_document(row: ExecutionEvidenceRow) -> JsonObject:
    """Render one execution row with its condition-1 class."""
    return {
        "id": row["id"],
        "class": classify_execution(row).value,
        "status": row["status"],
        "size": row["size"],
        "fee": row["fee"],
        "price": row["price"],
        "exec_id": row["exec_id"],
        "trade_id": row["trade_id"],
        "numeric_provenance": row["numeric_provenance"],
        "price_decimal": row["price_decimal"],
        "size_decimal": row["size_decimal"],
        "fee_decimal": row["fee_decimal"],
        "timestamp": _iso(row["timestamp"]),
    }


def _command_document(command: TradeCommandRow | None) -> JsonObject | None:
    """Render a command's identity and state, or ``None``."""
    if command is None:
        return None
    return {
        "public_id": command["public_id"],
        "command_type": command["command_type"],
        "exchange": command["exchange"],
        "status": command["status"],
        "last_error": command["last_error"],
        "created_at": _iso(command["created_at"]),
        "shard_key": command["shard_key"],
    }


def _shard_document(evidence: CancelEvidence) -> JsonObject:
    """Render the target shard's open commands, checkpoint and wallets."""
    checkpoint = evidence.checkpoint
    return {
        "open_commands": [command["public_id"] for command in evidence.open_commands],
        "checkpoint_open_command_ids": list(checkpoint_open_command_ids(checkpoint)),
        "checkpoint_last_venue_event_id": (
            None if checkpoint is None else checkpoint["last_venue_event_id"]
        ),
        "wallets": list(evidence.shard_wallets),
    }


def _kraken_document(case: KrakenCase) -> JsonObject:
    """Render a ``record_terminal`` line's planned row, gap and prediction."""
    canceled = cast(OrderVersionRow, case.canceled)
    fields = terminal_fields(case)
    planned: JsonObject = {
        key: (_iso(value) if isinstance(value, datetime) else value)
        for key, value in fields.items()
        if isinstance(value, str | int | float | datetime) or value is None
    }
    gap = (canceled["timestamp"] - case.cancel["created_at"]).total_seconds()
    return {
        "canceled_version_id": canceled["id"],
        "gap_seconds": gap,
        "planned_terminal": planned,
        "predicted": predict(case),
    }


def line_document(evidence: CancelEvidence, decision: Decision) -> JsonObject:
    """Render one cancel line of the preview.

    Args:
        evidence: What was read for the cancel.
        decision: The evaluated action.

    Returns:
        The line's deterministic preview document.
    """
    document: JsonObject = {
        "cancel_public_id": evidence.cancel_public_id,
        "action": decision.action.value,
        "reason": decision.reason,
        "cancel": _command_document(evidence.cancel),
        "target": _command_document(evidence.target),
        "order_versions": [
            {
                "id": version["id"],
                "public_id": version["public_id"],
                "status": version["status"],
                "exchange_order_id": version["exchange_order_id"],
                "timestamp": _iso(version["timestamp"]),
            }
            for version in evidence.order_versions
        ],
        "executions": [_execution_document(row) for row in evidence.executions],
        "venue_events": [
            {"id": event["id"], "event_type": event["event_type"], "status": event["status"]}
            for event in evidence.events
        ],
        "shard": _shard_document(evidence),
    }
    if decision.kraken is not None:
        document["record_terminal"] = _kraken_document(decision.kraken)
    return document


def code_revision(extra_modules: Sequence[ModuleType]) -> JsonObject:
    """Hash the source of every module that decides what the run writes.

    Args:
        extra_modules: Further imported modules to hash, such as the CLI.

    Returns:
        sha256 of each module's file, keyed by its dotted path as a file name.
    """
    modules = (
        sys.modules[__name__],
        reads,
        repository_module,
        repository_types_module,
        reconciler_module,
        cancel_target_module,
        engine_service,
        executor_base,
        *extra_modules,
    )
    return {
        f"{module.__name__.replace('.', '/')}.py": sha256_hex(
            Path(cast(str, module.__file__)).read_bytes()
        )
        for module in modules
    }


def _summary(lines: Sequence[PlanLine], database: JsonObject, code: JsonObject) -> JsonObject:
    """Count the actions and bind the database and the code."""
    counts: dict[str, int] = {}
    for line in lines:
        key = line.decision.action.value
        counts[key] = counts.get(key, 0) + 1
    action_counts: JsonObject = dict(sorted(counts.items()))
    return {"counts": action_counts, "database": database, "code_revision": code}


def refuse_duplicates(cancel_public_ids: Sequence[str]) -> None:
    """Refuse an empty or repeated cancel list.

    Args:
        cancel_public_ids: The cancels the operator named.

    Raises:
        CancelTargetRepairRefusedError: When the list is empty or repeats an id.
    """
    if not cancel_public_ids:
        raise CancelTargetRepairRefusedError("no --cancel was given")
    if len(set(cancel_public_ids)) != len(cancel_public_ids):
        raise CancelTargetRepairRefusedError("a --cancel was given twice")


async def plan_repair(
    repository: SQLAlchemyRepository,
    cancel_public_ids: Sequence[str],
    database: JsonObject,
    code: JsonObject,
) -> RepairPlan:
    """Read, evaluate and render every requested cancel.

    Args:
        repository: Repository to read from.
        cancel_public_ids: The cancels, in the order the operator named them.
        database: The credential-free identity of the target database.
        code: The run's code revision.

    Returns:
        The plan with its preview, summary and digest.
    """
    refuse_duplicates(cancel_public_ids)
    lines: list[PlanLine] = []
    for cancel_public_id in cancel_public_ids:
        evidence = await gather_evidence(repository, cancel_public_id)
        decision = evaluate(evidence)
        lines.append(PlanLine(evidence, decision, line_document(evidence, decision)))
    preview: list[JsonValue] = [line.preview for line in lines]
    summary = _summary(lines, database, code)
    digest = sha256_hex(canonical_json({"preview": preview, "summary": summary}))
    return RepairPlan(lines, preview, summary, digest)


def voted_digest(voted_dir: Path) -> str:
    """Return the digest of a voted run's preview and summary as stored.

    Args:
        voted_dir: Output directory of the voted dry run.

    Returns:
        sha256 of the canonical JSON of both stored documents.

    Raises:
        CancelTargetRepairRefusedError: When either file is missing or unreadable.
    """
    try:
        preview = cast(JsonValue, json.loads((voted_dir / "preview.json").read_text()))
        summary = cast(JsonValue, json.loads((voted_dir / "summary.json").read_text()))
    except (OSError, ValueError) as error:
        raise CancelTargetRepairRefusedError(
            f"the voted preview cannot be read: {type(error).__name__}"
        ) from error
    return sha256_hex(canonical_json({"preview": preview, "summary": summary}))


@dataclass(slots=True)
class WriteSession:
    """One write run: its plan, session, clock, sequence and results so far.

    Attributes:
        plan: The recomputed plan, already matched to the vote.
        session_id: The run's own session id, on every row it writes.
        write_time: The run's single write time.
        results: One result per applied line, appended as the run goes.
        sequence: The last sequence number the run used.
    """

    plan: RepairPlan
    session_id: str
    write_time: datetime
    results: list[JsonValue]
    sequence: int = 0

    def next_sequence(self, entry: JsonObject) -> int:
        """Allocate the run's next sequence number and stamp it on a line's entry.

        Args:
            entry: The line's result entry, which keeps the number even when
                the write that uses it fails.

        Returns:
            The run's next sequence number, counting from 1.
        """
        self.sequence += 1
        entry["sequence_id"] = self.sequence
        return self.sequence


async def _recheck(
    repository: SQLAlchemyRepository, line: PlanLine, action: RepairAction
) -> Decision | None:
    """Re-read one line and return its fresh decision when nothing has changed."""
    fresh = await gather_evidence(repository, line.evidence.cancel_public_id)
    decision = evaluate(fresh)
    if decision.action is not action or line_document(fresh, decision) != line.preview:
        return None
    return decision


async def _record_terminal(
    repository: SQLAlchemyRepository, line: PlanLine, run: WriteSession, entry: JsonObject
) -> JsonObject:
    """Recheck one Kraken line, record its terminal once and read the row back.

    An insert that reports success but whose row the run cannot read back by
    client order id, event type and session id is reported as
    ``recorded_unconfirmed``, which the command treats as a failure.
    """
    recheck = await _recheck(repository, line, RepairAction.RECORD_TERMINAL)
    if recheck is None:
        return {"result": "changed_before_write"}
    row = terminal_fields(cast(KrakenCase, recheck.kraken))
    row["timestamp"] = run.write_time
    row["session_id"] = run.session_id
    row["sequence_id"] = run.next_sequence(entry)
    if not await repository.insert_order_terminal_once(row):
        return {"result": "already_recorded"}
    written = await reads.get_terminal_written_by_session(
        repository, cast(str, row["client_order_id"]), run.session_id
    )
    if written is None:
        return {"result": "recorded_unconfirmed"}
    return {
        "result": "recorded",
        "venue_event_id": written["id"],
        "venue_event_public_id": written["public_id"],
    }


def _first_failed_time(versions: Sequence[TradeCommandRow]) -> datetime | None:
    """Return the timestamp of a command's first ``failed`` version."""
    for version in versions:
        if version["status"] == TradeCommandStatusEnum.FAILED:
            return version["timestamp"]
    return None


async def _fail_cancel(
    repository: SQLAlchemyRepository, line: PlanLine, run: WriteSession, entry: JsonObject
) -> JsonObject:
    """Recheck one Walutomat line and move its cancel to ``failed``."""
    if await _recheck(repository, line, RepairAction.FAIL_CANCEL) is None:
        return {"result": "changed_before_write"}
    cancel = cast(TradeCommandRow, line.evidence.cancel)
    target = cast(TradeCommandRow, line.evidence.target)
    terminal_at = target["terminal_at"] or _first_failed_time(line.evidence.target_versions)
    sequence_id = run.next_sequence(entry)
    applied = await repository.advance_trade_command_lifecycle(
        public_id=cancel["public_id"],
        expected_status=cancel["status"],
        new_status=TradeCommandStatusEnum.FAILED,
        bus_time=run.write_time,
        session_id=run.session_id,
        sequence_id=sequence_id,
        terminal_at=terminal_at,
        last_error=NEVER_SUBMITTED,
    )
    return {"result": "failed" if applied else "lost_compare_and_set"}


async def prepare_write(
    repository: SQLAlchemyRepository,
    cancel_public_ids: Sequence[str],
    voted: tuple[Path, str],
    binding: tuple[JsonObject, JsonObject],
    write_time: datetime,
) -> WriteSession:
    """Recompute the plan, match it to the vote and open the write's session.

    Args:
        repository: Repository to read from.
        cancel_public_ids: The cancels the vote covered.
        voted: The voted run's directory and the expected digest.
        binding: The database identity and the code revision.
        write_time: The run's single write time.

    Returns:
        The write session, with no line applied yet.

    Raises:
        CancelTargetRepairRefusedError: When the stored vote and the expected
            digest disagree, the recomputed plan differs from the vote, or the
            plan holds a refused line.
    """
    voted_dir, expect_digest = voted
    if voted_digest(voted_dir) != expect_digest:
        raise CancelTargetRepairRefusedError("the voted preview does not match --expect-digest")
    database, code = binding
    plan = await plan_repair(repository, cancel_public_ids, database, code)
    if plan.digest != expect_digest:
        raise CancelTargetRepairRefusedError(
            "the recomputed plan differs from the vote; rerun the dry run and vote again"
        )
    if any(line.decision.action is RepairAction.REFUSE for line in plan.lines):
        raise CancelTargetRepairRefusedError("the plan holds a refused line; drop it and re-vote")
    return WriteSession(plan=plan, session_id=str(uuid7()), write_time=write_time, results=[])


async def apply_write(repository: SQLAlchemyRepository, session: WriteSession) -> None:
    """Apply every line of a prepared write in order, recording each result as it lands.

    A line enters the results as ``in_flight`` before anything is written for
    it, and carries its sequence id as soon as the run allocates one. A
    failure after a commit therefore still leaves the line in the receipt;
    the rows carrying the session id are the authority for such a line.

    Args:
        repository: Repository to write through.
        session: The prepared write; its results grow line by line.
    """
    for line in session.plan.lines:
        entry: JsonObject = {
            "cancel_public_id": line.evidence.cancel_public_id,
            "result": "in_flight",
        }
        session.results.append(entry)
        outcome: JsonObject = {"result": "nothing_to_write"}
        if line.decision.action is RepairAction.RECORD_TERMINAL:
            outcome = await _record_terminal(repository, line, session, entry)
        elif line.decision.action is RepairAction.FAIL_CANCEL:
            outcome = await _fail_cancel(repository, line, session, entry)
        entry.update(outcome)


def receipt_document(session: WriteSession) -> JsonObject:
    """Render a write's receipt from what it applied so far.

    Args:
        session: The write session, complete or interrupted.

    Returns:
        The digest, the session id, the write time and every line result.
    """
    return {
        "digest": session.plan.digest,
        "session_id": session.session_id,
        "write_time": session.write_time.isoformat(),
        "results": list(session.results),
    }
