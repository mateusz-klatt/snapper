"""``snapper annulment`` — the operator surface for the execution-annulment manifest.

Four commands, in the order an operator uses them.

``inspect`` is read-only and is run FIRST. It answers, for one certification
scope, what actually blocks the opening prefix proof: the executions that no
durable ``fill_observed`` witness supports, each with its immutable id, scope
coordinates, economics, and — decisively — the canonical row digest the guarded
writer will recompute. Publishing that digest here is what lets an operator
copy a value the database just produced rather than derive one by hand, and the
writer refuses any mismatch, so the copy is a deliberate act rather than a
formality. ``inspect`` also prints the scope's current manifest and any
correction still missing its durability observation.

``annul`` records exactly ONE repudiation through
``Repository.record_execution_annulment``. It takes a JSON request document
rather than a fistful of flags on purpose: the request IS the audit artifact —
it carries the asserted scope, the copied digest, the typed reason, the asserted
acting user, and the diagnosis evidence in one reviewable, diffable file that can
be committed beside the runbook. There is deliberately NO bulk mode: a mode that
annulled everything unwitnessed would repudiate real money the moment a witness
was merely late, and this command cannot express it.

``complete-visibility`` closes the knowledge protocol's second half. A
correction whose visibility observation never landed is durable and honoured by
current truth, yet folded by NO historical horizon; it is invisible unless
something looks for it. This command discovers those corrections and completes
them idempotently, then reports what it completed and — from a count query that
does not share the discovery page's bound — how many remain in the whole scope.

``retire-derived`` performs the derived-plane half of the correction. A
repudiated execution leaves the projections built from it wrong, and the
adopted design's answer is to SCD2-retire the affected
``trade_projection_checkpoints`` and ``positions`` versions at the correction's
knowledge instant and let the trader's normal recovery path rebuild them. That
is one reviewed, scoped, idempotent writer rather than an UPDATE composed by
hand against live money state, and it never names ``execution_plan_checkpoints``
— control state the design deliberately does not rebuild — while reporting
those rows so their untouchedness is checked rather than trusted.

Acting identity, stated once and honestly. ``annulled_by_user_public_id`` in a
request document is an OPERATOR ASSERTION. This command runs on the database
host with no session, no token and no principal, so it authenticates nobody; the
guarded writer proves only that the asserted id resolves to an existing ACTIVE
user, which catches a typo and a fabricated id and nothing else. Whoever can run
this command can name any active user.

Exit codes are part of the contract:

    0 — the command did what it printed, completely.
    1 — refusal: malformed input, a missing confirmation, or a guarded-writer
        refusal (unknown target, crossed scope, digest mismatch, unknown or
        inactive acting user, witnessed target, already annulled, or a derived
        plane that is not in the state the request asserts).
    3 — the act succeeded but its knowledge proof is INCOMPLETE: a correction
        is durable with a pending observation, or corrections remain
        unobserved. Never treat this as success; a runbook must stop here.
"""

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Annotated
from typing import Final
from typing import Literal
from typing import NoReturn
from typing import cast
from uuid import uuid7

import typer

from snapper.config.settings import get_bootstrap_settings
from snapper.core.json_types import JsonObject
from snapper.core.json_types import JsonValue
from snapper.data.models import EXECUTION_ANNULMENT_REASONS
from snapper.data.repository import EXECUTION_ANNULMENT_DISCOVERY_LIMIT
from snapper.data.repository import DerivedProjectionRetirementError
from snapper.data.repository import ExecutionAnnulmentActorError
from snapper.data.repository import ExecutionAnnulmentConflictError
from snapper.data.repository import ExecutionAnnulmentTargetError
from snapper.data.repository import ExecutionAnnulmentWitnessedError
from snapper.data.repository import Repository
from snapper.data.repository import dispose_repositories
from snapper.data.repository import get_repository
from snapper.data.repository_types import DerivedProjectionRetirementRequest
from snapper.data.repository_types import DerivedProjectionRetirementResult
from snapper.data.repository_types import DerivedProjectionScopeRow
from snapper.data.repository_types import DerivedProjectionVersionRow
from snapper.data.repository_types import ExecutionAnnulmentReason
from snapper.data.repository_types import ExecutionAnnulmentRequest
from snapper.data.repository_types import ExecutionAnnulmentRow
from snapper.data.repository_types import ExecutionAnnulmentWriteResult
from snapper.data.repository_types import UnwitnessedExecutionRow

annulment_app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Inspect and correct the append-only execution-annulment manifest.",
)

EXIT_REFUSED: Final[int] = 1
"""Exit code for every refusal: bad input, missing confirmation, writer refusal."""

EXIT_INCOMPLETE_KNOWLEDGE: Final[int] = 3
"""Exit code for a durable act whose visibility observation is still missing."""

SUPPORTED_MODES: Final[tuple[str, ...]] = ("live", "paper")
"""Certification modes a scope may be spelled with."""

REQUEST_DOCUMENT_FIELDS: Final[tuple[str, ...]] = (
    "target_execution_public_id",
    "expected_execution_digest",
    "wallet_public_id",
    "exchange",
    "mode",
    "scope_sequence",
    "annulled_by_user_public_id",
    "correction_time",
    "reason",
    "evidence",
)
"""Exactly the fields an annulment request document must carry.

Every one is an ASSERTION the guarded writer proves against the stored row.
Unknown fields are refused rather than ignored: a typo in a scope key would
otherwise silently fall back to a different assertion than the operator wrote.
``session_id`` and ``sequence_id`` are deliberately absent — they name WHICH
session authored the act, so this command mints them itself."""


def _fatal(message: str) -> NoReturn:
    """Write a one-line stderr refusal and exit non-zero.

    Args:
        message: Single-line stderr message to echo before exiting.

    Raises:
        typer.Exit: Always — exit code :data:`EXIT_REFUSED`.
    """
    typer.echo(message, err=True)
    raise typer.Exit(code=EXIT_REFUSED)


def _repository() -> Repository:
    """Resolve the repository bound to the deployment's configured database.

    Returns:
        The repository for the bootstrap ``DB_URL``.
    """
    return get_repository(get_bootstrap_settings().db_url)


def _validated_mode(mode: str) -> str:
    """Refuse any certification mode outside the supported spellings.

    Args:
        mode: The operator-supplied mode.

    Returns:
        The same mode, once proven supported.
    """
    if mode not in SUPPORTED_MODES:
        _fatal(f"refused: mode must be one of {', '.join(SUPPORTED_MODES)}; got {mode!r}")
    return mode


def _document_field(document: JsonObject, key: str) -> JsonValue:
    """Read one required field from a request document.

    Args:
        document: The parsed request document.
        key: The field name to read.

    Returns:
        The field's raw JSON value.
    """
    if key not in document:
        _fatal(f"refused: annulment request is missing required field {key!r}")
    return document[key]


def _text_field(document: JsonObject, key: str) -> str:
    """Read one required non-empty string field.

    Args:
        document: The parsed request document.
        key: The field name to read.

    Returns:
        The field's stripped string value.
    """
    value = _document_field(document, key)
    if not isinstance(value, str) or not value.strip():
        _fatal(f"refused: annulment request field {key!r} must be a non-empty string")
    return value.strip()


def _sequence_field(document: JsonObject, key: str) -> int:
    """Read one required positive integer scope-sequence field.

    Args:
        document: The parsed request document.
        key: The field name to read.

    Returns:
        The field's integer value.
    """
    value = _document_field(document, key)
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        _fatal(f"refused: annulment request field {key!r} must be an integer of at least 1")
    return value


def _instant_field(document: JsonObject, key: str) -> datetime:
    """Read one required timezone-aware ISO-8601 instant field.

    A naive instant is refused rather than assumed UTC: the value is stored as
    the operator's DECLARED correction time and appears in the audit record, so
    an ambiguous spelling must never be silently resolved on their behalf.

    Args:
        document: The parsed request document.
        key: The field name to read.

    Returns:
        The parsed aware instant.
    """
    value = _text_field(document, key)
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        _fatal(f"refused: annulment request field {key!r} must be an ISO-8601 instant")
    if parsed.utcoffset() is None:
        _fatal(f"refused: annulment request field {key!r} must carry a UTC offset")
    return parsed


def _evidence_field(document: JsonObject) -> JsonObject:
    """Read the required non-empty diagnosis evidence envelope.

    Args:
        document: The parsed request document.

    Returns:
        The evidence object exactly as written.
    """
    value = _document_field(document, "evidence")
    if not isinstance(value, dict) or not value:
        _fatal("refused: annulment request field 'evidence' must be a non-empty JSON object")
    return value


def _reason_field(document: JsonObject) -> ExecutionAnnulmentReason:
    """Read the required reason and prove it belongs to the closed vocabulary.

    Args:
        document: The parsed request document.

    Returns:
        The typed reason.
    """
    value = _text_field(document, "reason")
    if value not in EXECUTION_ANNULMENT_REASONS:
        _fatal(
            "refused: annulment request field 'reason' must be one of "
            f"{', '.join(EXECUTION_ANNULMENT_REASONS)}; got {value!r}"
        )
    return cast(ExecutionAnnulmentReason, value)


def _load_request_document(path: Path) -> JsonObject:
    """Read and shape-check one annulment request document.

    Args:
        path: Path to the JSON request document.

    Returns:
        The parsed document, proven to be an object carrying exactly the
        supported fields.
    """
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as error:
        _fatal(f"refused: cannot read annulment request {path}: {error}")
    try:
        parsed = cast(JsonValue, json.loads(raw))
    except json.JSONDecodeError as error:
        _fatal(f"refused: annulment request {path} is not valid JSON: {error}")
    if not isinstance(parsed, dict):
        _fatal(f"refused: annulment request {path} must contain a JSON object")
    unknown = sorted(set(parsed) - set(REQUEST_DOCUMENT_FIELDS))
    if unknown:
        _fatal(f"refused: annulment request carries unknown fields: {', '.join(unknown)}")
    return parsed


def _annulment_request(document: JsonObject) -> ExecutionAnnulmentRequest:
    """Build one typed annulment request from a validated document.

    ``session_id`` and ``sequence_id`` are minted here rather than accepted from
    the document: they identify the session performing the act, and an operator
    supplying them could otherwise attribute one correction to another session's
    audit trail.

    Args:
        document: The parsed request document.

    Returns:
        The typed request the guarded writer consumes.
    """
    return {
        "target_execution_public_id": _text_field(document, "target_execution_public_id"),
        "expected_execution_digest": _text_field(document, "expected_execution_digest"),
        "wallet_public_id": _text_field(document, "wallet_public_id"),
        "exchange": _text_field(document, "exchange"),
        "mode": cast(
            Literal["live", "paper"],
            _validated_mode(_text_field(document, "mode")),
        ),
        "scope_sequence": _sequence_field(document, "scope_sequence"),
        "annulled_by_user_public_id": _text_field(document, "annulled_by_user_public_id"),
        "correction_time": _instant_field(document, "correction_time"),
        "reason": _reason_field(document),
        "evidence": _evidence_field(document),
        "session_id": str(uuid7()),
        "sequence_id": 1,
    }


def _echo_request_summary(request: ExecutionAnnulmentRequest) -> None:
    """Print the COMPLETE parsed request, evidence envelope included.

    Nothing is elided. This is the only rendering an operator sees before the
    correction becomes unwithdrawable, and the evidence envelope is the part a
    reviewer actually argues with — a summary that showed the scope and hid the
    diagnosis would invite approving a repudiation on its coordinates alone.
    The envelope is printed in the same canonical form the writer stores (sorted
    keys, compact separators), so what is reviewed is what is persisted.

    Args:
        request: The typed annulment request.

    Returns:
        None.
    """
    typer.echo("annulment request")
    typer.echo(f"  target execution : {request['target_execution_public_id']}")
    typer.echo(
        "  scope            : "
        f"{request['exchange']}/{request['mode']}/{request['scope_sequence']} "
        f"wallet={request['wallet_public_id']}"
    )
    typer.echo(f"  expected digest  : {request['expected_execution_digest']}")
    typer.echo(f"  reason           : {request['reason']}")
    typer.echo(f"  acting user      : {request['annulled_by_user_public_id']} (operator-asserted)")
    typer.echo(f"  correction time  : {request['correction_time'].isoformat()}")
    typer.echo(
        "  evidence         : "
        f"{json.dumps(request['evidence'], separators=(',', ':'), sort_keys=True)}"
    )


def _echo_unwitnessed(rows: list[UnwitnessedExecutionRow]) -> None:
    """Print the executions that block certification, with their digests.

    Args:
        rows: The unwitnessed executions in scope-coordinate order.

    Returns:
        None.
    """
    typer.echo(f"unwitnessed executions: {len(rows)}")
    for row in rows:
        standing = row["annulment_public_id"]
        status = "BLOCKING" if standing is None else f"annulled by {standing}"
        typer.echo(
            f"  {row['exchange']}/{row['scope_sequence']} execution={row['public_id']} "
            f"exec_id={row['exec_id']} size={row['size']} price={row['price']} "
            f"at={row['timestamp'].isoformat()}"
        )
        typer.echo(f"    digest={row['canonical_digest']} status={status}")


def _echo_manifest(rows: list[ExecutionAnnulmentRow]) -> None:
    """Print the scope's current annulment manifest.

    Args:
        rows: The manifest rows in scope-coordinate order.

    Returns:
        None.
    """
    typer.echo(f"annulment manifest: {len(rows)}")
    for row in rows:
        typer.echo(
            f"  {row['exchange']}/{row['scope_sequence']} annulment={row['public_id']} "
            f"target={row['target_execution_public_id']} reason={row['reason']}"
        )
        typer.echo(
            f"    known_at={row['timestamp'].isoformat()} "
            f"declared={row['correction_time'].isoformat()} "
            f"by={row['annulled_by_user_public_id']}"
        )


def _echo_pending(rows: list[ExecutionAnnulmentRow]) -> None:
    """Print the corrections that no durability observation has made visible.

    Args:
        rows: The unobserved manifest rows, oldest first.

    Returns:
        None.
    """
    typer.echo(f"corrections without a visibility observation: {len(rows)}")
    for row in rows:
        typer.echo(
            f"  annulment={row['public_id']} target={row['target_execution_public_id']} "
            f"known_at={row['timestamp'].isoformat()}"
        )
    if rows:
        typer.echo("  these are durable but folded by NO historical horizon; run:")
        typer.echo("  snapper annulment complete-visibility --wallet <wallet> --mode <mode>")


async def _run_inspect(wallet: str, mode: str, exchange: str | None, limit: int) -> None:
    """Load and print one scope's full annulment picture.

    Args:
        wallet: Wallet identity of the certification scope.
        mode: Trading mode of the certification scope.
        exchange: Optional single venue to narrow the scan to.
        limit: Maximum rows each discovery read may return.

    Returns:
        None.
    """
    repository = _repository()
    try:
        unwitnessed = await repository.get_unwitnessed_executions(wallet, mode, exchange, limit)
        manifest = await repository.get_execution_annulments(wallet, mode, exchange)
        pending = await repository.get_unobserved_execution_annulments(wallet, mode, limit)
    except ValueError as error:
        _fatal(f"refused: {error}")
    finally:
        await dispose_repositories()
    typer.echo(
        f"scope wallet={wallet} mode={mode} "
        f"exchange={'<all>' if exchange is None else exchange} limit={limit}"
    )
    _echo_unwitnessed(unwitnessed)
    _echo_manifest(manifest)
    _echo_pending(pending)


@annulment_app.command(name="inspect")
def inspect_scope(
    wallet: Annotated[str, typer.Option("--wallet", help="Wallet public id of the scope")],
    mode: Annotated[str, typer.Option("--mode", help="Certification mode: live or paper")],
    exchange: Annotated[str | None, typer.Option("--exchange", help="Narrow to one venue")] = None,
    limit: Annotated[
        int, typer.Option("--limit", help="Maximum rows per discovery read")
    ] = EXECUTION_ANNULMENT_DISCOVERY_LIMIT,
) -> None:
    """Report what blocks one scope's certification. Read-only; run this FIRST.

    Prints the unwitnessed executions with the canonical digest an annulment
    request must assert, the scope's current manifest, and every correction
    still missing its durability observation. Writes nothing.

    Args:
        wallet: Wallet public id of the certification scope.
        mode: Certification mode, ``live`` or ``paper``.
        exchange: Optional single venue to narrow the scan to.
        limit: Maximum rows each discovery read may return.

    Returns:
        None.
    """
    asyncio.run(_run_inspect(wallet, _validated_mode(mode), exchange, limit))


def _echo_annulment_result(result: ExecutionAnnulmentWriteResult) -> None:
    """Print the appended correction and the state of its knowledge proof.

    Args:
        result: The guarded writer's result.

    Returns:
        None.
    """
    appended = result["annulment"]
    typer.echo("appended annulment")
    typer.echo(f"  annulment        : {appended['public_id']}")
    typer.echo(f"  target execution : {appended['target_execution_public_id']}")
    typer.echo(
        "  scope            : "
        f"{appended['exchange']}/{appended['mode']}/{appended['scope_sequence']} "
        f"wallet={appended['wallet_public_id']}"
    )
    typer.echo(f"  known at         : {appended['timestamp'].isoformat()}")
    observation = result["visibility"]
    if observation is None:
        typer.echo("  visibility       : PENDING")
        typer.echo(
            "  the correction is durable and current truth honours it, but NO "
            "historical horizon folds it yet"
        )
        typer.echo(
            "  run: snapper annulment complete-visibility "
            f"--wallet {appended['wallet_public_id']} --mode {appended['mode']}"
        )
        return
    typer.echo(f"  visibility       : observed at {observation['observed_at'].isoformat()}")


async def _run_annul(request: ExecutionAnnulmentRequest) -> None:
    """Record exactly one repudiation through the guarded writer.

    Args:
        request: The typed annulment request.

    Returns:
        None.
    """
    repository = _repository()
    try:
        result = await repository.record_execution_annulment(request)
    except (
        ExecutionAnnulmentTargetError,
        ExecutionAnnulmentActorError,
        ExecutionAnnulmentWitnessedError,
        ExecutionAnnulmentConflictError,
        ValueError,
    ) as error:
        _fatal(f"refused by the guarded writer: {error}")
    finally:
        await dispose_repositories()
    _echo_annulment_result(result)
    if result["visibility_state"] == "pending":
        raise typer.Exit(code=EXIT_INCOMPLETE_KNOWLEDGE)


@annulment_app.command(name="annul")
def annul_execution(
    request_file: Annotated[
        Path,
        typer.Option("--request-file", help="JSON document describing ONE annulment request"),
    ],
    confirm: Annotated[
        bool,
        typer.Option("--confirm", help="Required: acknowledge this appends to a money ledger"),
    ] = False,
) -> None:
    """Append ONE annulment for ONE execution through the guarded writer.

    The request document asserts the target's immutable id, the canonical digest
    copied from ``inspect``, the certification scope, the acting user, a reason
    from the closed vocabulary, and a diagnosis evidence envelope. The writer
    proves every assertion against the stored row and refuses a digest mismatch,
    a crossed scope, an acting user that is unknown or deactivated, a witnessed
    target, and a duplicate. There is no bulk mode and there cannot be one.

    ``annulled_by_user_public_id`` is OPERATOR-ASSERTED and verified only to be
    an existing ACTIVE user. This command has no authentication context at all,
    so it cannot and does not prove who ran it.

    Without ``--confirm`` this is a PARSE PREVIEW: the document is read,
    validated and printed in full, and no database connection is opened. It
    proves the document is well formed and shows exactly what would be
    recorded — it proves nothing about the stored row, so a preview that looks
    right can still be refused by the writer.

    Args:
        request_file: Path to the JSON request document.
        confirm: Explicit acknowledgement that this appends to a money ledger.

    Returns:
        None.
    """
    request = _annulment_request(_load_request_document(request_file))
    _echo_request_summary(request)
    if not confirm:
        typer.echo("parse preview only: no database contact, nothing was written")
        _fatal("refused: --confirm was not supplied; nothing was written")
    asyncio.run(_run_annul(request))


async def _complete_one_visibility(repository: Repository, row: ExecutionAnnulmentRow) -> bool:
    """Complete one correction's durability observation, reporting the outcome.

    Args:
        repository: The repository holding the manifest.
        row: The unobserved correction to complete.

    Returns:
        Whether the observation now exists.
    """
    try:
        observation = await repository.observe_execution_annulment_visibility(row["public_id"])
    except Exception as error:
        typer.echo(f"  FAILED annulment={row['public_id']}: {error}", err=True)
        return False
    typer.echo(
        f"  completed annulment={row['public_id']} "
        f"observed_at={observation['observed_at'].isoformat()}"
    )
    return True


async def _run_complete_visibility(wallet: str, mode: str, limit: int) -> None:
    """Discover unobserved corrections, complete them, and count what is left.

    ``remaining`` is a COUNT QUERY over the whole scope, taken after the pass,
    never ``discovered - completed``. The discovery read is paged by ``--limit``,
    so the subtraction would print ``remaining 0`` whenever more corrections were
    unobserved than one page holds — announcing a closed knowledge gap that is
    still open, which is the single failure this protocol exists to prevent. Any
    non-zero remainder exits with the incomplete-knowledge code, whether it is
    left by a failure or merely by the page bound; rerunning the same command
    completes the next page.

    Args:
        wallet: Wallet identity of the certification scope.
        mode: Trading mode of the certification scope.
        limit: Maximum corrections to discover and complete in one pass.

    Returns:
        None.
    """
    repository = _repository()
    completed = 0
    try:
        try:
            pending = await repository.get_unobserved_execution_annulments(wallet, mode, limit)
        except ValueError as error:
            _fatal(f"refused: {error}")
        typer.echo(f"scope wallet={wallet} mode={mode} limit={limit}")
        typer.echo(f"corrections without a visibility observation in this pass: {len(pending)}")
        for row in pending:
            if await _complete_one_visibility(repository, row):
                completed += 1
        remaining = await repository.count_unobserved_execution_annulments(wallet, mode)
    finally:
        await dispose_repositories()
    typer.echo(f"completed {completed}, remaining {remaining} (whole scope)")
    if remaining:
        typer.echo(
            "  corrections remain unobserved; rerun this command "
            "(a full page means more await the next pass)"
        )
        raise typer.Exit(code=EXIT_INCOMPLETE_KNOWLEDGE)


@annulment_app.command(name="complete-visibility")
def complete_visibility(
    wallet: Annotated[str, typer.Option("--wallet", help="Wallet public id of the scope")],
    mode: Annotated[str, typer.Option("--mode", help="Certification mode: live or paper")],
    limit: Annotated[
        int, typer.Option("--limit", help="Maximum corrections to complete in one pass")
    ] = EXECUTION_ANNULMENT_DISCOVERY_LIMIT,
) -> None:
    """Complete the durability observations that corrections are still missing.

    Idempotent: a correction that already carries an observation keeps the one
    it has, because minting a second, later instant would move its proven
    knowledge time forward.

    PAGED. ``--limit`` bounds one pass, but the reported remainder is a count
    over the whole scope, so a pass that filled its page exits with the
    incomplete-knowledge code and rerunning takes the next page. The command is
    finished only when it prints ``remaining 0`` and exits zero.

    Args:
        wallet: Wallet public id of the certification scope.
        mode: Certification mode, ``live`` or ``paper``.
        limit: Maximum corrections to complete in one pass.

    Returns:
        None.
    """
    asyncio.run(_run_complete_visibility(wallet, _validated_mode(mode), limit))


def _echo_derived_versions(label: str, rows: list[DerivedProjectionVersionRow]) -> None:
    """Print one derived plane's rows with the identity an operator recognizes.

    Args:
        label: Human label for the plane.
        rows: The plane's rows, in scope order.

    Returns:
        None.
    """
    typer.echo(f"{label}: {len(rows)}")
    for row in rows:
        typer.echo(
            f"  {row['public_id']} identity={row['identity']} "
            f"version_started_at={row['version_started_at'].isoformat()}"
        )


def _echo_derived_scope(scope: DerivedProjectionScopeRow) -> None:
    """Print one scope's derived plane, including what must stay untouched.

    Args:
        scope: The loaded derived-plane picture.

    Returns:
        None.
    """
    applied = scope["applied_annulment_public_ids"]
    typer.echo(f"applied corrections (durable and observed): {len(applied)}")
    for public_id in applied:
        typer.echo(f"  annulment={public_id}")
    retired_at = scope["retired_at"]
    typer.echo(
        "  retirement instant : "
        f"{'NONE — nothing to retire' if retired_at is None else retired_at.isoformat()}"
    )
    _echo_derived_versions(
        "active trade_projection_checkpoints", scope["trade_projection_checkpoints"]
    )
    _echo_derived_versions("active positions", scope["positions"])
    plan_checkpoints = scope["execution_plan_checkpoints"]
    typer.echo(f"execution_plan_checkpoints (never rebuilt): {len(plan_checkpoints)}")
    for witness in plan_checkpoints:
        terminal = "terminal" if witness["plan_is_terminal"] else "NOT TERMINAL"
        typer.echo(
            f"  {witness['public_id']} plan={witness['plan_public_id']} "
            f"status={witness['plan_status']} {terminal}"
        )


async def _run_retire_derived_preflight(wallet: str, mode: str) -> None:
    """Publish the scope's derived plane read-only, then refuse for want of confirmation.

    A real preflight rather than a parse preview: the retirement's own inputs
    are database state, not a document, so the honest way to review it is to run
    the same read the writer runs. Every statement here is a SELECT.

    Args:
        wallet: Wallet identity of the certification scope.
        mode: Trading mode of the certification scope.

    Returns:
        None.
    """
    repository = _repository()
    try:
        scope = await repository.get_derived_projection_scope(wallet, mode)
    except ValueError as error:
        _fatal(f"refused: {error}")
    finally:
        await dispose_repositories()
    typer.echo(f"derived plane wallet={wallet} mode={mode}")
    _echo_derived_scope(scope)
    _fatal("refused: --confirm was not supplied; nothing was written")


def _echo_retirement_result(result: DerivedProjectionRetirementResult) -> None:
    """Print exactly what was retired and the scope the act left behind.

    Args:
        result: The reviewed writer's result.

    Returns:
        None.
    """
    typer.echo("retired derived projections")
    typer.echo(f"  retired at       : {result['retired_at'].isoformat()}")
    for row in result["retired"]:
        typer.echo(
            f"  {row['plane']} {row['public_id']} identity={row['identity']} "
            f"version_started_at={row['version_started_at'].isoformat()}"
        )
    typer.echo("verification — scope after the retirement")
    _echo_derived_scope(result["scope_after"])


async def _run_retire_derived(request: DerivedProjectionRetirementRequest) -> None:
    """Retire one scope's derived projections through the reviewed writer.

    Args:
        request: The typed retirement request.

    Returns:
        None.
    """
    repository = _repository()
    try:
        result = await repository.retire_execution_annulment_derived_projections(request)
    except (DerivedProjectionRetirementError, ValueError) as error:
        _fatal(f"refused by the reviewed writer: {error}")
    finally:
        await dispose_repositories()
    _echo_retirement_result(result)


@annulment_app.command(name="retire-derived")
def retire_derived(
    wallet: Annotated[str, typer.Option("--wallet", help="Wallet public id of the scope")],
    mode: Annotated[str, typer.Option("--mode", help="Certification mode: live or paper")],
    checkpoint: Annotated[
        list[str] | None,
        typer.Option("--checkpoint", help="Asserted active trade_projection_checkpoints public id"),
    ] = None,
    position: Annotated[
        list[str] | None,
        typer.Option("--position", help="Asserted active positions public id"),
    ] = None,
    confirm: Annotated[
        bool,
        typer.Option("--confirm", help="Required: acknowledge this closes projection versions"),
    ] = False,
) -> None:
    """SCD2-retire the projections a scope's corrections invalidated.

    The derived half of a correction, as ONE reviewed writer instead of an
    UPDATE typed against live money state. It closes the scope's active
    ``trade_projection_checkpoints`` and ``positions`` versions at the
    correction's knowledge instant — never deleting, never backdating, never
    inserting a successor — and the trader's normal recovery path rebuilds them
    from the annulment-aware ledger read. ``execution_plan_checkpoints`` is
    never named in a mutation here, because the adopted design keeps that
    control state as it stands; it is REPORTED so an operator verifies both that
    it is untouched and that the plans holding it are terminal.

    Every ``--checkpoint`` / ``--position`` is an ASSERTION about which rows are
    active. The writer computes that set itself and refuses unless the two are
    equal in both directions, so a row you did not list is never retired and a
    row you listed that is already retired is a refusal rather than a second
    close. Rerunning a completed retirement therefore refuses; that refusal is
    the idempotence.

    Without ``--confirm`` this is a READ-ONLY PREFLIGHT: it runs the writer's
    own scope read and prints the applied corrections, the retirement instant,
    the exact public ids to assert, and the plan checkpoints — then refuses.
    Every statement it issues is a SELECT.

    Args:
        wallet: Wallet public id of the certification scope.
        mode: Certification mode, ``live`` or ``paper``.
        checkpoint: Asserted active trade projection checkpoint public ids.
        position: Asserted active position public ids.
        confirm: Explicit acknowledgement that this closes projection versions.

    Returns:
        None.
    """
    validated_mode = _validated_mode(mode)
    if not confirm:
        asyncio.run(_run_retire_derived_preflight(wallet, validated_mode))
    request: DerivedProjectionRetirementRequest = {
        "wallet_public_id": wallet,
        "mode": cast(Literal["live", "paper"], validated_mode),
        "expected_trade_projection_checkpoint_public_ids": list(checkpoint or []),
        "expected_position_public_ids": list(position or []),
    }
    asyncio.run(_run_retire_derived(request))
