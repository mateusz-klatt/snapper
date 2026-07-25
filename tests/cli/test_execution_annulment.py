"""Tests for the ``snapper annulment`` operator surface.

The surface exists because a money-ledger correction is an operator decision:
``inspect`` publishes the evidence and the digest, ``annul`` records exactly one
repudiation behind an explicit confirmation, and ``complete-visibility`` closes
the knowledge protocol's second half. These tests pin the happy paths, every
refusal path, the confirmation gate, and the exit codes a runbook keys on.
"""

import json
import shlex
from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import click
import pytest
import typer.main
from typer.testing import CliRunner

from snapper.cli.app import app
from snapper.data.repository import DerivedProjectionRetirementError
from snapper.data.repository import ExecutionAnnulmentTargetError
from snapper.data.repository import Repository
from snapper.data.repository_types import DerivedProjectionRetirementResult
from snapper.data.repository_types import DerivedProjectionScopeRow
from snapper.data.repository_types import DerivedProjectionVersionRow
from snapper.data.repository_types import ExecutionAnnulmentRow
from snapper.data.repository_types import ExecutionAnnulmentVisibilityRow
from snapper.data.repository_types import ExecutionAnnulmentWriteResult
from snapper.data.repository_types import ExecutionPlanCheckpointWitnessRow
from snapper.data.repository_types import UnwitnessedExecutionRow

_WALLET = "0000face-0000-7000-8000-0000000000a1"
_USER = "0000face-0000-7000-8000-0000000000d1"
_SESSION = "00000000-0000-7000-8000-000000000901"
_EXECUTION = "00000000-0000-7000-8000-000000000e01"
_ANNULMENT = "00000000-0000-7000-8000-0000000009a1"
_CHECKPOINT = "00000000-0000-7000-8000-0000000009c1"
_POSITION = "00000000-0000-7000-8000-0000000009c2"
_DIGEST = "a" * 64
_PHANTOM_AT = datetime(2026, 7, 19, 21, 54, tzinfo=UTC)
_KNOWN_AT = datetime(2026, 7, 26, 10, 0, tzinfo=UTC)
_OBSERVED_AT = datetime(2026, 7, 26, 10, 0, 1, tzinfo=UTC)
_CORRECTION_AT = "2026-07-26T09:00:00+00:00"
_RUNBOOK_RELATIVE_PATH = "proprietary/plans/plan_2026_07_20_pnl_timeline_impl.md"


@pytest.fixture
def runner() -> CliRunner:
    """Provide an isolated Typer CLI runner."""
    return CliRunner()


@pytest.fixture
def repository() -> Iterator[MagicMock]:
    """Bind the CLI to a typed repository double for the length of one test."""
    double = MagicMock(spec=Repository)
    bootstrap = MagicMock()
    bootstrap.db_url = "sqlite+aiosqlite:///:memory:"
    with (
        patch(
            "snapper.cli.execution_annulment.get_repository",
            return_value=cast(Repository, double),
        ),
        patch(
            "snapper.cli.execution_annulment.get_bootstrap_settings",
            return_value=bootstrap,
        ),
        patch(
            "snapper.cli.execution_annulment.dispose_repositories",
            new=AsyncMock(return_value=None),
        ),
    ):
        yield double


def _unwitnessed(
    *,
    annulment_public_id: str | None = None,
    canonical_digest: str | None = _DIGEST,
) -> UnwitnessedExecutionRow:
    """Build one unwitnessed-execution row shaped like the Kraken phantom."""
    return {
        "public_id": _EXECUTION,
        "wallet_public_id": _WALLET,
        "exchange": "kraken",
        "mode": "live",
        "scope_sequence": 1,
        "exec_id": None,
        "price": 0.0,
        "size": 0.0,
        "price_decimal": None,
        "size_decimal": None,
        "timestamp": _PHANTOM_AT,
        "canonical_digest": canonical_digest,
        "annulment_public_id": annulment_public_id,
    }


def _manifest_row() -> ExecutionAnnulmentRow:
    """Build one persisted manifest row."""
    return {
        "public_id": _ANNULMENT,
        "session_id": _SESSION,
        "sequence_id": 1,
        "timestamp": _KNOWN_AT,
        "target_execution_public_id": _EXECUTION,
        "target_execution_digest": _DIGEST,
        "wallet_public_id": _WALLET,
        "exchange": "kraken",
        "mode": "live",
        "scope_sequence": 1,
        "annulled_by_user_public_id": _USER,
        "correction_time": _KNOWN_AT,
        "reason": "unwitnessed_phantom",
        "evidence_json": '{"diagnosis":"phantom"}',
    }


def _observation() -> ExecutionAnnulmentVisibilityRow:
    """Build one persisted durability observation."""
    return {
        "public_id": "00000000-0000-7000-8000-0000000009b1",
        "session_id": _SESSION,
        "sequence_id": 1,
        "timestamp": _OBSERVED_AT,
        "annulment_public_id": _ANNULMENT,
        "annulment_id": 1,
        "observed_at": _OBSERVED_AT,
        "wallet_public_id": _WALLET,
        "exchange": "kraken",
        "mode": "live",
    }


def _write_result(*, observed: bool) -> ExecutionAnnulmentWriteResult:
    """Build one guarded-writer result in either visibility state."""
    return {
        "annulment": _manifest_row(),
        "visibility_state": "observed" if observed else "pending",
        "visibility": _observation() if observed else None,
    }


def _request_document(**overrides: object) -> dict[str, object]:
    """Build one valid annulment request document, overriding named fields."""
    document: dict[str, object] = {
        "target_execution_public_id": _EXECUTION,
        "expected_execution_digest": _DIGEST,
        "wallet_public_id": _WALLET,
        "exchange": "kraken",
        "mode": "live",
        "scope_sequence": 1,
        "annulled_by_user_public_id": _USER,
        "correction_time": _CORRECTION_AT,
        "reason": "unwitnessed_phantom",
        "evidence": {"diagnosis": "size 0 / price 0 residue", "fixed_in_commit": "13a6a397"},
    }
    document.update(overrides)
    return document


def _write_request(tmp_path: Path, document: object) -> Path:
    """Write one request document to disk and return its path."""
    path = tmp_path / "annulment-request.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def test_inspect_publishes_the_blockers_the_manifest_and_the_pending_corrections(
    runner: CliRunner,
    repository: MagicMock,
) -> None:
    """The read-only first step must show everything an operator needs.

    Given: A scope holding one blocking phantom, one already-corrected row, a
        manifest entry, and one correction still missing its observation.
    When: ``annulment inspect`` runs.
    Then: It exits zero having printed the blocking row with the exact digest
        the guarded writer will demand, the standing correction for the
        corrected row, the manifest, and the pending correction together with
        the command that completes it.
    """
    repository.get_unwitnessed_executions = AsyncMock(
        return_value=[_unwitnessed(), _unwitnessed(annulment_public_id=_ANNULMENT)]
    )
    repository.get_execution_annulments = AsyncMock(return_value=[_manifest_row()])
    repository.get_unobserved_execution_annulments = AsyncMock(return_value=[_manifest_row()])

    result = runner.invoke(app, ["annulment", "inspect", "--wallet", _WALLET, "--mode", "live"])

    assert result.exit_code == 0, result.stderr
    assert "unwitnessed executions: 2" in result.stdout
    assert f"digest={_DIGEST} status=BLOCKING" in result.stdout
    assert f"status=annulled by {_ANNULMENT}" in result.stdout
    assert "annulment manifest: 1" in result.stdout
    assert "corrections without a visibility observation: 1" in result.stdout
    assert "snapper annulment complete-visibility" in result.stdout
    repository.get_unwitnessed_executions.assert_awaited_once_with(_WALLET, "live", None, 100)


def test_inspect_narrows_by_venue_and_bound_and_stays_quiet_when_nothing_blocks(
    runner: CliRunner,
    repository: MagicMock,
) -> None:
    """A clean scope must say so without inventing findings.

    Given: A scope with no blockers, no manifest, and no pending corrections.
    When: ``annulment inspect`` runs narrowed to one venue with an explicit
        bound.
    Then: It exits zero, echoes the narrowed scope, reports three empty
        sections, and does NOT print the completion instruction — which belongs
        only to an actual gap.
    """
    repository.get_unwitnessed_executions = AsyncMock(return_value=[])
    repository.get_execution_annulments = AsyncMock(return_value=[])
    repository.get_unobserved_execution_annulments = AsyncMock(return_value=[])

    result = runner.invoke(
        app,
        [
            "annulment",
            "inspect",
            "--wallet",
            _WALLET,
            "--mode",
            "live",
            "--exchange",
            "kraken",
            "--limit",
            "5",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert "exchange=kraken limit=5" in result.stdout
    assert "unwitnessed executions: 0" in result.stdout
    assert "corrections without a visibility observation: 0" in result.stdout
    assert "snapper annulment complete-visibility" not in result.stdout
    repository.get_unwitnessed_executions.assert_awaited_once_with(_WALLET, "live", "kraken", 5)


def test_inspect_refuses_a_malformed_scope_read(
    runner: CliRunner,
    repository: MagicMock,
) -> None:
    """A repository refusal must not be printed as an empty all-clear.

    Given: A discovery read that refuses the requested identity.
    When: ``annulment inspect`` runs.
    Then: It exits non-zero with the refusal on stderr and prints no scope
        report at all.
    """
    repository.get_unwitnessed_executions = AsyncMock(
        side_effect=ValueError("execution annulment wallet_public_id is not a valid uuid")
    )

    result = runner.invoke(app, ["annulment", "inspect", "--wallet", "nope", "--mode", "live"])

    assert result.exit_code == 1
    assert "refused: execution annulment wallet_public_id is not a valid uuid" in result.stderr
    assert "unwitnessed executions" not in result.stdout


@pytest.mark.parametrize("command", ["inspect", "complete-visibility"])
def test_every_command_refuses_a_mode_outside_the_closed_vocabulary(
    runner: CliRunner,
    repository: MagicMock,
    command: str,
) -> None:
    """A mode typo must never be read as a scope with nothing in it.

    Given: A mode spelling no certification scope uses.
    When: Either scope-taking command runs with it.
    Then: It exits non-zero before touching the database.
    """
    result = runner.invoke(app, ["annulment", command, "--wallet", _WALLET, "--mode", "LIVE"])

    assert result.exit_code == 1
    assert "mode must be one of live, paper" in result.stderr


def test_annul_without_confirmation_prints_the_request_and_writes_nothing(
    runner: CliRunner,
    repository: MagicMock,
    tmp_path: Path,
) -> None:
    """An unconfirmed run is a COMPLETE parse preview, not a partial summary.

    Given: A valid request document and no ``--confirm`` flag.
    When: ``annulment annul`` runs.
    Then: The whole parsed request is printed — target, scope, copied digest,
        the acting user labelled as operator-asserted, and the canonical
        evidence envelope, which is the part a reviewer actually argues with —
        the run names itself a parse preview that made no database contact, the
        guarded writer is never called, and the command exits non-zero so an
        unconfirmed run cannot read as success in a runbook.
    """
    repository.record_execution_annulment = AsyncMock(return_value=_write_result(observed=True))
    path = _write_request(tmp_path, _request_document())

    result = runner.invoke(app, ["annulment", "annul", "--request-file", str(path)])

    assert result.exit_code == 1
    assert f"target execution : {_EXECUTION}" in result.stdout
    assert f"expected digest  : {_DIGEST}" in result.stdout
    assert "scope            : kraken/live/1" in result.stdout
    assert f"acting user      : {_USER} (operator-asserted)" in result.stdout
    assert (
        'evidence         : {"diagnosis":"size 0 / price 0 residue",'
        '"fixed_in_commit":"13a6a397"}' in result.stdout
    )
    assert "parse preview only: no database contact, nothing was written" in result.stdout
    assert "--confirm was not supplied" in result.stderr
    repository.record_execution_annulment.assert_not_awaited()


def test_annul_records_exactly_one_correction_and_reports_its_observation(
    runner: CliRunner,
    repository: MagicMock,
    tmp_path: Path,
) -> None:
    """A confirmed annulment states exactly what it appended.

    Given: A valid request document and an explicit confirmation.
    When: ``annulment annul`` runs and the writer completes both transactions.
    Then: It exits zero, prints the appended correction with its scope and
        server-stamped knowledge instant, reports the observation that makes it
        historically visible, and hands the writer exactly one request carrying
        the operator's assertions with a session identity the CLI minted.
    """
    repository.record_execution_annulment = AsyncMock(return_value=_write_result(observed=True))
    path = _write_request(tmp_path, _request_document())

    result = runner.invoke(app, ["annulment", "annul", "--request-file", str(path), "--confirm"])

    assert result.exit_code == 0, result.stderr
    assert f"annulment        : {_ANNULMENT}" in result.stdout
    assert f"known at         : {_KNOWN_AT.isoformat()}" in result.stdout
    assert f"visibility       : observed at {_OBSERVED_AT.isoformat()}" in result.stdout
    request = repository.record_execution_annulment.await_args.args[0]
    assert request["target_execution_public_id"] == _EXECUTION
    assert request["expected_execution_digest"] == _DIGEST
    assert request["reason"] == "unwitnessed_phantom"
    assert request["annulled_by_user_public_id"] == _USER
    assert request["correction_time"] == datetime.fromisoformat(_CORRECTION_AT)
    assert request["sequence_id"] == 1
    assert request["session_id"] != _SESSION


def test_annul_exits_non_zero_when_the_knowledge_proof_is_left_pending(
    runner: CliRunner,
    repository: MagicMock,
    tmp_path: Path,
) -> None:
    """A durable correction with no observation must stop a runbook.

    Given: A writer whose second transaction failed, leaving the correction
        durable but folded by no historical horizon.
    When: ``annulment annul`` runs confirmed.
    Then: The appended correction is still reported — it is real and cannot be
        withdrawn — but the visibility is printed as PENDING with the exact
        completion command, and the exit code is the incomplete-knowledge code
        rather than success.
    """
    repository.record_execution_annulment = AsyncMock(return_value=_write_result(observed=False))
    path = _write_request(tmp_path, _request_document())

    result = runner.invoke(app, ["annulment", "annul", "--request-file", str(path), "--confirm"])

    assert result.exit_code == 3
    assert "visibility       : PENDING" in result.stdout
    assert f"complete-visibility --wallet {_WALLET} --mode live" in result.stdout


def test_annul_surfaces_a_guarded_writer_refusal_verbatim(
    runner: CliRunner,
    repository: MagicMock,
    tmp_path: Path,
) -> None:
    """The writer's proofs are the authority, and its refusal is the answer.

    Given: A writer that refuses the request because the stored row no longer
        matches the digest the operator copied.
    When: ``annulment annul`` runs confirmed.
    Then: The refusal reaches the operator unchanged and the command exits
        non-zero.
    """
    repository.record_execution_annulment = AsyncMock(
        side_effect=ExecutionAnnulmentTargetError(
            f"execution_digest_mismatch: execution_public_id={_EXECUTION}"
        )
    )
    path = _write_request(tmp_path, _request_document())

    result = runner.invoke(app, ["annulment", "annul", "--request-file", str(path), "--confirm"])

    assert result.exit_code == 1
    assert "refused by the guarded writer: execution_digest_mismatch" in result.stderr


def test_annul_refuses_a_request_file_that_cannot_be_read(
    runner: CliRunner,
    repository: MagicMock,
    tmp_path: Path,
) -> None:
    """A missing request document is a refusal, not an empty request.

    Given: A path no file occupies.
    When: ``annulment annul`` runs against it.
    Then: It exits non-zero naming the unreadable path.
    """
    result = runner.invoke(
        app,
        ["annulment", "annul", "--request-file", str(tmp_path / "absent.json"), "--confirm"],
    )

    assert result.exit_code == 1
    assert "cannot read annulment request" in result.stderr


@pytest.mark.parametrize(
    ("document", "expected"),
    [
        pytest.param("not json at all", "not valid JSON", id="malformed-json"),
        pytest.param(None, "must contain a JSON object", id="not-an-object"),
    ],
)
def test_annul_refuses_a_request_document_that_is_not_a_json_object(
    runner: CliRunner,
    repository: MagicMock,
    tmp_path: Path,
    document: object,
    expected: str,
) -> None:
    """A request that is not an object cannot carry a single assertion.

    Given: A file that is not JSON at all, or JSON that is not an object.
    When: ``annulment annul`` runs against it.
    Then: It exits non-zero with the shape refusal.
    """
    path = tmp_path / "annulment-request.json"
    if document == "not json at all":
        path.write_text("not json at all", encoding="utf-8")
    else:
        path.write_text(json.dumps(document), encoding="utf-8")

    result = runner.invoke(app, ["annulment", "annul", "--request-file", str(path), "--confirm"])

    assert result.exit_code == 1
    assert expected in result.stderr


def test_annul_refuses_a_request_document_carrying_unknown_fields(
    runner: CliRunner,
    repository: MagicMock,
    tmp_path: Path,
) -> None:
    """An unrecognized field is a typo in an assertion, never a harmless extra.

    Given: A request document carrying a field the writer knows nothing about.
    When: ``annulment annul`` runs against it.
    Then: It exits non-zero naming the unknown field, because ignoring it would
        silently substitute a default for the assertion the operator wrote.
    """
    path = _write_request(tmp_path, _request_document(annul_everything=True))

    result = runner.invoke(app, ["annulment", "annul", "--request-file", str(path), "--confirm"])

    assert result.exit_code == 1
    assert "unknown fields: annul_everything" in result.stderr


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        pytest.param({"exchange": None}, "must be a non-empty string", id="null-text"),
        pytest.param({"exchange": "  "}, "must be a non-empty string", id="blank-text"),
        pytest.param({"scope_sequence": "1"}, "integer of at least 1", id="text-sequence"),
        pytest.param({"scope_sequence": True}, "integer of at least 1", id="boolean-sequence"),
        pytest.param({"scope_sequence": 0}, "integer of at least 1", id="zero-sequence"),
        pytest.param({"correction_time": "yesterday"}, "ISO-8601 instant", id="unparseable-time"),
        pytest.param({"correction_time": "2026-07-26T09:00:00"}, "UTC offset", id="naive-time"),
        pytest.param({"evidence": {}}, "non-empty JSON object", id="empty-evidence"),
        pytest.param({"evidence": "phantom"}, "non-empty JSON object", id="text-evidence"),
        pytest.param({"reason": "because"}, "must be one of", id="unlisted-reason"),
        pytest.param({"mode": "shadow"}, "mode must be one of", id="unlisted-mode"),
    ],
)
def test_annul_refuses_every_malformed_request_field(
    runner: CliRunner,
    repository: MagicMock,
    tmp_path: Path,
    overrides: dict[str, object],
    expected: str,
) -> None:
    """Every assertion is validated before the ledger is touched.

    Given: A request document whose one field is malformed, empty, mistyped, or
        outside the closed vocabulary.
    When: ``annulment annul`` runs against it.
    Then: It exits non-zero with the field-specific refusal and never reaches
        the guarded writer.
    """
    repository.record_execution_annulment = AsyncMock(return_value=_write_result(observed=True))
    path = _write_request(tmp_path, _request_document(**overrides))

    result = runner.invoke(app, ["annulment", "annul", "--request-file", str(path), "--confirm"])

    assert result.exit_code == 1
    assert expected in result.stderr
    repository.record_execution_annulment.assert_not_awaited()


def test_annul_refuses_a_request_document_missing_a_required_field(
    runner: CliRunner,
    repository: MagicMock,
    tmp_path: Path,
) -> None:
    """An absent assertion cannot be defaulted on the operator's behalf.

    Given: A request document with the target execution id removed.
    When: ``annulment annul`` runs against it.
    Then: It exits non-zero naming the missing field.
    """
    document = _request_document()
    del document["target_execution_public_id"]
    path = _write_request(tmp_path, document)

    result = runner.invoke(app, ["annulment", "annul", "--request-file", str(path), "--confirm"])

    assert result.exit_code == 1
    assert "missing required field 'target_execution_public_id'" in result.stderr


def test_complete_visibility_completes_every_pending_correction(
    runner: CliRunner,
    repository: MagicMock,
) -> None:
    """The recovery path must close the gap and say that it did.

    Given: One correction discovered without a durability observation.
    When: ``annulment complete-visibility`` runs.
    Then: The observation is completed through the idempotent maintenance
        writer, the completion is reported with the proven instant, and the
        command exits zero with nothing remaining.
    """
    repository.get_unobserved_execution_annulments = AsyncMock(return_value=[_manifest_row()])
    repository.observe_execution_annulment_visibility = AsyncMock(return_value=_observation())
    repository.count_unobserved_execution_annulments = AsyncMock(return_value=0)

    result = runner.invoke(
        app, ["annulment", "complete-visibility", "--wallet", _WALLET, "--mode", "live"]
    )

    assert result.exit_code == 0, result.stderr
    assert "corrections without a visibility observation in this pass: 1" in result.stdout
    assert f"completed annulment={_ANNULMENT}" in result.stdout
    assert "completed 1, remaining 0 (whole scope)" in result.stdout
    repository.observe_execution_annulment_visibility.assert_awaited_once_with(_ANNULMENT)
    repository.count_unobserved_execution_annulments.assert_awaited_once_with(_WALLET, "live")


def test_complete_visibility_reports_what_it_could_not_complete(
    runner: CliRunner,
    repository: MagicMock,
) -> None:
    """A partial recovery must never be reported as a finished one.

    Given: A correction whose observation still cannot be written.
    When: ``annulment complete-visibility`` runs.
    Then: The failure is named on stderr, the tally reports one remaining, and
        the exit code is the incomplete-knowledge code so a runbook stops.
    """
    repository.get_unobserved_execution_annulments = AsyncMock(return_value=[_manifest_row()])
    repository.observe_execution_annulment_visibility = AsyncMock(
        side_effect=RuntimeError("visibility transaction unavailable")
    )
    repository.count_unobserved_execution_annulments = AsyncMock(return_value=1)

    result = runner.invoke(
        app, ["annulment", "complete-visibility", "--wallet", _WALLET, "--mode", "live"]
    )

    assert result.exit_code == 3
    assert f"FAILED annulment={_ANNULMENT}" in result.stderr
    assert "completed 0, remaining 1 (whole scope)" in result.stdout


def test_complete_visibility_on_a_clean_scope_completes_nothing_and_succeeds(
    runner: CliRunner,
    repository: MagicMock,
) -> None:
    """Running the recovery on a healthy scope is safe and honest.

    Given: A scope whose corrections all carry observations.
    When: ``annulment complete-visibility`` runs with an explicit bound.
    Then: It reports zero pending corrections, completes nothing, and exits
        zero.
    """
    repository.get_unobserved_execution_annulments = AsyncMock(return_value=[])
    repository.observe_execution_annulment_visibility = AsyncMock(return_value=_observation())
    repository.count_unobserved_execution_annulments = AsyncMock(return_value=0)

    result = runner.invoke(
        app,
        [
            "annulment",
            "complete-visibility",
            "--wallet",
            _WALLET,
            "--mode",
            "live",
            "--limit",
            "7",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert "completed 0, remaining 0 (whole scope)" in result.stdout
    repository.get_unobserved_execution_annulments.assert_awaited_once_with(_WALLET, "live", 7)
    repository.observe_execution_annulment_visibility.assert_not_awaited()


def test_complete_visibility_reports_the_true_remainder_beyond_its_page(
    runner: CliRunner,
    repository: MagicMock,
) -> None:
    """A full page must never be reported as a closed knowledge gap.

    Given: A scope holding more unobserved corrections than ``--limit`` returns,
        where every correction the page DID return completes successfully.
    When: ``annulment complete-visibility`` runs with that bound.
    Then: The remainder is the scope-wide count rather than
        ``discovered - completed``, so it reports the corrections still beyond
        the page instead of ``remaining 0``, and it exits with the
        incomplete-knowledge code so a runbook reruns rather than continuing.
    """
    repository.get_unobserved_execution_annulments = AsyncMock(return_value=[_manifest_row()])
    repository.observe_execution_annulment_visibility = AsyncMock(return_value=_observation())
    repository.count_unobserved_execution_annulments = AsyncMock(return_value=4)

    result = runner.invoke(
        app,
        [
            "annulment",
            "complete-visibility",
            "--wallet",
            _WALLET,
            "--mode",
            "live",
            "--limit",
            "1",
        ],
    )

    assert result.exit_code == 3
    assert "corrections without a visibility observation in this pass: 1" in result.stdout
    assert "completed 1, remaining 4 (whole scope)" in result.stdout
    assert "rerun this command" in result.stdout


def test_complete_visibility_refuses_an_unsupported_discovery_bound(
    runner: CliRunner,
    repository: MagicMock,
) -> None:
    """A bound the repository refuses must stop the command, not be clamped.

    Given: A discovery read that refuses the requested bound.
    When: ``annulment complete-visibility`` runs.
    Then: It exits non-zero with the refusal and completes nothing.
    """
    repository.get_unobserved_execution_annulments = AsyncMock(
        side_effect=ValueError("execution annulment discovery limit must be between 1 and 1000")
    )
    repository.observe_execution_annulment_visibility = AsyncMock(return_value=_observation())

    result = runner.invoke(
        app,
        ["annulment", "complete-visibility", "--wallet", _WALLET, "--mode", "live", "--limit", "0"],
    )

    assert result.exit_code == 1
    assert "discovery limit must be between 1 and 1000" in result.stderr
    repository.observe_execution_annulment_visibility.assert_not_awaited()


def _checkpoint_row() -> DerivedProjectionVersionRow:
    """Build one active trade projection checkpoint version row."""
    return {
        "plane": "trade_projection_checkpoints",
        "public_id": _CHECKPOINT,
        "identity": "kraken.EUR-PLN.live.wa10000000001",
        "version_started_at": _PHANTOM_AT,
    }


def _position_row() -> DerivedProjectionVersionRow:
    """Build one active position projection version row."""
    return {
        "plane": "positions",
        "public_id": _POSITION,
        "identity": "00000000-0000-7000-8000-000000000b01",
        "version_started_at": _PHANTOM_AT,
    }


def _plan_checkpoint_row(*, terminal: bool = True) -> ExecutionPlanCheckpointWitnessRow:
    """Build one plan checkpoint witness the retirement must leave alone."""
    return {
        "public_id": "00000000-0000-7000-8000-0000000000p1",
        "plan_public_id": "00000000-0000-7000-8000-0000000000p2",
        "plan_status": "completed" if terminal else "active",
        "plan_is_terminal": terminal,
    }


def _derived_scope(*, retired: bool) -> DerivedProjectionScopeRow:
    """Build one derived-plane scope before or after a retirement."""
    return {
        "wallet_public_id": _WALLET,
        "mode": "live",
        "applied_annulment_public_ids": [_ANNULMENT],
        "retired_at": _KNOWN_AT,
        "trade_projection_checkpoints": [] if retired else [_checkpoint_row()],
        "positions": [] if retired else [_position_row()],
        "execution_plan_checkpoints": [_plan_checkpoint_row()],
    }


def _retirement_result() -> DerivedProjectionRetirementResult:
    """Build one reviewed-writer retirement result."""
    return {
        "wallet_public_id": _WALLET,
        "mode": "live",
        "retired_at": _KNOWN_AT,
        "applied_annulment_public_ids": [_ANNULMENT],
        "retired": [_checkpoint_row(), _position_row()],
        "scope_after": _derived_scope(retired=True),
    }


def test_retire_derived_without_confirmation_is_a_read_only_preflight(
    runner: CliRunner,
    repository: MagicMock,
) -> None:
    """The ids an operator must assert have to come from a reviewed read.

    Given: A scope with one applied correction, one active checkpoint, one
        active position, and one plan checkpoint that must not be rebuilt.
    When: ``annulment retire-derived`` runs without ``--confirm``.
    Then: It prints the applied correction, the retirement instant, both public
        ids to assert, and the plan checkpoint with its plan's terminality; it
        never calls the writer, and it exits non-zero so a preflight cannot read
        as a completed act.
    """
    repository.get_derived_projection_scope = AsyncMock(return_value=_derived_scope(retired=False))
    repository.retire_execution_annulment_derived_projections = AsyncMock(
        return_value=_retirement_result()
    )

    result = runner.invoke(
        app, ["annulment", "retire-derived", "--wallet", _WALLET, "--mode", "live"]
    )

    assert result.exit_code == 1
    assert f"annulment={_ANNULMENT}" in result.stdout
    assert f"retirement instant : {_KNOWN_AT.isoformat()}" in result.stdout
    assert f"active trade_projection_checkpoints: 1\n  {_CHECKPOINT}" in result.stdout
    assert f"active positions: 1\n  {_POSITION}" in result.stdout
    assert "execution_plan_checkpoints (never rebuilt): 1" in result.stdout
    assert "status=completed terminal" in result.stdout
    assert "--confirm was not supplied" in result.stderr
    repository.retire_execution_annulment_derived_projections.assert_not_awaited()


def test_retire_derived_preflight_says_plainly_when_nothing_is_retirable(
    runner: CliRunner,
    repository: MagicMock,
) -> None:
    """A scope with no applied correction has nothing whose effects need retiring.

    Given: A scope whose manifest holds no correction that is durable AND
        observed.
    When: The read-only preflight runs.
    Then: It reports no retirement instant at all rather than printing a
        plausible-looking one, so an operator cannot copy a timestamp that no
        correction justifies.
    """
    scope = _derived_scope(retired=False)
    scope["applied_annulment_public_ids"] = []
    scope["retired_at"] = None
    repository.get_derived_projection_scope = AsyncMock(return_value=scope)

    result = runner.invoke(
        app, ["annulment", "retire-derived", "--wallet", _WALLET, "--mode", "live"]
    )

    assert result.exit_code == 1
    assert "applied corrections (durable and observed): 0" in result.stdout
    assert "retirement instant : NONE — nothing to retire" in result.stdout


def test_retire_derived_preflight_surfaces_a_malformed_scope_read(
    runner: CliRunner,
    repository: MagicMock,
) -> None:
    """A refused read must not be printed as an empty derived plane.

    Given: A scope read that refuses the requested wallet identity.
    When: The read-only preflight runs.
    Then: It exits non-zero with the refusal and prints no scope report.
    """
    repository.get_derived_projection_scope = AsyncMock(
        side_effect=ValueError("execution annulment wallet_public_id is not a valid uuid")
    )

    result = runner.invoke(
        app, ["annulment", "retire-derived", "--wallet", "nope", "--mode", "live"]
    )

    assert result.exit_code == 1
    assert "refused: execution annulment wallet_public_id is not a valid uuid" in result.stderr
    assert "applied corrections" not in result.stdout


def test_retire_derived_retires_exactly_the_asserted_rows_and_verifies_the_result(
    runner: CliRunner,
    repository: MagicMock,
) -> None:
    """A confirmed retirement states what it closed and proves the postconditions.

    Given: The asserted checkpoint and position ids, and a writer that closes
        both.
    When: ``annulment retire-derived`` runs confirmed.
    Then: It exits zero, names each retired version with the plane it belongs
        to, and prints the scope re-read afterwards — no active projection rows
        left, the plan checkpoints still present and their plans terminal — so
        the runbook's postconditions are the command's own output rather than a
        follow-up query. The writer receives exactly the asserted sets.
    """
    repository.retire_execution_annulment_derived_projections = AsyncMock(
        return_value=_retirement_result()
    )

    result = runner.invoke(
        app,
        [
            "annulment",
            "retire-derived",
            "--wallet",
            _WALLET,
            "--mode",
            "live",
            "--checkpoint",
            _CHECKPOINT,
            "--position",
            _POSITION,
            "--confirm",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert f"retired at       : {_KNOWN_AT.isoformat()}" in result.stdout
    assert f"trade_projection_checkpoints {_CHECKPOINT}" in result.stdout
    assert f"positions {_POSITION}" in result.stdout
    assert "verification — scope after the retirement" in result.stdout
    assert "active trade_projection_checkpoints: 0" in result.stdout
    assert "active positions: 0" in result.stdout
    assert "execution_plan_checkpoints (never rebuilt): 1" in result.stdout
    request = repository.retire_execution_annulment_derived_projections.await_args.args[0]
    assert request["wallet_public_id"] == _WALLET
    assert request["mode"] == "live"
    assert request["expected_trade_projection_checkpoint_public_ids"] == [_CHECKPOINT]
    assert request["expected_position_public_ids"] == [_POSITION]


def test_retire_derived_asserts_an_empty_set_when_no_row_is_named(
    runner: CliRunner,
    repository: MagicMock,
) -> None:
    """Naming no row asserts that no row is active, and is proven like any other.

    Given: A confirmed run with neither ``--checkpoint`` nor ``--position``.
    When: ``annulment retire-derived`` runs.
    Then: The writer is handed two EMPTY assertions rather than a wildcard, so
        an active row the operator did not see refuses the act instead of being
        retired by omission.
    """
    empty = _retirement_result()
    empty["retired"] = []
    repository.retire_execution_annulment_derived_projections = AsyncMock(return_value=empty)

    result = runner.invoke(
        app,
        ["annulment", "retire-derived", "--wallet", _WALLET, "--mode", "live", "--confirm"],
    )

    assert result.exit_code == 0, result.stderr
    request = repository.retire_execution_annulment_derived_projections.await_args.args[0]
    assert request["expected_trade_projection_checkpoint_public_ids"] == []
    assert request["expected_position_public_ids"] == []


def test_retire_derived_surfaces_a_reviewed_writer_refusal_verbatim(
    runner: CliRunner,
    repository: MagicMock,
) -> None:
    """Rerunning a completed retirement must refuse, and say which row it is.

    Given: A writer that refuses because the asserted checkpoint is already
        retired — the shape a second run of a completed retirement takes.
    When: ``annulment retire-derived`` runs confirmed.
    Then: The refusal reaches the operator unchanged and the command exits
        non-zero, so idempotence is a visible refusal rather than a silent
        second close.
    """
    repository.retire_execution_annulment_derived_projections = AsyncMock(
        side_effect=DerivedProjectionRetirementError(
            "already_retired_derived_projection: "
            f"plane=trade_projection_checkpoints public_id={_CHECKPOINT}"
        )
    )

    result = runner.invoke(
        app,
        [
            "annulment",
            "retire-derived",
            "--wallet",
            _WALLET,
            "--mode",
            "live",
            "--checkpoint",
            _CHECKPOINT,
            "--confirm",
        ],
    )

    assert result.exit_code == 1
    assert "refused by the reviewed writer: already_retired_derived_projection" in result.stderr


def test_retire_derived_refuses_a_mode_outside_the_closed_vocabulary(
    runner: CliRunner,
    repository: MagicMock,
) -> None:
    """A mode typo must never reach the writer as a scope with nothing in it.

    Given: A mode spelling no certification scope uses.
    When: ``annulment retire-derived`` runs confirmed.
    Then: It exits non-zero before any database contact.
    """
    repository.retire_execution_annulment_derived_projections = AsyncMock(
        return_value=_retirement_result()
    )

    result = runner.invoke(
        app,
        ["annulment", "retire-derived", "--wallet", _WALLET, "--mode", "SHADOW", "--confirm"],
    )

    assert result.exit_code == 1
    assert "mode must be one of live, paper" in result.stderr
    repository.retire_execution_annulment_derived_projections.assert_not_awaited()


def _annulment_group() -> click.Group:
    """Resolve the registered ``annulment`` command group from the real CLI."""
    root = cast(click.Group, typer.main.get_command(app))
    return cast(click.Group, root.commands["annulment"])


def _registered_options(command_name: str) -> set[str]:
    """Collect every long option one registered annulment command accepts."""
    command = _annulment_group().commands[command_name]
    return {opt for param in command.params for opt in param.opts if opt.startswith("--")}


def test_the_operator_surface_registers_exactly_four_commands() -> None:
    """The surface an operator is told to run must be the surface that exists.

    Given: The real Snapper CLI.
    When: The ``annulment`` group is resolved.
    Then: It registers exactly ``inspect``, ``annul``, ``complete-visibility``
        and ``retire-derived`` with the flags the runbook uses — and notably NO
        bulk flag, because a mode that annulled everything unwitnessed would
        repudiate real money the moment a witness was merely late.
    """
    assert set(_annulment_group().commands) == {
        "inspect",
        "annul",
        "complete-visibility",
        "retire-derived",
    }
    assert {"--wallet", "--mode", "--exchange", "--limit"} <= _registered_options("inspect")
    assert {"--request-file", "--confirm"} <= _registered_options("annul")
    assert {"--wallet", "--mode", "--limit"} <= _registered_options("complete-visibility")
    assert {"--wallet", "--mode", "--checkpoint", "--position", "--confirm"} <= _registered_options(
        "retire-derived"
    )
    assert not any("bulk" in option for option in _registered_options("annul"))


def _runbook_invocations(runbook: Path) -> list[list[str]]:
    """Extract every ``snapper annulment`` invocation the runbook instructs."""
    invocations: list[list[str]] = []
    for line in runbook.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("snapper annulment "):
            invocations.append(shlex.split(stripped))
    return invocations


def test_every_command_the_runbook_instructs_actually_exists() -> None:
    """A runbook naming a command that does not exist is worse than none.

    Given: The production correction runbook appended to the P&L timeline plan.
    When: Every ``snapper annulment`` invocation in it is extracted.
    Then: Each names a registered command and uses only options that command
        accepts, so an operator following the runbook under pressure cannot be
        stopped by a renamed flag.
    """
    runbook = Path(__file__).resolve().parents[2] / _RUNBOOK_RELATIVE_PATH
    if not runbook.exists():
        pytest.skip("proprietary plans are not checked out")
    invocations = _runbook_invocations(runbook)
    assert invocations
    registered = set(_annulment_group().commands)
    for invocation in invocations:
        command_name = invocation[2]
        assert command_name in registered, invocation
        options = {token for token in invocation if token.startswith("--")}
        assert options <= _registered_options(command_name), invocation
