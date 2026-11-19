"""Exercise the candle void's approval, original database binding and recovery artifacts."""

import json
import os
import sqlite3
import sys
from collections.abc import AsyncIterator
from collections.abc import Callable
from contextlib import closing
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Final
from typing import cast
from unittest.mock import AsyncMock
from uuid import uuid7

import pytest
import pytest_asyncio
from sqlalchemy.exc import DBAPIError

from snapper.application.updaters.historical import candle_void
from snapper.application.updaters.historical.candle_repair import canonical_json
from snapper.application.updaters.historical.candle_repair import database_endpoint
from snapper.application.updaters.historical.candle_repair import sha256_hex
from snapper.application.updaters.historical.candle_void import EvidenceFile
from snapper.application.updaters.historical.candle_void import VoidContext
from snapper.application.updaters.historical.candle_void import VoidOutcome
from snapper.application.updaters.historical.candle_void import VoidPlan
from snapper.application.updaters.historical.candle_void import VoidRefusedError
from snapper.application.updaters.historical.candle_void import VoidRequest
from snapper.application.updaters.historical.candle_void import VoteBinding
from snapper.core import json_types
from snapper.core.json_types import JsonObject
from snapper.core.json_types import JsonValue
from snapper.data.candle_void_types import CandleVersionKey
from snapper.data.candle_void_types import CandleVersionReading
from snapper.data.candle_void_types import CandleVoidRefusedError
from snapper.data.candle_void_types import CandleVoidRequest as RepositoryVoidRequest
from snapper.data.candle_void_types import CandleVoidResult
from snapper.data.candle_void_types import CandleVoidTarget
from snapper.data.candle_void_types import CatalogExpectation
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.repository import SQLAlchemyRepository
from tests.helpers.candle_repair import OPEN
from tests.helpers.candle_repair import RUN_TIME
from tests.helpers.candle_repair import candle
from tests.helpers.candle_repair import seed_instrument

_CHF = "00000000-0000-4000-8000-000000000c51"
_REAL_FSYNC_DIRECTORY: Final[Callable[[Path], None]] = candle_void._fsync_directory


@pytest.fixture(autouse=True)
def _fake_directory_fsync(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub parent-directory fsync so ledger tests stay independent of host durability."""

    def skip_directory_fsync(path: Path) -> None:
        """Accept the directory path without opening a directory descriptor."""
        del path

    monkeypatch.setattr(candle_void, "_fsync_directory", skip_directory_fsync)


@pytest_asyncio.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Provide retired synthetic CHF candles in an isolated file-backed database."""
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'void.db'}")
    await repo.create_all()
    await seed_instrument(repo, _CHF, "CHF-USD", None)
    await repo.upsert_candles(
        [
            candle(_CHF, OPEN, source="native", vwap=None, trades=None),
            candle(_CHF, OPEN, timeframe="5m", close=1.25),
        ]
    )
    try:
        yield repo
    finally:
        await repo.engine.dispose()


def _context(repository: SQLAlchemyRepository) -> VoidContext:
    """Bind the actual synthetic endpoint and fixed source identity."""
    return VoidContext(
        repository=repository,
        database=database_endpoint(str(repository.engine.url)),
        code_revision={"synthetic/candle_void.py": "0" * 64},
        clock=lambda: RUN_TIME,
    )


def _request(tmp_path: Path) -> VoidRequest:
    """Prepare a new dry-run directory and synthetic cessation evidence."""
    evidence = tmp_path / "writer-cessation.json"
    evidence.write_text('{"synthetic":true}\n')
    output = tmp_path / "plan"
    output.mkdir()
    return VoidRequest(
        instrument_public_ids=(_CHF,),
        evidence=(EvidenceFile("writer_cessation", evidence),),
        output_dir=output,
    )


def _write_request(request: VoidRequest) -> VoidRequest:
    """Give the write its own empty directory."""
    output = request.output_dir.parent / "write"
    output.mkdir()
    return replace(request, output_dir=output)


def _document(path: Path) -> JsonObject:
    """Read one synthetic run artifact."""
    return cast(JsonObject, json.loads(path.read_bytes()))


async def _acknowledged_write(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> tuple[VoidContext, Path]:
    """Execute a voted synthetic run and return its bound context and directory."""
    context = _context(repository)
    request = _request(tmp_path)
    plan = await candle_void.plan_void(context, request)
    writing = _write_request(request)
    result = await candle_void.write_void(
        context, writing, VoteBinding(request.output_dir, plan.digest)
    )
    assert result.status == "acknowledged"
    return context, writing.output_dir


def _target(
    *,
    exchange: str = "kraken",
    active_aliases: int = 0,
    market_data: bool = False,
    trading: bool = False,
) -> CandleVoidTarget:
    """Build a resolved target for catalog refusal tests."""
    return CandleVoidTarget(
        instrument_public_id=_CHF,
        exchange=exchange,
        native_symbol="CHF-USD",
        catalog=CatalogExpectation(
            instrument_public_id=_CHF,
            capability_public_id="capability",
            capability_timestamp=OPEN,
            can_market_data=market_data,
            can_trade=trading,
            active_alias_count=active_aliases,
        ),
    )


def _key(index: int = 0) -> CandleVersionKey:
    """Build one synthetic physical candle-version key."""
    return CandleVersionKey(
        instrument_public_id=_CHF,
        timeframe="1m",
        open_at=OPEN + timedelta(minutes=index),
        id=index + 1,
        public_id=f"public-{index}",
        session_id="session",
        sequence_id=1,
        timestamp=OPEN,
    )


async def _vote(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> tuple[VoidContext, VoidRequest, VoteBinding, VoidPlan]:
    """Create a valid dry run and an empty directory for its write."""
    context = _context(repository)
    request = _request(tmp_path)
    plan = await candle_void.plan_void(context, request)
    writing = _write_request(request)
    return context, writing, VoteBinding(request.output_dir, plan.digest), plan


def _db_error(sqlstate: str) -> DBAPIError:
    """Build a synthetic driver failure with a PostgreSQL SQLSTATE."""
    return DBAPIError("LOCK TABLE public.candles", {}, SimpleNamespace(sqlstate=sqlstate))


def _intent(context: VoidContext) -> JsonObject:
    """Build a valid recovery intent with one physical version key."""
    return {
        **candle_void._run_binding(context, str(uuid7()), "a" * 64),
        "keys": [candle_void.key_json(_key())],
    }


async def test_dry_run_binds_exact_artifacts_without_closing_rows(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """A vote sees both timeframes and lossless nullable values without a mutation.

    Given the reviewed cohort and an evidence directory.
    When the dry run prepares its bound artifacts.
    Then the artifacts are complete and no candle row is closed.
    """
    before = await repository.get_active_candle_versions((_CHF,))
    request = _request(tmp_path)
    plan = await candle_void.plan_void(_context(repository), request)
    approval = (request.output_dir / candle_void.APPROVAL_FILE).read_bytes()
    preview = (request.output_dir / candle_void.PREVIEW_FILE).read_bytes()
    summary = (request.output_dir / candle_void.SUMMARY_FILE).read_bytes()
    assert sha256_hex(approval) == plan.digest
    assert plan.approval["preview_sha256"] == sha256_hex(preview)
    assert plan.approval["summary_sha256"] == sha256_hex(summary)
    assert plan.summary["candles"] == 2
    assert len(plan.keys) == 2
    assert b",native,true," in preview
    assert await repository.get_active_candle_versions((_CHF,)) == before
    assert not (request.output_dir / candle_void.INTENT_FILE).exists()


async def test_acknowledged_run_can_be_reconciled_again_without_changing_its_conclusion(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """A second fenced recovery retains the exact first receipt.

    Given an acknowledged operation and its bound recovery evidence.
    When reconciliation is requested again.
    Then the existing conclusion remains unchanged.
    """
    context, directory = await _acknowledged_write(repository, tmp_path)
    result = await candle_void.reconcile_void(context, directory)
    assert result.status == "reconciled_applied"
    receipt = (directory / candle_void.RECONCILIATION_FILE).read_bytes()
    again = await candle_void.reconcile_void(context, directory)
    assert again == result
    assert (directory / candle_void.RECONCILIATION_FILE).read_bytes() == receipt
    assert await repository.get_active_candle_versions((_CHF,)) == []


async def test_recovery_refuses_a_prewrite_clone_without_poisoning_the_original_run(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """Identical physical keys on another database cannot conclude the source run.

    Given a prepared operation copied to a different database identity.
    When the clone is reconciled before the original operation.
    Then the clone is refused and the original operation remains usable.
    """
    clone_path = tmp_path / "prewrite-clone.db"
    with (
        closing(sqlite3.connect(tmp_path / "void.db")) as source,
        closing(sqlite3.connect(clone_path)) as destination,
    ):
        source.backup(destination)
    clone = SQLAlchemyRepository(f"sqlite+aiosqlite:///{clone_path}")
    try:
        context, directory = await _acknowledged_write(repository, tmp_path)
        assert len(await clone.get_active_candle_versions((_CHF,))) == 2
        with pytest.raises(VoidRefusedError):
            await candle_void.reconcile_void(_context(clone), directory)
        assert not (directory / candle_void.RECONCILIATION_FILE).exists()
        result = await candle_void.reconcile_void(context, directory)
        assert result.status == "reconciled_applied"
        assert len(await clone.get_active_candle_versions((_CHF,))) == 2
    finally:
        await clone.engine.dispose()


@pytest.mark.parametrize("field", ["run_id", "voted_digest"])
async def test_recovery_refuses_preparation_from_another_run(
    repository: SQLAlchemyRepository, tmp_path: Path, field: str
) -> None:
    """A foreign preparation cannot be attached to this run's version keys.

    Given recovery preparation bound to another run identity.
    When the recovery evidence is checked.
    Then the foreign preparation is refused.
    """
    context, directory = await _acknowledged_write(repository, tmp_path)
    prepared = directory / candle_void.PREPARED_FILE
    document = _document(prepared)
    document[field] = "foreign-run-artifact"
    prepared.write_text(json.dumps(document))
    with pytest.raises(VoidRefusedError):
        await candle_void.reconcile_void(context, directory)
    assert not (directory / candle_void.RECONCILIATION_FILE).exists()


@pytest.mark.parametrize("field", ["run_id", "voted_digest"])
async def test_recovery_refuses_a_foreign_prior_conclusion(
    repository: SQLAlchemyRepository, tmp_path: Path, field: str
) -> None:
    """An existing file's mere presence is insufficient evidence of this run's result.

    Given a prior conclusion bound to a different operation.
    When the recovery evidence is checked.
    Then the foreign conclusion is refused.
    """
    context, directory = await _acknowledged_write(repository, tmp_path)
    await candle_void.reconcile_void(context, directory)
    path = directory / candle_void.RECONCILIATION_FILE
    document = _document(path)
    document[field] = "foreign-run-artifact"
    path.write_text(json.dumps(document))
    before = path.read_bytes()
    with pytest.raises(VoidRefusedError):
        await candle_void.reconcile_void(context, directory)
    assert path.read_bytes() == before


async def test_recovery_refuses_an_empty_key_manifest(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """An empty set cannot turn vacuous equality into a successful repair receipt.

    Given prepared recovery evidence with an empty key manifest.
    When the recovery evidence is checked.
    Then the empty manifest is refused.
    """
    context, directory = await _acknowledged_write(repository, tmp_path)
    path = directory / candle_void.INTENT_FILE
    intent = _document(path)
    intent["keys"] = []
    path.write_text(json.dumps(intent))
    with pytest.raises(VoidRefusedError):
        await candle_void.reconcile_void(context, directory)
    assert not (directory / candle_void.RECONCILIATION_FILE).exists()


def test_utc_clock_returns_an_aware_utc_instant() -> None:
    """The default application clock always produces aware UTC.

    Given the default application clock.
    When it is read.
    Then the instant is timezone-aware UTC.
    """
    value = candle_void._utc_now()

    assert value.tzinfo is not None
    assert value.utcoffset() == timedelta(0)


def test_code_revision_hashes_every_deciding_module() -> None:
    """The approval binds repository, models, types, helpers and extra modules.

    Given the decision modules and an extra module.
    When their revision is built.
    Then every source hash is keyed by its module path.
    """
    revision = candle_void.void_code_revision([json_types])

    assert revision["snapper/data/repository.py"] == sha256_hex(
        Path(cast(str, candle_void.repository_module.__file__)).read_bytes()
    )
    assert revision["snapper/data/models.py"] == sha256_hex(
        Path(cast(str, candle_void.models.__file__)).read_bytes()
    )
    assert revision["snapper/data/candle_void_types.py"] == sha256_hex(
        Path(cast(str, candle_void.candle_void_types.__file__)).read_bytes()
    )
    assert revision["snapper/core/json_types.py"] == sha256_hex(
        Path(json_types.__file__).read_bytes()
    )
    assert revision["snapper/application/updaters/historical/candle_void.py"] == sha256_hex(
        Path(candle_void.__file__).read_bytes()
    )


@pytest.mark.parametrize("case", ["missing", "duplicate", "unreadable"])
def test_evidence_refusals_are_explicit(tmp_path: Path, case: str) -> None:
    """Missing, duplicated and unreadable cessation evidence never enters an approval.

    Given missing, duplicated or unreadable evidence.
    When evidence entries are built.
    Then the approval input is refused explicitly.
    """
    evidence = tmp_path / "evidence.json"
    evidence.write_text("synthetic")
    if case == "missing":
        files: tuple[EvidenceFile, ...] = ()
    elif case == "duplicate":
        files = (
            EvidenceFile("writer_cessation", evidence),
            EvidenceFile("writer_cessation", evidence),
        )
    else:
        files = (EvidenceFile("writer_cessation", tmp_path / "absent.json"),)

    with pytest.raises(VoidRefusedError):
        candle_void.evidence_entries(files)


@pytest.mark.parametrize("case", ["unknown", "exchange", "alias", "market", "trading"])
def test_target_refusals_cover_every_catalog_guard(tmp_path: Path, case: str) -> None:
    """Unknown, non-Kraken and still-live instruments are refused before planning.

    Given an unknown, non-Kraken or still-live target.
    When catalog guards run.
    Then the unsafe target is refused.
    """
    request = VoidRequest(instrument_public_ids=(_CHF,), evidence=(), output_dir=tmp_path)
    target = _target()
    if case == "unknown":
        request = replace(request, instrument_public_ids=(_CHF, "unknown"))
    elif case == "exchange":
        target = replace(target, exchange="polygon")
    elif case == "alias":
        target = replace(target, catalog=replace(target.catalog, active_alias_count=1))
    elif case == "market":
        target = replace(target, catalog=replace(target.catalog, can_market_data=True))
    else:
        target = replace(target, catalog=replace(target.catalog, can_trade=True))

    with pytest.raises(VoidRefusedError):
        candle_void.check_targets(request, [target])


async def test_build_plan_refuses_an_empty_cohort(
    repository: SQLAlchemyRepository, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An empty cohort produces a refusal and no dry-run artifact.

    Given no active cohort candles.
    When a plan is built.
    Then it refuses and writes no artifact.
    """
    request = _request(tmp_path)
    monkeypatch.setattr(repository, "get_active_candle_versions", AsyncMock(return_value=[]))

    with pytest.raises(VoidRefusedError, match="no active candle"):
        await candle_void.build_plan(_context(repository), request)

    assert list(request.output_dir.iterdir()) == []


async def test_build_plan_refuses_a_cohort_inside_the_author_age_window(
    repository: SQLAlchemyRepository, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A newest candle younger than sixty minutes blocks the vote.

    Given a newest candle inside the sixty-minute window.
    When a plan is built.
    Then the author-age refusal leaves no artifact.
    """
    request = _request(tmp_path)
    rows = await repository.get_active_candle_versions((_CHF,))
    young = {**rows[0], "timestamp": RUN_TIME}
    monkeypatch.setattr(repository, "get_active_candle_versions", AsyncMock(return_value=[young]))

    with pytest.raises(VoidRefusedError, match="minimum candle-author age"):
        await candle_void.build_plan(_context(repository), request)

    assert list(request.output_dir.iterdir()) == []


async def test_approval_digest_changes_with_every_bound_input(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """Preview, summary, evidence and source changes each produce another digest.

    Given a complete approval.
    When each bound input changes independently.
    Then every change produces a distinct digest.
    """
    context = _context(repository)
    request = _request(tmp_path)
    plan = await candle_void.build_plan(context, request)
    evidence = candle_void.evidence_entries(request.evidence)
    documents = [
        plan.approval,
        candle_void.approval_document(
            context, plan.targets, plan.preview + b"changed", plan.summary, evidence
        ),
        candle_void.approval_document(
            context, plan.targets, plan.preview, {**plan.summary, "candles": 0}, evidence
        ),
    ]
    request.evidence[0].path.write_text("changed evidence")
    documents.append(
        candle_void.approval_document(
            context,
            plan.targets,
            plan.preview,
            plan.summary,
            candle_void.evidence_entries(request.evidence),
        )
    )
    documents.append(
        candle_void.approval_document(
            replace(context, code_revision={"changed.py": "1" * 64}),
            plan.targets,
            plan.preview,
            plan.summary,
            evidence,
        )
    )

    digests = {sha256_hex(canonical_json(document)) for document in documents}

    assert len(digests) == len(documents)
    assert plan.digest in digests


async def test_load_vote_refuses_unreadable_and_wrongly_digested_artifacts(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """A vote needs readable files and the exact approval digest.

    Given unreadable files or a wrong approval digest.
    When the vote is loaded.
    Then it is refused before execution.
    """
    with pytest.raises(VoidRefusedError, match="cannot be read"):
        candle_void.load_vote(VoteBinding(tmp_path / "absent", "0" * 64))

    context = _context(repository)
    request = _request(tmp_path)
    await candle_void.plan_void(context, request)
    with pytest.raises(VoidRefusedError, match="does not match"):
        candle_void.load_vote(VoteBinding(request.output_dir, "0" * 64))


def test_preparation_refuses_a_run_already_reconciled(tmp_path: Path) -> None:
    """A late executor cannot prepare after a final recovery conclusion.

    Given an existing final reconciliation.
    When a late preparation runs.
    Then it refuses with reconciled and writes no preparation.
    """
    (tmp_path / candle_void.RECONCILIATION_FILE).write_text("{}")
    hook = candle_void._preparation_hook(tmp_path, {"run_id": str(uuid7())})

    with pytest.raises(CandleVoidRefusedError) as refusal:
        hook(RUN_TIME)

    assert refusal.value.code == "reconciled"
    assert not (tmp_path / candle_void.PREPARED_FILE).exists()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("run_id", None),
        ("run_id", "not-a-uuid"),
        ("voted_digest", None),
        ("voted_digest", "A" * 64),
    ],
)
async def test_recovery_request_rejects_an_invalid_identity(
    repository: SQLAlchemyRepository, field: str, value: str | None
) -> None:
    """Recovery binds a UUID run identity and lowercase sha256 vote digest.

    Given an invalid run UUID or vote digest.
    When recovery input is validated.
    Then the malformed identity is refused.
    """
    intent = _intent(_context(repository))
    intent[field] = value

    with pytest.raises(VoidRefusedError):
        candle_void._recovery_request(_context(repository), intent)


@pytest.mark.parametrize("case", ["non_mapping", "duplicate", "missing_field", "bad_id"])
async def test_recovery_request_rejects_invalid_version_keys(
    repository: SQLAlchemyRepository, case: str
) -> None:
    """Every recovery key must be a unique, complete physical-version identity.

    Given malformed or duplicate version keys.
    When recovery input is validated.
    Then the invalid manifest is refused.
    """
    intent = _intent(_context(repository))
    valid = candle_void.key_json(_key())
    if case == "non_mapping":
        keys: list[JsonValue] = ["not-a-mapping"]
    elif case == "duplicate":
        keys = [valid, valid]
    elif case == "missing_field":
        keys = [{"instrument_public_id": _CHF}]
    else:
        keys = [{**valid, "id": "not-an-integer"}]
    intent["keys"] = keys

    with pytest.raises(VoidRefusedError):
        candle_void._recovery_request(_context(repository), intent)


def test_conclude_classifies_every_fenced_reading() -> None:
    """Recovery distinguishes not-applied, applied and inconsistent version sets.

    Given active, closed, absent and mixed readings.
    When they are concluded.
    Then each receives its required recovery status.
    """
    active = CandleVersionReading(known_to=KNOWN_TO_MAX, active=True)
    closed = CandleVersionReading(known_to=RUN_TIME, active=False)
    other = CandleVersionReading(known_to=RUN_TIME - timedelta(seconds=1), active=False)

    assert candle_void.conclude({_key(): active}, None) == "not_applied"
    assert candle_void.conclude({_key(): closed, _key(1): closed}, RUN_TIME) == (
        "reconciled_applied"
    )
    assert candle_void.conclude({_key(): active, _key(1): active}, RUN_TIME) == "not_applied"
    assert candle_void.conclude({_key(): other, _key(1): other}, RUN_TIME) == "inconsistent"
    assert candle_void.conclude({_key(): active, _key(1): closed}, RUN_TIME) == "inconsistent"


def test_run_document_refusals_cover_missing_malformed_and_non_object_json(
    tmp_path: Path,
) -> None:
    """Recovery never accepts an unreadable or non-object artifact.

    Given a missing, malformed or non-object artifact.
    When it is read.
    Then the artifact is refused.
    """
    with pytest.raises(VoidRefusedError, match="cannot be read"):
        candle_void._read_document(tmp_path / "absent.json")

    malformed = tmp_path / "malformed.json"
    malformed.write_text("not-json")
    with pytest.raises(VoidRefusedError, match="cannot be read"):
        candle_void._read_document(malformed)

    array = tmp_path / "array.json"
    array.write_text("[]")
    with pytest.raises(VoidRefusedError, match="not a JSON object"):
        candle_void._read_document(array)


async def test_reconciliation_refuses_an_unknown_prior_status(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """An existing file is not a reusable conclusion without a terminal status.

    Given a prior file with a nonterminal status.
    When reconciliation reads it.
    Then the false conclusion is refused unchanged.
    """
    context, directory = await _acknowledged_write(repository, tmp_path)
    await candle_void.reconcile_void(context, directory)
    path = directory / candle_void.RECONCILIATION_FILE
    document = _document(path)
    document["status"] = "uncertain"
    path.write_text(json.dumps(document))

    with pytest.raises(VoidRefusedError, match="invalid status"):
        await candle_void.reconcile_void(context, directory)


async def test_write_refuses_a_vote_whose_preview_changed(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """A byte-different voted preview stops before intent or mutation.

    Given a byte-different voted preview.
    When the write rebuilds it.
    Then it refuses before intent or mutation.
    """
    context, writing, binding, plan = await _vote(repository, tmp_path)
    (binding.voted_dir / candle_void.PREVIEW_FILE).write_bytes(plan.preview + b"changed")

    with pytest.raises(VoidRefusedError, match="differs from the voted one"):
        await candle_void.write_void(context, writing, binding)

    assert not (writing.output_dir / candle_void.INTENT_FILE).exists()
    assert len(await repository.get_active_candle_versions((_CHF,))) == 2


async def test_write_refuses_a_new_candle_after_the_vote(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """A newly appearing active key changes the rebuilt plan and closes nothing.

    Given a new active candle after approval.
    When the write rebuilds the cohort.
    Then the changed plan refuses and closes nothing.
    """
    context, writing, binding, _ = await _vote(repository, tmp_path)
    await repository.upsert_candles([candle(_CHF, OPEN + timedelta(hours=1))])

    with pytest.raises(VoidRefusedError, match="differs from the voted one"):
        await candle_void.write_void(context, writing, binding)

    assert not (writing.output_dir / candle_void.INTENT_FILE).exists()
    assert len(await repository.get_active_candle_versions((_CHF,))) == 3


async def test_repository_refusal_is_recorded_and_can_be_reconciled_without_preparation(
    repository: SQLAlchemyRepository, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A repository refusal writes refusal.json; active keys reconcile as not-applied.

    Given a repository cohort refusal.
    When the write and recovery run.
    Then refusal is recorded and active keys conclude not-applied.
    """
    context, writing, binding, _ = await _vote(repository, tmp_path)
    monkeypatch.setattr(
        repository,
        "void_candle_versions",
        AsyncMock(side_effect=CandleVoidRefusedError("cohort_changed")),
    )

    outcome = await candle_void.write_void(context, writing, binding)

    assert outcome.status == "refused"
    assert outcome.record["reason"] == "cohort_changed"
    assert (writing.output_dir / candle_void.REFUSAL_FILE).exists()
    reconciled = await candle_void.reconcile_void(context, writing.output_dir)
    assert reconciled.status == "not_applied"
    assert reconciled.record["active"] == 2
    assert len(await repository.get_active_candle_versions((_CHF,))) == 2


async def test_write_retries_a_lock_timeout_before_preparation(
    repository: SQLAlchemyRepository, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A lost fence before prepared.json retries with the bounded backoff.

    Given one lock timeout before preparation.
    When the write retries.
    Then one bounded sleep precedes acknowledged execution.
    """
    context, writing, binding, plan = await _vote(repository, tmp_path)
    void = AsyncMock(
        side_effect=[_db_error("55P03"), CandleVoidResult(void_at=RUN_TIME, closed=plan.keys)]
    )
    monkeypatch.setattr(repository, "void_candle_versions", void)
    sleep = AsyncMock()

    outcome = await candle_void.write_void(replace(context, sleep=sleep), writing, binding)

    assert outcome.status == "acknowledged"
    assert void.await_count == 2
    sleep.assert_awaited_once_with(0.5)


async def test_write_does_not_retry_a_lock_timeout_after_preparation(
    repository: SQLAlchemyRepository, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Once prepared.json exists, a lock failure becomes a durable failure record.

    Given a lock timeout after preparation.
    When the write handles it.
    Then failure is durable and no retry occurs.
    """
    context, writing, binding, _ = await _vote(repository, tmp_path)

    async def fail_after_preparation(request: RepositoryVoidRequest) -> CandleVoidResult:
        """Prepare the run and then simulate a lost fence acknowledgement."""
        request.on_prepared(RUN_TIME)
        raise _db_error("55P03")

    monkeypatch.setattr(repository, "void_candle_versions", fail_after_preparation)
    sleep = AsyncMock()

    outcome = await candle_void.write_void(replace(context, sleep=sleep), writing, binding)

    assert outcome.status == "failed"
    assert outcome.record["outcome"] == "lock_timeout"
    assert outcome.record["sqlstate"] == "55P03"
    assert (writing.output_dir / candle_void.FAILURE_FILE).exists()
    sleep.assert_not_awaited()


async def test_write_delegates_an_uncertain_commit_to_reconciliation(
    repository: SQLAlchemyRepository, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An uncertain database acknowledgement goes directly to fenced recovery.

    Given an uncertain commit acknowledgement.
    When the write handles it.
    Then fenced reconciliation takes over without a failure artifact.
    """
    context, writing, binding, _ = await _vote(repository, tmp_path)
    monkeypatch.setattr(
        repository, "void_candle_versions", AsyncMock(side_effect=RuntimeError("synthetic"))
    )
    recovered = AsyncMock(
        return_value=VoidOutcome(status="uncertain", record={"status": "uncertain"})
    )
    monkeypatch.setattr(candle_void, "reconcile_void", recovered)

    outcome = await candle_void.write_void(context, writing, binding)

    assert outcome.status == "uncertain"
    recovered.assert_awaited_once_with(context, writing.output_dir)
    assert not (writing.output_dir / candle_void.FAILURE_FILE).exists()


async def test_reconciliation_retries_the_fence_then_stays_uncertain(
    repository: SQLAlchemyRepository, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Three lost fence attempts write no conclusion and preserve bounded sleeps.

    Given three lost fence attempts.
    When recovery retries.
    Then two bounded sleeps end uncertain with no conclusion.
    """
    context = _context(repository)
    directory = tmp_path / "run"
    directory.mkdir()
    (directory / candle_void.INTENT_FILE).write_bytes(canonical_json(_intent(context)))
    observe = AsyncMock(side_effect=[_db_error("55P03") for _ in range(3)])
    monkeypatch.setattr(repository, "observe_candle_void", observe)
    sleep = AsyncMock()

    outcome = await candle_void.reconcile_void(replace(context, sleep=sleep), directory)

    assert outcome.status == "uncertain"
    assert observe.await_count == 3
    assert [call.args[0] for call in sleep.await_args_list] == [0.5, 1.0]
    assert not (directory / candle_void.RECONCILIATION_FILE).exists()


async def test_reconciliation_overtaking_blocks_a_late_preparation(
    repository: SQLAlchemyRepository, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Recovery's final not-applied prevents the overtaken executor from closing rows.

    Given recovery concludes before the executor prepares.
    When the executor resumes.
    Then reconciled refusal leaves every row active.
    """
    context, writing, binding, _ = await _vote(repository, tmp_path)

    async def overtake(request: RepositoryVoidRequest) -> CandleVoidResult:
        """Reconcile the intent before the original executor can prepare."""
        conclusion = await candle_void.reconcile_void(context, writing.output_dir)
        assert conclusion.status == "not_applied"
        request.on_prepared(RUN_TIME)
        raise AssertionError("unreachable")

    monkeypatch.setattr(repository, "void_candle_versions", overtake)

    outcome = await candle_void.write_void(context, writing, binding)

    assert outcome.status == "refused"
    assert outcome.record["reason"] == "reconciled"
    assert len(await repository.get_active_candle_versions((_CHF,))) == 2


async def test_a_failed_receipt_write_is_recovered_by_reconciliation(
    repository: SQLAlchemyRepository, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A committed run whose receipt cannot be written recovers its exact applied state.

    Given a committed run whose receipt write fails.
    When recovery observes it.
    Then the exact applied outcome is recovered.
    """
    context, writing, binding, _ = await _vote(repository, tmp_path)
    original_settle = candle_void._settle

    def fail_receipt(directory: Path, name: str, status: str, record: JsonObject) -> VoidOutcome:
        """Fail only the acknowledgement artifact, after the repository commit."""
        if name == candle_void.RECEIPT_FILE:
            raise OSError("synthetic receipt failure")
        return original_settle(directory, name, status, record)

    monkeypatch.setattr(candle_void, "_settle", fail_receipt)
    with pytest.raises(OSError, match="synthetic receipt failure"):
        await candle_void.write_void(context, writing, binding)

    outcome = await candle_void.reconcile_void(context, writing.output_dir)

    assert outcome.status == "reconciled_applied"
    assert outcome.record["expected"] == 2
    assert await repository.get_active_candle_versions((_CHF,)) == []


async def test_a_second_write_of_the_same_vote_refuses_the_empty_cohort(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """An acknowledged vote cannot be applied a second time.

    Given an already applied vote.
    When its write is repeated.
    Then the empty cohort refuses another intent.
    """
    context, writing, binding, _ = await _vote(repository, tmp_path)
    outcome = await candle_void.write_void(context, writing, binding)
    assert outcome.status == "acknowledged"
    second_dir = writing.output_dir.parent / "second-write"
    second_dir.mkdir()
    second = replace(writing, output_dir=second_dir)

    with pytest.raises(VoidRefusedError, match="no active candle"):
        await candle_void.write_void(context, second, binding)

    assert not (second_dir / candle_void.INTENT_FILE).exists()


async def test_reconciliation_accepts_a_new_image_and_records_its_revision(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """Recovery binds the run and vote, while recording the reconciler's own source.

    Given a run created by one image and read by another.
    When recovery runs twice.
    Then the new reconciler is recorded and final bytes stay immutable.
    """
    context, directory = await _acknowledged_write(repository, tmp_path)
    revision = {"new-image/candle_void.py": "b" * 64}

    outcome = await candle_void.reconcile_void(replace(context, code_revision=revision), directory)

    assert outcome.status == "reconciled_applied"
    assert outcome.record["reconciler_code_revision"] == revision
    before = (directory / candle_void.RECONCILIATION_FILE).read_bytes()
    again = await candle_void.reconcile_void(
        replace(context, code_revision={"third-image.py": "c" * 64}), directory
    )
    assert again == outcome
    assert (directory / candle_void.RECONCILIATION_FILE).read_bytes() == before


def test_missing_preparation_with_a_non_active_key_is_inconsistent() -> None:
    """Without prepared.json, closed or absent expected versions are not not-applied.

    Given closed or absent keys without preparation.
    When they are concluded.
    Then both are inconsistent rather than not-applied.
    """
    closed = CandleVersionReading(known_to=RUN_TIME, active=False)
    absent = CandleVersionReading(known_to=None, active=False)

    assert candle_void.conclude({_key(): closed}, None) == "inconsistent"
    assert candle_void.conclude({_key(): absent}, None) == "inconsistent"


def test_directory_fsync_unsupported_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unsupported directory sync refuses to claim durable evidence.

    Given a platform without directory descriptors.
    When a directory sync is attempted.
    Then its failure propagates without a durability claim.
    """

    def refuse_open(path: object, flags: int) -> int:
        """Reject a directory descriptor as an unsupported platform would."""
        raise PermissionError

    monkeypatch.setattr(candle_void, "_fsync_directory", _REAL_FSYNC_DIRECTORY)
    monkeypatch.setattr(candle_void.os, "open", refuse_open)

    with pytest.raises(PermissionError):
        candle_void._fsync_directory(Path("unused"))


@pytest.mark.parametrize("fsync_fails", [False, True])
def test_directory_fsync_opens_syncs_and_always_closes_descriptor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fsync_fails: bool
) -> None:
    """Directory fsync opens the path read-only, syncs it, and closes the descriptor.

    Given fake os.open, os.fsync and os.close where fsync either succeeds or raises.
    When _fsync_directory runs on a directory path.
    Then the directory descriptor is synced and always closed even when fsync fails.
    """
    sentinel = 987654321
    opened: list[tuple[Path, int]] = []
    synced: list[int] = []
    closed: list[int] = []
    real_open = os.open
    real_fsync = os.fsync
    real_close = os.close

    def fake_open(
        target: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        """Return a sentinel descriptor for the target directory."""
        if dir_fd is None and isinstance(target, (str, os.PathLike)) and Path(target) == tmp_path:
            opened.append((Path(target), flags))
            return sentinel
        if dir_fd is None:
            return real_open(target, flags, mode)
        return real_open(target, flags, mode, dir_fd=dir_fd)

    def fake_fsync(descriptor: int) -> None:
        """Record the sentinel sync and optionally simulate a sync failure."""
        if descriptor == sentinel:
            synced.append(descriptor)
            if fsync_fails:
                raise OSError("synthetic fsync failure")
            return
        real_fsync(descriptor)

    def fake_close(descriptor: int) -> None:
        """Record the sentinel close and delegate real descriptors."""
        if descriptor == sentinel:
            closed.append(descriptor)
            return
        real_close(descriptor)

    monkeypatch.setattr(candle_void, "_fsync_directory", _REAL_FSYNC_DIRECTORY)
    monkeypatch.setattr(candle_void.os, "open", fake_open)
    monkeypatch.setattr(candle_void.os, "fsync", fake_fsync)
    monkeypatch.setattr(candle_void.os, "close", fake_close)

    if fsync_fails:
        with pytest.raises(OSError, match="synthetic fsync failure"):
            candle_void._fsync_directory(tmp_path)
    else:
        candle_void._fsync_directory(tmp_path)

    assert opened == [(tmp_path, os.O_RDONLY)]
    assert synced == [sentinel]
    assert closed == [sentinel]


def test_write_durable_unlinks_new_file_when_directory_open_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A directory-open PermissionError removes the unsynced file and fails closed.

    Given a parent directory whose os.open raises PermissionError.
    When write_durable writes a new file inside that directory.
    Then PermissionError propagates and the newly created file is unlinked.
    """
    target = tmp_path / "prepared.json"
    real_open = os.open

    def fake_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        """Refuse opening the target directory while allowing the file open."""
        if dir_fd is None and isinstance(path, (str, os.PathLike)) and Path(path) == tmp_path:
            raise PermissionError("directory descriptor unsupported")
        if dir_fd is None:
            return real_open(path, flags, mode)
        return real_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(candle_void, "_fsync_directory", _REAL_FSYNC_DIRECTORY)
    monkeypatch.setattr(candle_void.os, "open", fake_open)

    with pytest.raises(PermissionError, match="directory descriptor unsupported"):
        candle_void.write_durable(target, b'{"status":"prepared"}')

    assert not target.exists()


@pytest.mark.skipif(sys.platform == "win32", reason="directory fsync unsupported by design")
def test_write_durable_persists_new_file_and_syncs_parent_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Real-filesystem durable writes persist bytes exclusively with mode 0600.

    Given a POSIX host supporting directory descriptors.
    When write_durable writes a new file and is invoked a second time on the same path.
    Then the file has owner-only permissions, intact content, and refuses replacement.
    """
    monkeypatch.setattr(candle_void, "_fsync_directory", _REAL_FSYNC_DIRECTORY)
    target = tmp_path / "prepared.json"
    payload = b'{"status":"prepared"}'

    previous_umask = os.umask(0)
    try:
        candle_void.write_durable(target, payload)
    finally:
        os.umask(previous_umask)

    assert target.read_bytes() == payload
    assert target.stat().st_mode & 0o777 == 0o600
    with pytest.raises(FileExistsError):
        candle_void.write_durable(target, b"replacement")
    assert target.read_bytes() == payload


async def test_preparation_directory_fsync_failure_never_reports_success_or_closes_rows(
    repository: SQLAlchemyRepository, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed directory sync aborts before UPDATE and leaves the run uncertain.

    Given a failing parent-directory sync.
    When preparation and recovery run.
    Then the run stays uncertain without artifacts or closed rows.
    """
    context, writing, binding, _ = await _vote(repository, tmp_path)

    def fail_directory_sync(path: Path) -> None:
        """Simulate a filesystem without durable directory entries."""
        raise OSError("unsupported filesystem")

    monkeypatch.setattr(candle_void, "_fsync_directory", fail_directory_sync)

    outcome = await candle_void.write_void(context, writing, binding)

    assert outcome.status == "uncertain"
    assert not (writing.output_dir / candle_void.PREPARED_FILE).exists()
    assert not (writing.output_dir / candle_void.RECONCILIATION_FILE).exists()
    assert len(await repository.get_active_candle_versions((_CHF,))) == 2
