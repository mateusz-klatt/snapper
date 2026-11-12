"""Exercise the candle void's approval, original database binding and recovery artifacts."""

import json
import sqlite3
from collections.abc import AsyncIterator
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest
import pytest_asyncio

from snapper.application.updaters.historical import candle_void
from snapper.application.updaters.historical.candle_repair import database_endpoint
from snapper.application.updaters.historical.candle_repair import sha256_hex
from snapper.application.updaters.historical.candle_void import EvidenceFile
from snapper.application.updaters.historical.candle_void import VoidContext
from snapper.application.updaters.historical.candle_void import VoidRefusedError
from snapper.application.updaters.historical.candle_void import VoidRequest
from snapper.application.updaters.historical.candle_void import VoteBinding
from snapper.core.json_types import JsonObject
from snapper.data.repository import SQLAlchemyRepository
from tests.helpers.candle_repair import OPEN
from tests.helpers.candle_repair import RUN_TIME
from tests.helpers.candle_repair import candle
from tests.helpers.candle_repair import seed_instrument

_CHF = "00000000-0000-4000-8000-000000000c51"


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
