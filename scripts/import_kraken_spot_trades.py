"""Import missing Kraken Spot trades from verified recovery evidence."""

import argparse
import asyncio
import hashlib
import json
import re
import sys
from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from itertools import batched
from pathlib import Path
from typing import TypedDict
from typing import cast
from uuid import uuid7

from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine

from snapper.application.updaters.historical.kraken_spot_trade_evidence import (
    KrakenSpotTradeEvidence,
)
from snapper.application.updaters.historical.kraken_spot_trade_evidence import (
    KrakenSpotTradeEvidenceWindow,
)
from snapper.application.updaters.historical.kraken_spot_trade_evidence import confined_input
from snapper.application.updaters.historical.kraken_spot_trade_evidence import (
    load_kraken_spot_trade_evidence,
)
from snapper.application.updaters.historical.kraken_spot_trade_evidence import resolve_input_root
from snapper.application.updaters.historical.kraken_spot_trade_reconciliation import (
    KrakenSpotTradeRecoveryPlan,
)
from snapper.application.updaters.historical.kraken_spot_trade_reconciliation import (
    StoredKrakenSpotTrade,
)
from snapper.application.updaters.historical.kraken_spot_trade_reconciliation import (
    plan_kraken_spot_trade_recovery,
)
from snapper.cli.operator_support import configured_repository
from snapper.cli.operator_support import failure_text
from snapper.config.settings import get_bootstrap_settings
from snapper.core.types import ExchangeEnum
from snapper.core.types import MarketDataTypeEnum
from snapper.data.models import Trade
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import TradeUpsertRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.topics.builders import market_topic

_CHUNK_SIZE = 500


class _Arguments(argparse.Namespace):
    """Parsed options for one importer invocation."""

    input_root: Path
    evidence: Path
    evidence_sha256: str
    start: str
    end: str
    apply: bool
    expect_digest: str | None
    receipt: Path | None


class _ChunkCounts(TypedDict):
    """Rows inserted or already present in a committed chunk."""

    inserted: int
    already_present: int


class _Receipt(TypedDict):
    """Audit record for an attempted apply after the digest was accepted."""

    window: dict[str, str]
    evidence_sha256: str
    plan_digest: str
    chunks: list[_ChunkCounts]
    total_inserted: int
    total_already_present: int
    started_at: str
    finished_at: str
    verified_missing: int | None


def _arguments(argv: list[str] | None) -> _Arguments:
    """Parse the evidence and require both guards for an apply."""
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument(
        "--input-root", type=Path, default=Path("."), help="Allowed input directory"
    )
    parser.add_argument("--evidence", type=Path, required=True, help="Verified Kraken Spot CSV")
    parser.add_argument("--evidence-sha256", required=True, help="Expected SHA-256 of the evidence")
    parser.add_argument("--start", required=True, help="Inclusive UTC execution time")
    parser.add_argument("--end", required=True, help="Exclusive UTC execution time")
    parser.add_argument("--apply", action="store_true", help="Insert rows from the reviewed plan")
    parser.add_argument("--expect-digest", type=str.lower, help="Reviewed plan SHA-256")
    parser.add_argument("--receipt", type=Path, help="New file for the apply receipt")
    args = parser.parse_args(argv, namespace=_Arguments())
    if args.apply:
        if args.expect_digest is None or not re.fullmatch(r"[0-9a-f]{64}", args.expect_digest):
            parser.error("--apply requires a SHA-256 --expect-digest")
        if args.receipt is None:
            parser.error("--apply requires --receipt")
    elif args.expect_digest is not None or args.receipt is not None:
        parser.error("--expect-digest and --receipt require --apply")
    return args


async def _compare(
    session: AsyncSession, candidates: Sequence[KrakenSpotTradeEvidence]
) -> KrakenSpotTradeRecoveryPlan:
    """Compare evidence with all stored matches, including out-of-window executions."""
    ids_by_instrument: dict[str, set[str]] = {}
    for row in candidates:
        ids_by_instrument.setdefault(row.instrument_public_id, set()).add(row.trade_id)

    stored: list[StoredKrakenSpotTrade] = []
    for instrument, trade_ids in ids_by_instrument.items():
        for trade_id_chunk in batched(sorted(trade_ids), _CHUNK_SIZE, strict=False):
            statement = select(
                Trade.trade_id, Trade.executed_at, Trade.price, Trade.size, Trade.side
            )
            matches = await session.execute(
                statement.where(
                    Trade.instrument_public_id == instrument,
                    Trade.trade_id.in_(trade_id_chunk),
                )
            )
            stored.extend(
                StoredKrakenSpotTrade(
                    instrument, cast(str, trade_id), executed_at, price, size, side
                )
                for trade_id, executed_at, price, size, side in matches
            )
    return plan_kraken_spot_trade_recovery(candidates, stored)


async def _read_plan(
    repository: SQLAlchemyRepository, evidence: KrakenSpotTradeEvidenceWindow
) -> KrakenSpotTradeRecoveryPlan:
    """Read the full live comparison in one read-only snapshot.

    Open SQLite in read-only file mode to bypass the repository's writer pragmas.
    """
    if repository.dialect_name == "postgresql":
        async with repository.session() as session:
            await session.execute(
                text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            )
            return await _compare(session, evidence.rows)
    if repository.dialect_name != "sqlite":
        raise ValueError("Trade import supports PostgreSQL and SQLite")

    url = repository.engine.url
    database = url.database or ""
    if url.query.get("uri") != "true" or not database.startswith("file:"):
        database = Path(database).absolute().as_uri()
    reader = create_async_engine(
        url.set(database=database).update_query_dict({"mode": "ro", "uri": "true"})
    )
    try:
        async with AsyncSession(reader) as session:
            await session.execute(text("BEGIN"))
            return await _compare(session, evidence.rows)
    finally:
        await reader.dispose()


def _window(evidence: KrakenSpotTradeEvidenceWindow) -> dict[str, str]:
    """Serialize the canonical UTC window bounds."""
    return {"start": evidence.start.isoformat(), "end": evidence.end.isoformat()}


def _plan_digest(
    evidence: KrakenSpotTradeEvidenceWindow, missing: Sequence[KrakenSpotTradeEvidence]
) -> str:
    """Hash the window, evidence SHA-256, and sorted normalized missing rows."""
    payload = {
        "window": _window(evidence),
        "evidence_sha256": evidence.evidence_sha256,
        "missing": [
            (
                row.instrument_public_id,
                row.trade_id,
                row.symbol,
                row.executed_at.isoformat(),
                str(row.price),
                str(row.size),
                row.side,
            )
            for row in missing
        ],
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _trade_row(row: KrakenSpotTradeEvidence, tracker: SequenceTracker) -> TradeUpsertRow:
    """Materialize provenance the same way as historical replay receives a trade."""
    received_at = datetime.now(UTC)
    topic = market_topic(
        ExchangeEnum.PAPER,
        row.symbol,
        MarketDataTypeEnum.TRADES,
        source_exchange=ExchangeEnum.KRAKEN,
    )
    return TradeUpsertRow(
        instrument_public_id=row.instrument_public_id,
        trade_id=row.trade_id,
        executed_at=row.executed_at,
        price=float(row.price),
        size=float(row.size),
        side=row.side,
        timestamp=received_at,
        session_id=tracker.session_id,
        sequence_id=tracker.next_sequence(topic),
        public_id=str(uuid7()),
    )


async def _write_chunk(
    repository: SQLAlchemyRepository,
    candidates: tuple[KrakenSpotTradeEvidence, ...],
    tracker: SequenceTracker,
) -> _ChunkCounts:
    """Insert one chunk without updating existing rows and verify it by readback.

    PostgreSQL takes no table lock, so live trade writers are never blocked. The
    insert skips any identity that already exists, and a row the live feed stores
    concurrently is counted as already present instead of failing the chunk.
    """
    async with repository.session() as session:
        if repository.dialect_name == "postgresql":
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
            await session.execute(text("SET LOCAL lock_timeout = '2s'"))
            await session.execute(text("SET LOCAL statement_timeout = '15s'"))
        else:
            await session.execute(text("BEGIN IMMEDIATE"))
        before = await _compare(session, candidates)
        inserted = 0
        if before.missing:
            rows = [_trade_row(row, tracker) for row in before.missing]
            inserted = await repository.upsert_trades(
                rows, session=session, enqueue_integrity_work=True
            )
        after = await _compare(session, candidates)
        if after.missing or inserted > len(before.missing):
            raise ValueError("Trade import chunk did not match its live readback")
        await session.commit()
        return {"inserted": inserted, "already_present": after.already_present - inserted}


async def _apply(
    repository: SQLAlchemyRepository,
    evidence: KrakenSpotTradeEvidenceWindow,
    plan: KrakenSpotTradeRecoveryPlan,
    digest: str,
    receipt_path: Path,
) -> _Receipt:
    """Reserve the receipt before writing and record committed chunks on failure."""
    started_at = datetime.now(UTC).isoformat()
    chunks: list[_ChunkCounts] = []
    verified_missing = None
    tracker = SequenceTracker()
    with receipt_path.open("x", encoding="utf-8") as stream:
        try:
            for candidates in batched(plan.missing, _CHUNK_SIZE, strict=False):
                chunks.append(await _write_chunk(repository, candidates, tracker))
            verified_missing = len((await _read_plan(repository, evidence)).missing)
            if verified_missing:
                raise ValueError(
                    f"Trade import verification found {verified_missing} missing trades"
                )
        finally:
            receipt = _Receipt(
                window=_window(evidence),
                evidence_sha256=evidence.evidence_sha256,
                plan_digest=digest,
                chunks=chunks,
                total_inserted=sum(chunk["inserted"] for chunk in chunks),
                total_already_present=plan.already_present
                + sum(chunk["already_present"] for chunk in chunks),
                started_at=started_at,
                finished_at=datetime.now(UTC).isoformat(),
                verified_missing=verified_missing,
            )
            json.dump(receipt, stream, sort_keys=True)
            stream.write("\n")
    return receipt


def _preview(
    evidence: KrakenSpotTradeEvidenceWindow, plan: KrakenSpotTradeRecoveryPlan, digest: str
) -> None:
    """Print deterministic missing counts for all symbols covered by the evidence."""
    counts = dict.fromkeys((row.symbol for row in evidence.rows), 0)
    for row in plan.missing:
        counts[row.symbol] += 1
    print(
        json.dumps(
            {
                "missing_by_symbol": counts,
                "total_missing": len(plan.missing),
                "plan_digest": digest,
            },
            sort_keys=True,
        )
    )


async def _run(args: _Arguments, evidence: KrakenSpotTradeEvidenceWindow) -> int:
    """Plan against the configured database, then run only the approved mode."""
    repository = configured_repository(
        get_bootstrap_settings().db_url, application_name="snapper:import-kraken-spot-trades"
    )
    try:
        live = await _read_plan(repository, evidence)
        missing = tuple(
            sorted(
                live.missing,
                key=lambda row: (row.executed_at, row.instrument_public_id, row.trade_id),
            )
        )
        plan = KrakenSpotTradeRecoveryPlan(missing, live.already_present)
        digest = _plan_digest(evidence, plan.missing)
        if args.receipt is None:
            _preview(evidence, plan, digest)
            return 0
        if digest != args.expect_digest:
            print("Trade import refused: plan digest differs from --expect-digest", file=sys.stderr)
            return 2
        receipt = await _apply(repository, evidence, plan, digest, args.receipt)
        print(json.dumps(receipt, sort_keys=True))
        return 0
    finally:
        await repository.engine.dispose()


def main(argv: list[str] | None = None) -> int:
    """Import only the missing trades from a verified Kraken Spot capture.

    Args:
        argv: Command arguments, or None to use process arguments.

    Returns:
        Zero on success, two for a changed plan, or one for other failures.
    """
    args = _arguments(argv)
    try:
        path = confined_input(resolve_input_root(args.input_root), args.evidence)
        evidence = load_kraken_spot_trade_evidence(path, args.evidence_sha256, args.start, args.end)
        return asyncio.run(_run(args, evidence))
    except (OSError, ValueError) as error:
        print(f"Trade import refused: {error}", file=sys.stderr)
    except SQLAlchemyError as error:
        print(f"Trade import failed: {failure_text(error)}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
