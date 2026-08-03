"""Operator proof-backfill report, batching, and convergence witnesses."""

from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast

import pytest
from sqlalchemy import select

from snapper.application.portfolio.fx_conversion_shadow import FxShadowEvaluation
from snapper.application.portfolio.fx_proof_backfill import FxProofBackfillDiscovery
from snapper.application.portfolio.fx_proof_backfill import FxProofBackfillRequirement
from snapper.application.portfolio.fx_proof_backfill import _checkpoint_start
from snapper.application.portfolio.fx_proof_backfill import _consumer_evaluations
from snapper.application.portfolio.fx_proof_backfill import _is_pinned
from snapper.application.portfolio.fx_proof_backfill import _manifest_digest
from snapper.application.portfolio.fx_proof_backfill import _trusted_execution_context
from snapper.application.portfolio.fx_proof_backfill import _union_consumers
from snapper.application.portfolio.fx_proof_backfill import _write_checkpoint
from snapper.application.portfolio.fx_proof_backfill import discover_fx_proof_backfill
from snapper.application.portfolio.fx_proof_backfill import run_fx_proof_backfill
from snapper.application.portfolio.pnl_anchor_identity import portfolio_pnl_anchor_public_id
from snapper.data.models import PortfolioPnlPoint
from snapper.data.repository import Repository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import FxProofBackfillConsumer
from snapper.data.repository_types import InstrumentSymbolRefRow
from snapper.data.repository_types import PnlFxRateRow
from snapper.data.repository_types import PnlTimelineExecutionPrefix
from snapper.data.repository_types import PnlTimelineOpeningExecutionRow
from snapper.data.repository_types import PortfolioPnlAnchorRow

_MINUTE = datetime(2026, 8, 2, 12, 0, tzinfo=UTC)
_AS_OF = datetime(2026, 8, 2, 13, 0, tzinfo=UTC)


def _row() -> PnlFxRateRow:
    """Build one exact candle version consumed by the raw election."""
    return {
        "base": "EUR",
        "quote": "USD",
        "exchange": "kraken",
        "open_at": _MINUTE - timedelta(minutes=1),
        "close": 1.125,
        "native_symbol": "EUR/USD",
        "instrument_public_id": "00000000-0000-7000-8000-000000000802",
        "candle_id": 42,
        "candle_public_id": "00000000-0000-7000-8000-000000000803",
        "candle_session_id": "00000000-0000-7000-8000-000000000801",
        "candle_sequence_id": 7,
        "candle_timestamp": _MINUTE - timedelta(minutes=1),
        "candle_known_to": datetime.max.replace(tzinfo=UTC),
    }


def _requirement() -> FxProofBackfillRequirement:
    """Build one unpinned durable singleton-minute requirement."""
    row = _row()
    evaluation = FxShadowEvaluation(
        scope_kind="shared_pair",
        consumer_instrument_public_id=None,
        pair=("EUR", "USD"),
        target_currency="USD",
        required_minutes=frozenset({_MINUTE}),
        candidate_planes=frozenset({("EUR", "USD", "kraken")}),
        selected_plane=("EUR", "USD", "kraken"),
        requested_knowledge_at=_AS_OF,
        rows=(row,),
        authoritative_rates={("EUR", "USD", "kraken", _MINUTE): row["close"]},
    )
    return FxProofBackfillRequirement(
        wallet_public_id="00000000-0000-7000-8000-000000000810",
        valuation_ccy="USD",
        calculation_version="5B.2",
        consumer_public_id="00000000-0000-7000-8000-000000000811",
        evaluation=evaluation,
        pinned=False,
    )


async def _repository(tmp_path: Path) -> SQLAlchemyRepository:
    """Create an isolated repository carrying the real F1 identity contract."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'backfill.db'}")
    await repository.create_all()
    return repository


@pytest.mark.asyncio
async def test_report_is_no_write_and_apply_twice_converges(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Report writes nothing and a complete rerun reuses the canonical proof.

    Given one durable singleton requirement and the real F1 repository
    When report mode runs once and apply mode runs twice
    Then report creates nothing and the second apply reports reuse without creation
    """
    repository = await _repository(tmp_path)

    async def discover(_: object) -> FxProofBackfillDiscovery:
        """Return the same durable requirement on every operator invocation."""
        return FxProofBackfillDiscovery((_requirement(),), ())

    monkeypatch.setattr(
        "snapper.application.portfolio.fx_proof_backfill.discover_fx_proof_backfill",
        discover,
    )
    checkpoint = tmp_path / "checkpoint.json"
    report = await run_fx_proof_backfill(repository, False, checkpoint)
    first = await run_fx_proof_backfill(repository, True, checkpoint)
    second = await run_fx_proof_backfill(repository, True, checkpoint)
    assert report.processed == 0
    assert report.metrics.creation == 0
    assert not checkpoint.exists()
    assert first.metrics.creation == 1
    assert first.metrics.reuse == 0
    assert second.metrics.creation == 0
    assert second.metrics.reuse == 1
    await repository.engine.dispose()


def test_checkpoint_validation_and_execution_identity_refusal(tmp_path: Path) -> None:
    """Checkpoint drift and unprovable execution identity both refuse explicitly.

    Given absent, valid, malformed, and drifted checkpoints plus untrusted lineage
    When the cursor and trusted execution context are validated
    Then only the valid cursor is accepted and semantic identity loss is named
    """
    path = tmp_path / "nested" / "checkpoint.json"
    digest = _manifest_digest([_requirement()])
    assert _checkpoint_start(path, digest) == 0
    _write_checkpoint(path, digest, 3)
    assert _checkpoint_start(path, digest) == 3
    path.write_text("[]", encoding="utf-8")
    with pytest.raises(ValueError, match="does not match"):
        _checkpoint_start(path, digest)
    path.write_text('{"cursor":-1,"manifest_digest":"same"}', encoding="utf-8")
    with pytest.raises(ValueError, match="does not match"):
        _checkpoint_start(path, digest)
    path.write_text(f'{{"cursor":-1,"manifest_digest":"{digest}"}}', encoding="utf-8")
    with pytest.raises(ValueError, match="cursor"):
        _checkpoint_start(path, digest)
    with pytest.raises(ValueError, match="identity cannot be proven"):
        _trusted_execution_context([_execution()], cast(list[InstrumentSymbolRefRow], []))
    older = replace(_consumer(), watermarks={"coinbase": 3})
    newer = replace(
        _consumer(),
        public_id="00000000-0000-7000-8000-000000000825",
        knowledge_at=_AS_OF + timedelta(minutes=1),
        watermarks={"coinbase": 2, "kraken": 4},
    )
    different = replace(newer, calculation_version="5A.13")
    unions = _union_consumers([newer, older, different])
    assert len(unions) == 2
    assert unions[1].public_id == newer.public_id
    assert unions[1].watermarks == {"coinbase": 3, "kraken": 4}


class _DiscoveryRepository:
    """Minimal repository surface for exact backfill discovery tests."""

    def __init__(self, artifact_present: bool = False, identity_present: bool = True) -> None:
        self.artifact_present = artifact_present
        self.identity_present = identity_present

    async def get_pnl_timeline_execution_prefix_at_watermarks(
        self,
        wallet_public_id: str,
        mode: str,
        watermarks: dict[str, int],
        as_of: datetime,
    ) -> PnlTimelineExecutionPrefix:
        """Return one already-certified execution row."""
        return {"watermarks": watermarks, "executions": [_execution()], "annulments": []}

    async def get_instrument_symbol_refs(
        self, instrument_public_ids: list[str], as_of: datetime
    ) -> list[InstrumentSymbolRefRow]:
        """Return the immutable currency and venue identity for the execution."""
        return [_ref()] if self.identity_present else []

    async def get_pnl_fx_rate_exchanges(
        self,
        pairs: list[tuple[str, str]],
        start: datetime,
        end: datetime,
        as_of: datetime,
    ) -> list[tuple[str, str, str]]:
        """Expose the one eligible shared EUR/USD plane."""
        return [("EUR", "USD", "kraken")]

    async def get_pnl_fx_rate_candles(
        self,
        planes: list[tuple[str, str, str]],
        start: datetime,
        end: datetime,
        as_of: datetime,
    ) -> list[PnlFxRateRow]:
        """Return exact evidence for the requested singleton minute."""
        return [_row()]

    async def list_fx_proof_backfill_consumers(self) -> list[FxProofBackfillConsumer]:
        """Return one active durable sample cut."""
        return [_consumer()]

    async def get_latest_visible_fx_conversion_artifact(
        self, query: object, as_of: datetime
    ) -> object | None:
        """Represent whether the successful identity is already pinned."""
        return object() if self.artifact_present else None


def _execution() -> PnlTimelineOpeningExecutionRow:
    """Build one EUR-priced execution requiring an exact conversion minute."""
    return {
        "public_id": "00000000-0000-7000-8000-000000000820",
        "instrument_public_id": "00000000-0000-7000-8000-000000000821",
        "exchange": "coinbase",
        "scope_sequence": 1,
        "order_public_id": "00000000-0000-7000-8000-000000000822",
        "side": "buy",
        "status": "filled",
        "size": 1.0,
        "price": 10.0,
        "fee": 0.0,
        "fee_asset": "EUR",
        "executed_at": _MINUTE,
        "timestamp": _MINUTE,
        "exec_id": "fill-1",
        "trade_id": None,
        "client_order_id": "order-1",
        "shard_key": "spot",
    }


def _ref() -> InstrumentSymbolRefRow:
    """Build the stable BTC/EUR identity spanning the execution."""
    return {
        "instrument_public_id": "00000000-0000-7000-8000-000000000821",
        "native_symbol": "BTC/EUR",
        "exchange": "coinbase",
        "instrument_exchange": "coinbase",
        "base_currency": "BTC",
        "quote_currency": "EUR",
        "valid_from": _MINUTE - timedelta(days=1),
        "valid_to": datetime.max.replace(tzinfo=UTC),
    }


def _consumer() -> FxProofBackfillConsumer:
    """Build one active sample and its persisted sealed watermark."""
    return FxProofBackfillConsumer(
        public_id="00000000-0000-7000-8000-000000000823",
        wallet_public_id="00000000-0000-7000-8000-000000000824",
        mode="live",
        valuation_ccy="USD",
        calculation_version="5B.2",
        point_kind="sample",
        knowledge_at=_AS_OF,
        watermarks={"coinbase": 1},
    )


@pytest.mark.asyncio
async def test_discovery_uses_singleton_manifest_and_detects_existing_pin() -> None:
    """Discovery follows event semantics and checks the complete F1 identity.

    Given one sealed EUR-priced execution with exact current candle evidence
    When its consumer is discovered before and after a matching pin exists
    Then its true one-minute shared manifest is returned with the right pin state
    """
    missing_repo = cast(Repository, _DiscoveryRepository())
    evaluations = await _consumer_evaluations(missing_repo, _consumer(), _AS_OF)
    missing = await discover_fx_proof_backfill(missing_repo)
    present_repo = cast(Repository, _DiscoveryRepository(artifact_present=True))
    assert len(evaluations) == 1
    assert evaluations[0].required_minutes == frozenset({_MINUTE})
    assert not missing.requirements[0].pinned
    assert await _is_pinned(present_repo, evaluations[0], "5B.2")
    refused = await discover_fx_proof_backfill(
        cast(Repository, _DiscoveryRepository(identity_present=False))
    )
    assert "identity cannot be proven" in refused.semantic_refusals[0]


@pytest.mark.asyncio
async def test_failed_batch_does_not_advance_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed canonical write leaves the resumable cursor before its batch.

    Given one requirement and a repository whose pin writer fails
    When apply processes the bounded batch
    Then failure is counted and no checkpoint claims the batch completed
    """

    class _FailingRepository:
        async def pin_fx_conversion_artifact(self, election: object, proofs: object) -> object:
            """Fail the persistence boundary deterministically."""
            raise RuntimeError("write failed")

    async def discover(_: object) -> FxProofBackfillDiscovery:
        """Return one requirement for the failed batch."""
        return FxProofBackfillDiscovery((_requirement(),), ())

    monkeypatch.setattr(
        "snapper.application.portfolio.fx_proof_backfill.discover_fx_proof_backfill",
        discover,
    )
    checkpoint = tmp_path / "failed.json"
    result = await run_fx_proof_backfill(
        cast(Repository, _FailingRepository()),
        True,
        checkpoint,
    )
    assert result.processed == 0
    assert result.metrics.failure == 1
    assert not checkpoint.exists()


@pytest.mark.asyncio
async def test_repository_enumerates_only_active_pnl_cuts_and_proves_exact_empty_map(
    tmp_path: Path,
) -> None:
    """Repository discovery exposes P&L consumers and never captures a newer cut.

    Given one active P&L anchor with a persisted empty watermark map
    When F3 enumerates consumers and certifies that exact map
    Then the anchor is returned and the proven execution prefix remains empty
    """
    repository = await _repository(tmp_path)
    wallet = "00000000-0000-7000-8000-000000000830"
    anchor = PortfolioPnlAnchorRow(
        public_id=portfolio_pnl_anchor_public_id(wallet, "live", "USD"),
        session_id="00000000-0000-7000-8000-000000000831",
        sequence_id=1,
        timestamp=_AS_OF,
        wallet_public_id=wallet,
        mode="live",
        valuation_ccy="USD",
        point_time=_MINUTE,
        point_kind="anchor",
        epoch_public_id="00000000-0000-7000-8000-000000000832",
        calc_version="5A.13",
        valuation_status="complete",
        realized_pnl=0.0,
        fee_pnl=0.0,
        accrual_pnl=0.0,
        unrealized_pnl=0.0,
        external_flow_adjustment=0.0,
        cash_usd=None,
        position_value_usd=None,
        drawdown=None,
        mark_source="finalized_1m",
        mark_time=_MINUTE,
        watermarks_json="{}",
        opening_basket_json="{}",
        contributions_json="{}",
    )
    await repository.record_portfolio_pnl_anchor(anchor)
    consumers = await repository.list_fx_proof_backfill_consumers()
    prefix = await repository.get_pnl_timeline_execution_prefix_at_watermarks(
        wallet,
        "live",
        {},
        _AS_OF,
    )
    assert len(consumers) == 1
    assert consumers[0].point_kind == "anchor"
    assert consumers[0].watermarks == {}
    assert prefix["executions"] == []
    with pytest.raises(ValueError, match="watermark map"):
        await repository.get_pnl_timeline_execution_prefix_at_watermarks(
            wallet,
            "live",
            {"": -1},
            _AS_OF,
        )
    async with repository.session() as session:
        persisted = (await session.execute(select(PortfolioPnlPoint))).scalar_one()
        persisted.watermarks_json = "[]"
        await session.commit()
    with pytest.raises(ValueError, match="invalid execution watermarks"):
        await repository.list_fx_proof_backfill_consumers()
    await repository.engine.dispose()
