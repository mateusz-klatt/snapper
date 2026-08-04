"""Consumer-wide FX proof-backfill derivation and recovery witnesses."""

import json
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from types import ModuleType
from typing import Any
from typing import cast
from uuid import UUID

import pytest
from sqlalchemy.dialects import postgresql

from snapper.application.portfolio.execution_chain import ExecutionChainError
from snapper.application.portfolio.fx_conversion_shadow import FxShadowEvaluation
from snapper.application.portfolio.fx_conversion_shadow import FxShadowPinMetrics
from snapper.application.portfolio.fx_proof_backfill import FxProofBackfillConsumer
from snapper.application.portfolio.fx_proof_backfill import FxProofBackfillDiscovery
from snapper.application.portfolio.fx_proof_backfill import FxProofBackfillRequirement
from snapper.application.portfolio.fx_proof_backfill import _acquire_apply_lock
from snapper.application.portfolio.fx_proof_backfill import _checkpoint_start
from snapper.application.portfolio.fx_proof_backfill import _consumer_discovery
from snapper.application.portfolio.fx_proof_backfill import _consumer_key
from snapper.application.portfolio.fx_proof_backfill import _consumer_manifest
from snapper.application.portfolio.fx_proof_backfill import _expected_evaluation_count
from snapper.application.portfolio.fx_proof_backfill import _fsync_directory
from snapper.application.portfolio.fx_proof_backfill import _metric_delta
from snapper.application.portfolio.fx_proof_backfill import _ordered_consumers
from snapper.application.portfolio.fx_proof_backfill import _release_apply_lock
from snapper.application.portfolio.fx_proof_backfill import _write_checkpoint
from snapper.application.portfolio.fx_proof_backfill import discover_fx_proof_backfill
from snapper.application.portfolio.fx_proof_backfill import fx_proof_backfill_apply_lock
from snapper.application.portfolio.fx_proof_backfill import reset_fx_proof_backfill_checkpoint
from snapper.application.portfolio.fx_proof_backfill import run_fx_proof_backfill
from snapper.data.fx_conversion_digests import build_requirement_manifest_digest
from snapper.data.models import PortfolioPnlPoint
from snapper.data.models import UUIDColumn
from snapper.data.repository import Repository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import InstrumentSymbolRefRow
from snapper.data.repository_types import PnlFxRateRow
from snapper.data.repository_types import PnlTimelineAccrualRow
from snapper.data.repository_types import PnlTimelineExecutionPrefix
from snapper.data.repository_types import PnlTimelineOpeningExecutionRow

_MINUTE = datetime(2026, 8, 2, 12, 0, tzinfo=UTC)
_HORIZON = datetime(2026, 8, 2, 13, 0, tzinfo=UTC)
_WALLET = "00000000-0000-7000-8000-000000000810"
_INSTRUMENT = "00000000-0000-7000-8000-000000000821"
_UNTRUSTED = "00000000-0000-7000-8000-000000000829"


def _consumer(version: str = "5B.2") -> FxProofBackfillConsumer:
    """Build one SQL-aggregated active P&L consumer union."""
    return FxProofBackfillConsumer(
        wallet_public_id=_WALLET,
        mode="live",
        valuation_ccy="USD",
        calculation_version=version,
        knowledge_at=_HORIZON,
        epoch_public_id="00000000-0000-7000-8000-000000000850",
        epoch_start=_MINUTE - timedelta(hours=1),
        point_time_cut=_MINUTE + timedelta(minutes=5),
        watermarks={"coinbase": 3},
    )


def _execution(
    minute: datetime,
    sequence: int,
    instrument: str = _INSTRUMENT,
) -> PnlTimelineOpeningExecutionRow:
    """Build one EUR-priced execution in a sealed prefix."""
    return {
        "public_id": f"00000000-0000-7000-8000-{sequence:012d}",
        "instrument_public_id": instrument,
        "exchange": "coinbase",
        "scope_sequence": sequence,
        "order_public_id": f"00000000-0000-7000-8001-{sequence:012d}",
        "side": "buy",
        "status": "filled",
        "size": 1.0,
        "price": 10.0,
        "fee": 0.0,
        "fee_asset": "EUR",
        "executed_at": minute,
        "timestamp": minute,
        "exec_id": f"fill-{sequence}",
        "trade_id": None,
        "client_order_id": f"order-{sequence}",
        "shard_key": "spot",
    }


def _ref() -> InstrumentSymbolRefRow:
    """Build stable BTC/EUR denomination and venue evidence."""
    return {
        "instrument_public_id": _INSTRUMENT,
        "native_symbol": "BTC/EUR",
        "exchange": "coinbase",
        "instrument_exchange": "coinbase",
        "base_currency": "BTC",
        "quote_currency": "EUR",
        "valid_from": _MINUTE - timedelta(days=1),
        "valid_to": datetime.max.replace(tzinfo=UTC),
    }


def _accrual() -> PnlTimelineAccrualRow:
    """Build one durable foreign-currency funding conversion."""
    return {
        "instrument_public_id": _INSTRUMENT,
        "exchange": "coinbase",
        "mode": "live",
        "accrual_type": "funding",
        "accrued_at": _MINUTE + timedelta(minutes=2),
        "amount": 3.0,
        "amount_asset": "PLN",
    }


def _row(
    base: str,
    quote: str,
    exchange: str,
    minute: datetime,
    identity: int,
) -> PnlFxRateRow:
    """Build one exact candle version for a candidate plane minute."""
    return {
        "base": base,
        "quote": quote,
        "exchange": exchange,
        "open_at": minute - timedelta(minutes=1),
        "close": 1.125,
        "native_symbol": f"{base}/{quote}",
        "instrument_public_id": _INSTRUMENT,
        "candle_id": identity,
        "candle_public_id": f"00000000-0000-7000-8003-{identity:012d}",
        "candle_session_id": "00000000-0000-7000-8004-000000000001",
        "candle_sequence_id": identity,
        "candle_timestamp": minute - timedelta(minutes=1),
        "candle_known_to": datetime.max.replace(tzinfo=UTC),
    }


class _DiscoveryRepository:
    """Repository probe exposing competing planes and one lost instrument."""

    def __init__(self) -> None:
        self.horizons: list[datetime] = []

    async def get_pnl_timeline_execution_prefix_at_watermarks(
        self,
        wallet_public_id: str,
        mode: str,
        watermarks: dict[str, int],
        as_of: datetime,
    ) -> PnlTimelineExecutionPrefix:
        """Return the exact supplied cut and record its knowledge horizon."""
        self.horizons.append(as_of)
        return {
            "watermarks": watermarks,
            "executions": [
                _execution(_MINUTE, 1),
                _execution(_MINUTE + timedelta(minutes=1), 2),
                _execution(_MINUTE, 3, _UNTRUSTED),
            ],
            "annulments": [],
        }

    async def get_accruals_for_pnl(
        self, wallet_public_id: str, mode: str, as_of: datetime
    ) -> list[PnlTimelineAccrualRow]:
        """Return the sealed foreign funding row at the same horizon."""
        self.horizons.append(as_of)
        before = _accrual()
        before["accrued_at"] = _MINUTE - timedelta(hours=2)
        after = _accrual()
        after["accrued_at"] = _MINUTE + timedelta(minutes=6)
        return [before, _accrual(), after]

    async def get_instrument_symbol_refs(
        self, instrument_public_ids: list[str], as_of: datetime
    ) -> list[InstrumentSymbolRefRow]:
        """Prove only the trusted instrument and preserve the lost peer."""
        self.horizons.append(as_of)
        return [_ref()]

    async def get_pnl_fx_rate_exchanges(
        self,
        pairs: list[tuple[str, str]],
        start: datetime,
        end: datetime,
        as_of: datetime,
    ) -> list[tuple[str, str, str]]:
        """Expose partial Binance and complete Kraken candidate planes."""
        self.horizons.append(as_of)
        planes: list[tuple[str, str, str]] = []
        if ("EUR", "USD") in pairs or ("USD", "EUR") in pairs:
            planes.extend(("EUR", "USD", venue) for venue in ("binance", "kraken"))
        if ("PLN", "USD") in pairs or ("USD", "PLN") in pairs:
            planes.append(("PLN", "USD", "kraken"))
        return planes

    async def get_pnl_fx_rate_candles(
        self,
        planes: list[tuple[str, str, str]],
        start: datetime,
        end: datetime,
        as_of: datetime,
    ) -> list[PnlFxRateRow]:
        """Make Binance partial while Kraken covers the true electorate."""
        self.horizons.append(as_of)
        rows = [
            _row("EUR", "USD", "binance", _MINUTE, 1),
            _row("EUR", "USD", "kraken", _MINUTE, 2),
            _row("EUR", "USD", "kraken", _MINUTE + timedelta(minutes=1), 3),
            _row("PLN", "USD", "kraken", _MINUTE + timedelta(minutes=2), 4),
        ]
        return [
            row
            for row in rows
            if (row["base"], row["quote"], row["exchange"]) in planes
            and start <= row["open_at"] <= end
        ]

    async def get_latest_visible_fx_conversion_artifact(
        self, query: object, as_of: datetime
    ) -> None:
        """Report no existing proof and record the lookup horizon."""
        self.horizons.append(as_of)
        return None

    async def has_visible_fx_conversion_refusal(self, query: object, as_of: datetime) -> bool:
        """Report no existing refusal audit and record the lookup horizon."""
        self.horizons.append(as_of)
        return False

    async def list_fx_proof_backfill_consumers(self) -> list[FxProofBackfillConsumer]:
        """Return one SQL-aggregated consumer union."""
        return [_consumer()]


def _requirement(version: str = "5B.2") -> FxProofBackfillRequirement:
    """Build one complete full-set requirement for apply convergence tests."""
    rows = (
        _row("EUR", "USD", "kraken", _MINUTE, 10),
        _row("EUR", "USD", "kraken", _MINUTE + timedelta(minutes=1), 11),
    )
    evaluation = FxShadowEvaluation(
        scope_kind="shared_pair",
        consumer_instrument_public_id=None,
        pair=("EUR", "USD"),
        target_currency="USD",
        required_minutes=frozenset({_MINUTE, _MINUTE + timedelta(minutes=1)}),
        candidate_planes=frozenset({("EUR", "USD", "kraken")}),
        selected_plane=("EUR", "USD", "kraken"),
        requested_knowledge_at=_HORIZON,
        rows=rows,
        authoritative_rates={
            (
                row["base"],
                row["quote"],
                row["exchange"],
                row["open_at"] + timedelta(minutes=1),
            ): row["close"]
            for row in rows
        },
    )
    return FxProofBackfillRequirement(
        wallet_public_id=_WALLET,
        valuation_ccy="USD",
        calculation_version=version,
        knowledge_at=_HORIZON,
        evaluation=evaluation,
        pinned=False,
        refusal_audited=False,
    )


async def _repository(tmp_path: Path, name: str) -> SQLAlchemyRepository:
    """Create an isolated real F1 repository."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / name}")
    await repository.create_all()
    return repository


@pytest.mark.asyncio
async def test_true_pair_electorate_accrual_horizon_and_instrument_refusal() -> None:
    """Full consumer electorates reproduce election ranking and retain good peers.

    Given Binance covers one EUR minute, Kraken covers both, one PLN accrual, and one bad instrument
    When the consumer is derived at its sealed knowledge horizon
    Then Kraken wins the two-minute manifest, accrual conversion remains, and only the bad peer refuses
    """
    fake = _DiscoveryRepository()
    repo = cast(Repository, fake)
    discovery = await _consumer_discovery(repo, _consumer())
    by_pair = {item.evaluation.pair: item for item in discovery.requirements}
    eur = by_pair[("EUR", "USD")].evaluation
    assert eur.required_minutes == frozenset({_MINUTE, _MINUTE + timedelta(minutes=1)})
    assert eur.selected_plane == ("EUR", "USD", "kraken")
    assert by_pair[("PLN", "USD")].evaluation.required_minutes == frozenset(
        {_MINUTE + timedelta(minutes=2)}
    )
    true_digest = build_requirement_manifest_digest((_MINUTE + timedelta(minutes=2),))
    padded_digest = build_requirement_manifest_digest(
        (
            _MINUTE - timedelta(hours=2),
            _MINUTE + timedelta(minutes=2),
            _MINUTE + timedelta(minutes=6),
        )
    )
    assert true_digest != padded_digest
    assert discovery.semantic_refusals[0].instrument_public_id == _UNTRUSTED
    assert discovery.semantic_refusals[0].lost_requirements == (
        f"execution:00000000-0000-7000-8000-000000000003@{_MINUTE.isoformat()}",
    )
    assert fake.horizons
    assert set(fake.horizons) == {_HORIZON}


@pytest.mark.asyncio
async def test_apply_db_state_overrides_cursor_and_completed_rerun_skips(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cursor position never hides missing DB work and completed DB state skips writes.

    Given a cursor at consumer one, an empty restored DB, and two calculation-version requirements
    When apply runs, then runs again against the populated DB
    Then both missing proofs are created despite the hint and the rerun performs zero pin reuse calls
    """
    repository = await _repository(tmp_path, "restored.db")
    consumers = [_consumer("5A.13"), _consumer("5B.2")]

    async def list_consumers() -> list[FxProofBackfillConsumer]:
        """Return both durable calculation-version unions."""
        return consumers

    async def derive(
        repo: Repository, consumer: FxProofBackfillConsumer
    ) -> FxProofBackfillDiscovery:
        """Return the requirement belonging to the selected consumer."""
        return FxProofBackfillDiscovery((_requirement(consumer.calculation_version),), ())

    monkeypatch.setattr(repository, "list_fx_proof_backfill_consumers", list_consumers)
    monkeypatch.setattr(
        "snapper.application.portfolio.fx_proof_backfill._consumer_discovery", derive
    )
    checkpoint = tmp_path / "cursor.json"
    _write_checkpoint(checkpoint, _consumer_manifest(consumers), 1)
    first = await run_fx_proof_backfill(repository, True, checkpoint)
    second = await run_fx_proof_backfill(repository, True, checkpoint)
    assert first.proof_creations == 2
    assert first.metrics.creation == 2
    assert first.fully_verified
    assert not checkpoint.exists()
    assert second.proof_creations == 0
    assert second.metrics.creation == 0
    assert second.metrics.reuse == 0
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_refusal_audit_is_classified_without_claiming_proof_creation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A missing-candle election creates an audit but never claims a proof.

    Given one full-set requirement whose raw election has no selected plane
    When apply persists and verifies the canonical refusal
    Then the audit count increments while proof creations stay zero
    """
    repository = await _repository(tmp_path, "refusal.db")
    consumer = _consumer()
    refused = replace(
        _requirement(),
        evaluation=replace(_requirement().evaluation, selected_plane=None, rows=()),
    )

    async def consumers() -> list[FxProofBackfillConsumer]:
        """Return the refusal's durable consumer."""
        return [consumer]

    async def derive(
        repo: Repository, selected: FxProofBackfillConsumer
    ) -> FxProofBackfillDiscovery:
        """Return the raw refusal evaluation."""
        return FxProofBackfillDiscovery((refused,), ())

    monkeypatch.setattr(repository, "list_fx_proof_backfill_consumers", consumers)
    monkeypatch.setattr(
        "snapper.application.portfolio.fx_proof_backfill._consumer_discovery", derive
    )
    result = await run_fx_proof_backfill(repository, True, tmp_path / "refusal.json")
    assert result.proof_creations == 0
    assert result.refusal_audit_creations == 1
    assert result.requirements[0].refusal_audited
    assert not result.requirements[0].pinned
    owned = replace(
        refused,
        evaluation=replace(
            refused.evaluation,
            scope_kind="instrument_owned",
            consumer_instrument_public_id=_INSTRUMENT,
        ),
    )

    async def derive_owned(
        repo: Repository, selected: FxProofBackfillConsumer
    ) -> FxProofBackfillDiscovery:
        """Return one instrument-owned raw refusal."""
        return FxProofBackfillDiscovery((owned,), ())

    monkeypatch.setattr(
        "snapper.application.portfolio.fx_proof_backfill._consumer_discovery", derive_owned
    )
    owned_result = await run_fx_proof_backfill(repository, True, tmp_path / "owned-refusal.json")
    assert owned_result.requirements[0].refusal_audited
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_repository_aggregates_active_watermark_union_in_sql(tmp_path: Path) -> None:
    """Active point enumeration returns SQL maxima instead of ORM point entities.

    Given two active points in one scope with different exchange cuts and timestamps
    When the repository enumerates F3 consumers
    Then one union carries per-exchange maxima and the latest consumer horizon
    """
    repository = await _repository(tmp_path, "aggregate.db")
    common = {
        "wallet_public_id": _WALLET,
        "mode": "live",
        "valuation_ccy": "USD",
        "epoch_public_id": "00000000-0000-7000-8000-000000000850",
        "calc_version": "5B.2",
        "realized_pnl": 0.0,
        "fee_pnl": 0.0,
        "accrual_pnl": 0.0,
        "external_flow_adjustment": 0.0,
        "unrealized_pnl": None,
        "cash_usd": None,
        "position_value_usd": None,
        "drawdown": None,
        "mark_source": None,
        "mark_time": None,
        "opening_basket_json": "{}",
        "contributions_json": None,
    }
    first = PortfolioPnlPoint(
        public_id="00000000-0000-7000-8000-000000000851",
        session_id="00000000-0000-7000-8000-000000000852",
        sequence_id=1,
        timestamp=_HORIZON - timedelta(minutes=1),
        point_time=_MINUTE,
        point_kind="sample",
        valuation_status="incomplete",
        watermarks_json='{"coinbase":2,"kraken":4}',
        **common,
    )
    second = PortfolioPnlPoint(
        public_id="00000000-0000-7000-8000-000000000853",
        session_id="00000000-0000-7000-8000-000000000854",
        sequence_id=2,
        timestamp=_HORIZON,
        point_time=_MINUTE + timedelta(minutes=1),
        point_kind="sample",
        valuation_status="incomplete",
        watermarks_json='{"coinbase":7}',
        **common,
    )
    empty_scope = PortfolioPnlPoint(
        public_id="00000000-0000-7000-8000-000000000855",
        session_id="00000000-0000-7000-8000-000000000856",
        sequence_id=3,
        timestamp=_HORIZON,
        point_time=_MINUTE,
        point_kind="sample",
        valuation_status="incomplete",
        watermarks_json="{}",
        **{**common, "valuation_ccy": "EUR"},
    )
    anchor = PortfolioPnlPoint(
        public_id="00000000-0000-7000-8000-000000000857",
        session_id="00000000-0000-7000-8000-000000000858",
        sequence_id=4,
        timestamp=_HORIZON - timedelta(minutes=2),
        point_time=_MINUTE - timedelta(minutes=1),
        point_kind="anchor",
        valuation_status="incomplete",
        watermarks_json="{}",
        **common,
    )
    eur_anchor = PortfolioPnlPoint(
        public_id="00000000-0000-7000-8000-000000000859",
        session_id="00000000-0000-7000-8000-000000000860",
        sequence_id=5,
        timestamp=_HORIZON - timedelta(minutes=2),
        point_time=_MINUTE - timedelta(minutes=1),
        point_kind="anchor",
        valuation_status="incomplete",
        watermarks_json="{}",
        **{**common, "valuation_ccy": "EUR"},
    )
    async with repository.session() as session:
        session.add_all((first, second, empty_scope, anchor, eur_anchor))
        await session.commit()
    consumers = await repository.list_fx_proof_backfill_consumers()
    assert len(consumers) == 2
    usd = next(consumer for consumer in consumers if consumer.valuation_ccy == "USD")
    eur = next(consumer for consumer in consumers if consumer.valuation_ccy == "EUR")
    assert usd.watermarks == {"coinbase": 7, "kraken": 4}
    assert usd.knowledge_at == _HORIZON
    assert eur.watermarks == {}
    prefix = await repository.get_pnl_timeline_execution_prefix_at_watermarks(
        _WALLET, "live", {}, _HORIZON
    )
    assert prefix["executions"] == []
    with pytest.raises(ValueError, match="watermark map"):
        await repository.get_pnl_timeline_execution_prefix_at_watermarks(
            _WALLET, "live", {"": -1}, _HORIZON
        )
    for malformed in (
        "[]",
        '{"bad":-1}',
        '{"bad":"1"}',
        '{"bad":1.5}',
        '{"bad":true}',
        '{"bad":{}}',
        '{"bad":null}',
    ):
        async with repository.session() as session:
            second.watermarks_json = malformed
            session.add(second)
            await session.commit()
        with pytest.raises(ValueError, match="invalid execution watermarks"):
            await repository.list_fx_proof_backfill_consumers()
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_consumer_query_decodes_postgresql_uuid_identities(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Native PostgreSQL UUID results must cross the textual repository boundary.

    Given: The raw SQL consumer query and PostgreSQL-native UUID result values.
    When: Its declared result types decode the wallet and epoch identities.
    Then: Both identities become strings before execution-prefix certification.
    """
    repository = await _repository(tmp_path, "postgres-identities.db")
    captured: list[Any] = []

    epoch_id = UUID("00000000-0000-7000-8000-000000000850")

    class _NativeUuidResult:
        def all(self) -> list[tuple[object, ...]]:
            return [
                (
                    UUID(_WALLET),
                    "live",
                    "USD",
                    "5B.2",
                    epoch_id,
                    _HORIZON,
                    _MINUTE,
                    _MINUTE,
                    0,
                    0,
                    None,
                    None,
                )
            ]

    class _CaptureSession:
        async def execute(self, statement: object, _params: object) -> _NativeUuidResult:
            captured.append(statement)
            return _NativeUuidResult()

    @asynccontextmanager
    async def capture_session() -> AsyncIterator[_CaptureSession]:
        yield _CaptureSession()

    monkeypatch.setattr(repository, "session", capture_session)
    consumers = await repository.list_fx_proof_backfill_consumers()
    assert len(consumers) == 1
    assert consumers[0].wallet_public_id == _WALLET
    assert consumers[0].epoch_public_id == str(epoch_id)
    assert isinstance(consumers[0].wallet_public_id, str)
    assert isinstance(consumers[0].epoch_public_id, str)
    assert _consumer_key(consumers[0]).startswith(f"{_WALLET}|live|USD|5B.2|")
    statement = captured[0]
    dialect = postgresql.dialect()
    for column_name, native_value in (
        ("wallet_public_id", UUID(_WALLET)),
        ("epoch_public_id", epoch_id),
    ):
        column_type = statement.selected_columns[column_name].type
        assert isinstance(column_type, UUIDColumn)
        processor = column_type.dialect_impl(dialect).result_processor(dialect, None)
        assert processor is not None
        assert processor(native_value) == str(native_value)
    await repository.engine.dispose()


def test_checkpoint_lock_reset_rotation_and_metric_delta(tmp_path: Path) -> None:
    """Checkpoint durability helpers preserve hint-only and single-writer semantics.

    Given stale and valid cursor files, two consumers, and process metric snapshots
    When hints, rotation, reset, and lock contention are exercised
    Then stale hints restart, every consumer remains ordered, and only one writer enters
    """
    consumers = [_consumer("5A.13"), _consumer("5B.2")]
    digest = _consumer_manifest(consumers)
    checkpoint = tmp_path / "nested" / "cursor.json"
    assert _checkpoint_start(checkpoint, digest, 2) == 0
    _write_checkpoint(checkpoint, digest, 1)
    assert _checkpoint_start(checkpoint, digest, 2) == 1
    checkpoint.write_text(json.dumps({"cursor": 9, "manifest_digest": digest}), encoding="utf-8")
    assert _checkpoint_start(checkpoint, digest, 2) == 0
    checkpoint.write_text("[]", encoding="utf-8")
    assert _checkpoint_start(checkpoint, digest, 2) == 0
    assert [index for index, _ in _ordered_consumers(consumers, 1)] == [1, 0]
    before = FxShadowPinMetrics(creation=2, reuse=1)
    after = FxShadowPinMetrics(creation=5, reuse=4, failure=1)
    delta = _metric_delta(before, after)
    assert (delta.creation, delta.reuse, delta.failure) == (3, 3, 1)
    with (
        fx_proof_backfill_apply_lock(checkpoint),
        pytest.raises(ValueError, match="already running"),
        fx_proof_backfill_apply_lock(checkpoint),
    ):
        raise AssertionError("unreachable")
    reset_fx_proof_backfill_checkpoint(checkpoint)
    reset_fx_proof_backfill_checkpoint(checkpoint)
    _fsync_directory(tmp_path)
    assert not checkpoint.exists()


def test_directory_durability_follows_the_platform_capability(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Directory syncing runs only where a directory descriptor can be opened.

    Given the running platform, then one reporting support and one reporting none
    When the durability helper runs under each
    Then the real platform never raises, and the descriptor is synced only when supported

    The unpatched call is the regression guard: Windows refuses ``os.open`` on a
    directory with ``PermissionError``, which is why the capability is consulted
    at all. The patched pair then exercises both branches on every platform,
    since neither can reach the other one natively.
    """
    _fsync_directory(tmp_path)
    sentinel = 987654321
    opened: list[Path] = []
    synced: list[int] = []
    closed: list[int] = []
    real_open = os.open
    real_fsync = os.fsync
    real_close = os.close

    def fake_open(target: str | Path, flags: int) -> int:
        """Hand back a sentinel for the probed directory and defer other opens."""
        if Path(target) == tmp_path:
            opened.append(Path(target))
            return sentinel
        return real_open(target, flags)

    def fake_fsync(descriptor: int) -> None:
        """Record a sentinel sync and defer every real descriptor."""
        if descriptor == sentinel:
            synced.append(descriptor)
            return
        real_fsync(descriptor)

    def fake_close(descriptor: int) -> None:
        """Record a sentinel close and defer every real descriptor."""
        if descriptor == sentinel:
            closed.append(descriptor)
            return
        real_close(descriptor)

    monkeypatch.setattr(os, "open", fake_open)
    monkeypatch.setattr(os, "fsync", fake_fsync)
    monkeypatch.setattr(os, "close", fake_close)
    monkeypatch.setattr(
        "snapper.application.portfolio.fx_proof_backfill._directory_fsync_supported",
        lambda: True,
    )
    _fsync_directory(tmp_path)
    monkeypatch.setattr(
        "snapper.application.portfolio.fx_proof_backfill._directory_fsync_supported",
        lambda: False,
    )
    _fsync_directory(tmp_path)
    monkeypatch.undo()

    assert opened == [tmp_path]
    assert synced == [sentinel]
    assert closed == [sentinel]


@pytest.mark.asyncio
async def test_report_discovery_deduplicates_identical_requirements(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Identical shared identities across consumer discovery appear only once.

    Given the same consumer union is returned twice
    When the no-write discovery report is assembled
    Then shared-pair identity is deduplicated without changing its full manifest
    """
    fake = _DiscoveryRepository()

    async def consumers() -> list[FxProofBackfillConsumer]:
        """Return a duplicate union to probe report deduplication."""
        return [_consumer(), _consumer()]

    monkeypatch.setattr(fake, "list_fx_proof_backfill_consumers", consumers)
    discovery = await discover_fx_proof_backfill(cast(Repository, fake))
    assert len(discovery.requirements) == 2
    assert len(discovery.semantic_refusals) == 1


@pytest.mark.asyncio
async def test_evaluation_count_mismatch_becomes_enumerated_refusal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A swallowed collector factory failure cannot silently erase requirements.

    Given derived event requirements but a loader that collects no evaluations
    When the consumer invariant compares expected and actual electorates
    Then the whole lost input set is returned as an explicit semantic refusal
    """

    async def load_without_collection(
        *arguments: object,
    ) -> tuple[dict[object, object], dict[object, object], set[object]]:
        """Represent a collector factory failure after requirement derivation."""
        return {}, {}, set()

    monkeypatch.setattr(
        "snapper.application.portfolio.fx_proof_backfill._load_request_fx_rates",
        load_without_collection,
    )
    discovery = await _consumer_discovery(cast(Repository, _DiscoveryRepository()), _consumer())
    assert discovery.requirements == ()
    assert discovery.semantic_refusals[-1].reason == "fx_evaluation_count_mismatch"
    assert any("EUR-USD" in item for item in discovery.semantic_refusals[-1].lost_requirements)


@pytest.mark.asyncio
async def test_report_mode_and_adverse_apply_completion_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No-write reporting and an adverse writer expose distinct completion states.

    Given one consumer requirement and a repository writer that fails
    When report mode runs and apply attempts the same requirement
    Then report stays verified while apply aborts before checkpoint completion
    """
    repository = await _repository(tmp_path, "failure.db")
    consumer = _consumer()

    async def consumers() -> list[FxProofBackfillConsumer]:
        """Return three consumers so an abort exposes two unreached peers."""
        return [consumer, _consumer("5C.1"), _consumer("5D.1")]

    async def derive(
        repo: Repository, selected: FxProofBackfillConsumer
    ) -> FxProofBackfillDiscovery:
        """Return one missing proof requirement."""
        return FxProofBackfillDiscovery((_requirement(),), ())

    async def fail_pin(election: object, proofs: object) -> object:
        """Fail the F1 persistence boundary."""
        raise RuntimeError("write failed")

    monkeypatch.setattr(repository, "list_fx_proof_backfill_consumers", consumers)
    monkeypatch.setattr(
        "snapper.application.portfolio.fx_proof_backfill._consumer_discovery", derive
    )
    report = await run_fx_proof_backfill(repository, False, tmp_path / "report.json")
    monkeypatch.setattr(repository, "pin_fx_conversion_artifact", fail_pin)
    applied = await run_fx_proof_backfill(repository, True, tmp_path / "apply.json")
    assert not report.fully_verified
    assert applied.aborted
    assert not applied.fully_verified
    assert applied.processed_consumers == 0
    assert applied.unreached_consumers == 2
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_poisoned_prefix_is_isolated_in_report_and_apply(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One certifiably poisoned consumer cannot suppress its durable peers.

    Given three ordered consumers whose middle prefix fails certification
    When report and apply traverse the complete consumer universe
    Then the middle cut is enumerated once and both peers remain processed
    """
    repository = await _repository(tmp_path, "isolated.db")
    consumers = [_consumer("5A.13"), _consumer("5B.2"), _consumer("5C.1")]

    async def list_consumers() -> list[FxProofBackfillConsumer]:
        """Return the three durable cuts in poison-witness order."""
        return consumers

    async def derive(
        repo: Repository, selected: FxProofBackfillConsumer
    ) -> FxProofBackfillDiscovery:
        """Fail only the middle prefix using the repository's refusal class."""
        if selected.calculation_version == "5B.2":
            raise ExecutionChainError("non_contiguous_execution_prefix")
        return FxProofBackfillDiscovery((_requirement(selected.calculation_version),), ())

    monkeypatch.setattr(repository, "list_fx_proof_backfill_consumers", list_consumers)
    monkeypatch.setattr(
        "snapper.application.portfolio.fx_proof_backfill._consumer_discovery", derive
    )
    report = await run_fx_proof_backfill(repository, False, tmp_path / "report.json")
    applied = await run_fx_proof_backfill(repository, True, tmp_path / "apply.json")
    assert len(report.requirements) == 2
    assert len(report.semantic_refusals) == 1
    assert "non_contiguous_execution_prefix" in report.semantic_refusals[0].reason
    assert len(applied.requirements) == 2
    assert applied.processed_consumers == 3
    assert applied.unreached_consumers == 0
    await repository.engine.dispose()


def test_posix_apply_lock_uses_guarded_native_api(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The POSIX branch takes an exclusive nonblocking flock and releases it.

    Given a POSIX platform marker and a native lock API probe
    When the apply lock helpers acquire and release the file
    Then only the guarded POSIX operations execute in order

    This mirrors the Windows case deliberately. ``sys.platform`` decides which
    branch runs, so on Windows the POSIX body is unreachable through real calls
    and on POSIX the Windows body is; faking only one side leaves the other
    uncovered on that platform, and ``fail_under`` is enforced per platform.
    """
    calls: list[int] = []
    posix_api = ModuleType("fcntl")
    posix_api.LOCK_EX = 1
    posix_api.LOCK_NB = 4
    posix_api.LOCK_UN = 8

    def flock(descriptor: int, operation: int) -> None:
        """Record the selected POSIX lock operation."""
        calls.append(operation)

    posix_api.flock = flock

    def load_module(name: str) -> ModuleType:
        """Return the guarded POSIX API for the deferred import."""
        return posix_api

    monkeypatch.setattr("snapper.application.portfolio.fx_proof_backfill.sys.platform", "linux")
    monkeypatch.setattr(
        "snapper.application.portfolio.fx_proof_backfill.importlib.import_module",
        load_module,
    )
    lock_path = tmp_path / "native.lock"
    with lock_path.open("a+b") as stream:
        stream.write(b"\0")
        _acquire_apply_lock(stream)
        _release_apply_lock(stream)
    assert calls == [5, 8]


def test_windows_apply_lock_uses_guarded_native_api(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The Windows branch locks one byte without importing POSIX fcntl.

    Given a Windows platform marker and a native lock API probe
    When the apply lock helpers acquire and release one byte
    Then only the guarded Windows operations execute in order
    """
    calls: list[int] = []
    windows_api = ModuleType("msvcrt")
    windows_api.LK_NBLCK = 1
    windows_api.LK_UNLCK = 2

    def locking(descriptor: int, operation: int, length: int) -> None:
        """Record the selected Windows lock operation."""
        calls.append(operation)

    windows_api.locking = locking

    def load_module(name: str) -> ModuleType:
        """Return the guarded Windows API for the deferred import."""
        return windows_api

    monkeypatch.setattr("snapper.application.portfolio.fx_proof_backfill.sys.platform", "win32")
    monkeypatch.setattr(
        "snapper.application.portfolio.fx_proof_backfill.importlib.import_module",
        load_module,
    )
    lock_path = tmp_path / "native.lock"
    with lock_path.open("a+b") as stream:
        stream.write(b"\0")
        _acquire_apply_lock(stream)
        _release_apply_lock(stream)
    assert calls == [1, 2]


def test_owned_electorate_count_includes_only_required_identity_pair() -> None:
    """The evaluation invariant counts real instrument-owned electorates exactly.

    Given one identity instrument with its own-pair and one unrelated pair requirement
    When the expected F2 evaluation count is derived
    Then one owned and one shared electorate are required
    """
    requirements = {
        _INSTRUMENT: {
            ("BTC", "EUR"): {_MINUTE},
            ("EUR", "USD"): {_MINUTE},
        }
    }
    identities = {
        _INSTRUMENT: ("BTC", "EUR", "coinbase"),
        _UNTRUSTED: ("ETH", "EUR", "coinbase"),
    }
    assert _expected_evaluation_count(requirements, identities) == 2
