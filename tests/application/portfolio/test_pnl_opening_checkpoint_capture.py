"""Service-level opening-checkpoint capture ordering, writes, and refusals."""

from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import replace
from datetime import datetime
from types import SimpleNamespace
from typing import Literal
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import Mock

import pytest
from sqlalchemy.exc import SQLAlchemyError

from snapper.application.portfolio import pnl_engine_options
from snapper.application.portfolio import pnl_opening_checkpoint_capture
from snapper.application.portfolio.execution_chain import ExecutionChainError
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import PnlCheckpointStateBundle
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import PnlEventFxReplayStatePayload
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import PnlMarkFxReplayStatePayload
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import bundle_from_json
from snapper.application.portfolio.pnl_opening_checkpoint_capture import (
    PnlCheckpointCaptureAssembly,
)
from snapper.application.portfolio.pnl_opening_checkpoint_capture import PnlCheckpointCaptureRequest
from snapper.application.portfolio.pnl_opening_checkpoint_capture import _AccrualPrefixRequest
from snapper.application.portfolio.pnl_opening_checkpoint_capture import (
    _execution_watermark_partitions,
)
from snapper.application.portfolio.pnl_opening_checkpoint_capture import event_rate_source_payloads
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import (
    checkpoint_write_refusal,
)
from snapper.application.portfolio.pnl_timeline import PnlTimelineResult
from snapper.application.portfolio.pnl_timeline import TimelineCheckpointOpening
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReadPolicy
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReplayOptions
from snapper.application.portfolio.pnl_timeline_service import PnlWalletSeriesResult
from snapper.application.portfolio.pnl_timeline_service import _execution_manifest_digest
from snapper.application.portfolio.pnl_timeline_service import build_wallet_pnl_series
from snapper.data.pnl_correction_state import IdAggregate
from snapper.data.pnl_correction_state import PnlCorrectionChunk
from snapper.data.pnl_correction_state import PnlCorrectionEvidence
from snapper.data.pnl_correction_state import PnlCorrectionFrontier
from snapper.data.pnl_correction_state import PnlCorrectionKey
from snapper.data.repository import Repository
from snapper.data.repository_types import InstrumentSymbolRefRow
from snapper.data.repository_types import PnlFxRatePlane
from snapper.data.repository_types import PnlFxRateRow
from snapper.data.repository_types import PnlTimelineAccrualRow
from snapper.data.repository_types import PnlTimelineExecutionLineageRow
from snapper.data.repository_types import PnlTimelineExecutionPrefix
from snapper.data.repository_types import PnlTimelineFillWitnessCut
from snapper.data.repository_types import PortfolioPnlAnchorRow
from tests.application.portfolio import test_pnl_opening_checkpoint as checkpoint_fixtures
from tests.application.portfolio import test_pnl_timeline_service as fixtures

_CAPTURE_VERSION = "test-capture"
_WITNESS = PnlTimelineFillWitnessCut(watermarks={"kraken:spot": 10}, digest="d" * 64)
_MANIFEST = _execution_manifest_digest(())
_SOURCE_DIGEST = "c" * 64
_C = fixtures._m(5)
_AS_OF = fixtures._m(10)


def _anchor() -> PortfolioPnlAnchorRow:
    """Return the fixture anchor with its concrete repository row type.

    Returns:
        The canonical empty activation anchor used by the service fake.
    """
    return fixtures._empty_anchor(fixtures._W1, "live", "USD")


def _request(calc_version: str = _CAPTURE_VERSION) -> PnlCheckpointCaptureRequest:
    """Build one frozen capture request bound to the fixture frontier cut.

    Args:
        calc_version: Calculation version used by the engine-options resolver.

    Returns:
        A complete capture request with valid source evidence identities.
    """
    anchor = _anchor()
    key = PnlCorrectionKey(
        wallet_public_id=fixtures._W1,
        mode="live",
        valuation_ccy="USD",
        epoch_public_id=anchor["epoch_public_id"],
        calc_version=calc_version,
    )
    return PnlCheckpointCaptureRequest(
        key=key,
        source_generation=2,
        raw_execution_watermarks={"kraken": 1},
        execution_manifest_digest=_MANIFEST,
        expected_venues=frozenset({"kraken"}),
        venue_event_watermarks={"kraken:spot": 10},
        fill_witness_digest=_SOURCE_DIGEST,
        anchor_public_id=anchor["public_id"],
        anchor_digest="a" * 64,
        calc_version=calc_version,
    )


def _options(
    request: PnlCheckpointCaptureRequest | None,
    *,
    has_fill_gap: bool = False,
    replay_tip: datetime | None = None,
    pointwise_exact_fx: bool = True,
) -> PnlSeriesReplayOptions:
    """Build replay options with optional capture and configurable pointwise FX.

    Args:
        request: Capture request to carry, or ``None`` for the off path.
        has_fill_gap: Frozen scope-level gap verdict.
        replay_tip: Generation tip, defaulting to the settling chunk end.
        pointwise_exact_fx: Whether pointwise exact FX valuation is enabled.

    Returns:
        Frozen replay options with a certified execution cut.
    """
    return PnlSeriesReplayOptions(
        frozen_execution_watermarks=(
            dict(request.raw_execution_watermarks) if request is not None else {"kraken": 1}
        ),
        frozen_execution_manifest_digest=_MANIFEST,
        frozen_has_fill_gap=has_fill_gap,
        frozen_fill_witness_cut=_WITNESS,
        replay_generation_tip=_C if replay_tip is None else replay_tip,
        pointwise_exact_fx=pointwise_exact_fx,
        checkpoint_capture=request,
    )


def _fx_aggregate(rows: Sequence[PnlFxRateRow]) -> IdAggregate:
    """Aggregate fake candle identities with the repository's exact semantics.

    Args:
        rows: Selected fake FX candle rows.

    Returns:
        A coherent exact identity aggregate.
    """
    if not rows:
        return IdAggregate(0, 0, 0, None)
    return IdAggregate(
        count=len(rows),
        id_sum=sum(row["candle_id"] for row in rows),
        max_id=max(row["candle_id"] for row in rows),
        max_timestamp=max(row["candle_timestamp"] for row in rows),
    )


class _CaptureRepo(fixtures.FakeRepo):
    """Replay fake with separately logged certificate and domain reads."""

    def __init__(self) -> None:
        """Install one EUR conversion with event and mark evidence."""
        super().__init__(
            executions=[
                fixtures._exec_row(
                    fixtures._I1,
                    1,
                    3,
                    "buy",
                    1.0,
                    100.0,
                    2.0,
                    "EUR",
                )
            ],
            refs=[fixtures._ref(fixtures._I1, "BTC-EUR", "EUR")],
            candles=[fixtures._candle(fixtures._m(minute), 101.0) for minute in range(6)],
            lineage=[fixtures._lineage_row(f"order-{fixtures._I1}-1", "rest")],
            fx_rows=[
                fixtures._fx_row("EUR", "USD", 3, 2.0),
                fixtures._fx_row("EUR", "USD", 4, 2.0),
                fixtures._fx_row("EUR", "USD", 5, 2.0),
                fixtures._fx_row("EUR", "USD", 6, 2.0),
            ],
            execution_watermarks={"kraken": 1},
        )
        self.calls: list[str] = []
        self.lineage_scopes: list[list[str]] = []

    async def get_pnl_execution_prefix_counts(
        self,
        wallet_public_id: str,
        mode: str,
        watermarks: Mapping[str, int],
    ) -> dict[str, tuple[int, int]]:
        """Return exact counts for the certified fake prefix.

        Args:
            wallet_public_id: Wallet scope.
            mode: Trading mode.
            watermarks: Inclusive execution cut.

        Returns:
            Per-exchange count and maximum sequence.
        """
        del wallet_public_id, mode
        self.calls.append("execution_counts")
        return {exchange: (watermark, watermark) for exchange, watermark in watermarks.items()}

    async def get_spot_execution_chain_tip(self, *args: object) -> str:
        """Return one deterministic valid chain-tip digest.

        Args:
            args: Six positional repository chain-fold arguments.

        Returns:
            A syntactically valid chain-tip digest.
        """
        assert len(args) == 6
        self.calls.append("execution_chain")
        return "e" * 64

    async def get_pnl_timeline_execution_prefix_at_watermarks(
        self,
        wallet_public_id: str,
        mode: str,
        watermarks: Mapping[str, int],
        as_of: datetime,
        *,
        fill_witness_cut: PnlTimelineFillWitnessCut | Literal["capture"] | None = None,
    ) -> PnlTimelineExecutionPrefix:
        """Serve capture and revalidation reads with one stable witness cut.

        Args:
            wallet_public_id: Wallet scope.
            mode: Trading mode.
            watermarks: Inclusive execution cut.
            as_of: Knowledge horizon.
            fill_witness_cut: Capture marker or prior cut.

        Returns:
            The canned execution prefix and its witness cut.
        """
        del wallet_public_id, mode
        self.calls.append("execution_prefix")
        prefix = self._execution_prefix_at(as_of)
        assert dict(prefix["watermarks"]) == dict(watermarks)
        if fill_witness_cut is not None:
            prefix["fill_witness_cut"] = _WITNESS
        return prefix

    async def get_accruals_for_pnl(
        self,
        wallet_public_id: str,
        mode: str,
        as_of: datetime,
        *,
        after_exclusive: datetime | None = None,
        through: datetime | None = None,
    ) -> list[PnlTimelineAccrualRow]:
        """Log the replay accrual read after its certificate fingerprint.

        Args:
            wallet_public_id: Wallet scope.
            mode: Trading mode.
            as_of: Knowledge horizon.
            after_exclusive: Optional exclusive lower bound.
            through: Optional inclusive upper bound.

        Returns:
            Canned accrual rows within the requested bounds.
        """
        self.calls.append("accruals")
        return await super().get_accruals_for_pnl(
            wallet_public_id,
            mode,
            as_of,
            after_exclusive=after_exclusive,
            through=through,
        )

    async def get_pnl_accrual_prefix_fingerprint(
        self,
        wallet_public_id: str,
        mode: str,
        after_exclusive: datetime,
        through: datetime,
        as_of: datetime,
    ) -> IdAggregate:
        """Return the empty fake accrual-prefix fingerprint.

        Args:
            wallet_public_id: Wallet scope.
            mode: Trading mode.
            after_exclusive: Anchor boundary.
            through: Checkpoint boundary.
            as_of: Knowledge horizon.

        Returns:
            The zero aggregate for this fixture's empty accrual set.
        """
        del wallet_public_id, mode, after_exclusive, through, as_of
        self.calls.append("accrual_fingerprint")
        return IdAggregate(0, 0, 0, None)

    async def get_pnl_timeline_execution_lineage(
        self,
        order_public_ids: Sequence[str],
        as_of: datetime,
    ) -> list[PnlTimelineExecutionLineageRow]:
        """Log certificate and replay lineage reads in their actual order.

        Args:
            order_public_ids: Prefix order identities.
            as_of: Knowledge horizon.

        Returns:
            Canned lineage rows.
        """
        self.calls.append("lineage")
        self.lineage_scopes.append(list(order_public_ids))
        return await super().get_pnl_timeline_execution_lineage(order_public_ids, as_of)

    async def get_instrument_symbol_refs(
        self,
        instrument_public_ids: Sequence[str],
        as_of: datetime,
    ) -> list[InstrumentSymbolRefRow]:
        """Log certificate and replay symbol-reference reads.

        Args:
            instrument_public_ids: Requested instrument identities.
            as_of: Knowledge horizon.

        Returns:
            Canned symbol-reference rows.
        """
        self.calls.append("symbol_refs")
        return await super().get_instrument_symbol_refs(instrument_public_ids, as_of)

    async def get_pnl_fx_rate_exchanges_at(
        self,
        pairs: Sequence[tuple[str, str]],
        open_ats: Sequence[datetime],
        as_of: datetime,
    ) -> list[PnlFxRatePlane]:
        """Log certificate and replay FX-plane discovery reads.

        Args:
            pairs: Requested oriented currency pairs.
            open_ats: Exact candle opens.
            as_of: Knowledge horizon.

        Returns:
            Canned populated FX planes.
        """
        self.calls.append("fx_planes")
        return await super().get_pnl_fx_rate_exchanges_at(pairs, open_ats, as_of)

    async def get_pnl_event_fx_candle_fingerprints_by_plane(
        self,
        planes: Sequence[PnlFxRatePlane],
        open_ats: Sequence[datetime],
        as_of: datetime,
    ) -> dict[PnlFxRatePlane, IdAggregate]:
        """Return exact fake candle aggregates for each populated plane.

        Args:
            planes: Pinned event-FX planes.
            open_ats: Exact candle opens.
            as_of: Knowledge horizon.

        Returns:
            Populated per-plane aggregates.
        """
        del as_of
        self.calls.append("fx_by_plane")
        wanted = set(open_ats)
        return {
            plane: _fx_aggregate(
                [
                    row
                    for row in self._fx_rows
                    if (row["base"], row["quote"], row["exchange"]) == plane
                    and row["open_at"] in wanted
                ]
            )
            for plane in planes
        }

    async def get_pnl_event_fx_candle_fingerprint(
        self,
        planes: Sequence[PnlFxRatePlane],
        open_ats: Sequence[datetime],
        as_of: datetime,
    ) -> IdAggregate:
        """Return the exact total aggregate over selected fake candles.

        Args:
            planes: Pinned event-FX planes.
            open_ats: Exact candle opens.
            as_of: Knowledge horizon.

        Returns:
            Total candle identity aggregate.
        """
        del as_of
        self.calls.append("fx_total")
        selected = set(planes)
        wanted = set(open_ats)
        return _fx_aggregate(
            [
                row
                for row in self._fx_rows
                if (row["base"], row["quote"], row["exchange"]) in selected
                and row["open_at"] in wanted
            ]
        )

    async def get_pnl_fx_rate_candles_at(
        self,
        planes: Sequence[PnlFxRatePlane],
        open_ats: Sequence[datetime],
        as_of: datetime,
    ) -> list[PnlFxRateRow]:
        """Log the replay's exact FX candle read.

        Args:
            planes: Pinned event-FX planes.
            open_ats: Exact candle opens.
            as_of: Knowledge horizon.

        Returns:
            Canned FX candles.
        """
        self.calls.append("fx_candles")
        return await super().get_pnl_fx_rate_candles_at(planes, open_ats, as_of)


def _permit_capture(monkeypatch: pytest.MonkeyPatch) -> None:
    """Permit the synthetic capture version and retain the current pointwise profile.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    monkeypatch.setattr(
        pnl_engine_options,
        "_engine_options_table",
        lambda: {
            _CAPTURE_VERSION: pnl_engine_options.PnlEngineOptions(True, False, False),
            fixtures.PNL_SAMPLE_CALC_VERSION: pnl_engine_options.PnlEngineOptions(
                True,
                True,
                True,
            ),
        },
    )


async def _build_series(
    repo: _CaptureRepo,
    policy: PnlSeriesReadPolicy,
    options: PnlSeriesReplayOptions,
) -> PnlWalletSeriesResult:
    """Build the fixture series with explicit read policy and replay options.

    Args:
        repo: Spy repository.
        policy: Series read policy.
        options: Series replay options.

    Returns:
        The completed wallet series result.
    """
    return await build_wallet_pnl_series(
        cast(Repository, repo),
        fixtures._W1,
        "live",
        fixtures._m(0),
        _C,
        "1m",
        _AS_OF,
        "USD",
        policy=policy,
        options=options,
    )


async def _build(
    repo: _CaptureRepo,
    request: PnlCheckpointCaptureRequest | None,
    *,
    has_fill_gap: bool = False,
    replay_tip: datetime | None = None,
) -> PnlWalletSeriesResult:
    """Build the settling fixture series under one capture request.

    Args:
        repo: Spy repository.
        request: Optional capture request.
        has_fill_gap: Frozen gap verdict.
        replay_tip: Optional non-default generation tip.

    Returns:
        The completed wallet series result.
    """
    return await _build_series(
        repo,
        PnlSeriesReadPolicy(allow_anchor_creation=False),
        _options(request, has_fill_gap=has_fill_gap, replay_tip=replay_tip),
    )


def _matching_frontier(request: PnlCheckpointCaptureRequest) -> PnlCorrectionFrontier:
    """Build a settled frontier matching one capture request.

    Args:
        request: Capture request carrying the source cut.

    Returns:
        A settled frontier eligible for the generated certificate.
    """
    return PnlCorrectionFrontier(
        key=request.key,
        generation=request.source_generation,
        revision=4,
        window_start=fixtures._m(0),
        next_minute=None,
        tip=_C,
        as_of=_AS_OF,
        baseline_watermarks={},
        pending_watermarks=request.raw_execution_watermarks,
        prior_peak=0.0,
        repair_from=None,
        raw_execution_watermarks=request.raw_execution_watermarks,
        execution_manifest_digest=request.execution_manifest_digest,
        has_fill_gap=False,
        expected_venues=request.expected_venues,
        venue_event_watermarks=request.venue_event_watermarks,
        fill_witness_digest=request.fill_witness_digest,
    )


def _matching_chunk(request: PnlCheckpointCaptureRequest) -> PnlCorrectionChunk:
    """Build a settling chunk matching the capture request scope.

    Args:
        request: Capture request carrying the scope key.

    Returns:
        An empty settling correction chunk ending at C.
    """
    return PnlCorrectionChunk(
        end=_C,
        edits=(),
        prior_peak=0.0,
        evidence=PnlCorrectionEvidence(request.key, fixtures._m(0), _C, (), 0.0),
    )


async def test_settling_series_builds_valid_write_and_complete_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Capture a certified write with a complete round-2 state envelope.

    Given: A permitted settling series and a matching settled frontier.
    When: ``build_wallet_pnl_series`` runs with a capture request.
    Then: Its certificate passes the write predicate and its envelope decodes
        with engine state, spans, event sources, lineage, and both walk states.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    _permit_capture(monkeypatch)
    request = _request()
    result = await _build(_CaptureRepo(), request)
    assert result.checkpoint_capture_refusal is None
    write = result.checkpoint_write
    assert write is not None
    certificate = write.certificate
    assert certificate.key == request.key
    assert certificate.checkpoint_minute == _C
    assert certificate.cutoff_execution_counts == {"kraken": 1}
    assert certificate.prefix_fill_witness_digest == _WITNESS["digest"]
    assert certificate.consumed_fx_pairs == (("EUR", "USD"),)
    assert (
        checkpoint_write_refusal(
            _matching_frontier(request),
            _matching_chunk(request),
            certificate,
        )
        is None
    )
    bundle = bundle_from_json(write.state_json)
    assert isinstance(bundle, PnlCheckpointStateBundle)
    assert bundle.engine_state.checkpoint_minute == _C
    assert bundle.price_ref_spans_by_instrument
    assert bundle.display_activity_spans_by_instrument
    assert bundle.rate_sources_prefix
    assert bundle.prefix_lineage_order_ids == (f"order-{fixtures._I1}-1",)
    assert bundle.event_walk_state.requirements
    assert bundle.mark_walk_state.instruments
    assert bundle.event_fx_candles_by_plane
    assert bundle.bundle_version == 4
    assert bundle.prefix_fx_elections
    assert bundle.prefix_ref_bindings
    assert bundle.prefix_fx_coverage


async def test_capture_fingerprints_precede_each_replay_domain_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Read every certificate input immediately before its replay domain.

    Given: A permitted capture request and a spy repository.
    When: The service captures a settling series.
    Then: Execution, accrual, lineage, symbol-reference, and FX certificate reads
        precede their corresponding replay reads in plan order.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    _permit_capture(monkeypatch)
    repo = _CaptureRepo()
    result = await _build(repo, _request())
    assert result.checkpoint_write is not None
    assert repo.calls == [
        "execution_counts",
        "execution_chain",
        "execution_prefix",
        "execution_prefix",
        "accrual_fingerprint",
        "accruals",
        "lineage",
        "lineage",
        "symbol_refs",
        "symbol_refs",
        "fx_planes",
        "fx_by_plane",
        "fx_total",
        "fx_planes",
        "fx_candles",
    ]


async def test_capture_not_permitted_preserves_default_reads_and_points(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refuse 5C.5 capture without issuing any fingerprint reads.

    Given: A capture request for 5C.5 and an otherwise identical off-path read.
    When: Both service calls run.
    Then: The request returns ``capture_not_permitted`` and no write, while the
        repository call log and published points remain identical.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    del monkeypatch
    capture_repo = _CaptureRepo()
    off_repo = _CaptureRepo()
    captured = await _build(capture_repo, _request("5C.5"))
    off = await _build(off_repo, None)
    assert captured.checkpoint_write is None
    assert captured.checkpoint_capture_refusal == "capture_not_permitted"
    assert capture_repo.calls == off_repo.calls
    assert not set(capture_repo.calls) & {
        "execution_counts",
        "execution_chain",
        "accrual_fingerprint",
        "fx_by_plane",
        "fx_total",
    }
    checkpoint_fixtures._assert_points_bit_identical(off.points, captured.points)


async def test_capture_to_time_mismatch_is_soft_refusal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refuse capture when C is not the generation tip.

    Given: A permitted request whose replay generation tip follows the chunk end.
    When: The service builds the non-settling window.
    Then: It returns ``capture_not_at_tip`` without fingerprint reads or a write.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    _permit_capture(monkeypatch)
    repo = _CaptureRepo()
    result = await _build(repo, _request(), replay_tip=fixtures._m(6))
    assert result.checkpoint_write is None
    assert result.checkpoint_capture_refusal == "capture_not_at_tip"
    assert not set(repo.calls) & {
        "execution_counts",
        "execution_chain",
        "accrual_fingerprint",
        "fx_by_plane",
        "fx_total",
    }


async def test_certificate_value_error_becomes_capture_refusal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Contain certificate-construction validation failures.

    Given: A permitted request with an invalid zero source generation.
    When: Certificate assembly raises ``ValueError``.
    Then: The series returns normally with no write and ``capture_value_error``.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    _permit_capture(monkeypatch)
    request = replace(_request(), source_generation=0)
    result = await _build(_CaptureRepo(), request)
    assert result.checkpoint_write is None
    assert result.checkpoint_capture_refusal == "capture_value_error"


async def test_envelope_encode_value_error_becomes_capture_refusal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Contain strict envelope encoding failures.

    Given: A permitted valid capture whose envelope encoder raises ``ValueError``.
    When: The service assembles the write.
    Then: The series returns normally with no write and ``capture_value_error``.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    _permit_capture(monkeypatch)

    def refuse_encode(bundle: PnlCheckpointStateBundle) -> str:
        """Raise one planted encoding failure.

        Args:
            bundle: Otherwise valid state bundle.

        Returns:
            Never returns because encoding is refused.

        Raises:
            ValueError: Always, as the planted failure.
        """
        del bundle
        raise ValueError("planted encode failure")

    monkeypatch.setattr(pnl_opening_checkpoint_capture, "bundle_to_json", refuse_encode)
    result = await _build(_CaptureRepo(), _request())
    assert result.checkpoint_write is None
    assert result.checkpoint_capture_refusal == "capture_value_error"


async def test_pre_5c6_conversion_minute_gap_becomes_capture_refusal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Preserve the pre-5C.6 conversion-minute gap as a capture refusal.

    Given: A capture-enabled test profile with both valuation flags disabled, and mark
        evidence supplying a conversion absent from the mocked event requirements.
    When: The engine refuses its closing checkpoint.
    Then: The service returns no write with ``conversion_uses_mark_evidence``.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    capture_only = pnl_engine_options.PnlEngineOptions(
        capture_permitted=True,
        compensated_unrealized_fallback=False,
        request_all_conversion_minutes=False,
    )
    monkeypatch.setattr(
        pnl_engine_options,
        "_engine_options_table",
        lambda: {
            _CAPTURE_VERSION: capture_only,
            fixtures.PNL_SAMPLE_CALC_VERSION: capture_only,
        },
    )
    monkeypatch.setattr(
        "snapper.application.portfolio.pnl_timeline_service._event_fx_minutes",
        Mock(return_value={}),
    )
    result = await _build(_CaptureRepo(), _request())
    assert result.checkpoint_write is None
    assert result.checkpoint_capture_refusal == "conversion_uses_mark_evidence"


async def test_5c6_capture_is_permitted_under_unpatched_resolver() -> None:
    """Permit 5C.6 capture under the unpatched resolver when pointwise exact FX is active.

    Given: The unpatched engine-options resolver and a 5C.6 settling capture request.
    When: ``build_wallet_pnl_series`` evaluates the settling window.
    Then: It produces a certified ``checkpoint_write`` with ``checkpoint_capture_refusal is None``.
    """
    assert fixtures.PNL_SAMPLE_CALC_VERSION == "5C.6"
    result = await _build(_CaptureRepo(), _request(fixtures.PNL_SAMPLE_CALC_VERSION))
    assert result.checkpoint_write is not None
    assert result.checkpoint_capture_refusal is None


@pytest.mark.parametrize(("pointwise_exact_fx", "carry_valuation"), [(False, False), (True, True)])
async def test_capture_read_policy_guard_refuses_non_pointwise_or_carry_reads(
    monkeypatch: pytest.MonkeyPatch,
    pointwise_exact_fx: bool,
    carry_valuation: bool,
) -> None:
    """Refuse capture unless the read uses pointwise exact FX without carry valuation.

    Given: A capture-permitted engine-options table and a settling capture request
        paired with either ``pointwise_exact_fx=False`` or ``carry_valuation=True``.
    When: ``build_wallet_pnl_series`` activates checkpoint capture.
    Then: The request ends refused with ``capture_not_permitted``, produces no write,
        and issues no capture fingerprint reads.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        pointwise_exact_fx: Whether pointwise exact FX valuation is enabled.
        carry_valuation: Whether carried valuation is enabled on the read policy.
    """
    _permit_capture(monkeypatch)
    repo = _CaptureRepo()
    request = _request()
    result = await _build_series(
        repo,
        PnlSeriesReadPolicy(allow_anchor_creation=False, carry_valuation=carry_valuation),
        _options(request, pointwise_exact_fx=pointwise_exact_fx),
    )
    assert result.checkpoint_write is None
    assert result.checkpoint_capture_refusal == "capture_not_permitted"
    assert not request.active
    assert request.refusal == "capture_not_permitted"
    assert not set(repo.calls) & {
        "execution_counts",
        "execution_chain",
        "accrual_fingerprint",
        "fx_by_plane",
        "fx_total",
    }


async def test_capture_read_policy_guard_permits_pointwise_without_carry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Permit capture when the read uses pointwise exact FX with carry disabled.

    Given: A capture-permitted engine-options table and a settling capture request
        with ``pointwise_exact_fx=True`` and ``carry_valuation=False``.
    When: ``build_wallet_pnl_series`` builds the settling series.
    Then: Capture remains active and produces a valid checkpoint write with no refusal.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    _permit_capture(monkeypatch)
    request = _request()
    result = await _build_series(
        _CaptureRepo(),
        PnlSeriesReadPolicy(allow_anchor_creation=False, carry_valuation=False),
        _options(request, pointwise_exact_fx=True),
    )
    assert result.checkpoint_write is not None
    assert result.checkpoint_capture_refusal is None
    assert request.active
    assert request.refusal is None


async def test_global_withhold_becomes_capture_refusal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Drop the write when a global replay guard withholds the series.

    Given: A permitted capture whose frozen scope has a fill gap.
    When: The global withhold wrapper clears the closing checkpoint.
    Then: The service returns no write with ``global_withhold``.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    _permit_capture(monkeypatch)
    result = await _build(_CaptureRepo(), _request(), has_fill_gap=True)
    assert result.checkpoint_write is None
    assert result.checkpoint_capture_refusal == "global_withhold"


async def test_inactive_capture_request_skips_every_collector() -> None:
    """Make every collector a no-op after capture permission is refused.

    Given: A 5C.5 request that has completed activation and been refused.
    When: Every collector and the walk-state setter are called.
    Then: No repository method runs and the original refusal remains unchanged.
    """
    request = _request("5C.5")
    assert request.activate(_C, _C) == "capture_not_permitted"
    repo = AsyncMock()
    await request.capture_execution_prefix(repo, fixtures._W1, "live", _AS_OF)
    await request.capture_accrual_prefix(
        repo,
        _AccrualPrefixRequest(fixtures._W1, "live", fixtures._m(0), _C, _AS_OF),
    )
    await request.capture_prefix_lineage(repo, (), _AS_OF)
    await request.capture_symbol_refs(repo, (), _AS_OF)
    await request.capture_event_fx(repo, {}, _AS_OF)
    request.set_walk_states(
        cast(PnlEventFxReplayStatePayload, object()),
        cast(PnlMarkFxReplayStatePayload, object()),
    )
    assert repo.mock_calls == []
    assert request.refusal == "capture_not_permitted"


async def test_collector_value_errors_are_soft_refusals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Contain ValueError from every certificate evidence read.

    Given: An active request and repository methods planted to raise ValueError.
    When: Each collector reaches its failing read.
    Then: The request becomes inactive with ``capture_value_error`` and never raises.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    _permit_capture(monkeypatch)
    request = _request()
    repo = AsyncMock()
    request.activate(_C, _C)
    repo.get_pnl_execution_prefix_counts.side_effect = ValueError("counts")
    await request.capture_execution_prefix(repo, fixtures._W1, "live", _AS_OF)
    assert request.refusal == "capture_value_error"

    request.activate(_C, _C)
    repo.get_pnl_accrual_prefix_fingerprint.side_effect = ValueError("accruals")
    await request.capture_accrual_prefix(
        repo,
        _AccrualPrefixRequest(fixtures._W1, "live", fixtures._m(0), _C, _AS_OF),
    )
    assert request.refusal == "capture_value_error"

    request.activate(_C, _C)
    repo.get_pnl_timeline_execution_lineage.side_effect = ValueError("lineage")
    await request.capture_prefix_lineage(repo, (), _AS_OF)
    assert request.refusal == "capture_value_error"

    request.activate(_C, _C)
    repo.get_instrument_symbol_refs.side_effect = ValueError("refs")
    await request.capture_symbol_refs(repo, (), _AS_OF)
    assert request.refusal == "capture_value_error"

    request.activate(_C, _C)
    repo.get_pnl_fx_rate_exchanges_at.side_effect = ValueError("planes")
    await request.capture_event_fx(repo, {}, _AS_OF)
    assert request.refusal == "capture_value_error"
    assert not request.active


async def test_collector_database_errors_are_soft_refusals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Contain SQLAlchemyError from every certificate evidence read.

    Given: An active request and repository methods planted to raise database errors.
    When: Each collector reaches its failing read.
    Then: The request becomes inactive with ``capture_evidence_error`` and never raises.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    _permit_capture(monkeypatch)
    request = _request()
    repo = AsyncMock()
    request.activate(_C, _C)
    repo.get_pnl_execution_prefix_counts.side_effect = SQLAlchemyError("counts")
    await request.capture_execution_prefix(repo, fixtures._W1, "live", _AS_OF)
    assert request.refusal == "capture_evidence_error"

    request.activate(_C, _C)
    repo.get_pnl_accrual_prefix_fingerprint.side_effect = SQLAlchemyError("accruals")
    await request.capture_accrual_prefix(
        repo,
        _AccrualPrefixRequest(fixtures._W1, "live", fixtures._m(0), _C, _AS_OF),
    )
    assert request.refusal == "capture_evidence_error"

    request.activate(_C, _C)
    repo.get_pnl_timeline_execution_lineage.side_effect = SQLAlchemyError("lineage")
    await request.capture_prefix_lineage(repo, (), _AS_OF)
    assert request.refusal == "capture_evidence_error"

    request.activate(_C, _C)
    repo.get_instrument_symbol_refs.side_effect = SQLAlchemyError("refs")
    await request.capture_symbol_refs(repo, (), _AS_OF)
    assert request.refusal == "capture_evidence_error"

    request.activate(_C, _C)
    repo.get_pnl_fx_rate_exchanges_at.side_effect = SQLAlchemyError("planes")
    await request.capture_event_fx(repo, {}, _AS_OF)
    assert request.refusal == "capture_evidence_error"
    assert not request.active


async def test_capture_database_error_completes_series_without_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return the financial series after a capture-only database read fails.

    Given: A permitted capture whose accrual fingerprint read raises SQLAlchemyError.
    When: The real series loader continues with its independent domain reads.
    Then: The series completes with ``capture_evidence_error`` and no checkpoint write.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    _permit_capture(monkeypatch)
    repo = _CaptureRepo()
    monkeypatch.setattr(
        repo,
        "get_pnl_accrual_prefix_fingerprint",
        AsyncMock(side_effect=SQLAlchemyError("aborted capture transaction")),
    )
    result = await _build(repo, _request())
    assert result.points
    assert result.checkpoint_write is None
    assert result.checkpoint_capture_refusal == "capture_evidence_error"


async def test_missing_execution_witness_is_soft_refusal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refuse an execution capture that omits its required witness cut.

    Given: An active request and a prefix response without ``fill_witness_cut``.
    When: Execution evidence collection runs.
    Then: The missing witness becomes ``capture_value_error`` without raising.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    _permit_capture(monkeypatch)
    request = _request()
    request.activate(_C, _C)
    repo = AsyncMock()
    repo.get_pnl_execution_prefix_counts.return_value = {"kraken": (1, 1)}
    repo.get_spot_execution_chain_tip.return_value = "e" * 64
    repo.get_pnl_timeline_execution_prefix_at_watermarks.return_value = {
        "watermarks": {"kraken": 1},
        "executions": [],
        "annulments": [],
    }
    await request.capture_execution_prefix(repo, fixtures._W1, "live", _AS_OF)
    assert request.refusal == "capture_value_error"


def test_unknown_calc_version_and_incomplete_assembly_are_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refuse unknown versions and fail closed on missing assembly evidence.

    Given: An unknown calculation version and an active request with no evidence.
    When: Activation and certificate assembly are attempted.
    Then: Activation returns a soft reason and assembly raises ValueError for the
        service's outer refusal boundary.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    unknown = _request("unknown")
    assert unknown.activate(_C, _C) == "capture_value_error"
    unknown.refuse_value_error()
    _permit_capture(monkeypatch)
    request = _request()
    request.activate(_C, _C)
    with pytest.raises(ValueError, match="capture evidence is missing"):
        request.assemble(cast(PnlCheckpointCaptureAssembly, object()))


def test_execution_partitions_include_deferred_sequences() -> None:
    """Partition deferred sequences from applied watermarks without reordering.

    Given: Two prefix executions and an engine state deferring the second one.
    When: The certificate partitions are derived.
    Then: The first sequence is applied and the second is carried as deferred.
    """
    first = fixtures._exec_row(fixtures._I1, 1, 3, "buy", 1.0, 100.0, 0.0, "USD")
    second = fixtures._exec_row(fixtures._I1, 2, 4, "buy", 1.0, 100.0, 0.0, "USD")
    engine_state = cast(
        TimelineCheckpointOpening,
        SimpleNamespace(deferred_execution_keys=(("kraken", 2),)),
    )
    applied, deferred = _execution_watermark_partitions(
        {"watermarks": {"kraken": 2}, "executions": [first, second], "annulments": []},
        engine_state,
    )
    assert applied == {"kraken": 1}
    assert deferred == {"kraken": (2,)}


async def test_walk_state_value_error_becomes_capture_refusal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Contain a ValueError raised while converting real walk state.

    Given: An otherwise valid capture whose event-walk payload conversion fails.
    When: The service collects the walk states.
    Then: It returns no write with ``capture_value_error`` and never raises.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    _permit_capture(monkeypatch)
    monkeypatch.setattr(
        "snapper.application.portfolio.pnl_timeline_service.event_walk_state_from_replay",
        Mock(side_effect=ValueError("planted walk failure")),
    )
    result = await _build(_CaptureRepo(), _request())
    assert result.checkpoint_write is None
    assert result.checkpoint_capture_refusal == "capture_value_error"


async def test_missing_engine_checkpoint_without_reason_is_soft_refusal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refuse an absent engine checkpoint even when the engine gives no reason.

    Given: A permitted capture and an engine result with no closing checkpoint.
    When: The service reaches final write assembly.
    Then: It returns no write with ``engine_checkpoint_missing``.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    _permit_capture(monkeypatch)
    monkeypatch.setattr(
        "snapper.application.portfolio.pnl_timeline_service._timed_build_pnl_timeline",
        Mock(
            return_value=PnlTimelineResult(
                points=(),
                granularity="1m",
                valuation_ccy="USD",
            )
        ),
    )
    result = await _build(_CaptureRepo(), _request())
    assert result.checkpoint_write is None
    assert result.checkpoint_capture_refusal == "engine_checkpoint_missing"


async def test_deferred_prefix_executions_are_included_in_lineage_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Carry lineage for deferred fills that the engine has not applied by C.

    Given: Two out-of-order prefix fills whose clamped effective times follow C.
    When: A permitted settling series captures its checkpoint.
    Then: The certificate marks both sequences deferred and the envelope carries
        both order ids, while the replay lineage read remains limited to applied rows.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    _permit_capture(monkeypatch)
    repo = _CaptureRepo()
    repo._executions = [
        fixtures._exec_row(fixtures._I1, 1, 6, "buy", 1.0, 100.0, 0.0, "USD"),
        fixtures._exec_row(fixtures._I1, 2, 5, "buy", 1.0, 100.0, 0.0, "USD"),
    ]
    repo._execution_watermarks = {"kraken": 2}
    request = replace(_request(), raw_execution_watermarks={"kraken": 2})
    result = await _build(repo, request)
    assert result.checkpoint_capture_refusal is None
    write = result.checkpoint_write
    assert write is not None
    assert write.certificate.deferred_execution_sequences == {"kraken": (1, 2)}
    assert bundle_from_json(write.state_json).prefix_lineage_order_ids == (
        f"order-{fixtures._I1}-1",
        f"order-{fixtures._I1}-2",
    )
    assert repo.lineage_scopes[1] == []


class _UnusedEventEvidence:
    """Event evidence with an unused mark-only plane, an uncandled pin, and an absent pin."""

    used_planes: set[tuple[tuple[str, str], PnlFxRatePlane]] = {
        (("EUR", "USD"), ("EUR", "USD", "walutomat")),
        (("EUR", "USD"), ("EUR", "USD", "kraken")),
    }

    def venues_at(
        self,
        instrument_public_id: str,
        minute: datetime,
    ) -> dict[tuple[str, str], PnlFxRatePlane]:
        """Return an uncandled walutomat pin at M3, a kraken pin at M2, and empty at M4.

        Args:
            instrument_public_id: Event instrument.
            minute: Exact event minute.

        Returns:
            The synthetic venue map for the requested minute.
        """
        del instrument_public_id
        if minute == fixtures._m(2):
            return {("EUR", "USD"): ("EUR", "USD", "kraken")}
        if minute == fixtures._m(3):
            return {("EUR", "USD"): ("EUR", "USD", "walutomat")}
        return {}

    def has_usable_rate(self, plane: PnlFxRatePlane, minute: datetime) -> bool:
        """Report a usable candle only when kraken is consumed at M2.

        Args:
            plane: Candidate FX plane.
            minute: Exact event minute.

        Returns:
            True solely for kraken at M2.
        """
        return plane == ("EUR", "USD", "kraken") and minute == fixtures._m(2)


def test_event_rate_sources_exclude_absent_and_unused_planes() -> None:
    """Exclude absent pins and planes without a positive exact close at an event minute.

    Given: A mark-only walutomat plane in used_planes tied to an uncandled M3 event, an
        unpinned M4 event, and a truly consumed kraken close at M2.
    When: Event rate-source payloads are derived across M3 and M4 alone, then with M2.
    Then: Mark-only walutomat is never emitted and only kraken enters rate_sources_prefix.
    """
    excluded = event_rate_source_payloads(
        {fixtures._I1: {("EUR", "USD"): {fixtures._m(3), fixtures._m(4)}}},
        _UnusedEventEvidence(),
        "USD",
    )
    assert excluded == ()
    consumed = event_rate_source_payloads(
        {fixtures._I1: {("EUR", "USD"): {fixtures._m(2), fixtures._m(3), fixtures._m(4)}}},
        _UnusedEventEvidence(),
        "USD",
    )
    assert [source.exchange for source in consumed] == ["kraken"]


async def test_symbol_ref_digest_rejects_uncaptured_engine_seen_instrument(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail closed when engine_state.seen includes an instrument not captured before replay.

    Given: An active capture request that fetched symbol refs only for I1.
    When: ``_symbol_ref_digest`` is asked to hash refs for both I1 and uncaptured I2.
    Then: It raises ValueError so capture refuses instead of hashing a partial set.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    _permit_capture(monkeypatch)
    request = _request()
    request.activate(_C, _C)
    repo = _CaptureRepo()
    await request.capture_symbol_refs(cast(Repository, repo), [fixtures._I1], _AS_OF)
    with pytest.raises(ValueError, match="omitted an engine-seen instrument"):
        request._symbol_ref_digest((fixtures._I1, fixtures._I2))


async def test_global_withhold_sets_refusal_only_when_capture_requested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Leave closing_checkpoint_refusal as None on default globally withheld reads.

    Given: A scope with a global fill gap read once without capture and once
        with a permitted checkpoint capture request.
    When: ``build_wallet_pnl_series`` builds both globally withheld results.
    Then: The default read keeps ``closing_checkpoint_refusal=None`` while the
        capture read reports ``"global_withhold"``.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    _permit_capture(monkeypatch)
    default_result = await _build(_CaptureRepo(), None, has_fill_gap=True)
    assert default_result.closing_checkpoint is None
    assert default_result.closing_checkpoint_refusal is None
    assert default_result.checkpoint_capture_refusal is None
    capture_result = await _build(_CaptureRepo(), _request(), has_fill_gap=True)
    assert capture_result.closing_checkpoint is None
    assert capture_result.closing_checkpoint_refusal == "global_withhold"
    assert capture_result.checkpoint_capture_refusal == "global_withhold"


def test_capture_request_rejects_mismatched_calc_version() -> None:
    """Reject a capture request whose calc_version disagrees with key.calc_version.

    Given: A capture request whose correction key specifies ``"5C.5"``.
    When: ``calc_version`` is set to a different version string.
    Then: Construction raises ``ValueError``.
    """
    with pytest.raises(ValueError, match="calc_version"):
        replace(_request("5C.5"), calc_version=_CAPTURE_VERSION)


async def test_inactive_capture_request_under_global_withhold_preserves_inactive_refusal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep closing_checkpoint_refusal as None when an inactive capture sees a global withhold.

    Given: A scope with a global fill gap read with an unpermitted ``"5C.5"``
        capture request and with an off-tip permitted capture request.
    When: ``build_wallet_pnl_series`` builds both globally withheld results.
    Then: Neither mutates ``closing_checkpoint_refusal`` to ``"global_withhold"``
        and each retains its inactive capture refusal reason.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    unpermitted = await _build(_CaptureRepo(), _request("5C.5"), has_fill_gap=True)
    assert unpermitted.closing_checkpoint is None
    assert unpermitted.closing_checkpoint_refusal is None
    assert unpermitted.checkpoint_capture_refusal == "capture_not_permitted"
    _permit_capture(monkeypatch)
    off_tip = await _build(
        _CaptureRepo(),
        _request(),
        has_fill_gap=True,
        replay_tip=fixtures._m(8),
    )
    assert off_tip.closing_checkpoint is None
    assert off_tip.closing_checkpoint_refusal is None
    assert off_tip.checkpoint_capture_refusal == "capture_not_at_tip"


async def test_capture_execution_prefix_refuses_execution_chain_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Convert ExecutionChainError during capture_execution_prefix into a closed refusal.

    Given: An active capture request whose chain-tip or prefix-certification read
        raises ``ExecutionChainError``.
    When: ``capture_execution_prefix`` and ``build_wallet_pnl_series`` run.
    Then: ``ExecutionChainError`` does not escape, the request becomes inactive
        with ``capture_execution_chain_error``, and the series completes cleanly.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    _permit_capture(monkeypatch)
    for failing_method in (
        "get_spot_execution_chain_tip",
        "get_pnl_timeline_execution_prefix_at_watermarks",
    ):
        request = _request()
        request.activate(_C, _C)
        repo = _CaptureRepo()
        monkeypatch.setattr(
            repo,
            failing_method,
            AsyncMock(side_effect=ExecutionChainError("broken chain")),
        )
        await request.capture_execution_prefix(
            cast(Repository, repo),
            fixtures._W1,
            "live",
            _AS_OF,
        )
        assert not request.active
        assert request.refusal == "capture_execution_chain_error"
    end_to_end_repo = _CaptureRepo()
    monkeypatch.setattr(
        end_to_end_repo,
        "get_spot_execution_chain_tip",
        AsyncMock(side_effect=ExecutionChainError("broken chain")),
    )
    result = await _build(end_to_end_repo, _request())
    assert result.checkpoint_write is None
    assert result.checkpoint_capture_refusal == "capture_execution_chain_error"
