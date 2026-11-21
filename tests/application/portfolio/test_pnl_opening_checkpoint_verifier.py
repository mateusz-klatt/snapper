"""Tests for ordered checkpoint resume eligibility and evidence verification."""

from dataclasses import dataclass
from dataclasses import replace
from typing import cast
from unittest.mock import AsyncMock

import pytest

from snapper.application.portfolio.pnl_opening_checkpoint_capture import PnlCheckpointCaptureRequest
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import PnlCheckpointFallback
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import PnlCheckpointResume
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import (
    PnlCheckpointResumeEligibility,
)
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import (
    verify_checkpoint_for_resume,
)
from snapper.data.pnl_correction_state import IdAggregate
from snapper.data.pnl_correction_state import PnlOpeningCheckpointRecord
from snapper.data.repository import PortfolioPnlSampleScope
from snapper.data.repository_types import PnlTimelineFillWitnessCut
from tests.application.portfolio import test_pnl_opening_checkpoint_capture as capture
from tests.application.portfolio import test_pnl_timeline_service as fixtures

_RESUMING_AS_OF = fixtures._m(20)


@dataclass(frozen=True)
class _FallbackCase:
    """One isolated corruption or eligibility mutation and its delete policy."""

    reason: str
    delete_required: bool
    absent: bool = False
    corrupt: bool = False
    certificate_field: str = ""
    certificate_value: object = None
    eligibility_field: str = ""
    eligibility_value: object = None
    repository_method: str = ""
    repository_value: object = None
    repository_field: str = ""
    prefix_witness: dict[str, object] | None = None


def _eligibility(request: PnlCheckpointCaptureRequest) -> PnlCheckpointResumeEligibility:
    """Build eligible exact-policy resume inputs matching a captured request.

    Args:
        request: Source capture request carrying scope and anchor identity.

    Returns:
        Eligible resume policy and version inputs.
    """
    return PnlCheckpointResumeEligibility(
        mode="active",
        calc_version=request.calc_version,
        timeline_calc_version="5A.16",
        fx_policy_version="pnl-fiat-pointwise-v1",
        expected_anchor_public_id=request.anchor_public_id,
        expected_anchor_digest=request.anchor_digest,
        pointwise_exact_fx=True,
        replay_generation_tip=fixtures._m(8),
        metadata_only=False,
        from_time=fixtures._m(6),
        frontier_tip=fixtures._m(8),
        has_fill_gap=False,
        has_late_execution=False,
    )


def _scope(request: PnlCheckpointCaptureRequest) -> PortfolioPnlSampleScope:
    """Build the exact sample scope bound to the capture anchor.

    Args:
        request: Capture request carrying the correction key.

    Returns:
        Anchor-bound sample scope for verification.
    """
    return PortfolioPnlSampleScope(
        wallet_public_id=request.key.wallet_public_id,
        mode=request.key.mode,
        valuation_ccy=request.key.valuation_ccy,
        epoch_public_id=request.key.epoch_public_id,
        anchor_point_time=fixtures._T0,
    )


async def _valid_capture(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[PnlOpeningCheckpointRecord, PnlCheckpointCaptureRequest, capture._CaptureRepo]:
    """Capture one valid record through the real service and test seam.

    Args:
        monkeypatch: Pytest monkeypatch fixture.

    Returns:
        The persisted-shaped record, source request, and populated spy repository.
    """
    capture._permit_capture(monkeypatch)
    request = capture._request()
    repo = capture._CaptureRepo()
    result = await capture._build(repo, request)
    assert result.checkpoint_write is not None
    write = result.checkpoint_write
    return (
        PnlOpeningCheckpointRecord(write.certificate, write.state_json, _RESUMING_AS_OF),
        request,
        repo,
    )


def _cases() -> tuple[_FallbackCase, ...]:
    """Return every closed-vocabulary fallback with an isolated trigger.

    Returns:
        All absent, ineligible, version, corruption, and evidence-change cases.
    """
    return (
        _FallbackCase("checkpoint_absent", False, absent=True),
        _FallbackCase(
            "checkpoint_ineligible",
            False,
            eligibility_field="mode",
            eligibility_value="off",
        ),
        _FallbackCase(
            "checkpoint_version_mismatch",
            False,
            certificate_field="timeline_calc_version",
            certificate_value="other",
        ),
        _FallbackCase(
            "checkpoint_version_mismatch",
            False,
            eligibility_field="calc_version",
            eligibility_value="unknown",
        ),
        _FallbackCase(
            "checkpoint_version_mismatch",
            False,
            eligibility_field="calc_version",
            eligibility_value="5C.5",
        ),
        _FallbackCase(
            "checkpoint_fill_gap",
            False,
            eligibility_field="has_fill_gap",
            eligibility_value=True,
        ),
        _FallbackCase("checkpoint_payload_corrupt", True, corrupt=True),
        _FallbackCase(
            "checkpoint_anchor_mismatch",
            True,
            eligibility_field="expected_anchor_digest",
            eligibility_value="f" * 64,
        ),
        _FallbackCase(
            "checkpoint_execution_counts_changed",
            True,
            repository_method="get_pnl_execution_prefix_counts",
            repository_value={"kraken": (0, 0)},
        ),
        _FallbackCase(
            "checkpoint_execution_counts_changed",
            True,
            repository_method="get_pnl_execution_prefix_counts",
            repository_value={},
        ),
        _FallbackCase(
            "checkpoint_execution_counts_changed",
            True,
            repository_method="get_pnl_execution_prefix_counts",
            repository_value={"kraken": (1, 0)},
        ),
        _FallbackCase(
            "checkpoint_prefix_annulment_changed",
            True,
            repository_field="_applied_annulments",
            repository_value=[fixtures._applied_annulment()],
        ),
        _FallbackCase(
            "checkpoint_prefix_witness_changed",
            True,
            prefix_witness={"watermarks": {}, "digest": "f" * 64},
        ),
        _FallbackCase(
            "checkpoint_prefix_witness_changed",
            True,
            prefix_witness={},
        ),
        _FallbackCase(
            "checkpoint_accrual_prefix_changed",
            True,
            repository_method="get_pnl_accrual_prefix_fingerprint",
            repository_value=IdAggregate(1, 1, 1, fixtures._m(1)),
        ),
        _FallbackCase(
            "checkpoint_lineage_prefix_changed",
            True,
            repository_field="_lineage",
            repository_value=[
                fixtures._lineage_row(f"order-{fixtures._I1}-1", "rest", origin="paper")
            ],
        ),
        _FallbackCase(
            "checkpoint_symbol_refs_changed",
            True,
            repository_field="_refs",
            repository_value=[fixtures._ref(fixtures._I1, "BTC-USD", "USD")],
        ),
        _FallbackCase(
            "checkpoint_event_fx_exchanges_changed",
            True,
            repository_field="_fx_rows",
            repository_value=[
                fixtures._fx_row("EUR", "USD", 3, 2.0),
                fixtures._fx_row("EUR", "USD", 3, 2.0, exchange="walutomat"),
            ],
        ),
        _FallbackCase(
            "checkpoint_event_fx_candles_changed",
            True,
            repository_method="get_pnl_event_fx_candle_fingerprint",
            repository_value=IdAggregate(0, 0, 0, None),
        ),
        _FallbackCase(
            "checkpoint_event_fx_candles_changed",
            True,
            repository_method="get_pnl_event_fx_candle_fingerprints_by_plane",
            repository_value={},
        ),
        _FallbackCase(
            "checkpoint_late_execution",
            True,
            eligibility_field="has_late_execution",
            eligibility_value=True,
        ),
    )


@pytest.mark.parametrize("case", _cases())
async def test_each_fallback_reason_and_delete_policy(
    monkeypatch: pytest.MonkeyPatch,
    case: _FallbackCase,
) -> None:
    """Trigger each fallback through its own eligibility or evidence domain.

    Given: A real valid capture and one isolated mutation.
    When: Ordered resume verification runs at the resuming horizon.
    Then: The closed reason and required delete policy are returned without raising.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        case: Mutation and expected fallback policy.
    """
    record, request, repo = await _valid_capture(monkeypatch)
    eligibility = _eligibility(request)
    if case.certificate_field:
        record = replace(
            record,
            certificate=replace(
                record.certificate,
                **{case.certificate_field: case.certificate_value},
            ),
        )
    if case.eligibility_field:
        eligibility = replace(
            eligibility,
            **{case.eligibility_field: case.eligibility_value},
        )
    if case.corrupt:
        record = replace(record, state_json="{")
    if case.repository_field:
        setattr(repo, case.repository_field, case.repository_value)
    if case.repository_method:
        monkeypatch.setattr(
            repo,
            case.repository_method,
            AsyncMock(return_value=case.repository_value),
        )
    if case.prefix_witness is not None:
        prefix = repo._execution_prefix_at(capture._AS_OF)
        if case.prefix_witness:
            prefix["fill_witness_cut"] = cast(
                PnlTimelineFillWitnessCut,
                case.prefix_witness,
            )
        monkeypatch.setattr(
            repo,
            "get_pnl_timeline_execution_prefix_at_watermarks",
            AsyncMock(return_value=prefix),
        )
    outcome = await verify_checkpoint_for_resume(
        repo,
        None if case.absent else record,
        _scope(request),
        _RESUMING_AS_OF,
        eligibility,
    )
    assert isinstance(outcome, PnlCheckpointFallback)
    assert outcome.reason == case.reason
    assert outcome.delete_required is case.delete_required


async def test_valid_resume_uses_resuming_as_of_for_every_temporal_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify all evidence at the resuming horizon rather than certificate time.

    Given: A valid certificate whose original as_of precedes the resuming horizon.
    When: Verification succeeds.
    Then: Every repository read carrying a horizon receives the resuming as_of.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    record, request, repo = await _valid_capture(monkeypatch)
    repo.symbol_ref_as_of_calls.clear()
    repo.lineage_calls.clear()
    repo.fx_exact_candidate_calls.clear()
    outcome = await verify_checkpoint_for_resume(
        repo,
        record,
        _scope(request),
        _RESUMING_AS_OF,
        _eligibility(request),
    )
    assert isinstance(outcome, PnlCheckpointResume)
    assert outcome.record == record
    assert outcome.bundle.engine_state.checkpoint_minute == record.certificate.checkpoint_minute
    assert record.certificate.as_of != _RESUMING_AS_OF
    assert repo.symbol_ref_as_of_calls == [_RESUMING_AS_OF]
    assert [as_of for _, as_of in repo.lineage_calls] == [_RESUMING_AS_OF]
    assert repo.fx_exact_candidate_calls == [list(record.certificate.consumed_fx_open_ats)]
