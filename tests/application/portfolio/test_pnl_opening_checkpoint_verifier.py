"""Tests for ordered checkpoint resume eligibility and evidence verification."""

import json
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import replace
from datetime import datetime
from typing import Literal
from typing import cast
from unittest.mock import AsyncMock

import pytest

from snapper.application.portfolio.execution_chain import ExecutionChainError
from snapper.application.portfolio.pnl_opening_checkpoint_capture import PnlCheckpointCaptureRequest
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import PnlCheckpointFallback
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import PnlCheckpointResume
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import (
    PnlCheckpointResumeEligibility,
)
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import service_resume_fallback
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import (
    verify_checkpoint_for_resume,
)
from snapper.data.pnl_correction_state import IdAggregate
from snapper.data.pnl_correction_state import PnlOpeningCheckpointRecord
from snapper.data.repository import PortfolioPnlSampleScope
from snapper.data.repository_types import PnlTimelineExecutionPrefix
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
        timeline_calc_version="5A.18",
        fx_policy_version="pnl-fiat-pointwise-v1",
        expected_anchor_public_id=request.anchor_public_id,
        expected_anchor_digest=request.anchor_digest,
        pointwise_exact_fx=True,
        replay_generation_tip=fixtures._m(8),
        metadata_only=False,
        from_time=fixtures._m(6),
        frontier_tip=fixtures._m(8),
        has_fill_gap=False,
        expected_execution_manifest_digest=request.execution_manifest_digest,
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
        anchor_point_time=capture._anchor()["point_time"],
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
            "checkpoint_execution_manifest_changed",
            True,
            eligibility_field="expected_execution_manifest_digest",
            eligibility_value="f" * 64,
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


async def test_capture_and_verify_with_accrual_only_instrument(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a checkpoint when an instrument appears only via prefix accruals.

    Given: An executed instrument and a second instrument touched only by a
        prefix accrual before C, both with symbol references.
    When: The checkpoint is captured at C and verified at the resuming horizon.
    Then: Verification succeeds without falling back on symbol_ref_digest.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    capture._permit_capture(monkeypatch)
    request = capture._request()
    repo = capture._CaptureRepo()
    repo._accruals = [fixtures._accrual_row(fixtures._I2, 4, 1.5, "USD")]
    repo._refs = [
        fixtures._ref(fixtures._I1, "BTC-EUR", "EUR"),
        fixtures._ref(fixtures._I2, "ETH-USD", "USD"),
    ]
    result = await capture._build(repo, request)
    assert result.checkpoint_write is not None
    write = result.checkpoint_write
    record = PnlOpeningCheckpointRecord(write.certificate, write.state_json, _RESUMING_AS_OF)
    outcome = await verify_checkpoint_for_resume(
        repo,
        record,
        _scope(request),
        _RESUMING_AS_OF,
        _eligibility(request),
    )
    assert isinstance(outcome, PnlCheckpointResume)
    assert set(outcome.bundle.engine_state.seen) == {fixtures._I1, fixtures._I2}


async def test_capture_and_verify_with_deferred_execution_on_new_instrument(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a checkpoint when a deferred execution introduces a new instrument.

    Given: An applied execution on I1 before C and a deferred execution on a new
        instrument I2 after C within the same captured watermark cut.
    When: The checkpoint is captured at C and verified at the resuming horizon.
    Then: Verification succeeds without falling back on symbol_ref_digest.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    capture._permit_capture(monkeypatch)
    request = replace(capture._request(), raw_execution_watermarks={"kraken": 2})
    repo = capture._CaptureRepo()
    repo._executions = [
        fixtures._exec_row(fixtures._I1, 1, 3, "buy", 1.0, 100.0, 2.0, "EUR"),
        fixtures._exec_row(fixtures._I2, 2, 6, "buy", 1.0, 50.0, 0.0, "USD"),
    ]
    repo._lineage = [
        fixtures._lineage_row(row["order_public_id"], "rest") for row in repo._executions
    ]
    repo._refs = [
        fixtures._ref(fixtures._I1, "BTC-EUR", "EUR"),
        fixtures._ref(fixtures._I2, "ETH-USD", "USD"),
    ]
    repo._execution_watermarks = {"kraken": 2}
    result = await capture._build(repo, request)
    assert result.checkpoint_write is not None
    write = result.checkpoint_write
    record = PnlOpeningCheckpointRecord(write.certificate, write.state_json, _RESUMING_AS_OF)
    outcome = await verify_checkpoint_for_resume(
        repo,
        record,
        _scope(request),
        _RESUMING_AS_OF,
        _eligibility(request),
    )
    assert isinstance(outcome, PnlCheckpointResume)
    assert outcome.bundle.engine_state.seen == (fixtures._I1,)
    assert outcome.record.certificate.deferred_execution_sequences == {"kraken": (2,)}


@pytest.mark.parametrize(
    ("scope_field", "scope_value"),
    [
        ("wallet_public_id", "00000000-0000-7000-8000-000000008888"),
        ("mode", "paper"),
        ("valuation_ccy", "EUR"),
        ("epoch_public_id", "00000000-0000-7000-8000-000000009999"),
    ],
)
async def test_certificate_key_scope_mismatch_returns_corrupt_with_delete(
    monkeypatch: pytest.MonkeyPatch,
    scope_field: str,
    scope_value: str,
) -> None:
    """Reject a certificate whose scope identity differs from the resuming scope.

    Given: A valid captured checkpoint record and a resuming scope differing in one
        key field (wallet, mode, valuation currency, or epoch).
    When: ``verify_checkpoint_for_resume`` verifies the record.
    Then: It returns ``checkpoint_payload_corrupt`` with ``delete_required=True``.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        scope_field: Scope attribute to mutate.
        scope_value: Mismatched value to substitute.
    """
    record, request, repo = await _valid_capture(monkeypatch)
    mismatched_scope = replace(_scope(request), **{scope_field: scope_value})
    outcome = await verify_checkpoint_for_resume(
        repo,
        record,
        mismatched_scope,
        _RESUMING_AS_OF,
        _eligibility(request),
    )
    assert outcome == PnlCheckpointFallback("checkpoint_payload_corrupt", delete_required=True)


@pytest.mark.parametrize(
    ("identity_field", "identity_value"),
    [
        ("wallet_public_id", "00000000-0000-7000-8000-000000008888"),
        ("mode", "paper"),
        ("valuation_ccy", "EUR"),
        ("epoch_public_id", "00000000-0000-7000-8000-000000009999"),
        ("calc_version", "5C.5"),
        ("source_generation", 99),
    ],
)
async def test_bundle_identity_mismatch_returns_corrupt_with_delete(
    monkeypatch: pytest.MonkeyPatch,
    identity_field: str,
    identity_value: object,
) -> None:
    """Reject a state_json bundle whose embedded identity differs from the certificate.

    Given: A valid captured checkpoint whose serialized bundle identity has one
        mutated field.
    When: ``verify_checkpoint_for_resume`` verifies the record.
    Then: It returns ``checkpoint_payload_corrupt`` with ``delete_required=True``.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        identity_field: Embedded identity field to mutate.
        identity_value: Mismatched value to plant in ``state_json``.
    """
    record, request, repo = await _valid_capture(monkeypatch)
    payload = cast(dict[str, object], json.loads(record.state_json))
    assert "identity" in payload
    identity = cast(dict[str, object], payload["identity"])
    identity[identity_field] = identity_value
    payload["identity"] = identity
    tampered = replace(record, state_json=json.dumps(payload))
    outcome = await verify_checkpoint_for_resume(
        repo,
        tampered,
        _scope(request),
        _RESUMING_AS_OF,
        _eligibility(request),
    )
    assert outcome == PnlCheckpointFallback("checkpoint_payload_corrupt", delete_required=True)


@pytest.mark.parametrize(
    ("engine_field", "engine_value"),
    [
        ("checkpoint_minute", fixtures._m(6).isoformat()),
        ("activation_time", fixtures._m(0).isoformat()),
        ("activation_time", None),
        ("valuation_ccy", "EUR"),
        ("deferred_execution_keys", [{"exchange": "kraken", "scope_sequence": 1}]),
    ],
)
async def test_bundle_engine_header_mismatch_returns_corrupt_with_delete(
    monkeypatch: pytest.MonkeyPatch,
    engine_field: str,
    engine_value: object,
) -> None:
    """Reject a state_json engine header disagreeing with the certificate or scope.

    Given: A valid captured checkpoint whose serialized ``engine_state`` has one
        mismatched boundary, activation time, valuation currency, or deferred key.
    When: ``verify_checkpoint_for_resume`` verifies the record.
    Then: It returns ``checkpoint_payload_corrupt`` with ``delete_required=True``.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        engine_field: Engine-state field to mutate.
        engine_value: Mismatched JSON value to plant in ``state_json``.
    """
    record, request, repo = await _valid_capture(monkeypatch)
    payload = cast(dict[str, object], json.loads(record.state_json))
    engine_state = cast(dict[str, object], payload["engine_state"])
    engine_state[engine_field] = engine_value
    tampered = replace(record, state_json=json.dumps(payload))
    outcome = await verify_checkpoint_for_resume(
        repo,
        tampered,
        _scope(request),
        _RESUMING_AS_OF,
        _eligibility(request),
    )
    assert outcome == PnlCheckpointFallback("checkpoint_payload_corrupt", delete_required=True)


async def test_legacy_bundle_version_1_payload_refused_with_bundle_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refuse an old bundle_version=1 payload without embedded identity as bundle version.

    Given: A captured checkpoint whose ``state_json`` is downgraded to
        ``bundle_version=1`` without an ``identity`` block.
    When: ``verify_checkpoint_for_resume`` verifies the record.
    Then: It fails closed with ``checkpoint_bundle_version`` and ``delete_required=True``.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    record, request, repo = await _valid_capture(monkeypatch)
    payload = cast(dict[str, object], json.loads(record.state_json))
    payload["bundle_version"] = 1
    payload.pop("identity", None)
    legacy = replace(record, state_json=json.dumps(payload))
    outcome = await verify_checkpoint_for_resume(
        repo,
        legacy,
        _scope(request),
        _RESUMING_AS_OF,
        _eligibility(request),
    )
    assert outcome == PnlCheckpointFallback("checkpoint_bundle_version", delete_required=True)


async def test_verifier_refuses_manifest_and_prefix_assignment_drift_against_live_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify frozen manifest and effective prefix assignments against live prefix reads.

    Given: A valid captured checkpoint at C mutated along (1) suffix annulment changing
        live generation manifest while keeping 1..W_C prefix annulments identical,
        and (2) applied_watermarks_by_exchange disagreeing with live prefix executions.
    When: ``verify_checkpoint_for_resume`` verifies against live repository evidence.
    Then: Both refuse with closed delete-required fallback codes rather than resuming.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    record, request, repo = await _valid_capture(monkeypatch)
    suffix_annulment = fixtures._applied_annulment(scope_sequence=99)
    repo._applied_annulments = [suffix_annulment]
    manifest_outcome = await verify_checkpoint_for_resume(
        repo,
        record,
        _scope(request),
        _RESUMING_AS_OF,
        _eligibility(request),
    )
    assert manifest_outcome == PnlCheckpointFallback(
        "checkpoint_execution_manifest_changed",
        delete_required=True,
    )

    clean_record, clean_request, clean_repo = await _valid_capture(monkeypatch)
    bad_applied = replace(
        clean_record,
        certificate=replace(
            clean_record.certificate,
            applied_watermarks_by_exchange={"kraken": 0},
        ),
    )
    assignment_outcome = await verify_checkpoint_for_resume(
        clean_repo,
        bad_applied,
        _scope(clean_request),
        _RESUMING_AS_OF,
        _eligibility(clean_request),
    )
    assert assignment_outcome == PnlCheckpointFallback(
        "checkpoint_prefix_assignments_changed",
        delete_required=True,
    )


async def test_verifier_converts_unexpected_error_and_exposes_single_late_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Convert unexpected verify exceptions to closed refusals and avoid duplicate late flag.

    Given: A verification repository call raising RuntimeError.
    When: ``verify_checkpoint_for_resume`` runs, and ``PnlCheckpointResumeEligibility`` is inspected.
    Then: Verification returns ``checkpoint_verify_error`` with ``delete_required=False``,
        ``has_late_execution`` is absent from eligibility, and ``service_resume_fallback``
        resolves closed service reasons uniformly.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    record, request, repo = await _valid_capture(monkeypatch)
    monkeypatch.setattr(
        repo,
        "get_pnl_execution_prefix_counts",
        AsyncMock(side_effect=RuntimeError("db timeout")),
    )
    outcome = await verify_checkpoint_for_resume(
        repo,
        record,
        _scope(request),
        _RESUMING_AS_OF,
        _eligibility(request),
    )
    assert outcome == PnlCheckpointFallback("checkpoint_verify_error", delete_required=False)
    assert not hasattr(PnlCheckpointResumeEligibility, "has_late_execution")
    assert service_resume_fallback("checkpoint_late_execution") == PnlCheckpointFallback(
        "checkpoint_late_execution",
        delete_required=True,
    )
    assert service_resume_fallback("checkpoint_ineligible") == PnlCheckpointFallback(
        "checkpoint_ineligible",
        delete_required=False,
    )
    assert service_resume_fallback("unknown_code") == PnlCheckpointFallback(
        "checkpoint_payload_corrupt",
        delete_required=True,
    )


async def test_mutated_execution_chain_tip_returns_fallback_with_delete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Recompute execution chain tips and fall back on digest mismatch or chain error.

    Given: A valid captured checkpoint and a repository whose execution chain tip
        differs from the certificate digest or raises ``ExecutionChainError``.
    When: ``verify_checkpoint_for_resume`` verifies the record.
    Then: Both cases return ``checkpoint_execution_chain_changed`` with
        ``delete_required=True``.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    record, request, repo = await _valid_capture(monkeypatch)
    monkeypatch.setattr(repo, "get_spot_execution_chain_tip", AsyncMock(return_value="0" * 64))
    mismatch = await verify_checkpoint_for_resume(
        repo,
        record,
        _scope(request),
        _RESUMING_AS_OF,
        _eligibility(request),
    )
    assert mismatch == PnlCheckpointFallback(
        "checkpoint_execution_chain_changed",
        delete_required=True,
    )
    monkeypatch.setattr(
        repo,
        "get_spot_execution_chain_tip",
        AsyncMock(side_effect=ExecutionChainError("purged execution")),
    )
    chain_error = await verify_checkpoint_for_resume(
        repo,
        record,
        _scope(request),
        _RESUMING_AS_OF,
        _eligibility(request),
    )
    assert chain_error == PnlCheckpointFallback(
        "checkpoint_execution_chain_changed",
        delete_required=True,
    )


@pytest.mark.parametrize("mutation", ["dup_walk", "neg_index", "inv_shadow", "inv_span"])
async def test_invalid_walk_state_or_span_bundle_returns_corrupt_with_delete(
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    """Reject corrupt walk states or inverted spans during resume verification.

    Given: A captured checkpoint whose serialized bundle contains a duplicate
        walk-state key, a negative ``event_index``, an inverted regression shadow,
        or an inverted activity span.
    When: ``verify_checkpoint_for_resume`` decodes and verifies the record.
    Then: Every case returns ``checkpoint_payload_corrupt`` with ``delete_required=True``.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        mutation: Corruption variant to inject into ``state_json``.
    """
    record, request, repo = await _valid_capture(monkeypatch)
    payload = cast(dict[str, object], json.loads(record.state_json))
    if mutation == "dup_walk":
        event_walk = cast(dict[str, object], payload["event_walk_state"])
        quantities = cast(list[object], event_walk["position_qty"])
        event_walk["position_qty"] = [quantities[0], quantities[0]]
    elif mutation == "neg_index":
        mark_walk = cast(dict[str, object], payload["mark_walk_state"])
        instruments = cast(list[object], mark_walk["instruments"])
        first = cast(dict[str, object], instruments[0])
        first["event_index"] = -1
    elif mutation == "inv_shadow":
        mark_walk = cast(dict[str, object], payload["mark_walk_state"])
        mark_walk["regression_shadows"] = [
            {
                "event_time": fixtures._m(5).isoformat(),
                "previous_effective": fixtures._m(4).isoformat(),
            }
        ]
    else:
        spans = cast(dict[str, object], payload["price_ref_spans_by_instrument"])
        spans[fixtures._I1] = [fixtures._m(5).isoformat(), fixtures._m(3).isoformat()]
    tampered = replace(record, state_json=json.dumps(payload))
    outcome = await verify_checkpoint_for_resume(
        repo,
        tampered,
        _scope(request),
        _RESUMING_AS_OF,
        _eligibility(request),
    )
    assert outcome == PnlCheckpointFallback("checkpoint_payload_corrupt", delete_required=True)


async def test_newer_suffix_witness_does_not_change_certified_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bind verification to the certificate cut instead of newer suffix witnesses.

    Given: A valid checkpoint and a newer suffix fill witness at the resuming horizon.
    When: Verification reads the prefix with its own fenced witness cut.
    Then: Resume succeeds; the PR-C1 fresh-capture call would diverge and fail.

    Args:
        monkeypatch: Repository witness-read regression control.
    """
    record, request, repo = await _valid_capture(monkeypatch)
    original = repo.get_pnl_timeline_execution_prefix_at_watermarks

    async def read_with_suffix_witness(
        wallet_public_id: str,
        mode: str,
        watermarks: Mapping[str, int],
        as_of: datetime,
        *,
        fill_witness_cut: PnlTimelineFillWitnessCut | Literal["capture"] | None = None,
    ) -> PnlTimelineExecutionPrefix:
        """Simulate the real repository's newer suffix witness under fresh capture."""
        prefix = await original(
            wallet_public_id,
            mode,
            watermarks,
            as_of,
            fill_witness_cut=fill_witness_cut,
        )
        if fill_witness_cut == "capture":
            prefix["fill_witness_cut"] = {"watermarks": {"kraken:spot": 11}, "digest": "f" * 64}
        return prefix

    monkeypatch.setattr(
        repo,
        "get_pnl_timeline_execution_prefix_at_watermarks",
        read_with_suffix_witness,
    )
    outcome = await verify_checkpoint_for_resume(
        repo,
        record,
        _scope(request),
        _RESUMING_AS_OF,
        _eligibility(request),
    )
    assert isinstance(outcome, PnlCheckpointResume)


@pytest.mark.parametrize("legacy_version", [2, 3])
async def test_legacy_bundle_version_payload_refused_with_checkpoint_bundle_version(
    monkeypatch: pytest.MonkeyPatch,
    legacy_version: int,
) -> None:
    """Refuse legacy bundle_version 2 and 3 payloads with ``checkpoint_bundle_version``.

    Given: A valid captured checkpoint whose serialized payload is downgraded to
        ``bundle_version=2`` or ``bundle_version=3``.
    When: ``verify_checkpoint_for_resume`` verifies the record.
    Then: It refuses with ``checkpoint_bundle_version`` and ``delete_required=True``.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        legacy_version: Downgraded bundle schema version under test.
    """
    record, request, repo = await _valid_capture(monkeypatch)
    payload = cast(dict[str, object], json.loads(record.state_json))
    payload["bundle_version"] = legacy_version
    legacy = replace(record, state_json=json.dumps(payload))
    outcome = await verify_checkpoint_for_resume(
        repo,
        legacy,
        _scope(request),
        _RESUMING_AS_OF,
        _eligibility(request),
    )
    assert outcome == PnlCheckpointFallback("checkpoint_bundle_version", delete_required=True)
