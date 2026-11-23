"""Tests for pure opening-checkpoint canonical digests and write-refusal predicate.

Covers permutation invariance, single-field sensitivity across every identity
field, cross-kind non-collision, empty-input behavior, every write-refusal code
and exact evaluation precedence, and boundary tolerance at ``MAX_DEFERRED_EXECUTIONS``.
"""

from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import cast
from typing import get_args

import pytest

from snapper.application.portfolio.pnl_opening_checkpoint_certificate import MAX_DEFERRED_EXECUTIONS
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import (
    PnlCheckpointWriteRefusal,
)
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import (
    checkpoint_minute_fenced,
)
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import (
    checkpoint_write_refusal,
)
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import (
    event_fx_exchanges_digest,
)
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import (
    opening_checkpoint_fence,
)
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import prefix_annulment_digest
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import (
    prefix_fill_witness_digest,
)
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import prefix_lineage_digest
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import symbol_ref_digest
from snapper.data.pnl_correction_state import IdAggregate
from snapper.data.pnl_correction_state import PnlCorrectionChunk
from snapper.data.pnl_correction_state import PnlCorrectionEvidence
from snapper.data.pnl_correction_state import PnlCorrectionFrontier
from snapper.data.pnl_correction_state import PnlCorrectionKey
from snapper.data.pnl_correction_state import PnlOpeningCheckpointCertificate
from snapper.data.repository_types import PNL_SAMPLE_CALC_VERSION
from snapper.data.repository_types import InstrumentSymbolRefRow
from snapper.data.repository_types import PnlFxRatePlane
from snapper.data.repository_types import PnlTimelineAppliedAnnulment
from snapper.data.repository_types import PnlTimelineExecutionLineageRow

_M1 = datetime(2026, 11, 20, 10, 1, tzinfo=UTC)
_M2 = _M1 + timedelta(minutes=1)
_M3 = _M1 + timedelta(minutes=2)
_M4 = _M1 + timedelta(minutes=3)
_M5 = _M1 + timedelta(minutes=4)
_KEY = PnlCorrectionKey(
    wallet_public_id="00000000-0000-7000-8000-000000000001",
    mode="live",
    valuation_ccy="USD",
    epoch_public_id="00000000-0000-7000-8000-000000000002",
    calc_version=PNL_SAMPLE_CALC_VERSION,
)
_DIGEST_A = "a" * 64
_DIGEST_B = "b" * 64


def _settled_frontier() -> PnlCorrectionFrontier:
    """Build a settled frontier ending at M3 with complete captured evidence."""
    return PnlCorrectionFrontier(
        key=_KEY,
        generation=2,
        revision=5,
        window_start=_M1,
        next_minute=None,
        tip=_M3,
        as_of=_M5,
        baseline_watermarks={"kraken": 5},
        pending_watermarks={"kraken": 5},
        prior_peak=100.0,
        repair_from=None,
        raw_execution_watermarks={"kraken": 5},
        execution_manifest_digest=_DIGEST_A,
        has_fill_gap=False,
        expected_venues=frozenset({"kraken"}),
        venue_event_watermarks={"kraken:spot": 10},
        fill_witness_digest=_DIGEST_B,
    )


def _chunk(end: datetime = _M3) -> PnlCorrectionChunk:
    """Build a chunk terminating at ``end`` matching ``_KEY``."""
    return PnlCorrectionChunk(
        end=end,
        edits=(),
        prior_peak=100.0,
        evidence=PnlCorrectionEvidence(_KEY, _M1, end, (), 100.0),
    )


def _certificate() -> PnlOpeningCheckpointCertificate:
    """Build a valid certificate matching ``_settled_frontier()``."""
    return PnlOpeningCheckpointCertificate(
        key=_KEY,
        timeline_calc_version="5A.16",
        fx_policy_version="pnl-fiat-pointwise-v1",
        schema_version=1,
        anchor_public_id="00000000-0000-7000-8000-000000000003",
        anchor_digest=_DIGEST_B,
        source_generation=2,
        checkpoint_minute=_M3,
        as_of=_M5,
        cutoff_execution_watermarks={"kraken": 5},
        execution_chain_digests={"kraken": _DIGEST_B},
        cutoff_execution_counts={"kraken": 5},
        applied_watermarks_by_exchange={"kraken": 5},
        deferred_execution_sequences={},
        prefix_has_idless_executions=False,
        execution_manifest_digest=_DIGEST_A,
        prefix_annulment_digest=_DIGEST_B,
        accrual_prefix_fingerprint=IdAggregate(0, 0, 0, None),
        prefix_lineage_digest=_DIGEST_B,
        prefix_fill_witness_digest=_DIGEST_B,
        prefix_venue_event_watermarks={"kraken:spot": 10},
        source_expected_venues=frozenset({"kraken"}),
        source_venue_event_watermarks={"kraken:spot": 10},
        source_fill_witness_digest=_DIGEST_B,
        symbol_ref_digest=_DIGEST_B,
        consumed_fx_pairs=(),
        consumed_fx_open_ats=(),
        event_fx_exchanges_digest=_DIGEST_B,
        event_fx_candles_fingerprint=IdAggregate(0, 0, 0, None),
    )


def _annulment(**overrides: object) -> PnlTimelineAppliedAnnulment:
    """Build one applied annulment row with optional field overrides."""
    base: PnlTimelineAppliedAnnulment = {
        "public_id": "ann-1",
        "target_execution_public_id": "exe-1",
        "target_execution_digest": _DIGEST_A,
        "exchange": "kraken",
        "scope_sequence": 2,
        "reason": "unwitnessed_phantom",
        "correction_time": _M2,
    }
    base.update(cast(PnlTimelineAppliedAnnulment, overrides))
    return base


def _lineage(**overrides: object) -> PnlTimelineExecutionLineageRow:
    """Build one execution lineage row with optional field overrides."""
    base: PnlTimelineExecutionLineageRow = {
        "order_public_id": "ord-1",
        "source_surface": "rest",
        "plan_public_id": "plan-1",
        "signal_public_id": "sig-1",
        "origin": "live",
        "strategy_name": "momentum",
    }
    base.update(cast(PnlTimelineExecutionLineageRow, overrides))
    return base


def _symbol_ref(**overrides: object) -> InstrumentSymbolRefRow:
    """Build one instrument symbol-ref row with optional field overrides."""
    base: InstrumentSymbolRefRow = {
        "instrument_public_id": "inst-1",
        "native_symbol": "BTC/USD",
        "exchange": "kraken",
        "instrument_exchange": "kraken",
        "base_currency": "BTC",
        "quote_currency": "USD",
        "valid_from": _M1,
        "valid_to": _M5,
    }
    base.update(cast(InstrumentSymbolRefRow, overrides))
    return base


def _assert_sha256_hex(digest: str) -> None:
    """Assert 64 lowercase hexadecimal digest format."""
    assert len(digest) == 64
    assert all(character in "0123456789abcdef" for character in digest)


def test_opening_checkpoint_fence_selects_earliest_non_null_boundary() -> None:
    """Share one minimum fence across window, pending, and repair boundaries.

    Given: A frontier whose repair boundary precedes its window and next minute.
    When: The opening-checkpoint fence is resolved with and without optional boundaries.
    Then: The earliest boundary wins and absent optional boundaries are ignored.
    """
    frontier = replace(
        _settled_frontier(),
        window_start=_M3,
        next_minute=_M4,
        repair_from=_M2,
    )
    assert opening_checkpoint_fence(frontier) == _M2
    assert opening_checkpoint_fence(replace(frontier, next_minute=None, repair_from=None)) == _M3


@pytest.mark.parametrize(
    ("fence", "expected"),
    [
        (_M2, True),
        (_M3, True),
        (_M4, False),
        (None, False),
    ],
)
def test_checkpoint_minute_fenced_covers_equality_and_absence(
    fence: datetime | None,
    expected: bool,
) -> None:
    """Apply the same inclusive minute comparator at every fence boundary.

    Given: A checkpoint minute and a fence below, equal to, above, or absent from it.
    When: ``checkpoint_minute_fenced`` evaluates the pair.
    Then: Only a non-null fence at or below the checkpoint minute matches.

    Args:
        fence: Earlier, equal, later, or absent invalidation boundary.
        expected: Expected inclusive fence decision.
    """
    assert checkpoint_minute_fenced(_M3, fence) is expected


def test_empty_digests_are_well_formed_and_pairwise_distinct() -> None:
    """Empty inputs yield valid type-tagged SHA-256 digests that never collide.

    Given: Empty inputs for each of the five canonical digest functions.
    When: Each digest function is evaluated on an empty sequence.
    Then: Every result is a 64-character lowercase hex digest and all five are
        pairwise distinct because each carries a distinct kind tag.
    """
    digests = [
        prefix_annulment_digest([], {}),
        prefix_lineage_digest([]),
        symbol_ref_digest([]),
        event_fx_exchanges_digest([]),
        prefix_fill_witness_digest([]),
    ]
    for digest in digests:
        _assert_sha256_hex(digest)
    assert len(set(digests)) == 5


def test_cross_kind_non_collision_on_three_string_or_matching_shapes() -> None:
    """Different row kinds never collide even on structurally similar payloads.

    Given: Canonical inputs across all five digest kinds including a 3-string
        FX plane and rows carrying matching string values.
    When: Digests are computed across all five functions.
    Then: All resulting digests are distinct.
    """
    digests = [
        prefix_annulment_digest([_annulment()], {"kraken": 5}),
        prefix_lineage_digest([_lineage()]),
        symbol_ref_digest([_symbol_ref()]),
        event_fx_exchanges_digest([("BTC", "USD", "kraken")]),
        prefix_fill_witness_digest([(1, "BTC")]),
    ]
    assert len(set(digests)) == 5


def test_digest_permutation_invariance_across_all_five_functions() -> None:
    """Permuting input order leaves every canonical digest unchanged.

    Given: Two-element sequences for each of the five row kinds, including
        nullable fields mixing ``None`` and string values.
    When: Each input sequence is reversed.
    Then: All five digest functions produce identical SHA-256 digests before and
        after permutation.
    """
    annulments = [
        _annulment(),
        _annulment(public_id="ann-2", exchange="walutomat", scope_sequence=1),
    ]
    assert prefix_annulment_digest(annulments, {"kraken": 5, "walutomat": 2}) == (
        prefix_annulment_digest(list(reversed(annulments)), {"walutomat": 2, "kraken": 5})
    )

    lineages = [
        _lineage(),
        _lineage(
            order_public_id="ord-2",
            source_surface=None,
            plan_public_id=None,
            signal_public_id=None,
            origin=None,
            strategy_name=None,
        ),
    ]
    assert prefix_lineage_digest(lineages) == prefix_lineage_digest(list(reversed(lineages)))

    refs = [
        _symbol_ref(),
        _symbol_ref(instrument_public_id="inst-2", quote_currency=None),
    ]
    assert symbol_ref_digest(refs) == symbol_ref_digest(list(reversed(refs)))

    planes: list[PnlFxRatePlane] = [("EUR", "USD", "kraken"), ("EUR", "PLN", "walutomat")]
    assert event_fx_exchanges_digest(planes) == event_fx_exchanges_digest(list(reversed(planes)))

    witnesses = [(1, "evt-1"), (2, "evt-2")]
    assert prefix_fill_witness_digest(witnesses) == prefix_fill_witness_digest(
        list(reversed(witnesses))
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("public_id", "ann-other"),
        ("target_execution_public_id", "exe-other"),
        ("target_execution_digest", _DIGEST_B),
        ("exchange", "walutomat"),
        ("scope_sequence", 3),
    ],
)
def test_prefix_annulment_digest_single_field_sensitivity_and_prefix_filter(
    field: str, value: object
) -> None:
    """Changing any identity field alters the annulment digest; suffix rows do not.

    Given: A baseline prefix annulment and a cutoff watermark map.
    When: Any single identity field changes within the prefix, or an out-of-cut
        suffix annulment is added.
    Then: Every in-prefix field mutation changes the digest while suffix rows
        outside ``cutoff_watermarks`` leave the prefix digest untouched.
    """
    cut = {"kraken": 5, "walutomat": 5}
    base = prefix_annulment_digest([_annulment()], cut)
    mutated = prefix_annulment_digest([_annulment(**{field: value})], cut)
    assert mutated != base
    suffix_row = _annulment(public_id="ann-suffix", exchange="kraken", scope_sequence=6)
    other_exchange = _annulment(public_id="ann-other-ex", exchange="coinbase", scope_sequence=1)
    assert prefix_annulment_digest([_annulment(), suffix_row, other_exchange], cut) == base
    with pytest.raises(ValueError, match="nonnegative sequences"):
        prefix_annulment_digest([_annulment()], {"kraken": -1})
    with pytest.raises(ValueError, match="scope_sequence"):
        prefix_annulment_digest([_annulment(scope_sequence=True)], cut)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("order_public_id", "ord-2"),
        ("source_surface", None),
        ("source_surface", ""),
        ("plan_public_id", None),
        ("signal_public_id", None),
        ("origin", None),
        ("strategy_name", None),
    ],
)
def test_prefix_lineage_digest_single_field_sensitivity(field: str, value: object) -> None:
    """Changing any lineage field or switching None vs empty alters the digest.

    Given: A baseline execution lineage row.
    When: Any of its six fields changes, or switches between ``None`` and ``""``.
    Then: The resulting digest differs from the baseline and from empty-vs-None.
    """
    base = prefix_lineage_digest([_lineage()])
    assert prefix_lineage_digest([_lineage(**{field: value})]) != base
    assert prefix_lineage_digest([_lineage(source_surface=None)]) != prefix_lineage_digest(
        [_lineage(source_surface="")]
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("instrument_public_id", "inst-2"),
        ("native_symbol", "ETH/USD"),
        ("exchange", "walutomat"),
        ("instrument_exchange", "walutomat"),
        ("base_currency", "ETH"),
        ("quote_currency", "EUR"),
        ("quote_currency", None),
        ("valid_from", _M2),
        ("valid_to", _M4),
    ],
)
def test_symbol_ref_digest_single_field_sensitivity_and_utc_normalization(
    field: str, value: object
) -> None:
    """Changing any symbol-ref field changes its digest while equivalent offsets match.

    Given: A baseline symbol-ref row with UTC interval bounds.
    When: Any of the eight identity fields changes, a naive datetime is passed,
        or an equivalent non-UTC offset instant is supplied.
    Then: Every field change produces a distinct digest, naive datetimes fail,
        and equivalent UTC instants produce the identical digest.
    """
    base = symbol_ref_digest([_symbol_ref()])
    assert symbol_ref_digest([_symbol_ref(**{field: value})]) != base
    assert symbol_ref_digest([_symbol_ref(quote_currency=None)]) != symbol_ref_digest(
        [_symbol_ref(quote_currency="")]
    )
    offset = timezone(timedelta(hours=2))
    assert symbol_ref_digest([_symbol_ref(valid_from=_M1.astimezone(offset))]) == base
    with pytest.raises(ValueError, match="timezone-aware"):
        symbol_ref_digest([_symbol_ref(valid_from=_M1.replace(tzinfo=None))])


@pytest.mark.parametrize(
    "plane",
    [
        ("ETH", "USD", "kraken"),
        ("BTC", "EUR", "kraken"),
        ("BTC", "USD", "coinbase"),
    ],
)
def test_event_fx_exchanges_digest_single_field_sensitivity(plane: PnlFxRatePlane) -> None:
    """Changing base, quote, or exchange alters the FX plane digest.

    Given: A baseline ``(BTC, USD, kraken)`` FX plane.
    When: Any single tuple element changes.
    Then: The digest changes.
    """
    base = event_fx_exchanges_digest([("BTC", "USD", "kraken")])
    assert event_fx_exchanges_digest([plane]) != base


def test_prefix_fill_witness_digest_single_field_sensitivity_and_validation() -> None:
    """Changing id or public_id changes the witness digest; bool/negative fails.

    Given: A baseline ``(1, "evt-1")`` fill witness identity.
    When: Either the event id or public id changes, or a boolean/negative id is
        supplied.
    Then: Both valid changes alter the digest and invalid ids raise ``ValueError``.
    """
    base = prefix_fill_witness_digest([(1, "evt-1")])
    assert prefix_fill_witness_digest([(2, "evt-1")]) != base
    assert prefix_fill_witness_digest([(1, "evt-2")]) != base
    with pytest.raises(ValueError, match="non-negative integer"):
        prefix_fill_witness_digest([(True, "evt-1")])
    with pytest.raises(ValueError, match="non-negative integer"):
        prefix_fill_witness_digest([(-1, "evt-1")])


def test_checkpoint_write_refusal_returns_none_at_valid_boundary() -> None:
    """Accept a settled frontier at tip with up to MAX_DEFERRED_EXECUTIONS tails.

    Given: A settled frontier, matching chunk end, matching certificate, and
        exactly ``MAX_DEFERRED_EXECUTIONS`` (256) deferred sequences.
    When: ``checkpoint_write_refusal`` evaluates the candidate write.
    Then: It returns ``None`` and exports the documented threshold and 8-reason
        literal order.
    """
    assert MAX_DEFERRED_EXECUTIONS == 256
    assert get_args(PnlCheckpointWriteRefusal.__value__) == (
        "not_settled",
        "not_at_tip",
        "generation_mismatch",
        "frontier_evidence_incomplete",
        "fill_gap",
        "cut_mismatch",
        "key_mismatch",
        "deferred_tail_too_long",
    )
    frontier = replace(
        _settled_frontier(),
        raw_execution_watermarks={"coinbase": 150, "kraken": 150},
    )
    cert = replace(
        _certificate(),
        cutoff_execution_watermarks={"coinbase": 150, "kraken": 150},
        execution_chain_digests={"coinbase": _DIGEST_B, "kraken": _DIGEST_B},
        cutoff_execution_counts={"coinbase": 150, "kraken": 150},
        applied_watermarks_by_exchange={"coinbase": 150, "kraken": 150},
        deferred_execution_sequences={
            "coinbase": tuple(range(1, 129)),
            "kraken": tuple(range(1, 129)),
        },
    )
    assert checkpoint_write_refusal(frontier, _chunk(), cert) is None


@pytest.mark.parametrize(
    ("expected_reason", "frontier_patch", "chunk_patch", "cert_patch"),
    [
        ("not_settled", {"next_minute": _M2}, {}, {}),
        ("not_settled", {"repair_from": _M2}, {}, {}),
        ("not_at_tip", {}, {"end": _M2}, {}),
        ("not_at_tip", {}, {}, {"checkpoint_minute": _M2}),
        ("generation_mismatch", {}, {}, {"source_generation": 1}),
        ("generation_mismatch", {}, {}, {"as_of": _M4}),
        ("frontier_evidence_incomplete", {"raw_execution_watermarks": None}, {}, {}),
        ("frontier_evidence_incomplete", {"execution_manifest_digest": None}, {}, {}),
        ("frontier_evidence_incomplete", {"has_fill_gap": None}, {}, {}),
        ("frontier_evidence_incomplete", {"expected_venues": None}, {}, {}),
        ("frontier_evidence_incomplete", {"venue_event_watermarks": None}, {}, {}),
        ("frontier_evidence_incomplete", {"fill_witness_digest": None}, {}, {}),
        ("fill_gap", {"has_fill_gap": True}, {}, {}),
        ("cut_mismatch", {"raw_execution_watermarks": {"kraken": 6}}, {}, {}),
        ("cut_mismatch", {}, {}, {"execution_manifest_digest": _DIGEST_B}),
        ("cut_mismatch", {}, {}, {"source_expected_venues": frozenset({"walutomat"})}),
        ("cut_mismatch", {}, {}, {"source_venue_event_watermarks": {"kraken:spot": 11}}),
        ("cut_mismatch", {}, {}, {"source_fill_witness_digest": _DIGEST_A}),
        (
            "key_mismatch",
            {},
            {},
            {"key": replace(_KEY, calc_version="5C.0")},
        ),
        (
            "deferred_tail_too_long",
            {"raw_execution_watermarks": {"coinbase": 150, "kraken": 150}},
            {},
            {
                "cutoff_execution_watermarks": {"coinbase": 150, "kraken": 150},
                "execution_chain_digests": {"coinbase": _DIGEST_B, "kraken": _DIGEST_B},
                "cutoff_execution_counts": {"coinbase": 150, "kraken": 150},
                "applied_watermarks_by_exchange": {"coinbase": 150, "kraken": 150},
                "deferred_execution_sequences": {
                    "coinbase": tuple(range(1, 129)),
                    "kraken": tuple(range(1, 130)),
                },
            },
        ),
    ],
)
def test_checkpoint_write_refusal_reports_each_reason(
    expected_reason: PnlCheckpointWriteRefusal,
    frontier_patch: dict[str, object],
    chunk_patch: dict[str, object],
    cert_patch: dict[str, object],
) -> None:
    """Every single refusal gate fires its specific reason code.

    Given: A valid settled frontier, chunk, and certificate.
    When: Exactly one precondition is violated.
    Then: ``checkpoint_write_refusal`` returns the corresponding reason code.
    """
    frontier = replace(_settled_frontier(), **frontier_patch)
    chunk = replace(_chunk(), **chunk_patch)
    cert = replace(_certificate(), **cert_patch)
    assert checkpoint_write_refusal(frontier, chunk, cert) == expected_reason


def test_checkpoint_write_refusal_evaluates_reasons_in_exact_precedence_order() -> None:
    """Earlier refusal reasons take precedence whenever multiple gates fail.

    Given: A settled frontier, chunk, and certificate violating pairs of
        adjacent conditions in the eight-reason evaluation order.
    When: Both condition ``i`` and condition ``i + 1`` fail simultaneously.
    Then: The earlier reason ``i`` is returned first for every adjacent pair.
    """
    frontier_base = replace(
        _settled_frontier(),
        raw_execution_watermarks={"kraken": 300},
    )
    cert_base = replace(
        _certificate(),
        cutoff_execution_watermarks={"kraken": 300},
        cutoff_execution_counts={"kraken": 300},
        applied_watermarks_by_exchange={"kraken": 300},
        deferred_execution_sequences={"kraken": tuple(range(1, 258))},
    )

    assert (
        checkpoint_write_refusal(
            replace(frontier_base, next_minute=_M2), _chunk(_M2), _certificate()
        )
        == "not_settled"
    )
    assert (
        checkpoint_write_refusal(
            frontier_base, _chunk(_M2), replace(_certificate(), source_generation=1)
        )
        == "not_at_tip"
    )
    assert (
        checkpoint_write_refusal(
            replace(frontier_base, has_fill_gap=None),
            _chunk(),
            replace(_certificate(), source_generation=1),
        )
        == "generation_mismatch"
    )
    assert (
        checkpoint_write_refusal(
            replace(frontier_base, raw_execution_watermarks=None, has_fill_gap=True),
            _chunk(),
            _certificate(),
        )
        == "frontier_evidence_incomplete"
    )
    assert (
        checkpoint_write_refusal(
            replace(frontier_base, has_fill_gap=True),
            _chunk(),
            _certificate(),
        )
        == "fill_gap"
    )
    assert (
        checkpoint_write_refusal(
            frontier_base,
            _chunk(),
            replace(_certificate(), key=replace(_KEY, calc_version="5C.0")),
        )
        == "cut_mismatch"
    )
    assert (
        checkpoint_write_refusal(
            frontier_base,
            _chunk(),
            replace(cert_base, key=replace(_KEY, calc_version="5C.0")),
        )
        == "key_mismatch"
    )
