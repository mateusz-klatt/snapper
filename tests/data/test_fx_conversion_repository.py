"""Repository contract tests for durable FX conversion proof artifacts."""

import asyncio
from datetime import UTC
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest
from sqlalchemy.exc import IntegrityError

from snapper.data.fx_conversion_digests import build_decision_inputs_digest
from snapper.data.fx_conversion_digests import build_proof_digest
from snapper.data.fx_conversion_digests import build_requirement_manifest_digest
from snapper.data.repository import FxConversionArtifactConflictError
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import FxConversionArtifactRow
from snapper.data.repository_types import FxConversionElectionInsertRow
from snapper.data.repository_types import FxConversionOperation
from snapper.data.repository_types import FxConversionProofInsertRow
from snapper.data.repository_types import FxConversionScopeKind

_MINUTE = datetime(2026, 7, 26, 14, 52, tzinfo=UTC)
_HORIZON = datetime(2026, 8, 2, 9, 0, tzinfo=UTC)
_SESSION = "00000000-0000-7000-8000-000000000001"
_INSTRUMENT = "00000000-0000-7000-8000-000000000002"
_CANDLE = "00000000-0000-7000-8000-000000000003"


def _election(
    public_id: str,
    scope_kind: FxConversionScopeKind = "shared_pair",
    decision_digest: str = "a" * 64,
) -> FxConversionElectionInsertRow:
    """Build one successful election input."""
    return {
        "public_id": public_id,
        "session_id": _SESSION,
        "sequence_id": 1,
        "timestamp": _HORIZON,
        "scope_kind": scope_kind,
        "consumer_instrument_public_id": (
            _INSTRUMENT if scope_kind == "instrument_owned" else None
        ),
        "source_currency": "EUR",
        "target_currency": "USD",
        "unordered_pair": "EUR-USD",
        "requirement_manifest_digest": build_requirement_manifest_digest([_MINUTE]),
        "requested_knowledge_at": _HORIZON,
        "resolved_knowledge_at": _HORIZON,
        "election_policy_version": "fx-election-v1",
        "calculation_version": "pnl-v1",
        "selected_source_exchange": "kraken",
        "selected_source_instrument_public_id": _INSTRUMENT,
        "selected_native_symbol": "EUR/USD",
        "selected_base": "EUR",
        "selected_quote": "USD",
        "selected_orientation": "direct",
        "decision_inputs_digest": decision_digest,
        "completeness_state": "successful",
        "refusal_reason_json": None,
    }


def _proof(
    election_public_id: str,
    public_id: str,
    raw_close: Decimal = Decimal("1.123456789012345678901234567890"),
    conversion_rate: Decimal = Decimal("1.123456789012345678901234567890"),
    operation: FxConversionOperation = "direct",
) -> FxConversionProofInsertRow:
    """Build one exact-minute proof input."""
    return {
        "public_id": public_id,
        "session_id": _SESSION,
        "sequence_id": 2,
        "timestamp": _HORIZON,
        "election_public_id": election_public_id,
        "conversion_minute": _MINUTE,
        "candle_open_minute": _MINUTE,
        "candle_id": 42,
        "candle_public_id": _CANDLE,
        "candle_session_id": _SESSION,
        "candle_sequence_id": 7,
        "candle_timestamp": _MINUTE,
        "candle_known_to": _HORIZON,
        "raw_close": raw_close,
        "operation": operation,
        "conversion_rate": conversion_rate,
        "source_instrument_public_id": _INSTRUMENT,
        "proof_digest": "b" * 64,
    }


async def _repository(tmp_path: Path) -> SQLAlchemyRepository:
    """Create one isolated repository with the complete model schema."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'fx.db'}")
    await repository.create_all()
    return repository


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "raw_close", "conversion_rate"),
    [
        (
            "direct",
            Decimal("1.123456789012345678901234567890"),
            Decimal("1.123456789012345678901234567890"),
        ),
        (
            "inverse",
            Decimal("3.000000000000000000000000000001"),
            Decimal("0.333333333333333333333333333222"),
        ),
    ],
)
async def test_proof_decimals_round_trip_exactly(
    tmp_path: Path,
    operation: FxConversionOperation,
    raw_close: Decimal,
    conversion_rate: Decimal,
) -> None:
    """Direct and inverse proof values retain every supplied decimal digit."""
    repository = await _repository(tmp_path)
    election = _election("00000000-0000-7000-8000-000000000010")
    proof = _proof(
        election["public_id"],
        "00000000-0000-7000-8000-000000000011",
        raw_close,
        conversion_rate,
        operation,
    )
    artifact = await repository.pin_fx_conversion_artifact(election, [proof])
    assert artifact["proofs"][0]["raw_close"] == raw_close
    assert artifact["proofs"][0]["conversion_rate"] == conversion_rate
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_shared_and_instrument_owned_scopes_are_isolated(tmp_path: Path) -> None:
    """The same manifest can persist independently in both approved scopes."""
    repository = await _repository(tmp_path)
    shared = _election("00000000-0000-7000-8000-000000000020")
    owned = _election("00000000-0000-7000-8000-000000000021", "instrument_owned")
    shared_result = await repository.pin_fx_conversion_artifact(
        shared, [_proof(shared["public_id"], "00000000-0000-7000-8000-000000000022")]
    )
    owned_result = await repository.pin_fx_conversion_artifact(
        owned, [_proof(owned["public_id"], "00000000-0000-7000-8000-000000000023")]
    )
    assert shared_result["election"]["public_id"] == shared["public_id"]
    assert owned_result["election"]["public_id"] == owned["public_id"]
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_absent_and_refused_artifacts_have_no_proofs(tmp_path: Path) -> None:
    """An absent identity returns None and an auditable refusal has no children."""
    repository = await _repository(tmp_path)
    refusal = _election("00000000-0000-7000-8000-000000000024")
    refusal["requirement_manifest_digest"] = build_requirement_manifest_digest([])
    refusal["completeness_state"] = "refused"
    refusal["refusal_reason_json"] = '{"reason":"fx_conversion_unproven"}'
    refusal["selected_source_exchange"] = None
    refusal["selected_source_instrument_public_id"] = None
    refusal["selected_native_symbol"] = None
    refusal["selected_base"] = None
    refusal["selected_quote"] = None
    refusal["selected_orientation"] = None
    assert await repository.get_fx_conversion_artifact(refusal) is None
    artifact = await repository.pin_fx_conversion_artifact(refusal, [])
    assert artifact["proofs"] == ()
    assert await repository.get_fx_conversion_artifact(refusal) == artifact
    await repository.engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "violation",
    ["duplicate_minute", "manifest", "missing_proofs", "parent", "plane"],
)
async def test_pin_validates_complete_atomic_payload(tmp_path: Path, violation: str) -> None:
    """Malformed parent-child bundles are refused before database writes."""
    repository = await _repository(tmp_path)
    election = _election("00000000-0000-7000-8000-000000000025")
    proof = _proof(election["public_id"], "00000000-0000-7000-8000-000000000026")
    proofs = [proof]
    if violation == "duplicate_minute":
        proofs.append({**proof, "public_id": "00000000-0000-7000-8000-000000000027"})
    elif violation == "manifest":
        election["requirement_manifest_digest"] = "0" * 64
    elif violation == "missing_proofs":
        proofs = []
        election["requirement_manifest_digest"] = build_requirement_manifest_digest([])
    elif violation == "parent":
        proof["election_public_id"] = "00000000-0000-7000-8000-000000000028"
    else:
        proof["source_instrument_public_id"] = "00000000-0000-7000-8000-000000000029"
    with pytest.raises(ValueError):
        await repository.pin_fx_conversion_artifact(election, proofs)
    assert await repository.get_fx_conversion_artifact(election) is None
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_concurrent_identical_writers_converge(tmp_path: Path) -> None:
    """Two canonical writers observe one committed artifact identity."""
    first = await _repository(tmp_path)
    second = SQLAlchemyRepository(first.db_url)
    election_a = _election("00000000-0000-7000-8000-000000000030")
    election_b = _election("00000000-0000-7000-8000-000000000031")
    results = await asyncio.gather(
        first.pin_fx_conversion_artifact(
            election_a, [_proof(election_a["public_id"], "00000000-0000-7000-8000-000000000032")]
        ),
        second.pin_fx_conversion_artifact(
            election_b, [_proof(election_b["public_id"], "00000000-0000-7000-8000-000000000033")]
        ),
    )
    assert results[0]["election"]["public_id"] == results[1]["election"]["public_id"]
    await first.engine.dispose()
    await second.engine.dispose()


@pytest.mark.asyncio
async def test_concurrent_conflicting_writers_fail_closed(tmp_path: Path) -> None:
    """A different canonical decision at one identity raises the typed conflict."""
    first = await _repository(tmp_path)
    second = SQLAlchemyRepository(first.db_url)
    election_a = _election("00000000-0000-7000-8000-000000000040")
    election_b = _election("00000000-0000-7000-8000-000000000041", decision_digest="c" * 64)
    results = await asyncio.gather(
        first.pin_fx_conversion_artifact(
            election_a, [_proof(election_a["public_id"], "00000000-0000-7000-8000-000000000042")]
        ),
        second.pin_fx_conversion_artifact(
            election_b, [_proof(election_b["public_id"], "00000000-0000-7000-8000-000000000043")]
        ),
        return_exceptions=True,
    )
    assert sum(isinstance(result, FxConversionArtifactConflictError) for result in results) == 1
    await first.engine.dispose()
    await second.engine.dispose()


@pytest.mark.asyncio
async def test_unrelated_integrity_collision_is_not_misclassified(tmp_path: Path) -> None:
    """A proof-public-id collision without an election winner remains an integrity error."""
    repository = await _repository(tmp_path)
    first = _election("00000000-0000-7000-8000-000000000050")
    proof_id = "00000000-0000-7000-8000-000000000051"
    await repository.pin_fx_conversion_artifact(first, [_proof(first["public_id"], proof_id)])
    second = _election("00000000-0000-7000-8000-000000000052")
    second["resolved_knowledge_at"] = _HORIZON.replace(minute=1)
    with pytest.raises(IntegrityError):
        await repository.pin_fx_conversion_artifact(second, [_proof(second["public_id"], proof_id)])
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_committed_artifact_must_be_readable(tmp_path: Path) -> None:
    """The writer fails closed if its committed canonical read unexpectedly disappears."""
    repository = await _repository(tmp_path)
    election = _election("00000000-0000-7000-8000-000000000053")
    with (
        patch.object(repository, "_read_fx_conversion_artifact", AsyncMock(return_value=None)),
        pytest.raises(RuntimeError, match="could not be read back"),
    ):
        await repository.pin_fx_conversion_artifact(
            election,
            [_proof(election["public_id"], "00000000-0000-7000-8000-000000000054")],
        )
    await repository.engine.dispose()


def test_artifact_equivalence_checks_every_canonical_digest() -> None:
    """Completeness, refusal, decision, and ordered proof digests all participate."""
    election = _election("00000000-0000-7000-8000-000000000055")
    proof = _proof(election["public_id"], "00000000-0000-7000-8000-000000000056")
    winner: FxConversionArtifactRow = {
        "election": {**election, "known_to": _HORIZON.replace(year=9999)},
        "proofs": ({**proof, "known_to": _HORIZON.replace(year=9999)},),
    }
    assert SQLAlchemyRepository._fx_artifacts_equivalent(winner, election, [proof])
    for key, value in [
        ("decision_inputs_digest", "c" * 64),
        ("completeness_state", "refused"),
        ("refusal_reason_json", "{}"),
    ]:
        changed = {**election, key: value}
        assert not SQLAlchemyRepository._fx_artifacts_equivalent(winner, changed, [proof])
    changed_proof: FxConversionProofInsertRow = {**proof, "proof_digest": "d" * 64}
    assert not SQLAlchemyRepository._fx_artifacts_equivalent(winner, election, [changed_proof])


def test_canonical_digests_ignore_input_ordering() -> None:
    """Minute, decision-record, and object-key order cannot alter digests."""
    later = _MINUTE.replace(minute=54)
    assert build_requirement_manifest_digest(
        [later, _MINUTE, later]
    ) == build_requirement_manifest_digest([_MINUTE, later])
    first = [{"exchange": "kraken", "score": 1}, {"exchange": "coinbase", "score": 2}]
    second = [{"score": 2, "exchange": "coinbase"}, {"score": 1, "exchange": "kraken"}]
    assert build_decision_inputs_digest(first) == build_decision_inputs_digest(second)
    assert build_proof_digest({"close": "1.25", "id": 4}) == build_proof_digest(
        {"id": 4, "close": "1.25"}
    )
