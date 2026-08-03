"""Repository contract tests for durable FX conversion proof artifacts."""

import asyncio
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Literal
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import select
from sqlalchemy.sql.expression import Executable

from snapper.data.fx_conversion_digests import build_decision_inputs_digest
from snapper.data.fx_conversion_digests import build_proof_digest
from snapper.data.fx_conversion_digests import build_refusal_reason_digest
from snapper.data.fx_conversion_digests import build_requirement_manifest_digest
from snapper.data.fx_conversion_triggers import drop_fx_conversion_immutability_triggers
from snapper.data.fx_conversion_triggers import install_fx_conversion_immutability_triggers
from snapper.data.models import FxConversionElection
from snapper.data.models import FxConversionProof
from snapper.data.repository import FxConversionArtifactConflictError
from snapper.data.repository import FxConversionArtifactUpgradeRequiredError
from snapper.data.repository import FxConversionArtifactValueError
from snapper.data.repository import Repository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import FxConversionElectionInsertRow
from snapper.data.repository_types import FxConversionOperation
from snapper.data.repository_types import FxConversionProofInsertRow
from snapper.data.repository_types import FxConversionScopeKind
from snapper.data.repository_types import FxConversionSuccessfulQuery

_MINUTE = datetime(2026, 7, 26, 14, 52, tzinfo=UTC)
_HORIZON = datetime(2026, 8, 2, 9, 0, tzinfo=UTC)
_SESSION = "00000000-0000-7000-8000-000000000001"
_INSTRUMENT = "00000000-0000-7000-8000-000000000002"
_CANDLE = "00000000-0000-7000-8000-000000000003"


def _election(
    public_id: str,
    scope_kind: FxConversionScopeKind = "shared_pair",
    operation: FxConversionOperation = "direct",
) -> FxConversionElectionInsertRow:
    """Build one complete election input whose digests are repository-derived."""
    return {
        "public_id": public_id,
        "session_id": _SESSION,
        "sequence_id": 1,
        "timestamp": _HORIZON,
        "scope_kind": scope_kind,
        "consumer_instrument_public_id": _INSTRUMENT if scope_kind == "instrument_owned" else None,
        "source_currency": "EUR",
        "target_currency": "USD",
        "unordered_pair": "EUR-USD",
        "required_minutes": (_MINUTE,),
        "requested_knowledge_at": _HORIZON,
        "resolved_knowledge_at": _HORIZON,
        "election_policy_version": "fx-election-v1",
        "calculation_version": "pnl-v1",
        "selected_source_exchange": "kraken",
        "selected_source_instrument_public_id": _INSTRUMENT,
        "selected_native_symbol": "EUR/USD",
        "selected_base": "EUR",
        "selected_quote": "USD",
        "selected_orientation": operation,
        "considered_candidate_planes": (
            {
                "source_exchange": "kraken",
                "source_instrument_public_id": _INSTRUMENT,
                "native_symbol": "EUR/USD",
                "base": "EUR",
                "quote": "USD",
                "orientation": operation,
            },
        ),
        "completeness_state": "complete",
        "refusal_reason_json": None,
    }


def _proof(
    election_public_id: str,
    public_id: str,
    raw_close: Decimal = Decimal("1.123456789012345678901234567890"),
    conversion_rate: Decimal | None = Decimal("1.123456789012345678901234567890"),
    operation: FxConversionOperation = "direct",
) -> FxConversionProofInsertRow:
    """Build one proof using the production M-minus-one candle relation."""
    return {
        "public_id": public_id,
        "session_id": _SESSION,
        "sequence_id": 2,
        "timestamp": _HORIZON,
        "election_public_id": election_public_id,
        "conversion_minute": _MINUTE,
        "candle_open_minute": _MINUTE - timedelta(minutes=1),
        "carried_minutes": 0,
        "candle_id": 42,
        "candle_public_id": _CANDLE,
        "candle_session_id": _SESSION,
        "candle_sequence_id": 7,
        "candle_timestamp": _MINUTE - timedelta(minutes=1),
        "candle_known_to": _HORIZON,
        "raw_close": raw_close,
        "operation": operation,
        "conversion_rate": conversion_rate,
        "source_instrument_public_id": _INSTRUMENT,
    }


def _query(election: FxConversionElectionInsertRow) -> FxConversionSuccessfulQuery:
    """Project one insert request into the successful lookup coordinates."""
    return {
        "scope_kind": election["scope_kind"],
        "consumer_instrument_public_id": election["consumer_instrument_public_id"],
        "source_currency": election["source_currency"],
        "target_currency": election["target_currency"],
        "unordered_pair": election["unordered_pair"],
        "required_minutes": election["required_minutes"],
        "election_policy_version": election["election_policy_version"],
        "calculation_version": election["calculation_version"],
    }


async def _repository(tmp_path: Path) -> SQLAlchemyRepository:
    """Create one isolated repository with the model-produced schema."""
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
        ("inverse", Decimal("3"), None),
    ],
)
async def test_direct_and_inverse_decimals_round_trip_exactly(
    tmp_path: Path,
    operation: FxConversionOperation,
    raw_close: Decimal,
    conversion_rate: Decimal | None,
) -> None:
    """Direct and reciprocal inverse values retain every Decimal digit."""
    repository = await _repository(tmp_path)
    election = _election("00000000-0000-7000-8000-000000000010", operation=operation)
    if operation == "inverse":
        election["selected_native_symbol"] = "USD/EUR"
        election["selected_base"] = "USD"
        election["selected_quote"] = "EUR"
        election["considered_candidate_planes"] = (
            {
                "source_exchange": "kraken",
                "source_instrument_public_id": _INSTRUMENT,
                "native_symbol": "USD/EUR",
                "base": "USD",
                "quote": "EUR",
                "orientation": "inverse",
            },
        )
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
async def test_scopes_refusals_partial_and_successful_lookup_are_isolated(tmp_path: Path) -> None:
    """Only complete or partial artifacts become reusable conversion authority."""
    repository = await _repository(tmp_path)
    shared = _election("00000000-0000-7000-8000-000000000020")
    owned = _election("00000000-0000-7000-8000-000000000021", "instrument_owned")
    await repository.pin_fx_conversion_artifact(
        shared, [_proof(shared["public_id"], "00000000-0000-7000-8000-000000000022")]
    )
    await repository.pin_fx_conversion_artifact(
        owned, [_proof(owned["public_id"], "00000000-0000-7000-8000-000000000023")]
    )
    refusal = _election("00000000-0000-7000-8000-000000000024")
    refusal["required_minutes"] = (_MINUTE.replace(minute=54),)
    refusal["completeness_state"] = "refused"
    refusal["refusal_reason_json"] = '{"reason":"fx_conversion_unproven"}'
    refusal["selected_source_exchange"] = None
    refusal["selected_source_instrument_public_id"] = None
    refusal["selected_native_symbol"] = None
    refusal["selected_base"] = None
    refusal["selected_quote"] = None
    refusal["selected_orientation"] = None
    refused = await repository.pin_fx_conversion_artifact(refusal, [])
    assert refused["election"]["requirement_manifest_digest"] == build_requirement_manifest_digest(
        refusal["required_minutes"]
    )
    assert (
        await repository.get_latest_visible_fx_conversion_artifact(_query(refusal), _HORIZON)
        is None
    )
    shared_visible = await repository.get_latest_visible_fx_conversion_artifact(
        _query(shared), _HORIZON
    )
    owned_visible = await repository.get_latest_visible_fx_conversion_artifact(
        _query(owned), _HORIZON
    )
    assert shared_visible is not None
    assert owned_visible is not None
    assert shared_visible["election"]["public_id"] == shared["public_id"]
    assert owned_visible["election"]["public_id"] == owned["public_id"]
    assert await repository.get_fx_conversion_artifact(owned) is not None
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_partial_pins_nonempty_proper_subset(tmp_path: Path) -> None:
    """A partial artifact keeps its full manifest while pinning positive coverage."""
    repository = await _repository(tmp_path)
    election = _election("00000000-0000-7000-8000-000000000025")
    election["required_minutes"] = (_MINUTE, _MINUTE.replace(minute=54))
    election["completeness_state"] = "partial"
    election["refusal_reason_json"] = (
        '{"reason":"missing_candle","unproven_minutes":["2026-07-26T14:54:00+00:00"]}'
    )
    artifact = await repository.pin_fx_conversion_artifact(
        election, [_proof(election["public_id"], "00000000-0000-7000-8000-000000000026")]
    )
    assert artifact["election"]["completeness_state"] == "partial"
    assert artifact["election"]["requirement_manifest_digest"] == build_requirement_manifest_digest(
        election["required_minutes"]
    )
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_identical_writers_converge_but_canonical_value_conflicts(tmp_path: Path) -> None:
    """Opaque caller values cannot disguise a different plane or close."""
    first = await _repository(tmp_path)
    second = SQLAlchemyRepository(first.db_url)
    election_a = _election("00000000-0000-7000-8000-000000000030")
    election_b = _election("00000000-0000-7000-8000-000000000031")
    proof_a = _proof(election_a["public_id"], "00000000-0000-7000-8000-000000000032")
    proof_b = _proof(election_b["public_id"], "00000000-0000-7000-8000-000000000033")
    results = await asyncio.gather(
        first.pin_fx_conversion_artifact(election_a, [proof_a]),
        second.pin_fx_conversion_artifact(election_b, [proof_b]),
    )
    assert results[0]["election"]["public_id"] == results[1]["election"]["public_id"]
    conflict = _election("00000000-0000-7000-8000-000000000034")
    conflict["selected_source_exchange"] = "walutomat"
    conflicting_proof = _proof(
        conflict["public_id"],
        "00000000-0000-7000-8000-000000000035",
        Decimal("9.99"),
        Decimal("9.99"),
    )
    with pytest.raises(FxConversionArtifactConflictError):
        await second.pin_fx_conversion_artifact(conflict, [conflicting_proof])
    candidates_conflict = _election("00000000-0000-7000-8000-00000000006a")
    candidates_conflict["considered_candidate_planes"] = (
        *candidates_conflict["considered_candidate_planes"],
        {
            "source_exchange": "walutomat",
            "source_instrument_public_id": "00000000-0000-7000-8000-00000000006b",
            "native_symbol": "EURUSD",
            "base": "EUR",
            "quote": "USD",
            "orientation": "direct",
        },
    )
    with pytest.raises(FxConversionArtifactConflictError):
        await second.pin_fx_conversion_artifact(
            candidates_conflict,
            [
                _proof(
                    candidates_conflict["public_id"],
                    "00000000-0000-7000-8000-00000000006c",
                )
            ],
        )
    await first.engine.dispose()
    await second.engine.dispose()


@pytest.mark.asyncio
async def test_refusal_audit_does_not_shadow_winner_and_converges(tmp_path: Path) -> None:
    """Successful identity reads ignore refusals while identical audits converge."""
    repository = await _repository(tmp_path)
    winner = _election("00000000-0000-7000-8000-000000000036")
    artifact = await repository.pin_fx_conversion_artifact(
        winner, [_proof(winner["public_id"], "00000000-0000-7000-8000-000000000037")]
    )
    refusal = _election("00000000-0000-7000-8000-000000000038")
    refusal["completeness_state"] = "refused"
    refusal["refusal_reason_json"] = '{"reason":"candidate_timeout"}'
    refusal["selected_source_exchange"] = None
    refusal["selected_source_instrument_public_id"] = None
    refusal["selected_native_symbol"] = None
    refusal["selected_base"] = None
    refusal["selected_quote"] = None
    refusal["selected_orientation"] = None
    refusal["considered_candidate_planes"] = ()
    first_refusal = await repository.pin_fx_conversion_artifact(refusal, [])
    repeated = {**refusal, "public_id": "00000000-0000-7000-8000-000000000039"}
    second_refusal = await repository.pin_fx_conversion_artifact(repeated, [])
    assert first_refusal["election"]["public_id"] == second_refusal["election"]["public_id"]
    visible = await repository.get_fx_conversion_artifact(winner)
    assert visible is not None
    assert visible["election"]["public_id"] == artifact["election"]["public_id"]
    repinned = {**winner, "public_id": "00000000-0000-7000-8000-00000000003a"}
    repinned_proof = _proof(repinned["public_id"], "00000000-0000-7000-8000-00000000003b")
    converged = await repository.pin_fx_conversion_artifact(repinned, [repinned_proof])
    assert converged["election"]["public_id"] == artifact["election"]["public_id"]
    owned_refusal = {
        **refusal,
        "public_id": "00000000-0000-7000-8000-00000000006d",
        "scope_kind": "instrument_owned",
        "consumer_instrument_public_id": _INSTRUMENT,
    }
    await repository.pin_fx_conversion_artifact(owned_refusal, [])
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_refusal_collision_resolution_preserves_integrity_errors(tmp_path: Path) -> None:
    """The refusal race path returns a winner or preserves an unrelated collision."""
    repository = await _repository(tmp_path)
    refusal = _election("00000000-0000-7000-8000-00000000006e")
    refusal["completeness_state"] = "refused"
    refusal["refusal_reason_json"] = '{"reason":"timeout"}'
    refusal["selected_source_exchange"] = None
    refusal["selected_source_instrument_public_id"] = None
    refusal["selected_native_symbol"] = None
    refusal["selected_base"] = None
    refusal["selected_quote"] = None
    refusal["selected_orientation"] = None
    refusal["considered_candidate_planes"] = ()
    collision = IntegrityError("statement", {}, RuntimeError("collision"))
    winner = await repository.pin_fx_conversion_artifact(refusal, [])
    async with repository.session() as session:
        with patch.object(
            repository, "_read_fx_conversion_refusal", AsyncMock(return_value=winner)
        ):
            assert (
                await repository._resolve_fx_conversion_collision(
                    session, refusal, [], "digest", collision
                )
                == winner
            )
        with (
            patch.object(repository, "_read_fx_conversion_refusal", AsyncMock(return_value=None)),
            pytest.raises(IntegrityError) as raised,
        ):
            await repository._resolve_fx_conversion_collision(
                session, refusal, [], "digest", collision
            )
        assert raised.value is collision
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_partial_reason_and_complete_upgrade_fail_closed(tmp_path: Path) -> None:
    """Partial gaps are exact and same-horizon completion requires operator correction."""
    repository = await _repository(tmp_path)
    partial = _election("00000000-0000-7000-8000-00000000003c")
    missing = _MINUTE + timedelta(minutes=2)
    partial["required_minutes"] = (_MINUTE, missing)
    partial["completeness_state"] = "partial"
    partial["refusal_reason_json"] = (
        '{"reason":"missing_candle","unproven_minutes":["2026-07-26T14:54:00+00:00"]}'
    )
    await repository.pin_fx_conversion_artifact(
        partial, [_proof(partial["public_id"], "00000000-0000-7000-8000-00000000003d")]
    )
    malformed = {**partial, "public_id": "00000000-0000-7000-8000-00000000003e"}
    malformed["resolved_knowledge_at"] = _HORIZON + timedelta(minutes=1)
    malformed["refusal_reason_json"] = (
        '{"reason":"missing_candle","unproven_minutes":["2026-07-26T14:55:00+00:00"]}'
    )
    with pytest.raises(ValueError, match="exactly"):
        await repository.pin_fx_conversion_artifact(
            malformed,
            [_proof(malformed["public_id"], "00000000-0000-7000-8000-00000000003f")],
        )
    complete = {**partial, "public_id": "00000000-0000-7000-8000-00000000004a"}
    complete["completeness_state"] = "complete"
    complete["refusal_reason_json"] = None
    second_proof: FxConversionProofInsertRow = {
        **_proof(complete["public_id"], "00000000-0000-7000-8000-00000000004b"),
        "conversion_minute": missing,
        "candle_open_minute": missing - timedelta(minutes=1),
        "carried_minutes": 0,
    }
    with pytest.raises(FxConversionArtifactUpgradeRequiredError):
        await repository.pin_fx_conversion_artifact(
            complete,
            [
                _proof(complete["public_id"], "00000000-0000-7000-8000-00000000004c"),
                second_proof,
            ],
        )
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_evidence_growth_layers_complete_over_partial(tmp_path: Path) -> None:
    """Later evidence creates a complete version that outranks the active partial.

    Given a partial artifact followed by complete evidence at a newer horizon
    When latest-visible authority is read before and after that evidence arrives
    Then both versions remain active while the complete version becomes authoritative
    """
    repository = await _repository(tmp_path)
    missing = _MINUTE + timedelta(minutes=2)
    partial = _election("00000000-0000-7000-8000-000000000150")
    partial["required_minutes"] = (_MINUTE, missing)
    partial["completeness_state"] = "partial"
    partial["refusal_reason_json"] = (
        '{"reason":"missing_candle","unproven_minutes":["2026-07-26T14:54:00+00:00"]}'
    )
    await repository.pin_fx_conversion_artifact(
        partial, [_proof(partial["public_id"], "00000000-0000-7000-8000-000000000151")]
    )
    complete = {**partial, "public_id": "00000000-0000-7000-8000-000000000152"}
    complete["timestamp"] = _HORIZON + timedelta(minutes=1)
    complete["requested_knowledge_at"] = complete["timestamp"]
    complete["resolved_knowledge_at"] = complete["timestamp"]
    complete["completeness_state"] = "complete"
    complete["refusal_reason_json"] = None
    first_proof = _proof(complete["public_id"], "00000000-0000-7000-8000-000000000153")
    second_proof: FxConversionProofInsertRow = {
        **_proof(complete["public_id"], "00000000-0000-7000-8000-000000000154"),
        "conversion_minute": missing,
        "candle_open_minute": missing - timedelta(minutes=1),
        "carried_minutes": 0,
    }
    await repository.pin_fx_conversion_artifact(complete, [first_proof, second_proof])
    before_growth = await repository.get_latest_visible_fx_conversion_artifact(
        _query(partial), _HORIZON
    )
    after_growth = await repository.get_latest_visible_fx_conversion_artifact(
        _query(partial), _HORIZON + timedelta(minutes=1)
    )
    assert before_growth is not None
    assert after_growth is not None
    assert before_growth["election"]["public_id"] == partial["public_id"]
    assert after_growth["election"]["public_id"] == complete["public_id"]
    async with repository.session() as session:
        active_count = len(
            (
                await session.execute(
                    select(FxConversionElection).where(
                        FxConversionElection.public_id.in_(
                            (partial["public_id"], complete["public_id"])
                        )
                    )
                )
            )
            .scalars()
            .all()
        )
    assert active_count == 2
    await repository.engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["raw_close", "conversion_rate"])
async def test_non_decimal_runtime_values_are_typed_errors(tmp_path: Path, field: str) -> None:
    """A float cannot cross the exact-decimal repository boundary."""
    repository = await _repository(tmp_path)
    election = _election("00000000-0000-7000-8000-000000000040")
    proof = _proof(election["public_id"], "00000000-0000-7000-8000-000000000041")
    if field == "raw_close":
        proof["raw_close"] = cast(Decimal, 1.123456789012345)
    else:
        proof["raw_close"] = Decimal("1")
        proof["conversion_rate"] = cast(Decimal, 1.0)
    with pytest.raises(FxConversionArtifactValueError):
        await repository.pin_fx_conversion_artifact(election, [proof])
    await repository.engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("orientation", "conversion_rate", "message"),
    [
        ("direct", Decimal("2"), "must equal raw_close"),
        ("inverse", Decimal("1"), "must be null"),
    ],
)
async def test_conversion_rate_shape_matches_operation(
    tmp_path: Path,
    orientation: Literal["direct", "inverse"],
    conversion_rate: Decimal,
    message: str,
) -> None:
    """Direct and inverse proofs enforce their distinct replay representation."""
    repository = await _repository(tmp_path)
    election = _election("00000000-0000-7000-8000-000000000140")
    election["selected_orientation"] = orientation
    proof = _proof(
        election["public_id"],
        "00000000-0000-7000-8000-000000000141",
        operation=orientation,
        conversion_rate=conversion_rate,
    )
    with pytest.raises(FxConversionArtifactValueError, match=message):
        await repository.pin_fx_conversion_artifact(election, [proof])
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_operation_and_completeness_validation_fail_closed(tmp_path: Path) -> None:
    """Orientation mismatch and invalid complete or partial coverage are refused."""
    repository = await _repository(tmp_path)
    election = _election("00000000-0000-7000-8000-000000000042")
    inverse = _proof(
        election["public_id"], "00000000-0000-7000-8000-000000000043", operation="inverse"
    )
    with pytest.raises(ValueError, match="operation"):
        await repository.pin_fx_conversion_artifact(election, [inverse])
    election["completeness_state"] = "partial"
    with pytest.raises(ValueError, match="proper"):
        await repository.pin_fx_conversion_artifact(election, [])
    await repository.engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reason_json",
    [
        None,
        "[]",
        "{}",
        '{"reason":"missing","unproven_minutes":null}',
        (
            '{"reason":"missing","unproven_minutes":["2026-07-26T14:54:00+00:00",'
            '"2026-07-26T14:54:00+00:00"]}'
        ),
    ],
)
async def test_structured_partial_reason_rejects_every_invalid_shape(
    tmp_path: Path, reason_json: str | None
) -> None:
    """Partial reasons are objects with a reason and a duplicate-free exact gap set."""
    repository = await _repository(tmp_path)
    election = _election("00000000-0000-7000-8000-00000000006f")
    election["required_minutes"] = (_MINUTE, _MINUTE + timedelta(minutes=2))
    election["completeness_state"] = "partial"
    election["refusal_reason_json"] = reason_json
    with pytest.raises(ValueError):
        await repository.pin_fx_conversion_artifact(
            election,
            [_proof(election["public_id"], "00000000-0000-7000-8000-000000000070")],
        )
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_complete_reason_is_rejected(tmp_path: Path) -> None:
    """A complete election cannot retain a failure reason."""
    repository = await _repository(tmp_path)
    election = _election("00000000-0000-7000-8000-000000000071")
    election["refusal_reason_json"] = '{"reason":"stale"}'
    with pytest.raises(ValueError, match="must not"):
        await repository.pin_fx_conversion_artifact(
            election,
            [_proof(election["public_id"], "00000000-0000-7000-8000-000000000072")],
        )
    await repository.engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "violation",
    ["duplicate", "complete", "refused", "parent", "plane"],
)
async def test_parent_child_validation_rejects_every_incoherence(
    tmp_path: Path, violation: str
) -> None:
    """Coverage, parent, plane, and unique-minute invariants fail before writes."""
    repository = await _repository(tmp_path)
    election = _election("00000000-0000-7000-8000-000000000044")
    proof = _proof(election["public_id"], "00000000-0000-7000-8000-000000000045")
    proofs = [proof]
    if violation == "duplicate":
        proofs.append({**proof, "public_id": "00000000-0000-7000-8000-000000000046"})
    elif violation == "complete":
        proofs = []
    elif violation == "refused":
        election["completeness_state"] = "refused"
    elif violation == "parent":
        proof["election_public_id"] = "00000000-0000-7000-8000-000000000047"
    else:
        proof["source_instrument_public_id"] = "00000000-0000-7000-8000-000000000048"
    with pytest.raises(ValueError):
        await repository.pin_fx_conversion_artifact(election, proofs)
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_absent_reads_and_impossible_post_commit_disappearance_fail_closed(
    tmp_path: Path,
) -> None:
    """Absent authority is None and a vanished committed row is an operational error."""
    repository = await _repository(tmp_path)
    election = _election("00000000-0000-7000-8000-000000000052")
    assert await repository.get_fx_conversion_artifact(election) is None
    async with repository.session() as session:
        assert (
            await repository._read_fx_conversion_artifact_by_public_id(
                session, "00000000-0000-7000-8000-000000000099"
            )
            is None
        )
    assert (
        await repository.get_latest_visible_fx_conversion_artifact(
            _query(election), _HORIZON - timedelta(seconds=1)
        )
        is None
    )
    with (
        patch.object(
            repository,
            "_read_fx_conversion_artifact_by_public_id",
            AsyncMock(return_value=None),
        ),
        pytest.raises(RuntimeError, match="could not be read back"),
    ):
        await repository.pin_fx_conversion_artifact(
            election,
            [_proof(election["public_id"], "00000000-0000-7000-8000-000000000053")],
        )
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_latest_visible_reread_uses_the_same_as_of_interval(tmp_path: Path) -> None:
    """The second-stage election and proof read preserves historical visibility."""
    repository = await _repository(tmp_path)
    active = _election("00000000-0000-7000-8000-000000000056")
    active_proof = _proof(active["public_id"], "00000000-0000-7000-8000-000000000057")
    await repository.pin_fx_conversion_artifact(active, [active_proof])
    historical_time = _HORIZON - timedelta(minutes=2)
    historical_end = _HORIZON - timedelta(minutes=1)
    historical_id = "00000000-0000-7000-8000-000000000058"
    async with repository.session() as session:
        election_row = dict(
            (
                await session.execute(
                    select(FxConversionElection.__table__).where(
                        FxConversionElection.public_id == active["public_id"]
                    )
                )
            )
            .mappings()
            .one()
        )
        election_row.pop("id")
        election_row.update(
            public_id=historical_id,
            timestamp=historical_time,
            known_to=historical_end,
            requested_knowledge_at=historical_time,
            resolved_knowledge_at=historical_time,
        )
        proof_row = dict(
            (
                await session.execute(
                    select(FxConversionProof.__table__).where(
                        FxConversionProof.election_public_id == active["public_id"]
                    )
                )
            )
            .mappings()
            .one()
        )
        proof_row.pop("id")
        proof_row.update(
            public_id="00000000-0000-7000-8000-000000000059",
            election_public_id=historical_id,
            timestamp=historical_time,
            known_to=historical_end,
        )
        await session.execute(FxConversionElection.__table__.insert(), election_row)
        await session.execute(FxConversionProof.__table__.insert(), proof_row)
        await session.commit()
    visible = await repository.get_latest_visible_fx_conversion_artifact(
        _query(active), historical_time + timedelta(seconds=30)
    )
    assert visible is not None
    assert visible["election"]["public_id"] == historical_id
    assert visible["proofs"][0]["public_id"] == proof_row["public_id"]
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_unrelated_integrity_collision_is_not_reclassified(tmp_path: Path) -> None:
    """A proof-public-id collision without an election winner remains an integrity error."""
    repository = await _repository(tmp_path)
    first = _election("00000000-0000-7000-8000-000000000060")
    proof_id = "00000000-0000-7000-8000-000000000061"
    await repository.pin_fx_conversion_artifact(first, [_proof(first["public_id"], proof_id)])
    second = _election("00000000-0000-7000-8000-000000000062")
    second["resolved_knowledge_at"] = _HORIZON + timedelta(minutes=1)
    with pytest.raises(IntegrityError):
        await repository.pin_fx_conversion_artifact(second, [_proof(second["public_id"], proof_id)])
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_repository_base_fx_methods_are_explicitly_unimplemented() -> None:
    """Repository implementers receive an explicit failure for every new contract method."""
    repository = cast(Repository, object())
    election = _election("00000000-0000-7000-8000-000000000054")
    query = _query(election)
    with pytest.raises(NotImplementedError):
        await Repository.pin_fx_conversion_artifact(repository, election, [])
    with pytest.raises(NotImplementedError):
        await Repository.get_fx_conversion_artifact(repository, election)
    with pytest.raises(NotImplementedError):
        await Repository.get_latest_visible_fx_conversion_artifact(repository, query, _HORIZON)


def test_digest_canonicalization_normalizes_instants_and_rejects_bad_minutes() -> None:
    """Equivalent zones hash equally while naive and off-grid minutes are refused."""
    plus_two = _MINUTE.astimezone(timezone(timedelta(hours=2)))
    assert build_requirement_manifest_digest([plus_two]) == build_requirement_manifest_digest(
        [_MINUTE]
    )
    with pytest.raises(ValueError, match="timezone-aware"):
        build_requirement_manifest_digest([_MINUTE.replace(tzinfo=None)])
    with pytest.raises(ValueError, match="minute-aligned"):
        build_requirement_manifest_digest([_MINUTE.replace(second=37)])
    election = _election("00000000-0000-7000-8000-000000000050")
    proof = _proof(election["public_id"], "00000000-0000-7000-8000-000000000051")
    shifted: FxConversionProofInsertRow = {
        **proof,
        "conversion_minute": plus_two,
        "candle_open_minute": plus_two - timedelta(minutes=1),
        "carried_minutes": 0,
    }
    assert build_proof_digest(proof) == build_proof_digest(shifted)
    assert build_decision_inputs_digest(election, [proof]) == build_decision_inputs_digest(
        election, [shifted]
    )
    rejected = {
        "source_exchange": "walutomat",
        "source_instrument_public_id": "00000000-0000-7000-8000-000000000055",
        "native_symbol": "EURPLN",
        "base": "EUR",
        "quote": "PLN",
        "orientation": "direct",
    }
    with_rejected: FxConversionElectionInsertRow = {
        **election,
        "considered_candidate_planes": (*election["considered_candidate_planes"], rejected),
    }
    reordered = {
        **with_rejected,
        "considered_candidate_planes": tuple(
            reversed(with_rejected["considered_candidate_planes"])
        ),
    }
    assert build_decision_inputs_digest(with_rejected, [proof]) == build_decision_inputs_digest(
        reordered, [proof]
    )
    assert build_decision_inputs_digest(election, [proof]) != build_decision_inputs_digest(
        with_rejected, [proof]
    )
    invalid: FxConversionProofInsertRow = {
        **proof,
        "raw_close": cast(Decimal, 1.25),
    }
    with pytest.raises(TypeError, match="must be Decimal"):
        build_proof_digest(invalid)
    canonical_reason = '{"reason":"missing","unproven_minutes":[]}'
    assert build_refusal_reason_digest(canonical_reason) is not None
    with pytest.raises(ValueError, match="canonical JSON"):
        build_refusal_reason_digest('{"unproven_minutes": [], "reason": "missing"}')


def test_postgresql_trigger_ddl_covers_install_and_drop_shapes() -> None:
    """The production dialect emits row, truncate, ALWAYS, and drop statements."""
    statements: list[str] = []

    class _Recorder:
        dialect = SimpleNamespace(name="postgresql")

        def execute(self, statement: Executable) -> object:
            statements.append(str(statement))
            return object()

    connection = cast(Connection, _Recorder())
    install_fx_conversion_immutability_triggers(connection)
    assert len(statements) == 14
    assert sum("ENABLE ALWAYS" in statement for statement in statements) == 4
    statements.clear()
    drop_fx_conversion_immutability_triggers(connection)
    assert len(statements) == 6
