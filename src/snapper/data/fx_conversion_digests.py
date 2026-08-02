"""Canonical SHA-256 builders for durable FX conversion artifacts."""

import hashlib
import json
from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from decimal import Decimal

from snapper.data.repository_types import FxConversionElectionInsertRow
from snapper.data.repository_types import FxConversionProofInsertRow


def _canonical_json(payload: object) -> str:
    """Serialize one finite payload with the repository-wide canonical form."""
    return json.dumps(payload, allow_nan=False, separators=(",", ":"), sort_keys=True)


def _sha256(payload: object) -> str:
    """Hash one canonical JSON payload as lowercase hexadecimal."""
    return hashlib.sha256(_canonical_json(payload).encode()).hexdigest()


def canonical_fx_refusal_reason(payload: object) -> str:
    """Serialize a stable refusal reason without attempt-specific decoration."""
    return _canonical_json(payload)


def build_refusal_reason_digest(reason_json: str | None) -> str | None:
    """Digest one canonical structured reason for fixed-width uniqueness."""
    if reason_json is None:
        return None
    parsed: object = json.loads(reason_json)
    canonical = canonical_fx_refusal_reason(parsed)
    if reason_json != canonical:
        raise ValueError("FX election reasons must use canonical JSON serialization")
    return hashlib.sha256(canonical.encode()).hexdigest()


def _canonical_instant(value: datetime) -> str:
    """Return one aware instant in the canonical UTC spelling."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("FX digest datetimes must be timezone-aware")
    return value.astimezone(UTC).isoformat()


def _canonical_minute(value: datetime) -> str:
    """Return one aware, minute-aligned instant in canonical UTC spelling."""
    normalized = _canonical_instant(value)
    if value.second != 0 or value.microsecond != 0:
        raise ValueError("FX requirement datetimes must be minute-aligned")
    return normalized


def _canonical_decimal(value: Decimal) -> str:
    """Return an exact non-exponent decimal spelling."""
    if not isinstance(value, Decimal):
        raise TypeError("FX proof decimal values must be Decimal")
    return format(value, "f")


def build_requirement_manifest_digest(exact_minutes: Sequence[datetime]) -> str:
    """Digest the sorted, duplicate-free normalized requirement minute set."""
    normalized = sorted({_canonical_minute(minute) for minute in exact_minutes})
    return _sha256({"exact_minutes": normalized})


def build_proof_digest(proof: FxConversionProofInsertRow) -> str:
    """Derive one proof digest from its canonical evidence columns."""
    payload = {
        "conversion_minute": _canonical_minute(proof["conversion_minute"]),
        "candle_open_minute": _canonical_minute(proof["candle_open_minute"]),
        "candle_id": proof["candle_id"],
        "candle_public_id": proof["candle_public_id"],
        "candle_session_id": proof["candle_session_id"],
        "candle_sequence_id": proof["candle_sequence_id"],
        "candle_timestamp": _canonical_instant(proof["candle_timestamp"]),
        "candle_known_to": _canonical_instant(proof["candle_known_to"]),
        "raw_close": _canonical_decimal(proof["raw_close"]),
        "operation": proof["operation"],
        "conversion_rate": (
            None
            if proof["conversion_rate"] is None
            else _canonical_decimal(proof["conversion_rate"])
        ),
        "source_instrument_public_id": proof["source_instrument_public_id"],
    }
    return _sha256({"proof": payload})


def build_decision_inputs_digest(
    election: FxConversionElectionInsertRow,
    proofs: Sequence[FxConversionProofInsertRow],
) -> str:
    """Derive the digest from every considered plane, the winner, and proofs."""
    candidates = sorted(
        _canonical_json(candidate) for candidate in election["considered_candidate_planes"]
    )
    payload = {
        "considered_candidate_planes": candidates,
        "source_exchange": election["selected_source_exchange"],
        "source_instrument_public_id": election["selected_source_instrument_public_id"],
        "native_symbol": election["selected_native_symbol"],
        "base": election["selected_base"],
        "quote": election["selected_quote"],
        "orientation": election["selected_orientation"],
        "proof_digests": sorted(build_proof_digest(proof) for proof in proofs),
    }
    return _sha256({"decision_inputs": payload})
