"""Canonical SHA-256 builders for durable FX conversion elections."""

import hashlib
import json
from collections.abc import Sequence
from datetime import datetime

from snapper.core.json_types import JsonObject


def _canonical_json(payload: object) -> str:
    """Serialize one finite payload with the repository-wide canonical form."""
    return json.dumps(payload, allow_nan=False, separators=(",", ":"), sort_keys=True)


def _sha256(payload: object) -> str:
    """Hash one canonical JSON payload as lowercase hexadecimal."""
    return hashlib.sha256(_canonical_json(payload).encode()).hexdigest()


def build_requirement_manifest_digest(exact_minutes: Sequence[datetime]) -> str:
    """Digest the sorted, duplicate-free exact-minute requirement set."""
    normalized = sorted({minute.isoformat() for minute in exact_minutes})
    return _sha256({"exact_minutes": normalized})


def build_decision_inputs_digest(decision_inputs: Sequence[JsonObject]) -> str:
    """Digest decision records independently of record and key ordering."""
    normalized = sorted(_canonical_json(item) for item in decision_inputs)
    return _sha256({"decision_inputs": normalized})


def build_proof_digest(proof_inputs: JsonObject) -> str:
    """Digest one proof projection independently of input key ordering."""
    return _sha256({"proof": proof_inputs})
