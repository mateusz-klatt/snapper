"""Opt-in PostgreSQL witnesses for FX identity races and partial indexes."""

import asyncio
import os
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import FxConversionElectionInsertRow
from snapper.data.repository_types import FxConversionProofInsertRow


def _configured_backend_name() -> str:
    """Return the backend name of the session-configured database URL."""
    url = os.environ.get("DB_URL", "")
    if not url:
        return ""
    try:
        return make_url(url).get_backend_name()
    except ArgumentError:
        return ""


pytestmark = pytest.mark.skipif(
    _configured_backend_name() != "postgresql",
    reason="live PostgreSQL FX artifact tests require an opted-in scratch database",
)


def _artifact() -> tuple[FxConversionElectionInsertRow, FxConversionProofInsertRow]:
    """Build one random complete artifact for a PostgreSQL race."""
    minute = datetime(2026, 7, 26, 14, 52, tzinfo=UTC)
    horizon = datetime.now(UTC).replace(microsecond=0)
    election_id = str(uuid4())
    instrument_id = str(uuid4())
    session_id = str(uuid4())
    election: FxConversionElectionInsertRow = {
        "public_id": election_id,
        "session_id": session_id,
        "sequence_id": 1,
        "timestamp": horizon,
        "scope_kind": "shared_pair",
        "consumer_instrument_public_id": None,
        "source_currency": "EUR",
        "target_currency": "USD",
        "unordered_pair": "EUR-USD",
        "required_minutes": (minute,),
        "requested_knowledge_at": horizon,
        "resolved_knowledge_at": horizon,
        "election_policy_version": str(uuid4()),
        "calculation_version": "pnl-v1",
        "selected_source_exchange": "kraken",
        "selected_source_instrument_public_id": instrument_id,
        "selected_native_symbol": "EUR/USD",
        "selected_base": "EUR",
        "selected_quote": "USD",
        "selected_orientation": "direct",
        "considered_candidate_planes": (
            {
                "source_exchange": "kraken",
                "source_instrument_public_id": instrument_id,
                "native_symbol": "EUR/USD",
                "base": "EUR",
                "quote": "USD",
                "orientation": "direct",
            },
        ),
        "completeness_state": "complete",
        "refusal_reason_json": None,
    }
    proof: FxConversionProofInsertRow = {
        "public_id": str(uuid4()),
        "session_id": session_id,
        "sequence_id": 2,
        "timestamp": horizon,
        "election_public_id": election_id,
        "conversion_minute": minute,
        "carried_minutes": 0,
        "candle_open_minute": minute - timedelta(minutes=1),
        "candle_id": 42,
        "candle_public_id": str(uuid4()),
        "candle_session_id": session_id,
        "candle_sequence_id": 7,
        "candle_timestamp": minute - timedelta(minutes=1),
        "candle_known_to": horizon,
        "carried_minutes": 0,
        "raw_close": Decimal("1.10"),
        "operation": "direct",
        "conversion_rate": Decimal("1.10"),
        "source_instrument_public_id": instrument_id,
    }
    return election, proof


@pytest.mark.asyncio
async def test_postgresql_identity_race_and_refusal_partial_predicate() -> None:
    """Concurrent winners and byte-identical refusal audits both converge."""
    first = SQLAlchemyRepository(os.environ["DB_URL"])
    second = SQLAlchemyRepository(os.environ["DB_URL"])
    election_a, proof_a = _artifact()
    election_b: FxConversionElectionInsertRow = {**election_a, "public_id": str(uuid4())}
    proof_b: FxConversionProofInsertRow = {
        **proof_a,
        "public_id": str(uuid4()),
        "election_public_id": election_b["public_id"],
    }
    winners = await asyncio.gather(
        first.pin_fx_conversion_artifact(election_a, [proof_a]),
        second.pin_fx_conversion_artifact(election_b, [proof_b]),
    )
    assert winners[0]["election"]["public_id"] == winners[1]["election"]["public_id"]
    refusal: FxConversionElectionInsertRow = {
        **election_a,
        "public_id": str(uuid4()),
        "completeness_state": "refused",
        "refusal_reason_json": '{"reason":"no_candidate"}',
        "selected_source_exchange": None,
        "selected_source_instrument_public_id": None,
        "selected_native_symbol": None,
        "selected_base": None,
        "selected_quote": None,
        "selected_orientation": None,
    }
    second_refusal: FxConversionElectionInsertRow = {
        **refusal,
        "public_id": str(uuid4()),
    }
    refused = await asyncio.gather(
        first.pin_fx_conversion_artifact(refusal, []),
        second.pin_fx_conversion_artifact(second_refusal, []),
    )
    assert refused[0]["election"]["public_id"] == refused[1]["election"]["public_id"]
    await first.engine.dispose()
    await second.engine.dispose()
