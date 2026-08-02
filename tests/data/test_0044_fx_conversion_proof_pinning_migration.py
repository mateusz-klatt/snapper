"""Executable schema contract tests for FX proof migration 0044."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.exc import IntegrityError

_ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_ACTIVE = datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC)
_HORIZON = datetime(2026, 8, 2, 9, 0, tzinfo=UTC)
_MINUTE = datetime(2026, 7, 26, 14, 52, tzinfo=UTC)
_OPEN = _MINUTE - timedelta(minutes=1)


def _config(db_url: str) -> Config:
    """Build an Alembic configuration for one isolated database."""
    config = Config(str(_ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", db_url)
    return config


def _election(public_id: str, **overrides: object) -> dict[str, object]:
    """Build one valid complete shared-pair election row."""
    row: dict[str, object] = {
        "public_id": public_id,
        "session_id": "00000000-0000-7000-8000-000000000001",
        "sequence_id": 1,
        "timestamp": _HORIZON,
        "known_to": _ACTIVE,
        "scope_kind": "shared_pair",
        "consumer_instrument_public_id": None,
        "source_currency": "EUR",
        "target_currency": "USD",
        "unordered_pair": "EUR-USD",
        "requirement_manifest_digest": "a" * 64,
        "requested_knowledge_at": _HORIZON,
        "resolved_knowledge_at": _HORIZON,
        "election_policy_version": "fx-v1",
        "calculation_version": "pnl-v1",
        "selected_source_exchange": "kraken",
        "selected_source_instrument_public_id": "00000000-0000-7000-8000-000000000002",
        "selected_native_symbol": "EUR/USD",
        "selected_base": "EUR",
        "selected_quote": "USD",
        "selected_orientation": "direct",
        "decision_inputs_digest": "b" * 64,
        "completeness_state": "complete",
        "refusal_reason_json": None,
    }
    row.update(overrides)
    return row


def _proof(public_id: str, election_public_id: str, **overrides: object) -> dict[str, object]:
    """Build one valid direct proof using the M-minus-one candle."""
    row: dict[str, object] = {
        "public_id": public_id,
        "session_id": "00000000-0000-7000-8000-000000000001",
        "sequence_id": 2,
        "timestamp": _HORIZON,
        "known_to": _ACTIVE,
        "election_public_id": election_public_id,
        "conversion_minute": _MINUTE,
        "candle_open_minute": _OPEN,
        "candle_id": 42,
        "candle_public_id": "00000000-0000-7000-8000-000000000003",
        "candle_session_id": "00000000-0000-7000-8000-000000000001",
        "candle_sequence_id": 7,
        "candle_timestamp": _OPEN,
        "candle_known_to": _HORIZON,
        "raw_close_decimal": "1.10",
        "operation": "direct",
        "conversion_rate_decimal": "1.10",
        "source_instrument_public_id": "00000000-0000-7000-8000-000000000002",
        "proof_digest": "c" * 64,
    }
    row.update(overrides)
    return row


def _insert(engine: sa.Engine, table: str, row: dict[str, object]) -> None:
    """Insert one complete row through reflected migration-produced metadata."""
    metadata = sa.MetaData()
    reflected = sa.Table(table, metadata, autoload_with=engine)
    with engine.begin() as connection:
        connection.execute(reflected.insert(), row)


def test_0044_upgrade_downgrade_and_reupgrade_contract(migrated_db_path: Path) -> None:
    """Both append-only tables round-trip as one additive revision."""
    db_url = f"sqlite:///{migrated_db_path}"
    config = _config(db_url)
    engine = sa.create_engine(db_url)
    assert {"fx_conversion_elections", "fx_conversion_proofs"} <= set(
        sa.inspect(engine).get_table_names()
    )
    command.downgrade(config, "0043")
    assert {"fx_conversion_elections", "fx_conversion_proofs"}.isdisjoint(
        sa.inspect(engine).get_table_names()
    )
    command.upgrade(config, "0044")
    assert {"fx_conversion_elections", "fx_conversion_proofs"} <= set(
        sa.inspect(engine).get_table_names()
    )
    engine.dispose()


@pytest.mark.parametrize(
    "overrides",
    [
        {"scope_kind": "bad"},
        {"scope_kind": "instrument_owned", "consumer_instrument_public_id": None},
        {"completeness_state": "bad"},
        {"selected_orientation": "sideways"},
        {"completeness_state": "refused", "refusal_reason_json": "{}"},
        {"completeness_state": "partial", "refusal_reason_json": None},
        {"completeness_state": "complete", "selected_source_exchange": None},
    ],
)
def test_0044_election_checks_reject_invalid_rows(
    migrated_db_path: Path, overrides: dict[str, object]
) -> None:
    """Every election CHECK rejects a concrete violating insert."""
    engine = sa.create_engine(f"sqlite:///{migrated_db_path}")
    with pytest.raises(IntegrityError):
        _insert(
            engine,
            "fx_conversion_elections",
            _election("00000000-0000-7000-8000-000000000010", **overrides),
        )
    engine.dispose()


@pytest.mark.parametrize(
    "overrides",
    [
        {"operation": "sideways"},
        {"candle_open_minute": _MINUTE},
    ],
)
def test_0044_proof_checks_enforce_operation_and_m_minus_one_relation(
    migrated_db_path: Path, overrides: dict[str, object]
) -> None:
    """The proof CHECKs accept only a valid operation and prior-minute candle."""
    engine = sa.create_engine(f"sqlite:///{migrated_db_path}")
    with pytest.raises(IntegrityError):
        _insert(
            engine,
            "fx_conversion_proofs",
            _proof(
                "00000000-0000-7000-8000-000000000011",
                "00000000-0000-7000-8000-000000000010",
                **overrides,
            ),
        )
    engine.dispose()


def test_0044_partial_unique_indexes_and_refusal_predicate(migrated_db_path: Path) -> None:
    """Successful identities collide and identical refusal audits are unique."""
    engine = sa.create_engine(f"sqlite:///{migrated_db_path}")
    first = _election("00000000-0000-7000-8000-000000000020")
    _insert(engine, "fx_conversion_elections", first)
    with pytest.raises(IntegrityError):
        _insert(
            engine,
            "fx_conversion_elections",
            _election(
                "00000000-0000-7000-8000-000000000021",
                requested_knowledge_at=_HORIZON + timedelta(hours=1),
            ),
        )
    refused = _election(
        "00000000-0000-7000-8000-000000000022",
        completeness_state="refused",
        refusal_reason_json="{}",
        selected_source_exchange=None,
        selected_source_instrument_public_id=None,
        selected_native_symbol=None,
        selected_base=None,
        selected_quote=None,
        selected_orientation=None,
    )
    _insert(engine, "fx_conversion_elections", refused)
    with pytest.raises(IntegrityError):
        _insert(
            engine,
            "fx_conversion_elections",
            {**refused, "public_id": "00000000-0000-7000-8000-000000000023"},
        )
    _insert(
        engine,
        "fx_conversion_elections",
        {
            **refused,
            "public_id": "00000000-0000-7000-8000-000000000024",
            "refusal_reason_json": '{"reason":"different"}',
        },
    )
    _insert(
        engine,
        "fx_conversion_elections",
        {
            **refused,
            "public_id": "00000000-0000-7000-8000-000000000025",
            "requirement_manifest_digest": "d" * 64,
        },
    )
    engine.dispose()


def test_0044_proof_unique_index_and_immutability_triggers(migrated_db_path: Path) -> None:
    """The minute key is unique and raw UPDATE or DELETE is physically refused."""
    engine = sa.create_engine(f"sqlite:///{migrated_db_path}")
    election_id = "00000000-0000-7000-8000-000000000030"
    _insert(engine, "fx_conversion_elections", _election(election_id))
    _insert(
        engine, "fx_conversion_proofs", _proof("00000000-0000-7000-8000-000000000031", election_id)
    )
    with pytest.raises(IntegrityError):
        _insert(
            engine,
            "fx_conversion_proofs",
            _proof("00000000-0000-7000-8000-000000000032", election_id),
        )
    with engine.begin() as connection, pytest.raises(IntegrityError, match="append-only"):
        connection.execute(sa.text("UPDATE fx_conversion_elections SET sequence_id = 9"))
    with engine.begin() as connection, pytest.raises(IntegrityError, match="append-only"):
        connection.execute(sa.text("DELETE FROM fx_conversion_proofs"))
    engine.dispose()
