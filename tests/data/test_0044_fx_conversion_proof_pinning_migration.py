"""Upgrade and downgrade contract tests for migration 0044."""

from pathlib import Path

import sqlalchemy as sa
from alembic import command
from alembic.config import Config

_ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"

_ELECTION_COLUMNS = {
    "id",
    "public_id",
    "session_id",
    "sequence_id",
    "timestamp",
    "known_to",
    "scope_kind",
    "consumer_instrument_public_id",
    "source_currency",
    "target_currency",
    "unordered_pair",
    "requirement_manifest_digest",
    "requested_knowledge_at",
    "resolved_knowledge_at",
    "election_policy_version",
    "calculation_version",
    "selected_source_exchange",
    "selected_source_instrument_public_id",
    "selected_native_symbol",
    "selected_base",
    "selected_quote",
    "selected_orientation",
    "decision_inputs_digest",
    "completeness_state",
    "refusal_reason_json",
}
_PROOF_COLUMNS = {
    "id",
    "public_id",
    "session_id",
    "sequence_id",
    "timestamp",
    "known_to",
    "election_public_id",
    "conversion_minute",
    "candle_open_minute",
    "candle_id",
    "candle_public_id",
    "candle_session_id",
    "candle_sequence_id",
    "candle_timestamp",
    "candle_known_to",
    "raw_close_decimal",
    "operation",
    "conversion_rate_decimal",
    "source_instrument_public_id",
    "proof_digest",
}


def _schema(engine: sa.Engine) -> tuple[set[str], set[str], set[str]]:
    """Return tables plus exact column sets for both proof artifacts."""
    inspector = sa.inspect(engine)
    tables = set(inspector.get_table_names())
    elections = (
        {str(column["name"]) for column in inspector.get_columns("fx_conversion_elections")}
        if "fx_conversion_elections" in tables
        else set()
    )
    proofs = (
        {str(column["name"]) for column in inspector.get_columns("fx_conversion_proofs")}
        if "fx_conversion_proofs" in tables
        else set()
    )
    return tables, elections, proofs


def _config(db_url: str) -> Config:
    """Build an Alembic configuration for one isolated database."""
    config = Config(str(_ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", db_url)
    return config


def test_0044_upgrade_downgrade_and_reupgrade_contract(migrated_db_path: Path) -> None:
    """Both exact schemas and indexes disappear and return as one revision."""
    db_url = f"sqlite:///{migrated_db_path}"
    config = _config(db_url)
    engine = sa.create_engine(db_url)
    tables, elections, proofs = _schema(engine)
    assert {"fx_conversion_elections", "fx_conversion_proofs"} <= tables
    assert elections == _ELECTION_COLUMNS
    assert proofs == _PROOF_COLUMNS
    assert {
        index["name"] for index in sa.inspect(engine).get_indexes("fx_conversion_elections")
    } == {
        "ix_fx_elections_public_id",
        "uq_fx_elections_instrument_identity",
        "uq_fx_elections_shared_identity",
    }
    assert {index["name"] for index in sa.inspect(engine).get_indexes("fx_conversion_proofs")} == {
        "ix_fx_proofs_public_id",
        "uq_fx_proofs_election_minute",
    }
    command.downgrade(config, "0043")
    downgraded, elections, proofs = _schema(engine)
    assert {"fx_conversion_elections", "fx_conversion_proofs"}.isdisjoint(downgraded)
    assert not elections
    assert not proofs
    command.upgrade(config, "0044")
    upgraded, elections, proofs = _schema(engine)
    assert {"fx_conversion_elections", "fx_conversion_proofs"} <= upgraded
    assert elections == _ELECTION_COLUMNS
    assert proofs == _PROOF_COLUMNS
    engine.dispose()
