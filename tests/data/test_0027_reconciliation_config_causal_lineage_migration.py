"""Tests for reconciliation configuration causal-lineage migration 0027."""

from pathlib import Path
from uuid import uuid4

import sqlalchemy as sa
from alembic import command
from alembic.config import Config

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_ACTIVE = "9999-12-31 23:59:59.000000"
_TS = "2026-07-16 08:00:00.000000"


def _config(db_url: str) -> Config:
    """Build Alembic configuration for one throwaway database."""
    config = Config(str(ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", db_url)
    return config


def test_0027_sqlite_upgrade_and_downgrade_preserve_config_rows(
    tmp_path: Path,
) -> None:
    """SQLite adds nullable lineage and removes it without rebuilding config data."""
    db_url = f"sqlite:///{tmp_path / 'reconciliation-config-causal-lineage.db'}"
    config = _config(db_url)
    command.upgrade(config, "0026")
    engine = sa.create_engine(db_url)
    public_id = str(uuid4())
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                "INSERT INTO portfolio_reconciliation_method_configs "
                "(wallet_public_id, exchange, mode, method, public_id, session_id, sequence_id, "
                "timestamp, known_to) VALUES "
                "(:wallet_public_id, 'kraken', 'live', 'futures_position', :public_id, "
                ":session_id, 1, :timestamp, :known_to)"
            ),
            {
                "wallet_public_id": str(uuid4()),
                "public_id": public_id,
                "session_id": str(uuid4()),
                "timestamp": _TS,
                "known_to": _ACTIVE,
            },
        )

    command.upgrade(config, "0027")
    columns = {
        column["name"]: column
        for column in sa.inspect(engine).get_columns("portfolio_reconciliation_method_configs")
    }
    assert "classified_after_observation_id" in columns
    assert columns["classified_after_observation_id"]["nullable"] is True
    with engine.begin() as connection:
        assert (
            connection.execute(
                sa.text(
                    "SELECT classified_after_observation_id "
                    "FROM portfolio_reconciliation_method_configs WHERE public_id = :public_id"
                ),
                {"public_id": public_id},
            ).scalar_one()
            is None
        )
        connection.execute(
            sa.text(
                "UPDATE portfolio_reconciliation_method_configs "
                "SET classified_after_observation_id = 41 WHERE public_id = :public_id"
            ),
            {"public_id": public_id},
        )
        assert (
            connection.execute(
                sa.text(
                    "SELECT classified_after_observation_id "
                    "FROM portfolio_reconciliation_method_configs WHERE public_id = :public_id"
                ),
                {"public_id": public_id},
            ).scalar_one()
            == 41
        )

    command.downgrade(config, "0026")
    downgraded_columns = {
        column["name"]
        for column in sa.inspect(engine).get_columns("portfolio_reconciliation_method_configs")
    }
    assert "classified_after_observation_id" not in downgraded_columns
    with engine.begin() as connection:
        assert (
            connection.execute(
                sa.text(
                    "SELECT COUNT(*) FROM portfolio_reconciliation_method_configs "
                    "WHERE public_id = :public_id"
                ),
                {"public_id": public_id},
            ).scalar_one()
            == 1
        )
    engine.dispose()
