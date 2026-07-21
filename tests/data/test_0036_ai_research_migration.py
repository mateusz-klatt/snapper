"""Tests for the additive AI-research persistence migration 0036."""

from pathlib import Path
from uuid import uuid7

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config

_ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_TABLES = {"ai_research_rounds", "market_views", "market_view_sources"}
_TRIGGERS = {
    "market_view_sources_reject_delete",
    "market_view_sources_reject_update",
    "market_views_reject_delete",
    "market_views_reject_update",
}
_TS = "2026-07-21 08:00:00+00:00"


def _config(db_url: str) -> Config:
    """Build an Alembic configuration for one throwaway SQLite database."""
    config = Config(str(_ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", db_url)
    return config


def _new_tables(engine: sa.Engine) -> set[str]:
    """Return the subset of AI-research tables currently present."""
    return set(sa.inspect(engine).get_table_names()) & _TABLES


def _artifact_triggers(engine: sa.Engine) -> set[str]:
    """Return the installed SQLite artifact immutability triggers."""
    with engine.connect() as connection:
        return {
            str(row[0])
            for row in connection.execute(
                sa.text(
                    "SELECT name FROM sqlite_master WHERE type = 'trigger' "
                    "AND tbl_name IN ('market_views', 'market_view_sources')"
                )
            )
        }


def _insert_round(
    engine: sa.Engine,
    *,
    public_id: str,
    trigger: str = "periodic",
    status: str = "pending",
    resolved_at: str | None = None,
) -> None:
    """Insert one research round through raw migrated SQL."""
    statement = sa.text(
        "INSERT INTO ai_research_rounds "
        "(public_id, trigger, status, created_at, updated_at, resolved_at) "
        "VALUES (:public_id, :trigger, :status, :created_at, :updated_at, :resolved_at)"
    )
    with engine.begin() as connection:
        connection.execute(
            statement,
            {
                "public_id": public_id,
                "trigger": trigger,
                "status": status,
                "created_at": _TS,
                "updated_at": _TS,
                "resolved_at": resolved_at,
            },
        )


def _insert_view(engine: sa.Engine, **overrides: object) -> str:
    """Insert one market view while allowing CHECK-constraint probes."""
    public_id = str(uuid7())
    values: dict[str, object] = {
        "public_id": public_id,
        "research_round_public_id": str(uuid7()),
        "trigger": "periodic",
        "status": "completed",
        "as_of": _TS,
        "valid_until": "2026-07-21 10:00:00+00:00",
        "regime": "neutral",
        "bias": "longs_ok",
        "confidence": 0.75,
        "horizon_hours": 2,
        "key_risks": "[]",
        "next_events": "[]",
        "rationale": "Balanced conditions.",
    }
    values.update(overrides)
    statement = sa.text(
        "INSERT INTO market_views "
        "(public_id, research_round_public_id, trigger, status, as_of, valid_until, "
        "regime, bias, confidence, horizon_hours, key_risks, next_events, rationale) "
        "VALUES (:public_id, :research_round_public_id, :trigger, :status, :as_of, "
        ":valid_until, :regime, :bias, :confidence, :horizon_hours, :key_risks, "
        ":next_events, :rationale)"
    )
    with engine.begin() as connection:
        connection.execute(statement, values)
    return public_id


def test_0036_upgrade_round_trips_three_tables_and_required_clocks(tmp_path: Path) -> None:
    """Upgrade, downgrade, and re-upgrade preserve the exact additive schema."""
    db_url = f"sqlite:///{tmp_path / 'roundtrip.db'}"
    config = _config(db_url)
    command.upgrade(config, "0035")
    engine = sa.create_engine(db_url)
    assert _new_tables(engine) == set()

    command.upgrade(config, "0036")
    assert _new_tables(engine) == _TABLES
    assert _artifact_triggers(engine) == _TRIGGERS
    view_columns = {
        column["name"]: column for column in sa.inspect(engine).get_columns("market_views")
    }
    assert view_columns["as_of"]["nullable"] is False
    assert view_columns["submitted_at"]["nullable"] is False
    assert view_columns["valid_until"]["nullable"] is False

    view_id = _insert_view(engine)
    with engine.connect() as connection:
        submitted_at = connection.execute(
            sa.text("SELECT submitted_at FROM market_views WHERE public_id = :public_id"),
            {"public_id": view_id},
        ).scalar_one()
    assert submitted_at is not None

    command.downgrade(config, "0035")
    assert _new_tables(engine) == set()
    command.upgrade(config, "0036")
    assert _new_tables(engine) == _TABLES
    assert _artifact_triggers(engine) == _TRIGGERS
    engine.dispose()


def test_0036_enforces_single_pending_round_and_status_consistency(tmp_path: Path) -> None:
    """The migrated round table enforces latest-wins lifecycle invariants."""
    db_url = f"sqlite:///{tmp_path / 'rounds.db'}"
    command.upgrade(_config(db_url), "0036")
    engine = sa.create_engine(db_url)
    first_id = str(uuid7())
    _insert_round(engine, public_id=first_id)
    with pytest.raises(sa.exc.IntegrityError):
        _insert_round(engine, public_id=str(uuid7()), trigger="market_move")

    with engine.begin() as connection:
        connection.execute(
            sa.text(
                "UPDATE ai_research_rounds SET status = 'superseded', resolved_at = :resolved_at "
                "WHERE public_id = :public_id"
            ),
            {"resolved_at": _TS, "public_id": first_id},
        )
    _insert_round(engine, public_id=str(uuid7()), trigger="market_move")

    with pytest.raises(sa.exc.IntegrityError):
        _insert_round(engine, public_id=str(uuid7()), status="completed", resolved_at=None)
    with pytest.raises(sa.exc.IntegrityError):
        _insert_round(
            engine,
            public_id=str(uuid7()),
            status="unknown",
            resolved_at=_TS,
        )
    with pytest.raises(sa.exc.IntegrityError):
        _insert_round(
            engine,
            public_id=str(uuid7()),
            trigger="   ",
            status="expired",
            resolved_at=_TS,
        )
    engine.dispose()


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"regime": "bullish"}, id="regime"),
        pytest.param({"bias": "buy_everything"}, id="bias"),
        pytest.param({"confidence": -0.01}, id="confidence_low"),
        pytest.param({"confidence": 1.01}, id="confidence_high"),
        pytest.param({"trigger": " "}, id="blank_trigger"),
        pytest.param({"status": " "}, id="blank_status"),
    ],
)
def test_0036_market_view_checks_reject_invalid_artifacts(
    tmp_path: Path,
    overrides: dict[str, object],
) -> None:
    """Migrated market-view CHECKs reject invalid closed vocabularies and ranges."""
    db_url = f"sqlite:///{tmp_path / 'view-checks.db'}"
    command.upgrade(_config(db_url), "0036")
    engine = sa.create_engine(db_url)
    with pytest.raises(sa.exc.IntegrityError):
        _insert_view(engine, **overrides)
    engine.dispose()


def test_0036_source_identity_order_and_nonnegative_ordinal_are_enforced(
    tmp_path: Path,
) -> None:
    """Source IDs and per-view ordinals are unique, and ordinals cannot be negative."""
    db_url = f"sqlite:///{tmp_path / 'sources.db'}"
    command.upgrade(_config(db_url), "0036")
    engine = sa.create_engine(db_url)
    view_id = _insert_view(engine)
    source_id = str(uuid7())
    statement = sa.text(
        "INSERT INTO market_view_sources "
        "(public_id, market_view_public_id, ordinal, url, title, retrieved_at) "
        "VALUES (:public_id, :market_view_public_id, :ordinal, :url, :title, :retrieved_at)"
    )
    values: dict[str, object] = {
        "public_id": source_id,
        "market_view_public_id": view_id,
        "ordinal": 0,
        "url": "https://example.com/source",
        "title": "Source",
        "retrieved_at": _TS,
    }
    with engine.begin() as connection:
        connection.execute(statement, values)
    duplicate_ordinal = dict(values)
    duplicate_ordinal["public_id"] = str(uuid7())
    with pytest.raises(sa.exc.IntegrityError), engine.begin() as connection:
        connection.execute(statement, duplicate_ordinal)
    duplicate_id = dict(values)
    duplicate_id["ordinal"] = 1
    with pytest.raises(sa.exc.IntegrityError), engine.begin() as connection:
        connection.execute(statement, duplicate_id)
    negative_ordinal = dict(values)
    negative_ordinal["public_id"] = str(uuid7())
    negative_ordinal["market_view_public_id"] = str(uuid7())
    negative_ordinal["ordinal"] = -1
    with pytest.raises(sa.exc.IntegrityError), engine.begin() as connection:
        connection.execute(statement, negative_ordinal)
    engine.dispose()
