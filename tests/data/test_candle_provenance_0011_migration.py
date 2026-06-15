"""Tests for the 0011 candle-provenance migration.

Verifies that ``alembic upgrade head`` on a fresh SQLite database adds the
``source`` and ``complete`` columns to ``candles`` with their server defaults
(``native`` / true), that the ``ck_candle_source`` CHECK rejects an
out-of-vocabulary source while accepting ``native``/``synthesized``, that an
insert omitting the columns picks up the defaults, and that the downgrade drops
the CHECK + both columns.
"""

from collections.abc import Iterator
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_TS = "2026-06-16 12:00:00.000000"
_ACTIVE = "9999-12-31 23:59:59.000000"


def _make_alembic_config(db_url: str) -> Config:
    """Build an Alembic config pointed at the supplied database URL."""
    cfg = Config(str(ALEMBIC_INI))
    cfg.set_main_option("sqlalchemy.url", db_url)
    return cfg


@pytest.fixture
def migrated_db(tmp_path: Path) -> Iterator[tuple[sa.Engine, Config]]:
    """Provide a SQLite database upgraded through the latest migration."""
    db_path = tmp_path / "candle_provenance.db"
    db_url = f"sqlite:///{db_path}"
    cfg = _make_alembic_config(db_url)
    command.upgrade(cfg, "head")
    engine = sa.create_engine(db_url, future=True)
    try:
        yield engine, cfg
    finally:
        engine.dispose()


def _insert_candle(
    engine: sa.Engine,
    *,
    open_at: str,
    source: str | None = None,
    complete: int | None = None,
    public_id: str = "cd-1",
) -> None:
    """Insert one active candle row, optionally overriding source/complete."""
    cols = [
        "public_id",
        "instrument_public_id",
        "open_at",
        "timeframe",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "session_id",
        "sequence_id",
        "timestamp",
        "known_to",
    ]
    vals: dict[str, object] = {
        "pid": public_id,
        "oa": open_at,
        "ts": _TS,
        "active": _ACTIVE,
    }
    placeholders = [
        ":pid",
        "'inst-1'",
        ":oa",
        "'1m'",
        "1.0",
        "2.0",
        "0.5",
        "1.5",
        "10.0",
        "'sess-1'",
        "1",
        ":ts",
        ":active",
    ]
    if source is not None:
        cols.append("source")
        placeholders.append(":source")
        vals["source"] = source
    if complete is not None:
        cols.append("complete")
        placeholders.append(":complete")
        vals["complete"] = complete
    stmt = f"INSERT INTO candles ({', '.join(cols)}) VALUES ({', '.join(placeholders)})"
    with engine.begin() as conn:
        conn.execute(sa.text(stmt), vals)


def test_0011_adds_source_and_complete_columns(
    migrated_db: tuple[sa.Engine, Config],
) -> None:
    """Migration adds source + complete to candles.

    Given: a fresh DB upgraded to head,
    When: the candles table schema is inspected,
    Then: both source and complete columns exist.
    """
    engine, _ = migrated_db
    cols = {c["name"] for c in sa.inspect(engine).get_columns("candles")}
    assert "source" in cols
    assert "complete" in cols


def test_0011_defaults_native_and_complete(
    migrated_db: tuple[sa.Engine, Config],
) -> None:
    """An insert omitting source/complete picks up the server defaults.

    Given: an upgraded DB,
    When: a candle is inserted without source or complete,
    Then: source defaults to 'native' and complete defaults to truthy.
    """
    engine, _ = migrated_db
    _insert_candle(engine, open_at="2026-06-16 00:00:00.000000")
    with engine.begin() as conn:
        row = conn.execute(
            sa.text("SELECT source, complete FROM candles WHERE public_id = 'cd-1'")
        ).one()
    assert row.source == "native"
    assert bool(row.complete) is True


def test_0011_source_check_accepts_vocabulary(
    migrated_db: tuple[sa.Engine, Config],
) -> None:
    """The source CHECK accepts native and synthesized.

    Given: an upgraded DB,
    When: candles are inserted with source 'native' and 'synthesized',
    Then: both succeed.
    """
    engine, _ = migrated_db
    _insert_candle(engine, open_at="2026-06-16 00:00:00.000000", source="native", public_id="cd-n")
    _insert_candle(
        engine, open_at="2026-06-16 00:01:00.000000", source="synthesized", public_id="cd-s"
    )
    with engine.begin() as conn:
        count = conn.execute(sa.text("SELECT COUNT(*) FROM candles")).scalar_one()
    assert count == 2


def test_0011_source_check_rejects_unknown(
    migrated_db: tuple[sa.Engine, Config],
) -> None:
    """The source CHECK rejects an out-of-vocabulary value.

    Given: an upgraded DB,
    When: a candle is inserted with source 'bogus',
    Then: the CHECK constraint raises IntegrityError.
    """
    engine, _ = migrated_db
    with pytest.raises(sa.exc.IntegrityError):
        _insert_candle(engine, open_at="2026-06-16 00:00:00.000000", source="bogus")


def test_0011_downgrade_drops_columns(
    migrated_db: tuple[sa.Engine, Config],
) -> None:
    """Downgrade removes the provenance columns + CHECK.

    Given: a DB at head,
    When: it is downgraded one revision,
    Then: candles no longer has source or complete.
    """
    engine, cfg = migrated_db
    command.downgrade(cfg, "0010")
    cols = {c["name"] for c in sa.inspect(engine).get_columns("candles")}
    assert "source" not in cols
    assert "complete" not in cols
