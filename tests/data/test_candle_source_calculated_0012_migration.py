"""Tests for the 0012 candle-source taxonomy migration (forward-only).

Verifies that ``alembic upgrade 0012`` widens the ``ck_candle_source`` CHECK to
the three-value vocabulary (``native`` | ``calculated`` | ``synthesized``)
WITHOUT retagging existing rows, that the widened CHECK accepts ``calculated``
and still rejects unknown values, and that the downgrade reverts any
``calculated`` rows back to ``native`` and narrows the CHECK so ``calculated`` is
rejected again.
"""

from collections.abc import Iterator
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_TS = "2026-06-19 12:00:00.000000"
_ACTIVE = "9999-12-31 23:59:59.000000"


def _make_alembic_config(db_url: str) -> Config:
    """Build an Alembic config pointed at the supplied database URL."""
    cfg = Config(str(ALEMBIC_INI))
    cfg.set_main_option("sqlalchemy.url", db_url)
    return cfg


@pytest.fixture
def db_at_0011(tmp_path: Path) -> Iterator[tuple[sa.Engine, Config]]:
    """Provide a SQLite database upgraded to 0011 (pre-taxonomy)."""
    db_path = tmp_path / "candle_source.db"
    db_url = f"sqlite:///{db_path}"
    cfg = _make_alembic_config(db_url)
    command.upgrade(cfg, "0011")
    engine = sa.create_engine(db_url, future=True)
    try:
        yield engine, cfg
    finally:
        engine.dispose()


def _insert_candle(
    engine: sa.Engine,
    *,
    public_id: str,
    source: str,
    open_at: str = "2026-06-19 00:00:00.000000",
) -> None:
    """Insert one active candle row with an explicit source tag."""
    stmt = (
        "INSERT INTO candles "
        "(public_id, instrument_public_id, open_at, timeframe, open, high, low, "
        " close, volume, source, complete, session_id, sequence_id, timestamp, known_to) "
        "VALUES (:pid, 'inst-1', :oa, '1m', 1.0, 2.0, 0.5, 1.5, 10.0, :src, 1, "
        " 'sess-1', 1, :ts, :active)"
    )
    with engine.begin() as conn:
        conn.execute(
            sa.text(stmt),
            {"pid": public_id, "oa": open_at, "src": source, "ts": _TS, "active": _ACTIVE},
        )


def _source_of(engine: sa.Engine, public_id: str) -> str:
    """Return the source tag of the candle with the given public_id."""
    with engine.begin() as conn:
        return str(
            conn.execute(
                sa.text("SELECT source FROM candles WHERE public_id = :pid"), {"pid": public_id}
            ).scalar_one()
        )


def test_0012_does_not_retag_existing_rows(
    db_at_0011: tuple[sa.Engine, Config],
) -> None:
    """The forward-only upgrade leaves pre-existing rows untouched.

    Given: native and synthesized rows present before the migration,
    When: the 0012 migration runs,
    Then: their source tags are unchanged (no historical retag).
    """
    engine, cfg = db_at_0011
    _insert_candle(engine, public_id="c-native", source="native")
    _insert_candle(
        engine, public_id="c-synth", source="synthesized", open_at="2026-06-19 00:05:00.000000"
    )
    command.upgrade(cfg, "0012")
    assert _source_of(engine, "c-native") == "native"
    assert _source_of(engine, "c-synth") == "synthesized"


def test_0012_check_accepts_calculated(
    db_at_0011: tuple[sa.Engine, Config],
) -> None:
    """The widened CHECK accepts a calculated row.

    Given: a DB upgraded to 0012,
    When: a candle is inserted with source='calculated',
    Then: it succeeds.
    """
    engine, cfg = db_at_0011
    command.upgrade(cfg, "0012")
    _insert_candle(engine, public_id="c-calc", source="calculated")
    assert _source_of(engine, "c-calc") == "calculated"


def test_0012_check_rejects_unknown(
    db_at_0011: tuple[sa.Engine, Config],
) -> None:
    """The widened CHECK still rejects an out-of-vocabulary value.

    Given: a DB upgraded to 0012,
    When: a candle is inserted with source='bogus',
    Then: the CHECK raises IntegrityError.
    """
    engine, cfg = db_at_0011
    command.upgrade(cfg, "0012")
    with pytest.raises(sa.exc.IntegrityError):
        _insert_candle(engine, public_id="c-bad", source="bogus")


def test_0012_downgrade_reverts_calculated_and_narrows_check(
    db_at_0011: tuple[sa.Engine, Config],
) -> None:
    """Downgrade reverts calculated to native and rejects calculated again.

    Given: a DB at 0012 with a calculated row,
    When: it is downgraded to 0011,
    Then: the calculated row is native again and the CHECK rejects calculated.
    """
    engine, cfg = db_at_0011
    command.upgrade(cfg, "0012")
    _insert_candle(engine, public_id="c-calc", source="calculated")
    command.downgrade(cfg, "0011")
    assert _source_of(engine, "c-calc") == "native"
    with pytest.raises(sa.exc.IntegrityError):
        _insert_candle(engine, public_id="c-calc2", source="calculated")
