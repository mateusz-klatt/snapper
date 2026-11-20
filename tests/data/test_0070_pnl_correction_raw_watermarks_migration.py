"""Migration tests for preserving legacy P&L correction checkpoints."""

from importlib import import_module
from pathlib import Path

from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from sqlalchemy import create_engine
from sqlalchemy import text


def test_0070_distinguishes_legacy_null_and_a_frozen_empty_cut(tmp_path: Path) -> None:
    """Preserve existing checkpoints when adding captured execution evidence.

    Given a persisted revision 0069 checkpoint.
    When migration 0070 adds evidence columns, stores an empty cut and downgrades.
    Then the upgraded checkpoint distinguishes legacy nulls from the captured values.
    The downgrade preserves the original checkpoint fields.
    """
    engine = create_engine(f"sqlite:///{tmp_path / 'raw_watermarks.db'}")
    legacy = import_module("snapper.data.migrations.versions.0069_pnl_correction_frontiers")
    current = import_module("snapper.data.migrations.versions.0070_pnl_correction_raw_watermarks")
    read = text("SELECT * FROM portfolio_pnl_correction_frontiers")
    try:
        with engine.begin() as connection, Operations.context(
            MigrationContext.configure(connection)
        ):
            legacy.upgrade()
            connection.execute(text("""
                    INSERT INTO portfolio_pnl_correction_frontiers (
                        wallet_public_id, mode, valuation_ccy, epoch_public_id, calc_version,
                        generation, revision, window_start, next_minute, tip, as_of,
                        baseline_watermarks_json, pending_watermarks_json, prior_peak
                    )
                    VALUES (
                        '00000000-0000-7000-8000-000000000124', 'live', 'USD',
                        '00000000-0000-7000-8000-000000000125', '5C.4',
                        1, 2, '2026-11-20 10:01:00', '2026-11-20 10:02:00',
                        '2026-11-20 10:03:00', '2026-11-20 10:05:00', '{}', '{}', 25.0
                    )
                    """))
            before = dict(connection.execute(read).mappings().one())
            current.upgrade()
            assert dict(connection.execute(read).mappings().one()) == {
                **before,
                "raw_execution_watermarks_json": None,
                "execution_manifest_digest": None,
                "has_fill_gap": None,
                "expected_venues_json": None,
                "venue_event_watermarks_json": None,
                "fill_witness_digest": None,
            }
            connection.execute(
                text(
                    "UPDATE portfolio_pnl_correction_frontiers "
                    "SET raw_execution_watermarks_json = :raw, execution_manifest_digest = :manifest, "
                    "has_fill_gap = :gap, expected_venues_json = :venues, "
                    "venue_event_watermarks_json = :witness_cut, fill_witness_digest = :witness_digest"
                ),
                {
                    "raw": "{}",
                    "manifest": "a" * 64,
                    "gap": False,
                    "venues": '["kraken"]',
                    "witness_cut": "{}",
                    "witness_digest": "b" * 64,
                },
            )
            assert dict(connection.execute(read).mappings().one()) == {
                **before,
                "raw_execution_watermarks_json": "{}",
                "execution_manifest_digest": "a" * 64,
                "has_fill_gap": 0,
                "expected_venues_json": '["kraken"]',
                "venue_event_watermarks_json": "{}",
                "fill_witness_digest": "b" * 64,
            }
            current.downgrade()
            assert dict(connection.execute(read).mappings().one()) == before
    finally:
        engine.dispose()
