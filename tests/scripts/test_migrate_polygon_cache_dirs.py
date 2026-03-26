"""Tests for Polygon cache directory migration script."""

from datetime import UTC
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from scripts.migrate_polygon_cache_dirs import _build_legacy_to_archive
from scripts.migrate_polygon_cache_dirs import _migrate
from scripts.migrate_polygon_cache_dirs import main
from snapper.data.models import Base
from snapper.data.models import Symbol
from snapper.data.models import SymbolAlias
from snapper.data.repository import DatabaseRepository


def _seed_db(repo: DatabaseRepository) -> None:
    """Seed a test database with one symbol + polygon alias."""
    with repo.get_session() as session:
        ts = datetime(2024, 1, 1, tzinfo=UTC)
        sym = Symbol(
            public_id="pub-btc",
            native_symbol="BTC-USD",
            base="BTC",
            quote="USD",
            asset_type="crypto",
            session_id="test",
            sequence_id=1,
            timestamp=ts,
            created_at=ts,
        )
        session.add(sym)
        session.flush()
        alias = SymbolAlias(
            symbol_public_id="pub-btc",
            exchange="polygon",
            channel="rest",
            exchange_symbol="X:BTCUSD",
            session_id="test",
            sequence_id=2,
            timestamp=ts,
            created_at=ts,
        )
        session.add(alias)
        session.commit()


def test_build_legacy_to_archive_maps_symbols(tmp_path: Path) -> None:
    """Build legacy-to-archive mapping from database symbol aliases.

    Given: Database with BTC-USD symbol and X:BTCUSD polygon alias,
    When: _build_legacy_to_archive is called,
    Then: Returns mapping X_BTCUSD -> BTC-USD.
    """
    db_url = f"sqlite:///{tmp_path / 'test.db'}"
    repo = DatabaseRepository(db_url)
    Base.metadata.create_all(repo.engine)
    _seed_db(repo)
    repo.dispose()
    mapping = _build_legacy_to_archive(db_url)
    assert mapping == {"X_BTCUSD": "BTC-USD"}


def test_build_legacy_to_archive_skips_matching(tmp_path: Path) -> None:
    """Skip symbols where legacy directory name matches archive_symbol.

    Given: Database with AAPL symbol and AAPL polygon alias (no colon),
    When: _build_legacy_to_archive is called,
    Then: Returns empty mapping since names already match.
    """
    db_url = f"sqlite:///{tmp_path / 'test.db'}"
    repo = DatabaseRepository(db_url)
    Base.metadata.create_all(repo.engine)
    ts = datetime(2024, 1, 1, tzinfo=UTC)
    with repo.get_session() as session:
        sym = Symbol(
            public_id="pub-aapl",
            native_symbol="AAPL",
            base="AAPL",
            quote="",
            asset_type="equity",
            session_id="test",
            sequence_id=1,
            timestamp=ts,
            created_at=ts,
        )
        session.add(sym)
        session.flush()
        alias = SymbolAlias(
            symbol_public_id="pub-aapl",
            exchange="polygon",
            channel="rest",
            exchange_symbol="AAPL",
            session_id="test",
            sequence_id=2,
            timestamp=ts,
            created_at=ts,
        )
        session.add(alias)
        session.commit()
    repo.dispose()
    mapping = _build_legacy_to_archive(db_url)
    assert mapping == {}


def test_migrate_renames_directories(tmp_path: Path) -> None:
    """Rename legacy cache directories to archive_symbol format.

    Given: Cache directory with minute/X_BTCUSD/ layout,
    When: _migrate is called with X_BTCUSD -> BTC-USD mapping,
    Then: Directory is renamed to minute/BTC-USD/.
    """
    minute_dir = tmp_path / "minute" / "X_BTCUSD" / "2024"
    minute_dir.mkdir(parents=True)
    (minute_dir / "2024-01-01.csv").write_text("data\n")
    mapping = {"X_BTCUSD": "BTC-USD"}
    renamed = _migrate(tmp_path, mapping, dry_run=False)
    assert renamed == 1
    assert (tmp_path / "minute" / "BTC-USD" / "2024" / "2024-01-01.csv").exists()
    assert not (tmp_path / "minute" / "X_BTCUSD").exists()


def test_migrate_dry_run_does_not_rename(tmp_path: Path) -> None:
    """Dry run reports renames without actually moving directories.

    Given: Cache directory with minute/X_BTCUSD/ layout,
    When: _migrate is called with dry_run=True,
    Then: Reports 1 rename but original directory still exists.
    """
    minute_dir = tmp_path / "minute" / "X_BTCUSD" / "2024"
    minute_dir.mkdir(parents=True)
    (minute_dir / "2024-01-01.csv").write_text("data\n")
    mapping = {"X_BTCUSD": "BTC-USD"}
    renamed = _migrate(tmp_path, mapping, dry_run=True)
    assert renamed == 1
    assert (tmp_path / "minute" / "X_BTCUSD").exists()
    assert not (tmp_path / "minute" / "BTC-USD").exists()


def test_migrate_replaces_empty_target(tmp_path: Path) -> None:
    """Replace empty target with legacy directory contents.

    Given: Legacy dir has data, target dir exists with no files or subdirs,
    When: _migrate is called,
    Then: Legacy content replaces the empty target.
    """
    legacy = tmp_path / "minute" / "X_BTCUSD" / "2024"
    legacy.mkdir(parents=True)
    (legacy / "2024-01-01.csv").write_text("data\n")
    (tmp_path / "minute" / "BTC-USD").mkdir(parents=True)
    mapping = {"X_BTCUSD": "BTC-USD"}
    renamed = _migrate(tmp_path, mapping, dry_run=False)
    assert renamed == 1
    assert (tmp_path / "minute" / "BTC-USD" / "2024" / "2024-01-01.csv").exists()
    assert not (tmp_path / "minute" / "X_BTCUSD").exists()


def test_migrate_replaces_empty_target_dry_run(tmp_path: Path) -> None:
    """Dry run reports replacing empty target without making changes.

    Given: Legacy dir has data, target dir exists with no files or subdirs,
    When: _migrate is called with dry_run=True,
    Then: Reports replace but legacy dir still exists.
    """
    legacy = tmp_path / "minute" / "X_BTCUSD" / "2024"
    legacy.mkdir(parents=True)
    (legacy / "2024-01-01.csv").write_text("data\n")
    (tmp_path / "minute" / "BTC-USD").mkdir(parents=True)
    mapping = {"X_BTCUSD": "BTC-USD"}
    renamed = _migrate(tmp_path, mapping, dry_run=True)
    assert renamed == 1
    assert (tmp_path / "minute" / "X_BTCUSD").exists()


def test_migrate_merges_into_non_empty_target_dry_run(tmp_path: Path) -> None:
    """Dry run reports merge without making changes.

    Given: Both legacy and target dirs have data,
    When: _migrate is called with dry_run=True,
    Then: Reports merge but legacy dir still exists.
    """
    legacy = tmp_path / "minute" / "X_BTCUSD" / "2024"
    legacy.mkdir(parents=True)
    (legacy / "2024-01-01.csv").write_text("legacy\n")
    target = tmp_path / "minute" / "BTC-USD" / "2024"
    target.mkdir(parents=True)
    (target / "2024-01-02.csv").write_text("existing\n")
    mapping = {"X_BTCUSD": "BTC-USD"}
    renamed = _migrate(tmp_path, mapping, dry_run=True)
    assert renamed == 1
    assert (tmp_path / "minute" / "X_BTCUSD").exists()


def test_migrate_merges_into_non_empty_target(tmp_path: Path) -> None:
    """Merge legacy dir contents into non-empty target.

    Given: Both legacy and target dirs have data,
    When: _migrate is called,
    Then: Legacy content is merged into target.
    """
    legacy = tmp_path / "minute" / "X_BTCUSD" / "2024"
    legacy.mkdir(parents=True)
    (legacy / "2024-01-01.csv").write_text("legacy\n")
    target = tmp_path / "minute" / "BTC-USD" / "2024"
    target.mkdir(parents=True)
    (target / "2024-01-02.csv").write_text("existing\n")
    mapping = {"X_BTCUSD": "BTC-USD"}
    renamed = _migrate(tmp_path, mapping, dry_run=False)
    assert renamed == 1
    assert (tmp_path / "minute" / "BTC-USD" / "2024" / "2024-01-01.csv").exists()
    assert (tmp_path / "minute" / "BTC-USD" / "2024" / "2024-01-02.csv").exists()
    assert not (tmp_path / "minute" / "X_BTCUSD").exists()


def test_migrate_skips_grouped_directory(tmp_path: Path) -> None:
    """Skip the grouped/ subdirectory which has different structure.

    Given: Cache root with only grouped/ subdirectory,
    When: _migrate is called with matching mapping,
    Then: No rename is performed.
    """
    (tmp_path / "grouped" / "crypto" / "2024").mkdir(parents=True)
    mapping = {"crypto": "CRYPTO-NEW"}
    renamed = _migrate(tmp_path, mapping, dry_run=False)
    assert renamed == 0


def test_migrate_skips_unmapped_directories(tmp_path: Path) -> None:
    """Skip directories not present in the mapping.

    Given: Cache with directory not in mapping,
    When: _migrate is called,
    Then: No rename is performed.
    """
    (tmp_path / "minute" / "UNKNOWN" / "2024").mkdir(parents=True)
    mapping = {"X_BTCUSD": "BTC-USD"}
    renamed = _migrate(tmp_path, mapping, dry_run=False)
    assert renamed == 0


def test_migrate_skips_non_directory_entries(tmp_path: Path) -> None:
    """Skip files (not directories) at both timespan and symbol levels.

    Given: Cache with files mixed among directories,
    When: _migrate is called,
    Then: Files are skipped, only matching directories are renamed.
    """
    (tmp_path / "stray_file.txt").write_text("stray\n")
    timespan_dir = tmp_path / "minute"
    timespan_dir.mkdir()
    (timespan_dir / "stray_inside.txt").write_text("stray\n")
    (timespan_dir / "X_BTCUSD" / "2024").mkdir(parents=True)
    mapping = {"X_BTCUSD": "BTC-USD"}
    renamed = _migrate(tmp_path, mapping, dry_run=False)
    assert renamed == 1
    assert (tmp_path / "minute" / "BTC-USD").exists()


def test_main_cache_root_missing(tmp_path: Path) -> None:
    """Exit with error code when cache root does not exist.

    Given: Non-existent cache root path,
    When: main is called,
    Then: Returns exit code 1.
    """
    missing = tmp_path / "nonexistent"
    with patch(
        "scripts.migrate_polygon_cache_dirs.argparse.ArgumentParser.parse_args"
    ) as mock_args:
        mock_args.return_value = type("Args", (), {"cache_root": missing, "dry_run": False})()
        result = main()
    assert result == 1


def test_main_nothing_to_migrate(tmp_path: Path) -> None:
    """Exit cleanly when database has no symbols to migrate.

    Given: Empty database and existing cache root,
    When: main is called,
    Then: Returns exit code 0 without errors.
    """
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    db_url = f"sqlite:///{tmp_path / 'test.db'}"
    repo = DatabaseRepository(db_url)
    Base.metadata.create_all(repo.engine)
    repo.dispose()
    with (
        patch("scripts.migrate_polygon_cache_dirs.argparse.ArgumentParser.parse_args") as mock_args,
        patch("scripts.migrate_polygon_cache_dirs.BootstrapSettingsLoader") as mock_bootstrap,
    ):
        mock_args.return_value = type("Args", (), {"cache_root": cache_dir, "dry_run": False})()
        mock_bootstrap.return_value = type("S", (), {"db_url": db_url})()
        result = main()
    assert result == 0


def test_main_renames_successfully(tmp_path: Path) -> None:
    """Full main flow renames legacy directories.

    Given: Cache with X_BTCUSD directory and database with BTC-USD symbol,
    When: main is called,
    Then: Directory is renamed to BTC-USD.
    """
    cache_dir = tmp_path / "cache"
    (cache_dir / "minute" / "X_BTCUSD" / "2024").mkdir(parents=True)
    (cache_dir / "minute" / "X_BTCUSD" / "2024" / "2024-01-01.csv").write_text("data\n")
    db_url = f"sqlite:///{tmp_path / 'test.db'}"
    repo = DatabaseRepository(db_url)
    Base.metadata.create_all(repo.engine)
    _seed_db(repo)
    repo.dispose()
    with (
        patch("scripts.migrate_polygon_cache_dirs.argparse.ArgumentParser.parse_args") as mock_args,
        patch("scripts.migrate_polygon_cache_dirs.BootstrapSettingsLoader") as mock_bootstrap,
    ):
        mock_args.return_value = type("Args", (), {"cache_root": cache_dir, "dry_run": False})()
        mock_bootstrap.return_value = type("S", (), {"db_url": db_url})()
        result = main()
    assert result == 0
    assert (cache_dir / "minute" / "BTC-USD" / "2024" / "2024-01-01.csv").exists()
