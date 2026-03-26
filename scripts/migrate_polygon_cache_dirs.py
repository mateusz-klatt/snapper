"""One-time migration: rename Polygon cache directories to archive_symbol format.

Renames directories like ``X_BTCUSD`` to ``BTC-USD`` using the archive_symbol
mapping from the database.  When the target already exists, merges contents
(source files win on conflict).

Usage::

    python -m scripts.migrate_polygon_cache_dirs [--dry-run] [--cache-root PATH]

Requires a seeded database (``make migrate-dev`` + ``make run-static``).
"""

import argparse
import shutil
from pathlib import Path

from sqlalchemy import select

from snapper.config.bootstrap import BootstrapSettingsLoader
from snapper.data.models import SymbolAlias
from snapper.data.repository import DatabaseRepository


def _build_legacy_to_archive(db_url: str) -> dict[str, str]:
    """Build mapping from legacy directory names to archive_symbols.

    Queries SymbolAlias for polygon rest aliases, resolves archive_symbol
    for each, and builds ``{legacy_dir_name: archive_symbol}`` mapping.
    """
    repo = DatabaseRepository(db_url)
    archive_symbols = repo.get_archive_symbols()

    mapping: dict[str, str] = {}
    with repo.get_session() as session:
        stmt = select(
            SymbolAlias.exchange_symbol,
            SymbolAlias.symbol_public_id,
        ).where(
            SymbolAlias.exchange == "polygon",
            SymbolAlias.channel == "rest",
            SymbolAlias.exchange_symbol.is_not(None),
        )
        for exchange_symbol, symbol_public_id in session.execute(stmt).all():
            legacy_name = exchange_symbol.replace(":", "_")
            arch_sym = archive_symbols.get(symbol_public_id)
            if arch_sym and legacy_name != arch_sym:
                mapping[legacy_name] = arch_sym
    repo.dispose()
    return mapping


def _merge_dirs(source: Path, target: Path) -> None:
    """Recursively merge source into target (source files win conflicts).

    Args:
        source: Directory to merge from (removed after merge).
        target: Directory to merge into (created if needed).
    """
    target.mkdir(parents=True, exist_ok=True)
    for item in source.iterdir():
        dest = target / item.name
        if item.is_dir():
            _merge_dirs(item, dest)
        else:
            shutil.move(str(item), str(dest))
    source.rmdir()


def _migrate_symbol_dir(symbol_dir: Path, target: Path, dry_run: bool) -> None:
    """Migrate a single symbol directory (rename, merge, or replace empty).

    Args:
        symbol_dir: Legacy source directory.
        target: New archive_symbol target directory.
        dry_run: If True, report without making changes.
    """
    if not target.exists():
        if dry_run:
            print(f"  WOULD RENAME {symbol_dir} -> {target}")
        else:
            symbol_dir.rename(target)
            print(f"  RENAMED {symbol_dir} -> {target}")
    elif any(target.rglob("*")):
        if dry_run:
            print(f"  WOULD MERGE {symbol_dir} -> {target}")
        else:
            _merge_dirs(symbol_dir, target)
            print(f"  MERGED {symbol_dir} -> {target}")
    elif dry_run:
        print(f"  WOULD REPLACE empty {target} with {symbol_dir}")
    else:
        shutil.rmtree(target)
        symbol_dir.rename(target)
        print(f"  REPLACED empty {target} with {symbol_dir}")


def _migrate(cache_root: Path, mapping: dict[str, str], dry_run: bool) -> int:
    """Rename or merge legacy directories to archive_symbol format.

    Returns:
        Number of directories migrated.
    """
    renamed = 0
    for timespan_dir in sorted(cache_root.iterdir()):
        if not timespan_dir.is_dir() or timespan_dir.name == "grouped":
            continue
        for symbol_dir in sorted(timespan_dir.iterdir()):
            if not symbol_dir.is_dir():
                continue
            new_name = mapping.get(symbol_dir.name)
            if new_name is None:
                continue
            target = symbol_dir.parent / new_name
            _migrate_symbol_dir(symbol_dir, target, dry_run)
            renamed += 1
    return renamed


def main() -> int:
    """Entry point for the migration script.

    Returns:
        Exit code (0 for success, 1 for error).
    """
    parser = argparse.ArgumentParser(
        description="Rename Polygon cache directories from legacy X_BTCUSD to BTC-USD format."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be renamed without making changes.",
    )
    parser.add_argument(
        "--cache-root",
        type=Path,
        default=Path("data/polygon/cache"),
        help="Root directory of the Polygon CSV cache.",
    )
    args = parser.parse_args()

    if not args.cache_root.exists():
        print(f"Cache root does not exist: {args.cache_root}")
        return 1

    settings = BootstrapSettingsLoader()
    mapping = _build_legacy_to_archive(settings.db_url)
    print(f"Found {len(mapping)} symbols to migrate")
    if not mapping:
        print("Nothing to migrate.")
        return 0

    renamed = _migrate(args.cache_root, mapping, args.dry_run)
    label = "would rename" if args.dry_run else "renamed"
    print(f"Done: {label} {renamed} directories")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
