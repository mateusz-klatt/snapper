"""Build and atomically publish the fixed SQLite test fixture."""

import errno
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator
from contextlib import closing
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO
from typing import cast

if sys.platform == "win32":
    import msvcrt
else:
    import fcntl


PROJECT_ROOT = Path(__file__).resolve().parent.parent
TARGET_PATH = PROJECT_ROOT / "data" / "dev.db"
LOCK_PATH = TARGET_PATH.with_name(f"{TARGET_PATH.name}.lock")
LOCK_RETRY_SECONDS = 0.1
SIDECAR_SUFFIXES = ("-wal", "-shm", "-journal")


def _acquire_lock(lock_file: BinaryIO) -> None:
    """Acquire an exclusive cross-platform advisory lock.

    Args:
        lock_file: Open binary lock file.
    """
    if sys.platform == "win32":
        while True:
            lock_file.seek(0)
            try:
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
                return
            except OSError as error:
                if error.errno not in {errno.EACCES, errno.EAGAIN, errno.EDEADLK}:
                    raise
                time.sleep(LOCK_RETRY_SECONDS)
    else:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)


def _release_lock(lock_file: BinaryIO) -> None:
    """Release a cross-platform advisory lock.

    Args:
        lock_file: Open binary lock file.
    """
    lock_file.seek(0)
    if sys.platform == "win32":
        msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
    else:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


@contextmanager
def _fixture_lock(lock_path: Path) -> Iterator[None]:
    """Hold the fixture lock for the complete build and publish.

    Args:
        lock_path: Path to the stable lock file.

    Yields:
        Control while the exclusive lock is held.
    """
    with lock_path.open("a+b") as lock_file:
        if os.fstat(lock_file.fileno()).st_size == 0:
            lock_file.write(b"\0")
            lock_file.flush()
        _acquire_lock(lock_file)
        try:
            yield
        finally:
            _release_lock(lock_file)


def _sidecar_paths(database_path: Path) -> tuple[Path, ...]:
    """Return the explicit SQLite sidecars for a database path.

    Args:
        database_path: Main SQLite database path.

    Returns:
        Exact WAL, shared-memory, and journal paths.
    """
    return tuple(Path(f"{database_path}{suffix}") for suffix in SIDECAR_SUFFIXES)


def _reserve_staging_path(target_path: Path) -> Path:
    """Reserve a unique staging database beside the target.

    Args:
        target_path: Final fixture path.

    Returns:
        Unique staging path on the target filesystem.
    """
    descriptor, raw_path = tempfile.mkstemp(
        prefix=f".{target_path.name}.staging-",
        suffix=".db",
        dir=target_path.parent,
    )
    os.close(descriptor)
    return Path(raw_path)


def _database_url(database_path: Path) -> str:
    """Build an absolute aiosqlite URL for a staging database.

    Args:
        database_path: SQLite database path.

    Returns:
        SQLAlchemy URL using the aiosqlite driver.
    """
    return f"sqlite+aiosqlite:///{database_path.resolve().as_posix()}"


def _run_snapper(arguments: tuple[str, ...], database_path: Path) -> None:
    """Run one Snapper command against the staging database.

    Args:
        arguments: Command arguments after the Snapper module name.
        database_path: Unique staging database path.
    """
    environment = os.environ.copy()
    environment["DB_URL"] = _database_url(database_path)
    subprocess.run(
        [sys.executable, "-m", "snapper", *arguments],
        cwd=PROJECT_ROOT,
        env=environment,
        check=True,
    )


def _require_checkpoint(connection: sqlite3.Connection, database_path: Path) -> None:
    """Require a non-busy truncating WAL checkpoint.

    Args:
        connection: Open SQLite connection.
        database_path: Database path used in a failure message.

    Raises:
        RuntimeError: If SQLite reports a busy checkpoint.
    """
    checkpoint = cast(
        tuple[int, int, int] | None,
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone(),
    )
    if checkpoint is None or checkpoint[0] != 0:
        raise RuntimeError(f"WAL checkpoint failed for {database_path}: {checkpoint}")


def _checkpoint_database(database_path: Path) -> None:
    """Checkpoint and close a database before touching its sidecars.

    Args:
        database_path: SQLite database to checkpoint.
    """
    with closing(sqlite3.connect(database_path)) as connection:
        _require_checkpoint(connection, database_path)


def _validate_staging(database_path: Path) -> None:
    """Checkpoint and integrity-check the completed staging database.

    Args:
        database_path: Completed staging database.

    Raises:
        RuntimeError: If SQLite quick_check does not return exactly ``ok``.
    """
    with closing(sqlite3.connect(database_path)) as connection:
        _require_checkpoint(connection, database_path)
        quick_check = cast(
            list[tuple[str]],
            connection.execute("PRAGMA quick_check").fetchall(),
        )
    if quick_check != [("ok",)]:
        raise RuntimeError(f"SQLite quick_check failed for {database_path}: {quick_check}")


def _delete_sidecars(database_path: Path) -> None:
    """Delete only the explicit sidecars for one checkpointed database.

    Args:
        database_path: Checkpointed SQLite database path.
    """
    for sidecar_path in _sidecar_paths(database_path):
        sidecar_path.unlink(missing_ok=True)


def _require_publishable_target(target_path: Path) -> None:
    """Checkpoint an existing target or reject orphan target sidecars.

    Args:
        target_path: Fixed fixture publication path.

    Raises:
        RuntimeError: If sidecars exist without a database to checkpoint.
    """
    if target_path.exists():
        _checkpoint_database(target_path)
        return
    orphan_sidecars = [path for path in _sidecar_paths(target_path) if path.exists()]
    if orphan_sidecars:
        raise RuntimeError(f"Target sidecars exist without {target_path}: {orphan_sidecars}")


def _publish_staging(staging_path: Path, target_path: Path) -> None:
    """Validate and atomically replace the fixed fixture.

    Args:
        staging_path: Unique completed staging database.
        target_path: Fixed fixture publication path.
    """
    _validate_staging(staging_path)
    _require_publishable_target(target_path)
    _delete_sidecars(staging_path)
    _delete_sidecars(target_path)
    os.replace(staging_path, target_path)


def _discard_staging(staging_path: Path) -> None:
    """Safely discard one failed staging database when possible.

    Args:
        staging_path: Unique staging database path.
    """
    if not staging_path.exists():
        return
    try:
        _checkpoint_database(staging_path)
    except (RuntimeError, sqlite3.Error) as error:
        print(
            f"Leaving unsafe staging database {staging_path}: {error}",
            file=sys.stderr,
        )
        return
    _delete_sidecars(staging_path)
    staging_path.unlink()


def rebuild_fixture() -> None:
    """Build a fresh fixture and atomically publish it under the fixed path."""
    TARGET_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _fixture_lock(LOCK_PATH):
        staging_path = _reserve_staging_path(TARGET_PATH)
        published = False
        try:
            _run_snapper(("db-init",), staging_path)
            _run_snapper(("db-seed", "--profile", "dev"), staging_path)
            _publish_staging(staging_path, TARGET_PATH)
            published = True
        finally:
            if not published:
                _discard_staging(staging_path)
    print(f"Published fresh SQLite test fixture at {TARGET_PATH}")


def main() -> int:
    """Rebuild the SQLite test fixture.

    Returns:
        Process exit code.
    """
    rebuild_fixture()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
