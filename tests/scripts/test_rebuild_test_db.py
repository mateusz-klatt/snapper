"""Mutation-sensitive tests for the isolated SQLite fixture builder."""

import builtins
import errno
import os
import sqlite3
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from types import SimpleNamespace
from unittest.mock import ANY
from unittest.mock import MagicMock
from unittest.mock import call

import pytest

from scripts import rebuild_test_db as builder


class _PlatformImport:
    """Resolve selected imports to isolated platform-module substitutes."""

    def __init__(
        self,
        base_import: Callable[
            [str, dict[str, object] | None, dict[str, object] | None, tuple[str, ...], int],
            object,
        ],
        modules: dict[str, ModuleType],
    ) -> None:
        """Store the real importer and the isolated module substitutions.

        Args:
            base_import: Interpreter importer used for all unmodified modules.
            modules: Module substitutes keyed by their import names.
        """
        self._base_import = base_import
        self._modules = modules

    def __call__(
        self,
        name: str,
        globals_namespace: dict[str, object] | None = None,
        locals_namespace: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        """Import a substitute module or delegate to the interpreter.

        Args:
            name: Requested import name.
            globals_namespace: Importing global namespace.
            locals_namespace: Importing local namespace.
            fromlist: Requested attributes for a from-import.
            level: Relative import level.

        Returns:
            The selected substitute or normal imported module.
        """
        if name in self._modules:
            return self._modules[name]
        return self._base_import(
            name,
            globals_namespace,
            locals_namespace,
            fromlist,
            level,
        )


def _windows_api(locking: MagicMock) -> ModuleType:
    """Build the minimal Windows locking API used by the builder.

    Args:
        locking: Mock implementation of ``msvcrt.locking``.

    Returns:
        Module-shaped object with both lock mode constants.
    """
    windows_api = ModuleType("msvcrt")
    windows_api.LK_NBLCK = 11
    windows_api.LK_UNLCK = 12
    windows_api.locking = locking
    return windows_api


def _posix_api(flock: MagicMock) -> ModuleType:
    """Build the minimal POSIX locking API used by the builder.

    Args:
        flock: Mock implementation of ``fcntl.flock``.

    Returns:
        Module-shaped object with both flock mode constants.
    """
    posix_api = ModuleType("fcntl")
    posix_api.LOCK_EX = 21
    posix_api.LOCK_UN = 22
    posix_api.flock = flock
    return posix_api


def _execute_builder_for_platform(
    platform: str,
    locking_name: str,
    locking_module: ModuleType,
) -> dict[str, object]:
    """Execute the builder with isolated platform and locking imports.

    Args:
        platform: Platform name exposed by the isolated ``sys`` module.
        locking_name: Locking module name selected for that platform.
        locking_module: Fake platform locking module.

    Returns:
        Executed builder namespace.
    """
    fake_sys = ModuleType("sys")
    fake_sys.platform = platform
    platform_import = _PlatformImport(
        builtins.__import__,
        {"sys": fake_sys, locking_name: locking_module},
    )
    builtins_namespace = vars(builtins).copy()
    builtins_namespace["__import__"] = platform_import
    source_path = Path(builder.__file__)
    namespace: dict[str, object] = {
        "__builtins__": builtins_namespace,
        "__file__": str(source_path),
        "__name__": f"isolated_{platform}_builder",
    }
    exec(compile(source_path.read_text(encoding="utf-8"), source_path, "exec"), namespace)
    return namespace


def test_import_selects_each_platform_locking_module() -> None:
    """Verify module import selects the locking API for the active platform.

    Given: Isolated fake Windows and POSIX locking modules.
    When: The builder source executes once for each fake platform.
    Then: Each namespace binds only its corresponding locking module.
    """
    fake_msvcrt = ModuleType("msvcrt")
    fake_fcntl = ModuleType("fcntl")

    windows_namespace = _execute_builder_for_platform("win32", "msvcrt", fake_msvcrt)
    posix_namespace = _execute_builder_for_platform("linux", "fcntl", fake_fcntl)

    assert windows_namespace["msvcrt"] is fake_msvcrt
    assert "fcntl" not in windows_namespace
    assert posix_namespace["fcntl"] is fake_fcntl
    assert "msvcrt" not in posix_namespace


def test_acquire_lock_uses_blocking_flock_on_linux(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the Linux lock is an exclusive blocking advisory lock.

    Given: A binary lock file and a mocked ``fcntl.flock``.
    When: The builder acquires the lock on Linux.
    Then: It passes the exact descriptor and ``LOCK_EX`` mode once.
    """
    lock_file = MagicMock()
    lock_file.fileno.return_value = 37
    flock = MagicMock()
    posix_api = _posix_api(flock)
    monkeypatch.setattr(builder, "sys", SimpleNamespace(platform="linux"))
    monkeypatch.setattr(builder, "fcntl", posix_api, raising=False)

    builder._acquire_lock(lock_file)

    flock.assert_called_once_with(37, posix_api.LOCK_EX)
    lock_file.seek.assert_not_called()


@pytest.mark.parametrize(
    "retry_errno",
    [
        pytest.param(errno.EACCES, id="access_denied"),
        pytest.param(errno.EAGAIN, id="try_again"),
        pytest.param(errno.EDEADLK, id="deadlock"),
    ],
)
def test_acquire_lock_retries_only_retryable_windows_errors(
    retry_errno: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify every retryable Windows error sleeps and retries from byte zero.

    Given: A Windows lock call that fails once with a retryable errno.
    When: A second lock attempt succeeds.
    Then: Both attempts use byte zero and exactly one configured retry delay.
    """
    lock_file = MagicMock()
    lock_file.fileno.return_value = 41
    locking = MagicMock(side_effect=[OSError(retry_errno, "busy"), None])
    sleep = MagicMock()
    windows_api = _windows_api(locking)
    monkeypatch.setattr(builder, "sys", SimpleNamespace(platform="win32"))
    monkeypatch.setattr(builder, "msvcrt", windows_api, raising=False)
    monkeypatch.setattr(builder.time, "sleep", sleep)

    builder._acquire_lock(lock_file)

    assert locking.call_args_list == [
        call(41, windows_api.LK_NBLCK, 1),
        call(41, windows_api.LK_NBLCK, 1),
    ]
    assert lock_file.seek.call_args_list == [call(0), call(0)]
    sleep.assert_called_once_with(builder.LOCK_RETRY_SECONDS)


def test_acquire_lock_propagates_nonretryable_windows_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify an unrelated Windows lock error is never retried.

    Given: A Windows lock call that raises a nonretryable I/O error.
    When: The builder attempts to acquire the lock.
    Then: The same error escapes after one attempt without sleeping.
    """
    lock_file = MagicMock()
    lock_file.fileno.return_value = 43
    failure = OSError(errno.EIO, "device failure")
    locking = MagicMock(side_effect=failure)
    sleep = MagicMock()
    windows_api = _windows_api(locking)
    monkeypatch.setattr(builder, "sys", SimpleNamespace(platform="win32"))
    monkeypatch.setattr(builder, "msvcrt", windows_api, raising=False)
    monkeypatch.setattr(builder.time, "sleep", sleep)

    with pytest.raises(OSError) as raised:
        builder._acquire_lock(lock_file)

    assert raised.value is failure
    locking.assert_called_once_with(43, windows_api.LK_NBLCK, 1)
    lock_file.seek.assert_called_once_with(0)
    sleep.assert_not_called()


def test_release_lock_uses_unlock_flock_on_linux(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify Linux release unlocks the exact locked descriptor.

    Given: A binary lock file and a mocked ``fcntl.flock``.
    When: The builder releases the lock on Linux.
    Then: It rewinds and passes the descriptor with ``LOCK_UN`` once.
    """
    lock_file = MagicMock()
    lock_file.fileno.return_value = 47
    flock = MagicMock()
    posix_api = _posix_api(flock)
    monkeypatch.setattr(builder, "sys", SimpleNamespace(platform="linux"))
    monkeypatch.setattr(builder, "fcntl", posix_api, raising=False)

    builder._release_lock(lock_file)

    lock_file.seek.assert_called_once_with(0)
    flock.assert_called_once_with(47, posix_api.LOCK_UN)


def test_release_lock_uses_msvcrt_unlock_on_windows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify Windows release unlocks one byte from the start of the file.

    Given: A binary lock file and a mocked Windows locking API.
    When: The builder releases the lock on Windows.
    Then: It rewinds and unlocks exactly one byte on the same descriptor.
    """
    lock_file = MagicMock()
    lock_file.fileno.return_value = 53
    locking = MagicMock()
    windows_api = _windows_api(locking)
    monkeypatch.setattr(builder, "sys", SimpleNamespace(platform="win32"))
    monkeypatch.setattr(builder, "msvcrt", windows_api, raising=False)

    builder._release_lock(lock_file)

    lock_file.seek.assert_called_once_with(0)
    locking.assert_called_once_with(53, windows_api.LK_UNLCK, 1)


def test_fixture_lock_initializes_empty_file_and_releases_after_body(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify an empty stable lock file is initialized and held around the body.

    Given: A missing lock file and mocked acquire and release operations.
    When: The fixture lock context is entered and exited normally.
    Then: One byte exists before acquisition and release happens only on exit.
    """
    lock_path = tmp_path / "fixture.lock"
    acquire = MagicMock()
    release = MagicMock()
    monkeypatch.setattr(builder, "_acquire_lock", acquire)
    monkeypatch.setattr(builder, "_release_lock", release)

    with builder._fixture_lock(lock_path):
        assert lock_path.read_bytes() == b"\0"
        acquire.assert_called_once()
        release.assert_not_called()

    acquired_file = acquire.call_args.args[0]
    release.assert_called_once_with(acquired_file)
    assert acquired_file.closed is True


def test_fixture_lock_preserves_existing_file_and_releases_after_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a populated lock file is unchanged and exceptions still release it.

    Given: A nonempty stable lock file and a context body that raises.
    When: The fixture lock context unwinds.
    Then: The byte remains unchanged and the acquired handle is released.
    """
    lock_path = tmp_path / "fixture.lock"
    lock_path.write_bytes(b"x")
    acquire = MagicMock()
    release = MagicMock()
    monkeypatch.setattr(builder, "_acquire_lock", acquire)
    monkeypatch.setattr(builder, "_release_lock", release)

    body_failure = RuntimeError("body failed")

    with pytest.raises(RuntimeError, match="body failed"), builder._fixture_lock(lock_path):
        assert lock_path.read_bytes() == b"x"
        raise body_failure

    acquired_file = acquire.call_args.args[0]
    release.assert_called_once_with(acquired_file)
    assert lock_path.read_bytes() == b"x"


def test_sidecar_paths_are_explicit_and_ordered(tmp_path: Path) -> None:
    """Verify only the three known SQLite sidecar names are derived.

    Given: A database path with a nontrivial filename.
    When: The builder derives its sidecars.
    Then: WAL, shared-memory, and journal paths are returned in fixed order.
    """
    database_path = tmp_path / "fixture.data.db"

    sidecars = builder._sidecar_paths(database_path)

    assert sidecars == (
        Path(f"{database_path}-wal"),
        Path(f"{database_path}-shm"),
        Path(f"{database_path}-journal"),
    )


def test_reserve_staging_path_creates_unique_empty_sibling_files(
    tmp_path: Path,
) -> None:
    """Verify staging reservations are unique files beside the target.

    Given: A target directory with no staging files.
    When: Two staging paths are reserved.
    Then: Both empty files have the exact prefix and suffix and never collide.
    """
    target_path = tmp_path / "dev.db"

    first = builder._reserve_staging_path(target_path)
    second = builder._reserve_staging_path(target_path)

    assert first != second
    assert first.parent == target_path.parent
    assert second.parent == target_path.parent
    assert first.name.startswith(".dev.db.staging-")
    assert second.name.startswith(".dev.db.staging-")
    assert first.suffix == ".db"
    assert second.suffix == ".db"
    assert first.read_bytes() == b""
    assert second.read_bytes() == b""


def test_database_url_is_absolute_and_uses_aiosqlite(tmp_path: Path) -> None:
    """Verify a staging path becomes an absolute aiosqlite URL.

    Given: A path containing a parent-directory segment.
    When: The builder creates the database URL.
    Then: The URL uses the resolved POSIX path and the aiosqlite driver.
    """
    database_path = tmp_path / "nested" / ".." / "staging db.sqlite"

    result = builder._database_url(database_path)

    assert result == f"sqlite+aiosqlite:///{database_path.resolve().as_posix()}"


def test_run_snapper_uses_isolated_environment_and_exact_command(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a Snapper subprocess is confined to the staging database URL.

    Given: An outer database URL, a staging path, and a mocked subprocess.
    When: The builder invokes a Snapper command.
    Then: A copied environment overrides only DB_URL and the checked command is exact.
    """
    database_path = tmp_path / "staging.db"
    run = MagicMock()
    monkeypatch.setenv("DB_URL", "sqlite:///outer.db")
    monkeypatch.setenv("SNAPPER_TEST_SENTINEL", "preserved")
    monkeypatch.setattr(builder, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(builder.subprocess, "run", run)
    expected_environment = os.environ.copy()
    expected_environment["DB_URL"] = builder._database_url(database_path)

    builder._run_snapper(("db-seed", "--profile", "dev"), database_path)

    run.assert_called_once_with(
        [sys.executable, "-m", "snapper", "db-seed", "--profile", "dev"],
        cwd=tmp_path,
        env=expected_environment,
        check=True,
    )
    assert os.environ["DB_URL"] == "sqlite:///outer.db"


def test_require_checkpoint_accepts_only_zero_busy_status(tmp_path: Path) -> None:
    """Verify a successful checkpoint is decided by the first result column.

    Given: A checkpoint tuple whose busy status is zero and other fields are nonzero.
    When: The builder requires a truncating checkpoint.
    Then: It executes the exact pragma and returns without error.
    """
    connection = MagicMock()
    cursor = MagicMock()
    cursor.fetchone.return_value = (0, 7, 5)
    connection.execute.return_value = cursor
    database_path = tmp_path / "fixture.db"

    builder._require_checkpoint(connection, database_path)

    connection.execute.assert_called_once_with("PRAGMA wal_checkpoint(TRUNCATE)")
    cursor.fetchone.assert_called_once_with()


def test_require_checkpoint_rejects_busy_status(tmp_path: Path) -> None:
    """Verify a nonzero busy status prevents unsafe file handling.

    Given: SQLite reports one busy connection in the checkpoint result.
    When: The builder requires a truncating checkpoint.
    Then: It raises an exact diagnostic containing the path and full tuple.
    """
    connection = MagicMock()
    cursor = MagicMock()
    cursor.fetchone.return_value = (1, 0, 0)
    connection.execute.return_value = cursor
    database_path = tmp_path / "fixture.db"

    with pytest.raises(RuntimeError) as raised:
        builder._require_checkpoint(connection, database_path)

    assert str(raised.value) == f"WAL checkpoint failed for {database_path}: (1, 0, 0)"
    connection.execute.assert_called_once_with("PRAGMA wal_checkpoint(TRUNCATE)")


def test_require_checkpoint_rejects_missing_result(tmp_path: Path) -> None:
    """Verify an absent checkpoint row is a hard failure rather than success.

    Given: SQLite returns no row for the checkpoint pragma.
    When: The builder requires a truncating checkpoint.
    Then: It raises an exact diagnostic without indexing the missing result.
    """
    connection = MagicMock()
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    connection.execute.return_value = cursor
    database_path = tmp_path / "fixture.db"

    with pytest.raises(RuntimeError) as raised:
        builder._require_checkpoint(connection, database_path)

    assert str(raised.value) == f"WAL checkpoint failed for {database_path}: None"
    connection.execute.assert_called_once_with("PRAGMA wal_checkpoint(TRUNCATE)")


def test_checkpoint_database_closes_connection_after_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify checkpointing always closes the connection on success.

    Given: A mocked SQLite connection and successful checkpoint requirement.
    When: The database checkpoint helper completes.
    Then: It connects to the exact path, validates that connection, and closes it.
    """
    database_path = tmp_path / "fixture.db"
    connection = MagicMock(spec=sqlite3.Connection)
    connect = MagicMock(return_value=connection)
    require = MagicMock()
    monkeypatch.setattr(builder.sqlite3, "connect", connect)
    monkeypatch.setattr(builder, "_require_checkpoint", require)

    builder._checkpoint_database(database_path)

    connect.assert_called_once_with(database_path)
    require.assert_called_once_with(connection, database_path)
    connection.close.assert_called_once_with()


def test_checkpoint_database_closes_connection_after_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify checkpointing closes its connection before propagating failure.

    Given: A mocked SQLite connection whose checkpoint requirement fails.
    When: The database checkpoint helper unwinds.
    Then: The same failure escapes and the connection is closed exactly once.
    """
    database_path = tmp_path / "fixture.db"
    connection = MagicMock(spec=sqlite3.Connection)
    connect = MagicMock(return_value=connection)
    failure = RuntimeError("busy")
    require = MagicMock(side_effect=failure)
    monkeypatch.setattr(builder.sqlite3, "connect", connect)
    monkeypatch.setattr(builder, "_require_checkpoint", require)

    with pytest.raises(RuntimeError) as raised:
        builder._checkpoint_database(database_path)

    assert raised.value is failure
    require.assert_called_once_with(connection, database_path)
    connection.close.assert_called_once_with()


def test_validate_staging_requires_checkpoint_and_exact_quick_check(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify staging validation performs both safety checks before closing.

    Given: A connection with a successful checkpoint and exactly one ``ok`` row.
    When: The staging database is validated.
    Then: Checkpoint precedes the exact quick-check query and the connection closes.
    """
    database_path = tmp_path / "staging.db"
    connection = MagicMock(spec=sqlite3.Connection)
    quick_cursor = MagicMock()
    quick_cursor.fetchall.return_value = [("ok",)]
    connection.execute.return_value = quick_cursor
    connect = MagicMock(return_value=connection)
    require = MagicMock()
    operations = MagicMock()
    operations.attach_mock(require, "checkpoint")
    operations.attach_mock(connection.execute, "execute")
    monkeypatch.setattr(builder.sqlite3, "connect", connect)
    monkeypatch.setattr(builder, "_require_checkpoint", require)

    builder._validate_staging(database_path)

    assert operations.mock_calls == [
        call.checkpoint(connection, database_path),
        call.execute("PRAGMA quick_check"),
        call.execute().fetchall(),
    ]
    quick_cursor.fetchall.assert_called_once_with()
    connection.close.assert_called_once_with()


def test_validate_staging_rejects_nonexact_quick_check(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify any quick-check result other than one ``ok`` row is rejected.

    Given: A checkpointed staging database with an additional quick-check row.
    When: The staging database is validated.
    Then: Validation raises the exact result and still closes the connection.
    """
    database_path = tmp_path / "staging.db"
    connection = MagicMock(spec=sqlite3.Connection)
    quick_cursor = MagicMock()
    quick_check = [("ok",), ("unexpected",)]
    quick_cursor.fetchall.return_value = quick_check
    connection.execute.return_value = quick_cursor
    monkeypatch.setattr(builder.sqlite3, "connect", MagicMock(return_value=connection))
    monkeypatch.setattr(builder, "_require_checkpoint", MagicMock())

    with pytest.raises(RuntimeError) as raised:
        builder._validate_staging(database_path)

    assert str(raised.value) == (f"SQLite quick_check failed for {database_path}: {quick_check}")
    connection.execute.assert_called_once_with("PRAGMA quick_check")
    connection.close.assert_called_once_with()


def test_validate_staging_stops_if_checkpoint_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify quick-check never runs after a failed staging checkpoint.

    Given: A staging connection whose checkpoint requirement raises.
    When: Validation begins.
    Then: The failure propagates, no quick-check executes, and the connection closes.
    """
    database_path = tmp_path / "staging.db"
    connection = MagicMock(spec=sqlite3.Connection)
    failure = RuntimeError("busy")
    monkeypatch.setattr(
        builder.sqlite3,
        "connect",
        MagicMock(return_value=connection),
    )
    monkeypatch.setattr(
        builder,
        "_require_checkpoint",
        MagicMock(side_effect=failure),
    )

    with pytest.raises(RuntimeError) as raised:
        builder._validate_staging(database_path)

    assert raised.value is failure
    connection.execute.assert_not_called()
    connection.close.assert_called_once_with()


def test_delete_sidecars_removes_only_explicit_paths(tmp_path: Path) -> None:
    """Verify sidecar cleanup never widens beyond the known suffixes.

    Given: A main database, all explicit sidecars, and a similarly named sibling.
    When: Sidecars are deleted.
    Then: Only WAL, shared-memory, and journal files disappear.
    """
    database_path = tmp_path / "fixture.db"
    database_path.write_bytes(b"main")
    sidecars = builder._sidecar_paths(database_path)
    for sidecar in sidecars:
        sidecar.write_bytes(b"sidecar")
    sibling = tmp_path / "fixture.db-backup"
    sibling.write_bytes(b"keep")

    builder._delete_sidecars(database_path)

    assert database_path.read_bytes() == b"main"
    assert all(not sidecar.exists() for sidecar in sidecars)
    assert sibling.read_bytes() == b"keep"


def test_publishable_existing_target_is_checkpointed_and_returns(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify an existing target is checkpointed without orphan-sidecar scanning.

    Given: An existing target and a sidecar enumerator that must not run.
    When: Publishability is required.
    Then: The target is checkpointed once and control returns immediately.
    """
    target_path = tmp_path / "dev.db"
    target_path.write_bytes(b"existing")
    checkpoint = MagicMock()
    sidecar_paths = MagicMock(side_effect=AssertionError("unexpected scan"))
    monkeypatch.setattr(builder, "_checkpoint_database", checkpoint)
    monkeypatch.setattr(builder, "_sidecar_paths", sidecar_paths)

    builder._require_publishable_target(target_path)

    checkpoint.assert_called_once_with(target_path)
    sidecar_paths.assert_not_called()


def test_publishable_absent_target_without_sidecars_is_allowed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a completely absent target needs no checkpoint.

    Given: No target database and no explicit target sidecars.
    When: Publishability is required.
    Then: The helper returns without opening a database or raising.
    """
    target_path = tmp_path / "dev.db"
    checkpoint = MagicMock()
    monkeypatch.setattr(builder, "_checkpoint_database", checkpoint)

    builder._require_publishable_target(target_path)

    checkpoint.assert_not_called()
    assert not target_path.exists()


def test_publishable_absent_target_rejects_exact_orphan_sidecars(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify sidecars without their target are treated as unsafe orphans.

    Given: No target database, two explicit sidecars, and one absent sidecar.
    When: Publishability is required.
    Then: The exact existing orphan list is reported without attempting checkpoint.
    """
    target_path = tmp_path / "dev.db"
    wal_path, shm_path, journal_path = builder._sidecar_paths(target_path)
    wal_path.write_bytes(b"wal")
    journal_path.write_bytes(b"journal")
    checkpoint = MagicMock()
    monkeypatch.setattr(builder, "_checkpoint_database", checkpoint)

    with pytest.raises(RuntimeError) as raised:
        builder._require_publishable_target(target_path)

    assert str(raised.value) == (
        f"Target sidecars exist without {target_path}: {[wal_path, journal_path]}"
    )
    checkpoint.assert_not_called()
    assert not shm_path.exists()


def test_publish_staging_orders_validation_cleanup_and_atomic_replace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify publishing performs every safety step in the required order.

    Given: A staging path, target path, and mocked publication operations.
    When: The staging database is published.
    Then: Validation and target checks precede both cleanups and atomic replacement.
    """
    staging_path = tmp_path / "staging.db"
    target_path = tmp_path / "dev.db"
    validate = MagicMock()
    require = MagicMock()
    delete = MagicMock()
    replace = MagicMock()
    operations = MagicMock()
    operations.attach_mock(validate, "validate")
    operations.attach_mock(require, "require")
    operations.attach_mock(delete, "delete")
    operations.attach_mock(replace, "replace")
    monkeypatch.setattr(builder, "_validate_staging", validate)
    monkeypatch.setattr(builder, "_require_publishable_target", require)
    monkeypatch.setattr(builder, "_delete_sidecars", delete)
    monkeypatch.setattr(builder.os, "replace", replace)

    builder._publish_staging(staging_path, target_path)

    assert operations.mock_calls == [
        call.validate(staging_path),
        call.require(target_path),
        call.delete(staging_path),
        call.delete(target_path),
        call.replace(staging_path, target_path),
    ]


def test_publish_staging_validation_failure_touches_no_target_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify failed staging validation stops before target inspection or cleanup.

    Given: A staging validator that raises and mocked downstream operations.
    When: Publication is attempted.
    Then: The same failure escapes and no target-related operation runs.
    """
    staging_path = tmp_path / "staging.db"
    target_path = tmp_path / "dev.db"
    failure = RuntimeError("invalid staging")
    validate = MagicMock(side_effect=failure)
    require = MagicMock()
    delete = MagicMock()
    replace = MagicMock()
    monkeypatch.setattr(builder, "_validate_staging", validate)
    monkeypatch.setattr(builder, "_require_publishable_target", require)
    monkeypatch.setattr(builder, "_delete_sidecars", delete)
    monkeypatch.setattr(builder.os, "replace", replace)

    with pytest.raises(RuntimeError) as raised:
        builder._publish_staging(staging_path, target_path)

    assert raised.value is failure
    validate.assert_called_once_with(staging_path)
    require.assert_not_called()
    delete.assert_not_called()
    replace.assert_not_called()


def test_publish_staging_replace_failure_propagates_after_sidecar_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify atomic replacement errors escape after the prescribed cleanups.

    Given: Successful safety checks and an atomic replacement that fails.
    When: Publication reaches the replacement step.
    Then: Both sidecar cleanups occurred in order before the same error escapes.
    """
    staging_path = tmp_path / "staging.db"
    target_path = tmp_path / "dev.db"
    failure = OSError(errno.EIO, "replace failed")
    validate = MagicMock()
    require = MagicMock()
    delete = MagicMock()
    replace = MagicMock(side_effect=failure)
    operations = MagicMock()
    operations.attach_mock(validate, "validate")
    operations.attach_mock(require, "require")
    operations.attach_mock(delete, "delete")
    operations.attach_mock(replace, "replace")
    monkeypatch.setattr(builder, "_validate_staging", validate)
    monkeypatch.setattr(builder, "_require_publishable_target", require)
    monkeypatch.setattr(builder, "_delete_sidecars", delete)
    monkeypatch.setattr(builder.os, "replace", replace)

    with pytest.raises(OSError) as raised:
        builder._publish_staging(staging_path, target_path)

    assert raised.value is failure
    assert operations.mock_calls == [
        call.validate(staging_path),
        call.require(target_path),
        call.delete(staging_path),
        call.delete(target_path),
        call.replace(staging_path, target_path),
    ]


def test_discard_staging_leaves_orphan_sidecars_when_main_is_absent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify discard never guesses that orphan sidecars are safe to delete.

    Given: An absent staging database with an orphan WAL sidecar.
    When: Failed staging is discarded.
    Then: No checkpoint or deletion runs and the orphan remains untouched.
    """
    staging_path = tmp_path / "staging.db"
    wal_path = Path(f"{staging_path}-wal")
    wal_path.write_bytes(b"orphan")
    checkpoint = MagicMock()
    delete = MagicMock()
    monkeypatch.setattr(builder, "_checkpoint_database", checkpoint)
    monkeypatch.setattr(builder, "_delete_sidecars", delete)

    builder._discard_staging(staging_path)

    checkpoint.assert_not_called()
    delete.assert_not_called()
    assert wal_path.read_bytes() == b"orphan"


@pytest.mark.parametrize(
    "failure",
    [
        pytest.param(RuntimeError("busy"), id="checkpoint_refused"),
        pytest.param(sqlite3.DatabaseError("malformed"), id="sqlite_failure"),
    ],
)
def test_discard_staging_leaves_unsafe_database_and_sidecars(
    failure: Exception,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify checkpoint failures preserve all staging evidence.

    Given: A staging database and WAL whose checkpoint raises a safety failure.
    When: Failed staging is discarded.
    Then: Both files remain, deletion is skipped, and stderr identifies the reason.
    """
    staging_path = tmp_path / "staging.db"
    staging_path.write_bytes(b"database")
    wal_path = Path(f"{staging_path}-wal")
    wal_path.write_bytes(b"wal")
    checkpoint = MagicMock(side_effect=failure)
    delete = MagicMock()
    monkeypatch.setattr(builder, "_checkpoint_database", checkpoint)
    monkeypatch.setattr(builder, "_delete_sidecars", delete)

    builder._discard_staging(staging_path)

    checkpoint.assert_called_once_with(staging_path)
    delete.assert_not_called()
    assert staging_path.read_bytes() == b"database"
    assert wal_path.read_bytes() == b"wal"
    assert capsys.readouterr().err == (
        f"Leaving unsafe staging database {staging_path}: {failure}\n"
    )


def test_discard_staging_checkpoints_then_removes_database_and_sidecars(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify safe failed staging is removed only after checkpointing.

    Given: A staging database with all explicit sidecars and a safe checkpoint.
    When: Failed staging is discarded.
    Then: Checkpoint precedes sidecar deletion and every staging file disappears.
    """
    staging_path = tmp_path / "staging.db"
    staging_path.write_bytes(b"database")
    sidecars = builder._sidecar_paths(staging_path)
    for sidecar in sidecars:
        sidecar.write_bytes(b"sidecar")
    checkpoint = MagicMock()
    real_delete_sidecars = builder._delete_sidecars
    delete = MagicMock(side_effect=real_delete_sidecars)
    operations = MagicMock()
    operations.attach_mock(checkpoint, "checkpoint")
    operations.attach_mock(delete, "delete")
    monkeypatch.setattr(builder, "_checkpoint_database", checkpoint)
    monkeypatch.setattr(builder, "_delete_sidecars", delete)

    builder._discard_staging(staging_path)

    assert operations.mock_calls == [
        call.checkpoint(staging_path),
        call.delete(staging_path),
    ]
    assert not staging_path.exists()
    assert all(not sidecar.exists() for sidecar in sidecars)


def test_rebuild_fixture_publishes_exact_commands_without_discard(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify a successful rebuild creates, seeds, and publishes one staging file.

    Given: Temporary fixture constants and mocked lock, staging, and command helpers.
    When: The fixture rebuild succeeds.
    Then: Exact commands run under the lock and published staging is never discarded.
    """
    target_path = tmp_path / "nested" / "dev.db"
    lock_path = target_path.with_name("dev.db.lock")
    staging_path = target_path.with_name(".dev.db.staging-test.db")
    fixture_lock = MagicMock()
    reserve = MagicMock(return_value=staging_path)
    run = MagicMock()
    publish = MagicMock()
    discard = MagicMock()
    operations = MagicMock()
    operations.attach_mock(fixture_lock, "lock")
    operations.attach_mock(reserve, "reserve")
    operations.attach_mock(run, "run")
    operations.attach_mock(publish, "publish")
    operations.attach_mock(discard, "discard")
    monkeypatch.setattr(builder, "TARGET_PATH", target_path)
    monkeypatch.setattr(builder, "LOCK_PATH", lock_path)
    monkeypatch.setattr(builder, "_fixture_lock", fixture_lock)
    monkeypatch.setattr(builder, "_reserve_staging_path", reserve)
    monkeypatch.setattr(builder, "_run_snapper", run)
    monkeypatch.setattr(builder, "_publish_staging", publish)
    monkeypatch.setattr(builder, "_discard_staging", discard)

    builder.rebuild_fixture()

    assert operations.mock_calls == [
        call.lock(lock_path),
        call.lock().__enter__(),
        call.reserve(target_path),
        call.run(("db-init",), staging_path),
        call.run(("db-seed", "--profile", "dev"), staging_path),
        call.publish(staging_path, target_path),
        call.lock().__exit__(None, None, None),
    ]
    assert target_path.parent.is_dir()
    assert capsys.readouterr().out == (f"Published fresh SQLite test fixture at {target_path}\n")


def test_rebuild_fixture_discards_staging_after_seed_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify a failed seed command discards staging before releasing the lock.

    Given: Initialization succeeds but the staging seed subprocess fails.
    When: The fixture rebuild unwinds.
    Then: Publication is skipped, staging is discarded, and the same failure escapes.
    """
    target_path = tmp_path / "dev.db"
    lock_path = tmp_path / "dev.db.lock"
    staging_path = tmp_path / ".staging.db"
    failure = subprocess.CalledProcessError(5, ["snapper", "db-seed"])
    fixture_lock = MagicMock()
    reserve = MagicMock(return_value=staging_path)
    run = MagicMock(side_effect=[None, failure])
    publish = MagicMock()
    discard = MagicMock()
    operations = MagicMock()
    operations.attach_mock(fixture_lock, "lock")
    operations.attach_mock(reserve, "reserve")
    operations.attach_mock(run, "run")
    operations.attach_mock(publish, "publish")
    operations.attach_mock(discard, "discard")
    monkeypatch.setattr(builder, "TARGET_PATH", target_path)
    monkeypatch.setattr(builder, "LOCK_PATH", lock_path)
    monkeypatch.setattr(builder, "_fixture_lock", fixture_lock)
    monkeypatch.setattr(builder, "_reserve_staging_path", reserve)
    monkeypatch.setattr(builder, "_run_snapper", run)
    monkeypatch.setattr(builder, "_publish_staging", publish)
    monkeypatch.setattr(builder, "_discard_staging", discard)

    with pytest.raises(subprocess.CalledProcessError) as raised:
        builder.rebuild_fixture()

    assert raised.value is failure
    assert operations.mock_calls == [
        call.lock(lock_path),
        call.lock().__enter__(),
        call.reserve(target_path),
        call.run(("db-init",), staging_path),
        call.run(("db-seed", "--profile", "dev"), staging_path),
        call.discard(staging_path),
        call.lock().__exit__(subprocess.CalledProcessError, failure, ANY),
    ]
    publish.assert_not_called()
    assert capsys.readouterr().out == ""


def test_rebuild_fixture_discards_staging_after_publish_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify a publication failure retains cleanup responsibility in rebuild.

    Given: Successful staging commands and a publication helper that raises.
    When: The fixture rebuild unwinds.
    Then: Staging is discarded under the lock and no success message is printed.
    """
    target_path = tmp_path / "dev.db"
    lock_path = tmp_path / "dev.db.lock"
    staging_path = tmp_path / ".staging.db"
    failure = OSError(errno.EIO, "publish failed")
    fixture_lock = MagicMock()
    reserve = MagicMock(return_value=staging_path)
    run = MagicMock()
    publish = MagicMock(side_effect=failure)
    discard = MagicMock()
    operations = MagicMock()
    operations.attach_mock(fixture_lock, "lock")
    operations.attach_mock(reserve, "reserve")
    operations.attach_mock(run, "run")
    operations.attach_mock(publish, "publish")
    operations.attach_mock(discard, "discard")
    monkeypatch.setattr(builder, "TARGET_PATH", target_path)
    monkeypatch.setattr(builder, "LOCK_PATH", lock_path)
    monkeypatch.setattr(builder, "_fixture_lock", fixture_lock)
    monkeypatch.setattr(builder, "_reserve_staging_path", reserve)
    monkeypatch.setattr(builder, "_run_snapper", run)
    monkeypatch.setattr(builder, "_publish_staging", publish)
    monkeypatch.setattr(builder, "_discard_staging", discard)

    with pytest.raises(OSError) as raised:
        builder.rebuild_fixture()

    assert raised.value is failure
    assert operations.mock_calls == [
        call.lock(lock_path),
        call.lock().__enter__(),
        call.reserve(target_path),
        call.run(("db-init",), staging_path),
        call.run(("db-seed", "--profile", "dev"), staging_path),
        call.publish(staging_path, target_path),
        call.discard(staging_path),
        call.lock().__exit__(OSError, failure, ANY),
    ]
    assert capsys.readouterr().out == ""


def test_main_rebuilds_fixture_and_returns_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the synchronous entry point delegates once and returns zero.

    Given: A mocked successful fixture rebuild.
    When: The builder entry point runs.
    Then: It invokes the rebuild exactly once and returns success.
    """
    rebuild = MagicMock()
    monkeypatch.setattr(builder, "rebuild_fixture", rebuild)

    result = builder.main()

    assert result == 0
    rebuild.assert_called_once_with()
