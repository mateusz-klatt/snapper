"""Prepare synthetic0066 evidence on WSL and probe installed older application reads.

Prepare with the current checkout's test dependencies and SDK. Probe runs with
only this script and the newly created fixture mounted into the exact old
image under network isolation. It imports no new inventory model or migration,
applies no downgrade and loads no deployment configuration. Its bounded claim
is SettingsService initialization and existing repository reads while actual
pending/captured inventory evidence remains byte-identical.

The owner runs the old-image command with --network none, --cap-drop ALL,
--security-opt no-new-privileges, --user HOST_UID:HOST_GID, --workdir /fixture
and PYTHONPATH unset. Mount only this script and the new fixture directory.
This tests database/application compatibility, not default-image UID access,
complete service boot, venue behavior or deployment health.
"""

import argparse
import asyncio
import hashlib
import json
import os
import sqlite3
import sys
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Mapping
from contextlib import chdir
from contextlib import closing
from contextlib import contextmanager
from dataclasses import asdict
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from importlib import import_module
from pathlib import Path
from typing import Protocol
from typing import cast

FIXTURE_KIND = "snapper-inventory-rollback-fixture-v1"
FIXTURE_SETTING = "inventory_rollback.synthetic"
FIXTURE_VALUE = {"fixture": "inventory-0066", "live_orders": False}
FIXTURE_WALLET = "00000000-0000-7000-8000-000000000001"
FIXTURE_INSTRUMENT = "00000000-0000-7000-8000-000000000085"


class InventoryRollbackProbeError(RuntimeError):
    """An explicit synthetic-fixture or old-application read requirement failed."""


class _FixtureBuilder(Protocol):
    """Development-only synthetic builder loaded exclusively by prepare mode."""

    async def build_rollback_fixture(self, directory: Path) -> None:
        """Create actual migrated SDK evidence inside the newly owned directory."""
        ...


class _Engine(Protocol):
    """Existing installed engine cleanup surface, independent of new inventory classes."""

    async def dispose(self) -> None:
        """Close the actual application engine after the bounded read probe."""
        ...


class _Repository(Protocol):
    """Established old repository read API used by the compatibility probe."""

    engine: _Engine

    async def get_setting_by_key(self, key: str, as_of: datetime) -> Mapping[str, object] | None:
        """Read an existing plaintext synthetic setting through the installed application."""
        ...

    async def list_active_wallets(self, as_of: datetime) -> list[Mapping[str, object]]:
        """Read existing wallet models against the populated later schema."""
        ...

    async def get_symbol_for_instrument(
        self, instrument_public_id: str, as_of: datetime
    ) -> str | None:
        """Exercise the existing active instrument/symbol temporal join."""
        ...

    async def get_active_orders_for_recovery(
        self, *, exchange: str, as_of: datetime, wallet_public_id: str
    ) -> list[Mapping[str, object]]:
        """Execute the installed old recovery query without constructing orders."""
        ...


class _Settings(Protocol):
    """Actual installed SettingsService lifecycle and loaded-cache read contract."""

    async def initialize(self) -> None:
        """Perform the original database readiness, settings load and publisher initialization."""
        ...

    async def shutdown(self) -> None:
        """Close the actual local publisher and context."""
        ...

    def get_setting(self, key: str) -> object:
        """Read the value loaded by actual initialization, without injecting a cache."""
        ...


@dataclass(frozen=True, slots=True)
class InventoryRollbackSnapshot:
    """Exact new history fingerprint, independent of whether installed old models know its tables."""

    revision: str
    attempts: int
    outcomes: int
    pending: int
    captured: int
    evidence_sha256: str


def _require(condition: bool, reason: str) -> None:
    """Refuse an unproven fixture or application result without printing source contents."""
    if not condition:
        raise InventoryRollbackProbeError(reason)


def fixture_snapshot(directory: Path) -> InventoryRollbackSnapshot:
    """Read only the explicitly named synthetic database and fingerprint its actual inventory rows.

    Args:
        directory: Newly owned fixture namespace, never a DB URL or production database argument.

    Returns:
        Actual revision, independent pending/captured counts and complete inventory row fingerprint.
    """
    path = directory / "inventory.db"
    _require(path.is_file() and not path.is_symlink(), "synthetic_fixture_database_required")
    with closing(sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)) as connection:
        revision = connection.execute("SELECT version_num FROM alembic_version").fetchone()
        attempts = connection.execute(
            "SELECT * FROM futures_inventory_read_attempts ORDER BY public_id"
        ).fetchall()
        outcomes = connection.execute(
            "SELECT * FROM futures_inventory_read_outcomes ORDER BY public_id"
        ).fetchall()
        pending = connection.execute(
            "SELECT count(*) FROM futures_inventory_read_attempts AS a WHERE NOT EXISTS "
            "(SELECT 1 FROM futures_inventory_read_outcomes AS o WHERE o.attempt_public_id=a.public_id)"
        ).fetchone()
        captured = connection.execute(
            "SELECT count(*) FROM futures_inventory_read_outcomes WHERE result_kind='captured'"
        ).fetchone()
        _require(
            connection.execute("PRAGMA integrity_check").fetchone() == ("ok",),
            "fixture_integrity_failed",
        )
    _require(revision == ("0066",), "fixture_revision_0066_required")
    _require(
        (len(attempts), len(outcomes), pending, captured) == (2, 1, (1,), (1,)),
        "fixture_requires_genuine_pending_and_captured_inventory",
    )
    digest = hashlib.sha256(
        json.dumps([attempts, outcomes], separators=(",", ":")).encode()
    ).hexdigest()
    return InventoryRollbackSnapshot("0066", 2, 1, 1, 1, digest)


@contextmanager
def _isolated_environment(directory: Path, url: str) -> Iterator[None]:
    """Exclude inherited application settings and dotenv files before importing application code."""
    original = dict(os.environ)
    safe = {key: value for key, value in original.items() if key in {"PATH", "HOME", "LANG"}}
    safe.update(
        SNAPPER_ENV="testing",
        DB_URL=url,
        MASTER_PASSWORD="synthetic-inventory-rollback-password",
        SERVER_API_ONLY="true",
        ZMQ_BROKER_XSUB="inproc://inventory-rollback-probe",
        ZMQ_BROKER_XPUB="inproc://inventory-rollback-probe-reader",
    )
    try:
        os.environ.clear()
        os.environ.update(safe)
        with chdir(directory):
            yield
    finally:
        os.environ.clear()
        os.environ.update(original)


async def prepare_fixture(directory: Path) -> InventoryRollbackSnapshot:
    """Create a fresh synthetic fixture using existing migrated-SDK test helpers on WSL.

    Args:
        directory: A nonexistent output directory; existing data is never replaced or migrated.

    Returns:
        Verified actual pending/captured evidence fingerprint written to the fixture manifest.
    """
    directory = directory.absolute()
    directory.mkdir(mode=0o700, parents=False, exist_ok=False)
    with _isolated_environment(directory, f"sqlite+aiosqlite:///{directory / 'inventory.db'}"):
        builder = cast(
            _FixtureBuilder, import_module("tests.scripts.test_verify_inventory_rollback")
        )
        await builder.build_rollback_fixture(directory)
    (directory / "inventory.db").chmod(0o600)
    snapshot = fixture_snapshot(directory)
    manifest = {"kind": FIXTURE_KIND, "snapshot": asdict(snapshot)}
    (directory / "manifest.json").write_text(json.dumps(manifest, sort_keys=True) + "\n")
    (directory / "manifest.json").chmod(0o600)
    return snapshot


def _application(url: str) -> tuple[_Repository, _Settings]:
    """Load only the installed old application after isolation, without any new inventory imports."""
    repositories = import_module("snapper.data.repository")
    factory = cast(Callable[[str], _Repository], repositories.get_repository)
    repository = factory(url)
    _require(
        isinstance(repository, cast(type[object], repositories.SQLAlchemyRepository)),
        "actual_sqlalchemy_repository_required",
    )
    service_type = cast(
        Callable[[str, str], _Settings],
        import_module("snapper.application.services.settings").SettingsService,
    )
    return repository, service_type(url, "inproc://inventory-rollback-probe")


async def _read_application(repository: _Repository, service: _Settings) -> None:
    """Run the actual installed settings lifecycle and existing read APIs with explicit cleanup."""
    try:
        await service.initialize()
        _require(
            service.get_setting(FIXTURE_SETTING) == FIXTURE_VALUE, "settings_startup_read_failed"
        )
        now = datetime.now(UTC)
        setting = await repository.get_setting_by_key(FIXTURE_SETTING, now)
        _require(setting is not None, "repository_setting_read_failed")
        value = cast(Mapping[str, object], setting)["value"]
        _require(isinstance(value, str), "repository_setting_value_required")
        _require(json.loads(cast(str, value)) == FIXTURE_VALUE, "repository_setting_read_failed")
        wallets = await repository.list_active_wallets(now)
        _require(
            any(row["public_id"] == FIXTURE_WALLET for row in wallets),
            "repository_wallet_read_failed",
        )
        symbol = await repository.get_symbol_for_instrument(FIXTURE_INSTRUMENT, now)
        _require(symbol == "BTC-USD", "repository_symbol_read_failed")
        orders = await repository.get_active_orders_for_recovery(
            exchange="kraken_futures", as_of=now, wallet_public_id=FIXTURE_WALLET
        )
        _require(orders == [], "synthetic_fixture_unexpected_order")
    finally:
        try:
            await service.shutdown()
        finally:
            await repository.engine.dispose()


async def probe_fixture(directory: Path) -> InventoryRollbackSnapshot:
    """Initialize installed SettingsService and perform actual old repository reads without DDL/DML.

    Args:
        directory: Only the new synthetic fixture mounted into the exact old image.

    Returns:
        Unchanged real0066 evidence after successful application startup and repository reads.
    """
    directory = directory.resolve(strict=True)
    manifest = json.loads((directory / "manifest.json").read_text())
    _require(isinstance(manifest, dict), "synthetic_fixture_manifest_required")
    _require(manifest.get("kind") == FIXTURE_KIND, "synthetic_fixture_manifest_required")
    before = fixture_snapshot(directory)
    _require(manifest.get("snapshot") == asdict(before), "synthetic_fixture_manifest_mismatch")
    path = (directory / "inventory.db").resolve(strict=True)
    url = f"sqlite+aiosqlite:///file:{path.as_posix()}?mode=ro&uri=true"
    with _isolated_environment(directory, url):
        await _read_application(*_application(url))
    after = fixture_snapshot(directory)
    _require(after == before, "old_application_changed_inventory_evidence")
    return after


def main() -> int:
    """Run the explicitly selected synthetic preparation or installed-code application probe.

    Returns:
        Zero only after the selected real operation succeeds, otherwise two.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "probe"))
    parser.add_argument("directory", type=Path)
    arguments = parser.parse_args()
    try:
        if arguments.mode == "probe":
            _require(not os.environ.get("PYTHONPATH"), "old_image_probe_requires_unset_PYTHONPATH")
            result = asyncio.run(probe_fixture(arguments.directory))
        else:
            result = asyncio.run(prepare_fixture(arguments.directory))
    except (InventoryRollbackProbeError, OSError, ValueError, sqlite3.Error) as failure:
        print(f"inventory rollback probe refused: {failure}", file=sys.stderr)
        return 2
    print(json.dumps({"mode": arguments.mode, "snapshot": asdict(result)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
