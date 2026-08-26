"""Tests for the deterministic paper-only P&L browser-UAT fixture."""

import json
import sys
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from shutil import copy2
from typing import Never
from typing import cast
from unittest.mock import AsyncMock

import bcrypt
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg
from sqlalchemy.engine import URL
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import Pool

from scripts import pnl_uat_fixture
from snapper.application.engine.service import compute_shard_key
from snapper.application.portfolio.pnl_timeline import PnlTimelinePoint
from snapper.application.portfolio.pnl_timeline_service import PnlAiDecisionMarker
from snapper.application.portfolio.pnl_timeline_service import PnlFillMarker
from snapper.application.portfolio.pnl_timeline_service import PnlSignalMarker
from snapper.application.portfolio.pnl_timeline_service import PnlWalletTimelineResult
from snapper.application.portfolio.pnl_timeline_service import build_wallet_pnl_timeline
from snapper.application.portfolio.pnl_timeline_service import ensure_wallet_pnl_anchor
from snapper.core.types import ExchangeEnum
from snapper.core.types import ExecutionModeEnum
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Instrument
from snapper.data.models import PortfolioPnlPoint
from snapper.data.models import Symbol
from snapper.data.models import TradeCommand
from snapper.data.models import VenueEvent
from snapper.data.models import Wallet
from snapper.data.models import WalletCredential
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.seed.loader import run_seed
from snapper.infrastructure.security.encryption import get_encryption_service

_ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_ANCHOR = datetime(2026, 7, 22, 12, 0, tzinfo=UTC)
_TEST_SESSION_ID = "00000000-0000-7000-8000-00000000c001"
_COMPLETE_FILL_INDEX = 720
_INCOMPLETE_FILL_INDEX = 1_320
_EXPECTED_INCOMPLETE_POINT_COUNT = 121
_EXPECTED_INCOMPLETE_REASON = "mark_unavailable"
_EXPECTED_INCOMPLETE_INSTRUMENT_ID = "00000000-0000-7000-8000-00000000a313"


@dataclass(frozen=True)
class _ExpectedEconomics:
    """Independent exact economics expected from one valuation series."""

    realized_pnl: float
    fee_pnl: float
    accrual_pnl: float
    unrealized_pnl: float
    net_pnl: float

    def components(self) -> tuple[float, float, float, float, float]:
        """Return the ordered public component contract."""
        return (
            self.realized_pnl,
            self.fee_pnl,
            self.accrual_pnl,
            self.unrealized_pnl,
            self.net_pnl,
        )


_EXPECTED_COMPLETE = {
    "USD": _ExpectedEconomics(0.0, -0.04, 0.0, 5.0, 4.96),
    "PLN": _ExpectedEconomics(0.0, -0.16, 0.0, 20.0, 19.84),
    "EUR": _ExpectedEconomics(0.0, -0.04, 0.0, 0.0, -0.04),
}


def _alembic_config(db_path: Path) -> Config:
    """Build an Alembic config for one isolated SQLite database."""
    config = Config(str(_ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", f"sqlite:///{db_path}")
    return config


@pytest.fixture(scope="module")
def oss_seeded_template(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Create one migrated and bundled-OSS-seeded SQLite template."""
    template_dir = tmp_path_factory.mktemp("pnl-uat-oss-template")
    template_path = template_dir / "template.db"
    command.upgrade(_alembic_config(template_path), "head")
    with pytest.MonkeyPatch.context() as patch:
        patch.chdir(template_dir)
        patch.setenv("DB_URL", f"sqlite+aiosqlite:///{template_path}")
        assert run_seed("dev") == (3, 13)
    return template_path


@pytest.fixture
def oss_seeded_db_url(
    tmp_path: Path,
    oss_seeded_template: Path,
) -> URL:
    """Clone the pristine OSS baseline for one isolated test."""
    database_path = tmp_path / "pnl-uat.db"
    copy2(oss_seeded_template, database_path)
    return make_url(f"sqlite+aiosqlite:///{database_path}")


async def _add_wallet(
    db_url: URL,
    public_id: str,
    label: str,
) -> None:
    """Insert one unrelated paper wallet into a baseline database."""
    engine = create_async_engine(db_url)
    try:
        async with AsyncSession(engine) as session, session.begin():
            session.add(
                Wallet(
                    public_id=public_id,
                    label=label,
                    description="Test-only contamination",
                    is_paper=True,
                    timestamp=_ANCHOR,
                    known_to=KNOWN_TO_MAX,
                    session_id=_TEST_SESSION_ID,
                    sequence_id=1,
                )
            )
    finally:
        await engine.dispose()


async def _add_nonpaper_credential(db_url: URL) -> None:
    """Insert one opaque non-paper credential without real credential material."""
    engine = create_async_engine(db_url)
    try:
        async with AsyncSession(engine) as session, session.begin():
            wallet_public_id = (await session.execute(select(Wallet.public_id))).scalar_one()
            session.add(
                WalletCredential(
                    public_id="00000000-0000-7000-8000-00000000c101",
                    wallet_public_id=wallet_public_id,
                    exchange="kraken",
                    credential_type="api_key_secret",
                    encrypted_payload="opaque-test-envelope",
                    label="test-only non-paper fence",
                    timestamp=_ANCHOR,
                    known_to=KNOWN_TO_MAX,
                    session_id=_TEST_SESSION_ID,
                    sequence_id=2,
                )
            )
    finally:
        await engine.dispose()


async def _symbol_timestamps(db_url: URL) -> dict[str, datetime]:
    """Return all canonical symbol timestamps from one database."""
    engine = create_async_engine(db_url)
    try:
        async with AsyncSession(engine) as session:
            rows = (
                await session.execute(
                    select(Symbol.native_symbol, Symbol.timestamp).order_by(Symbol.native_symbol)
                )
            ).all()
            return dict(rows)
    finally:
        await engine.dispose()


async def _instrument_count(db_url: URL) -> int:
    """Return the number of instrument rows in one database."""
    engine = create_async_engine(db_url)
    try:
        async with AsyncSession(engine) as session:
            count = (
                await session.execute(select(func.count()).select_from(Instrument))
            ).scalar_one()
            return int(count)
    finally:
        await engine.dispose()


async def _execute_sql(db_url: URL, statement: str) -> None:
    """Execute one test-only mutation in an isolated database."""
    engine = create_async_engine(db_url)
    try:
        async with AsyncSession(engine) as session, session.begin():
            await session.execute(text(statement))
    finally:
        await engine.dispose()


async def _replace_seed_password(db_url: URL, username: str) -> None:
    """Replace one seed password with a valid bcrypt hash for a wrong value."""
    password_hash = bcrypt.hashpw(b"wrong-test-only-password", bcrypt.gensalt()).decode()
    engine = create_async_engine(db_url)
    try:
        async with AsyncSession(engine) as session, session.begin():
            await session.execute(
                text("UPDATE users SET password_hash = :password_hash WHERE username = :username"),
                {"password_hash": password_hash, "username": username},
            )
    finally:
        await engine.dispose()


async def _replace_paper_envelope(db_url: URL, envelope: dict[str, str]) -> None:
    """Replace the paper envelope with another valid encrypted JSON object."""
    encrypted = get_encryption_service().encrypt(
        json.dumps(envelope, sort_keys=True, separators=(",", ":"))
    )
    engine = create_async_engine(db_url)
    try:
        async with AsyncSession(engine) as session, session.begin():
            await session.execute(
                text("UPDATE wallet_credentials SET encrypted_payload = :encrypted"),
                {"encrypted": encrypted},
            )
    finally:
        await engine.dispose()


async def _require_admin_for_test(db_url: URL) -> str:
    """Run the internal admin guard in one isolated session."""
    engine = create_async_engine(db_url)
    try:
        async with AsyncSession(engine) as session:
            return await pnl_uat_fixture._require_admin_user(session)
    finally:
        await engine.dispose()


async def _prepare_symbols_for_test(db_url: URL) -> dict[str, str]:
    """Run the internal symbol guard in one isolated transaction."""
    engine = create_async_engine(db_url)
    try:
        async with AsyncSession(engine) as session, session.begin():
            return await pnl_uat_fixture._prepare_symbols(
                session,
                _ANCHOR - timedelta(days=2),
            )
    finally:
        await engine.dispose()


@pytest.mark.parametrize("host", ["127.0.0.1", "127.255.255.255", "[::1]"])
def test_validate_target_database_accepts_only_literal_loopback_addresses(host: str) -> None:
    """Accept the complete IPv4 loopback block and IPv6 loopback literal.

    Given: A dedicated asyncpg URL with a literal IPv4 or IPv6 loopback host,
    When: The target database safety fence validates the URL,
    Then: The parsed single-host URL is returned without query parameters.
    """
    parsed = pnl_uat_fixture.validate_target_database(
        f"postgresql+asyncpg://uat:local@{host}:5432/snapper_pnl_uat_run_42"
    )

    assert parsed.host in {"127.0.0.1", "127.255.255.255", "::1"}
    assert parsed.database == "snapper_pnl_uat_run_42"
    assert not parsed.query


def test_asyncpg_connect_args_preserve_every_validated_authority_component() -> None:
    """Prove asyncpg receives the exact authority accepted by the fence.

    Given: A validated IPv6 URL with encoded credentials and an explicit port,
    When: The asyncpg dialect creates its connection arguments,
    Then: Host, database, user, password, and port remain exact and single-host.
    """
    parsed = pnl_uat_fixture.validate_target_database(
        "postgresql+asyncpg://uat:p%40ss%3Aword@[::1]:6543/snapper_pnl_uat_run_42"
    )

    positional, keyword = PGDialect_asyncpg().create_connect_args(parsed)

    assert positional == []
    assert keyword == {
        "database": "snapper_pnl_uat_run_42",
        "user": "uat",
        "password": "p@ss:word",
        "host": "::1",
        "port": 6543,
    }
    assert not isinstance(keyword["host"], list)


@pytest.mark.parametrize(
    "db_url",
    [
        "sqlite+aiosqlite:///snapper_pnl_uat_run.db",
        "postgresql://uat:local@localhost/snapper_pnl_uat_run",
        "postgresql+asyncpg://uat:local@localhost/snapper_pnl_uat_run",
        "postgresql+asyncpg://uat:local@localhost./snapper_pnl_uat_run",
        "postgresql+asyncpg://uat:local@127.0.0.1./snapper_pnl_uat_run",
        "postgresql+asyncpg://uat:local@127.1/snapper_pnl_uat_run",
        "postgresql+asyncpg://uat:local@2130706433/snapper_pnl_uat_run",
        "postgresql+asyncpg://uat:local@0x7f000001/snapper_pnl_uat_run",
        "postgresql+asyncpg://uat:local@[::ffff:127.0.0.1]/snapper_pnl_uat_run",
        "postgresql+asyncpg://uat:local@127.0.0.1,example.com/snapper_pnl_uat_run",
        "postgresql+asyncpg://uat:local@example.com/snapper_pnl_uat_run",
        "postgresql+asyncpg:///snapper_pnl_uat_run",
        "postgresql+asyncpg://uat:local@127.0.0.1/snapper_pnl_uat_run?sslmode=disable",
        "postgresql+asyncpg://uat:local@127.0.0.1/snapper_pnl_uat_run?host=example.com",
        (
            "postgresql+asyncpg://uat:local@127.0.0.1/snapper_pnl_uat_run"
            "?host=127.0.0.1&host=example.com"
        ),
        "postgresql+asyncpg://uat:local@127.0.0.1/snapper",
        "postgresql+asyncpg://uat:local@127.0.0.1/snapper_pnl_uat_",
        "postgresql+asyncpg://uat:local@127.0.0.1/snapper_pnl_uat_RUN",
        "postgresql+asyncpg://uat:local@127.0.0.1:notaport/snapper_pnl_uat_run",
        "not a database url",
    ],
)
def test_validate_target_database_rejects_unsafe_urls(db_url: str) -> None:
    """Reject every dialect, host, and namespace outside the UAT fence.

    Given: A URL using an unsafe dialect, host form, query, or database name,
    When: The target database safety fence validates it,
    Then: The URL is rejected before any database access can occur.
    """
    with pytest.raises(pnl_uat_fixture.PnlUatFixtureError):
        pnl_uat_fixture.validate_target_database(db_url)


def test_parse_anchor_is_utc_minute_deterministic() -> None:
    """Floor the default clock and normalize an explicit offset anchor.

    Given: A fixed clock and an offset-aware whole-minute anchor,
    When: The anchor parser derives or parses the requested instant,
    Then: Both results are deterministic whole minutes in UTC.
    """
    now = datetime(2026, 7, 23, 12, 34, 56, 789, tzinfo=UTC)

    assert pnl_uat_fixture.parse_anchor(None, now) == datetime(2026, 7, 23, 12, 34, tzinfo=UTC)
    assert pnl_uat_fixture.parse_anchor("2026-07-23T14:00:00+02:00", now) == datetime(
        2026, 7, 23, 12, 0, tzinfo=UTC
    )


@pytest.mark.parametrize(
    "raw",
    [
        "not-a-datetime",
        "2026-07-23T12:00:00",
        "2026-07-23T12:00:01Z",
        "2026-07-23T12:35:00Z",
    ],
)
def test_parse_anchor_rejects_ambiguous_or_future_values(raw: str) -> None:
    """Reject malformed, naive, sub-minute, and future anchor values.

    Given: An invalid, timezone-ambiguous, sub-minute, or future anchor,
    When: The anchor parser validates it against a fixed clock,
    Then: A safe fixture refusal is raised.
    """
    now = datetime(2026, 7, 23, 12, 34, 56, tzinfo=UTC)

    with pytest.raises(pnl_uat_fixture.PnlUatFixtureError):
        pnl_uat_fixture.parse_anchor(raw, now)


def test_manifest_is_stable_and_contains_no_credential_material() -> None:
    """Build byte-stable public expectations without credential content.

    Given: The same deterministic fixture anchor,
    When: Two manifests are built and serialized,
    Then: Their identities and economics match without credential material.
    """
    first = pnl_uat_fixture.build_manifest(_ANCHOR)
    second = pnl_uat_fixture.build_manifest(_ANCHOR)
    serialized = first.model_dump_json()

    assert first == second
    assert first.version == 2
    assert first.mode == "paper"
    assert first.times.window_from == _ANCHOR - timedelta(hours=24)
    usd_components = (
        first.expected.complete.usd.realized_pnl,
        first.expected.complete.usd.fee_pnl,
        first.expected.complete.usd.accrual_pnl,
        first.expected.complete.usd.unrealized_pnl,
        first.expected.complete.usd.net_pnl,
    )
    assert usd_components == _EXPECTED_COMPLETE["USD"].components()
    pln_components = (
        first.expected.complete.pln.realized_pnl,
        first.expected.complete.pln.fee_pnl,
        first.expected.complete.pln.accrual_pnl,
        first.expected.complete.pln.unrealized_pnl,
        first.expected.complete.pln.net_pnl,
    )
    assert pln_components == _EXPECTED_COMPLETE["PLN"].components()
    eur_components = (
        first.expected.complete.eur.realized_pnl,
        first.expected.complete.eur.fee_pnl,
        first.expected.complete.eur.accrual_pnl,
        first.expected.complete.eur.unrealized_pnl,
        first.expected.complete.eur.net_pnl,
    )
    assert eur_components == _EXPECTED_COMPLETE["EUR"].components()
    assert first.expected.incomplete.incomplete_point_count == _EXPECTED_INCOMPLETE_POINT_COUNT
    assert first.expected.incomplete.reason == _EXPECTED_INCOMPLETE_REASON
    assert (
        first.expected.incomplete.trigger_instrument_public_id == _EXPECTED_INCOMPLETE_INSTRUMENT_ID
    )
    assert (
        first.expected.incomplete.realized_pnl,
        first.expected.incomplete.fee_pnl,
        first.expected.incomplete.accrual_pnl,
        first.expected.incomplete.unrealized_pnl,
        first.expected.incomplete.net_pnl,
    ) == (0.0, 0.0, 0.0, None, None)
    assert first.expected.marker_kinds == ("fill", "signal", "ai_decision")
    assert first.expected.marker_outcomes == ("executed", "no_fill", "rejected")
    assert "password" not in serialized.lower()
    assert "secret" not in serialized.lower()
    assert "credential" not in serialized.lower()
    assert "change-me-after-first-login" not in serialized
    assert "postgresql+asyncpg" not in serialized


def test_write_manifest_creates_once_and_refuses_unsafe_paths(tmp_path: Path) -> None:
    """Create one manifest without overwriting or creating parent directories.

    Given: A new manifest path, then an occupied and a parentless path,
    When: Manifest creation is attempted for each path,
    Then: Only the new path is written and both unsafe paths are refused.
    """
    manifest = pnl_uat_fixture.build_manifest(_ANCHOR)
    manifest_path = tmp_path / "manifest.json"

    pnl_uat_fixture.write_manifest(manifest_path, manifest)

    loaded = pnl_uat_fixture.PnlUatManifest.model_validate_json(
        manifest_path.read_text(encoding="utf-8")
    )
    assert loaded == manifest
    with pytest.raises(pnl_uat_fixture.PnlUatFixtureError, match="already exists"):
        pnl_uat_fixture.write_manifest(manifest_path, manifest)
    with pytest.raises(pnl_uat_fixture.PnlUatFixtureError, match="parent"):
        pnl_uat_fixture.write_manifest(tmp_path / "missing" / "manifest.json", manifest)


def test_write_manifest_wraps_creation_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Convert filesystem creation failures into a safe one-shot refusal.

    Given: A new manifest path whose exclusive open operation fails,
    When: The fixture attempts to create the manifest,
    Then: The operating-system failure becomes a credential-free refusal.
    """
    manifest_path = tmp_path / "manifest.json"
    manifest = pnl_uat_fixture.build_manifest(_ANCHOR)

    def fail_open(_path: Path, *_args: object, **_kwargs: object) -> Never:
        raise OSError("forced test failure")

    monkeypatch.setattr(Path, "open", fail_open)

    with pytest.raises(pnl_uat_fixture.PnlUatFixtureError, match="creation failed"):
        pnl_uat_fixture.write_manifest(manifest_path, manifest)


def test_write_manifest_refuses_symlink_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject a manifest target that redirects the exclusive write.

    Given: A new manifest path reported as a symlink.
    When: The fixture attempts to create the manifest exclusively.
    Then: Path validation refuses the target before any file is written.
    """
    manifest_path = tmp_path / "manifest.json"
    manifest = pnl_uat_fixture.build_manifest(_ANCHOR)

    def fake_is_symlink(path: Path) -> bool:
        """Report only the selected manifest as a symlink."""
        return path == manifest_path

    monkeypatch.setattr(Path, "is_symlink", fake_is_symlink)
    with pytest.raises(pnl_uat_fixture.PnlUatFixtureError, match="path is unsafe"):
        pnl_uat_fixture.write_manifest(manifest_path, manifest)


@pytest.mark.asyncio
async def test_seed_fixture_database_revalidates_and_preserves_the_exact_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Revalidate a parsed URL without losing encoded connection components.

    Given: A manually parsed safe URL with encoded credentials,
    When: The public database seed function invokes its engine-layer helper,
    Then: A freshly validated equivalent URL and the anchor are forwarded.
    """
    caller_url = make_url(
        "postgresql+asyncpg://uat:p%40ss%3Aword@127.0.0.1:5432/snapper_pnl_uat_public_seed"
    )
    expected_manifest = pnl_uat_fixture.build_manifest(_ANCHOR)

    async def fake_seed_unchecked(db_url: URL, anchor: datetime) -> pnl_uat_fixture.PnlUatManifest:
        assert db_url is not caller_url
        assert db_url.render_as_string(hide_password=False) == caller_url.render_as_string(
            hide_password=False
        )
        assert db_url.password == "p@ss:word"
        assert anchor == _ANCHOR
        return expected_manifest

    monkeypatch.setattr(
        pnl_uat_fixture,
        "_seed_fixture_database_unchecked",
        fake_seed_unchecked,
    )

    assert await pnl_uat_fixture.seed_fixture_database(caller_url, _ANCHOR) == expected_manifest


@pytest.mark.parametrize(
    "unsafe_url",
    [
        URL.create(
            "postgresql+asyncpg",
            username="uat",
            password="local",
            host="example.com",
            database="snapper_pnl_uat_remote",
        ),
        URL.create(
            "postgresql+asyncpg",
            username="uat",
            password="local",
            host="127.0.0.1",
            database="production",
        ),
        URL.create(
            "postgresql+asyncpg",
            username="uat",
            password="local",
            host="127.0.0.1",
            database="snapper_pnl_uat_query_override",
            query={"host": "example.com"},
        ),
        URL.create(
            "postgresql+asyncpg",
            username="uat",
            password="local",
            host="127.0.0.1",
            database="snapper_pnl_uat_multihost",
            query={"host": ("127.0.0.1", "example.com"), "port": ("5432", "5432")},
        ),
    ],
)
@pytest.mark.asyncio
async def test_seed_fixture_database_rejects_forged_url_objects(
    unsafe_url: URL,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject remote, renamed, query-override, and multihost parsed URLs.

    Given: A caller-forged immutable URL that was never returned by validation,
    When: The public seed boundary receives that object,
    Then: It refuses before the unchecked database helper can run.
    """

    async def fail_unchecked(_db_url: URL, _anchor: datetime) -> Never:
        raise AssertionError("unchecked database helper must not run")

    monkeypatch.setattr(
        pnl_uat_fixture,
        "_seed_fixture_database_unchecked",
        fail_unchecked,
    )

    with pytest.raises(pnl_uat_fixture.PnlUatFixtureError) as error:
        await pnl_uat_fixture.seed_fixture_database(unsafe_url, _ANCHOR)

    assert unsafe_url.password is not None
    assert unsafe_url.password not in str(error.value)


@pytest.mark.asyncio
async def test_run_fixture_refuses_existing_manifest_before_database_access(
    tmp_path: Path,
) -> None:
    """Refuse an existing artifact before parsing or contacting a database.

    Given: An occupied manifest path and an invalid database URL,
    When: The public fixture runner is invoked,
    Then: The path collision is reported before database validation or access.
    """
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text("occupied", encoding="utf-8")

    with pytest.raises(pnl_uat_fixture.PnlUatFixtureError, match="already exists"):
        await pnl_uat_fixture.run_fixture("not a database url", _ANCHOR, manifest_path)


@pytest.mark.asyncio
async def test_run_fixture_refuses_a_missing_manifest_parent(tmp_path: Path) -> None:
    """Refuse an absent manifest parent before validating the database URL.

    Given: A manifest path under a missing parent and an invalid database URL,
    When: The public fixture runner is invoked,
    Then: The parent-path refusal occurs before database validation or access.
    """
    with pytest.raises(pnl_uat_fixture.PnlUatFixtureError, match="parent"):
        await pnl_uat_fixture.run_fixture(
            "not a database url",
            _ANCHOR,
            tmp_path / "missing" / "manifest.json",
        )


@pytest.mark.asyncio
async def test_run_fixture_validates_raw_url_and_writes_the_committed_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Route only the exact parsed URL to public seed and persist its manifest.

    Given: A raw URL whose validator returns one immutable parsed URL,
    When: The public fixture runner seeds and writes the fixture,
    Then: Seed receives the identical URL and the committed manifest is persisted.
    """
    raw_url = "postgresql+asyncpg://uat:local@127.0.0.1:5432/snapper_pnl_uat_public_run"
    validated_url = make_url(raw_url)
    expected_manifest = pnl_uat_fixture.build_manifest(_ANCHOR)
    manifest_path = tmp_path / "manifest.json"

    def fake_validate(candidate: str) -> URL:
        assert candidate == raw_url
        return validated_url

    async def fake_seed(db_url: URL, anchor: datetime) -> pnl_uat_fixture.PnlUatManifest:
        assert db_url is validated_url
        assert anchor == _ANCHOR
        return expected_manifest

    monkeypatch.setattr(pnl_uat_fixture, "validate_target_database", fake_validate)
    monkeypatch.setattr(pnl_uat_fixture, "seed_fixture_database", fake_seed)

    actual = await pnl_uat_fixture.run_fixture(raw_url, _ANCHOR, manifest_path)

    assert actual == expected_manifest
    assert (
        pnl_uat_fixture.PnlUatManifest.model_validate_json(
            manifest_path.read_text(encoding="utf-8")
        )
        == expected_manifest
    )


def test_main_requires_db_url(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Return a safe CLI error when the environment has no database URL.

    Given: CLI arguments with no DB_URL environment variable,
    When: The public main function runs,
    Then: It returns status two and emits only the required-URL error.
    """
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setattr(
        sys,
        "argv",
        ["pnl_uat_fixture.py", "--manifest", str(tmp_path / "manifest.json")],
    )

    assert pnl_uat_fixture.main() == 2
    assert capsys.readouterr().err == "error: DB_URL is required\n"


def test_main_reports_fixture_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Return a safe CLI error for an invalid requested anchor.

    Given: A database URL and malformed CLI anchor,
    When: The public main function parses the request,
    Then: It returns status two and emits the anchor validation error.
    """
    monkeypatch.setenv(
        "DB_URL",
        "postgresql+asyncpg://uat:local@127.0.0.1/snapper_pnl_uat_cli_error",
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "pnl_uat_fixture.py",
            "--manifest",
            str(tmp_path / "manifest.json"),
            "--anchor",
            "not-a-datetime",
        ],
    )

    assert pnl_uat_fixture.main() == 2
    assert "anchor must be a valid ISO-8601 datetime" in capsys.readouterr().err


def test_main_normalizes_a_malformed_database_port(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Return status two without a traceback for a malformed URL port.

    Given: A loopback-looking asyncpg URL whose port is not numeric,
    When: The public CLI validates the target,
    Then: It emits only the generic safe URL refusal and returns status two.
    """
    monkeypatch.setenv(
        "DB_URL",
        "postgresql+asyncpg://uat:local@127.0.0.1:notaport/snapper_pnl_uat_bad_port",
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "pnl_uat_fixture.py",
            "--manifest",
            str(tmp_path / "manifest.json"),
            "--anchor",
            _ANCHOR.isoformat(),
        ],
    )

    assert pnl_uat_fixture.main() == 2
    output = capsys.readouterr()
    assert output.out == ""
    assert output.err == "error: DB_URL is not a valid SQLAlchemy URL\n"
    assert "Traceback" not in output.err


def test_main_reports_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Run the public CLI path and report the committed anchor and manifest.

    Given: Valid CLI inputs and a successful fixture runner,
    When: The public main function completes,
    Then: It returns zero and reports the committed anchor and manifest path.
    """
    raw_url = "postgresql+asyncpg://uat:local@127.0.0.1:5432/snapper_pnl_uat_cli_success"
    manifest_path = tmp_path / "manifest.json"
    expected_manifest = pnl_uat_fixture.build_manifest(_ANCHOR)

    async def fake_run(
        db_url: str,
        anchor: datetime,
        path: Path,
    ) -> pnl_uat_fixture.PnlUatManifest:
        assert db_url == raw_url
        assert anchor == _ANCHOR
        assert path == manifest_path
        return expected_manifest

    monkeypatch.setenv("DB_URL", raw_url)
    monkeypatch.setattr(pnl_uat_fixture, "run_fixture", fake_run)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "pnl_uat_fixture.py",
            "--manifest",
            str(manifest_path),
            "--anchor",
            _ANCHOR.isoformat(),
        ],
    )

    assert pnl_uat_fixture.main() == 0
    output = capsys.readouterr()
    assert output.err == ""
    assert expected_manifest.times.anchor.isoformat() in output.out
    assert str(manifest_path) in output.out


def test_alembic_head_rejects_a_headless_checkout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refuse a checkout whose migration graph has no current head.

    Given: An Alembic script directory that exposes no current head,
    When: The fixture resolves the repository migration head,
    Then: A safe headless-checkout refusal is raised.
    """

    class HeadlessDirectory:
        """Represent a test migration graph without a head."""

        def get_current_head(self) -> None:
            """Return no migration head."""
            return None

    class HeadlessDirectoryFactory:
        """Build the headless test graph."""

        @staticmethod
        def from_config(_config: Config) -> HeadlessDirectory:
            """Return the headless graph for any Alembic config."""
            return HeadlessDirectory()

    monkeypatch.setattr(
        pnl_uat_fixture,
        "ScriptDirectory",
        HeadlessDirectoryFactory,
    )

    with pytest.raises(pnl_uat_fixture.PnlUatFixtureError, match="no Alembic head"):
        pnl_uat_fixture._alembic_head()


@pytest.mark.asyncio
async def test_postgresql_stable_cut_locks_every_mapped_table_in_sorted_order() -> None:
    """Acquire one deterministic writer-exclusive lock for the entire ORM schema.

    Given: A PostgreSQL fixture transaction,
    When: The stable-cut lock helper runs,
    Then: Every mapped table is named once under a writer-exclusive lock mode.
    """
    session_mock = AsyncMock(spec=AsyncSession)
    session = cast(AsyncSession, session_mock)

    await pnl_uat_fixture._lock_fixture_tables_for_stable_cut(session, "postgresql")

    statement = str(session_mock.execute.await_args.args[0])
    prefix = "LOCK TABLE "
    suffix = " IN SHARE ROW EXCLUSIVE MODE"
    assert statement.startswith(prefix)
    assert statement.endswith(suffix)
    locked_names = [
        name.removeprefix('"').removesuffix('"')
        for name in statement.removeprefix(prefix).removesuffix(suffix).split(", ")
    ]
    expected_names = sorted(table.name for table in pnl_uat_fixture.Base.metadata.tables.values())
    assert locked_names == expected_names
    assert len(locked_names) == len(set(locked_names))


@pytest.mark.asyncio
async def test_seed_preconditions_lock_after_schema_and_before_mutable_checks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pin the stable-cut lock between schema validation and all safety reads.

    Given: Instrumented precondition helpers,
    When: PostgreSQL seed preconditions execute,
    Then: Schema is checked first and the table lock precedes every mutable read.
    """
    events: list[str] = []

    async def schema(_session: AsyncSession) -> None:
        events.append("schema")

    async def lock(_session: AsyncSession, dialect_name: str) -> None:
        assert dialect_name == "postgresql"
        events.append("lock")

    async def credentials(_session: AsyncSession) -> None:
        events.append("credentials")

    async def namespace(_session: AsyncSession) -> None:
        events.append("namespace")

    async def baseline(_session: AsyncSession) -> None:
        events.append("baseline")

    monkeypatch.setattr(pnl_uat_fixture, "_require_schema_head", schema)
    monkeypatch.setattr(pnl_uat_fixture, "_lock_fixture_tables_for_stable_cut", lock)
    monkeypatch.setattr(pnl_uat_fixture, "_require_paper_only_credentials", credentials)
    monkeypatch.setattr(pnl_uat_fixture, "_require_pristine_namespace", namespace)
    monkeypatch.setattr(pnl_uat_fixture, "_require_fresh_oss_seed_baseline", baseline)

    await pnl_uat_fixture._require_seed_preconditions(
        cast(AsyncSession, AsyncMock(spec=AsyncSession)),
        "postgresql",
    )

    assert events == ["schema", "lock", "credentials", "namespace", "baseline"]


@pytest.mark.asyncio
async def test_seed_normalizes_local_connection_refusal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Convert a local engine connection failure into a safe fixture refusal.

    Given: A validated-looking local URL whose engine construction refuses,
    When: The unchecked engine boundary is exercised,
    Then: The operating-system error is wrapped without exposing URL material.
    """

    def refuse_engine(
        _db_url: URL,
        *,
        poolclass: type[Pool],
    ) -> Never:
        assert poolclass is pnl_uat_fixture.NullPool
        raise ConnectionRefusedError("forced local refusal")

    monkeypatch.setattr(pnl_uat_fixture, "create_async_engine", refuse_engine)
    never_opened_db_url = make_url("sqlite+aiosqlite:////tmp/pnl-uat-never-opened.db")

    with pytest.raises(
        pnl_uat_fixture.PnlUatFixtureError,
        match="no transaction committed",
    ) as error:
        await pnl_uat_fixture._seed_fixture_database_unchecked(
            never_opened_db_url,
            _ANCHOR,
        )

    assert isinstance(error.value.__cause__, ConnectionRefusedError)


@pytest.mark.asyncio
async def test_seed_rejects_a_database_behind_schema_head(
    oss_seeded_db_url: URL,
) -> None:
    """Reject a disposable database whose migration version is not current.

    Given: A seeded SQLite clone with a stale Alembic version,
    When: The internal fixture transaction validates its schema,
    Then: It refuses the database before inserting fixture rows.
    """
    await _execute_sql(
        oss_seeded_db_url,
        "UPDATE alembic_version SET version_num = '0000'",
    )

    with pytest.raises(pnl_uat_fixture.PnlUatFixtureError, match="schema is not at head"):
        await pnl_uat_fixture._seed_fixture_database_unchecked(
            oss_seeded_db_url,
            _ANCHOR,
        )


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE users SET role = 'viewer' WHERE username = 'admin'",
        "UPDATE users SET email = 'changed@example.test' WHERE username = 'admin'",
        "UPDATE users SET default_language = 'pl' WHERE username = 'admin'",
        "UPDATE users SET known_to = '2030-01-01 00:00:00+00:00' WHERE username = 'admin'",
        "UPDATE settings SET key = 'unexpected_uat_key' WHERE key = 'ui_origin'",
        "UPDATE settings SET value = 'enabled' WHERE key = 'live_trading_mode'",
        "UPDATE settings SET category = 'unsafe' WHERE key = 'live_trading_mode'",
        "UPDATE settings SET description = 'changed' WHERE key = 'live_trading_mode'",
        "UPDATE settings SET updated_by = 'tester' WHERE key = 'live_trading_mode'",
        "UPDATE settings SET known_to = '2030-01-01 00:00:00+00:00' WHERE key = 'ui_origin'",
        "UPDATE settings SET value = 'not-a-token' WHERE key = 'gemini_api_key'",
        "UPDATE symbols SET base = 'XBT' WHERE native_symbol = 'BTC-USD'",
        (
            "UPDATE symbols SET known_to = '2030-01-01 00:00:00+00:00' "
            "WHERE native_symbol = 'BTC-USD'"
        ),
        (
            "UPDATE symbol_aliases SET exchange_symbol = 'X:BTCUSD-MUTATED' "
            "WHERE exchange = 'polygon' AND channel = 'rest' "
            "AND exchange_symbol = 'X:BTCUSD'"
        ),
        (
            "UPDATE symbol_exchange_capabilities SET can_trade = 1, source = 'mutated' "
            "WHERE exchange = 'polygon' AND symbol_public_id = "
            "(SELECT public_id FROM symbols WHERE native_symbol = 'BTC-USD')"
        ),
        "UPDATE wallets SET label = 'unexpected baseline wallet' WHERE label = 'paper'",
        "UPDATE wallets SET description = 'changed' WHERE label = 'paper'",
        "UPDATE wallet_credentials SET label = 'changed'",
        "UPDATE operators SET description = 'changed'",
        "UPDATE user_operator_memberships SET is_primary = 0 WHERE id = 1",
    ],
)
@pytest.mark.asyncio
async def test_seed_rejects_semantically_mutated_oss_baselines(
    oss_seeded_db_url: URL,
    statement: str,
) -> None:
    """Reject count-preserving mutations across every semantic baseline guard.

    Given: A fresh seed clone with one count-preserving semantic mutation,
    When: The fixture validates the exact bundled OSS baseline,
    Then: It refuses the clone without inserting instruments or backdating symbols.
    """
    before = await _symbol_timestamps(oss_seeded_db_url)
    await _execute_sql(oss_seeded_db_url, statement)

    with pytest.raises(
        pnl_uat_fixture.PnlUatFixtureError,
        match="fresh migrated and bundled-OSS-seeded",
    ):
        await pnl_uat_fixture._seed_fixture_database_unchecked(
            oss_seeded_db_url,
            _ANCHOR,
        )

    if not statement.startswith("UPDATE symbols"):
        assert await _symbol_timestamps(oss_seeded_db_url) == before
    assert await _instrument_count(oss_seeded_db_url) == 0


@pytest.mark.parametrize("username", ["admin", "operator", "viewer"])
@pytest.mark.asyncio
async def test_seed_rejects_changed_seed_passwords(
    oss_seeded_db_url: URL,
    username: str,
) -> None:
    """Verify the canonical dev password for every seeded login identity.

    Given: A valid bcrypt hash for a non-default password on one seeded user,
    When: The exact baseline guard verifies all login credentials,
    Then: It refuses before any fixture row is inserted.
    """
    await _replace_seed_password(oss_seeded_db_url, username)

    with pytest.raises(
        pnl_uat_fixture.PnlUatFixtureError,
        match="fresh migrated and bundled-OSS-seeded",
    ):
        await pnl_uat_fixture._seed_fixture_database_unchecked(
            oss_seeded_db_url,
            _ANCHOR,
        )

    assert await _instrument_count(oss_seeded_db_url) == 0


@pytest.mark.asyncio
async def test_seed_rejects_a_malformed_seed_password_hash(
    oss_seeded_db_url: URL,
) -> None:
    """Normalize malformed bcrypt evidence into a baseline refusal.

    Given: A seeded login whose password hash is not valid bcrypt,
    When: The exact baseline guard verifies the default dev password,
    Then: It refuses without leaking hash or password material.
    """
    await _execute_sql(
        oss_seeded_db_url,
        "UPDATE users SET password_hash = 'malformed' WHERE username = 'admin'",
    )

    with pytest.raises(
        pnl_uat_fixture.PnlUatFixtureError,
        match="fresh migrated and bundled-OSS-seeded",
    ):
        await pnl_uat_fixture._seed_fixture_database_unchecked(
            oss_seeded_db_url,
            _ANCHOR,
        )


@pytest.mark.parametrize(
    "envelope",
    [
        {"initial_balance": "9999.0"},
        {"initial_balance": "10000.0", "unexpected": "opaque-test-value"},
    ],
)
@pytest.mark.asyncio
async def test_seed_rejects_changed_encrypted_paper_envelopes(
    oss_seeded_db_url: URL,
    envelope: dict[str, str],
) -> None:
    """Require exactly the harmless default paper balance envelope.

    Given: A decryptable paper credential with a changed balance or extra field,
    When: The exact baseline guard inspects the encrypted envelope,
    Then: It refuses without exposing the plaintext payload.
    """
    await _replace_paper_envelope(oss_seeded_db_url, envelope)

    with pytest.raises(
        pnl_uat_fixture.PnlUatFixtureError,
        match="fresh migrated and bundled-OSS-seeded",
    ):
        await pnl_uat_fixture._seed_fixture_database_unchecked(
            oss_seeded_db_url,
            _ANCHOR,
        )


@pytest.mark.asyncio
async def test_seed_rejects_an_undecryptable_paper_envelope(
    oss_seeded_db_url: URL,
) -> None:
    """Treat corrupted encrypted paper evidence as a baseline refusal.

    Given: A paper credential whose encrypted envelope is not a Fernet token,
    When: The baseline guard attempts safe decryption,
    Then: It refuses without inserting fixture rows.
    """
    await _execute_sql(
        oss_seeded_db_url,
        "UPDATE wallet_credentials SET encrypted_payload = 'corrupted'",
    )

    with pytest.raises(
        pnl_uat_fixture.PnlUatFixtureError,
        match="fresh migrated and bundled-OSS-seeded",
    ):
        await pnl_uat_fixture._seed_fixture_database_unchecked(
            oss_seeded_db_url,
            _ANCHOR,
        )


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE users SET username = 'former-admin' WHERE username = 'admin'",
        "UPDATE users SET known_to = '2030-01-01 00:00:00+00:00' WHERE username = 'admin'",
    ],
)
@pytest.mark.asyncio
async def test_admin_guard_rejects_missing_or_inactive_seed_identity(
    oss_seeded_db_url: URL,
    statement: str,
) -> None:
    """Cover both absence and temporal invalidity of the required admin.

    Given: A seeded admin that is renamed or no longer temporally current,
    When: The internal admin identity guard runs,
    Then: It refuses to produce manual-command lineage.
    """
    await _execute_sql(oss_seeded_db_url, statement)

    with pytest.raises(pnl_uat_fixture.PnlUatFixtureError, match="active admin"):
        await _require_admin_for_test(oss_seeded_db_url)


@pytest.mark.parametrize(
    "statement,error",
    [
        (
            "UPDATE symbols SET native_symbol = 'X-BTC' WHERE native_symbol = 'BTC-USD'",
            "symbols are missing",
        ),
        (
            "UPDATE symbols SET base = 'XBT' WHERE native_symbol = 'BTC-USD'",
            "metadata is incompatible",
        ),
    ],
)
@pytest.mark.asyncio
async def test_symbol_guard_rejects_missing_or_incompatible_seed_symbols(
    oss_seeded_db_url: URL,
    statement: str,
    error: str,
) -> None:
    """Cover both structural and semantic canonical-symbol failures.

    Given: A required canonical symbol that is renamed or semantically altered,
    When: The internal symbol preparation guard runs,
    Then: It refuses to backdate or return fixture symbol identities.
    """
    await _execute_sql(oss_seeded_db_url, statement)

    with pytest.raises(pnl_uat_fixture.PnlUatFixtureError, match=error):
        await _prepare_symbols_for_test(oss_seeded_db_url)


def test_candle_close_rejects_an_unknown_source_instrument() -> None:
    """Refuse any source identity outside the deterministic candle fixture.

    Given: A source instrument identity outside the fixture namespace,
    When: Its deterministic candle close is requested,
    Then: The unsupported identity is rejected.
    """
    window_start = _ANCHOR - timedelta(hours=12)

    with pytest.raises(pnl_uat_fixture.PnlUatFixtureError, match="unsupported candle"):
        pnl_uat_fixture._candle_close(
            "00000000-0000-7000-8000-00000000ffff",
            _ANCHOR,
            window_start,
            _ANCHOR,
        )


@pytest.mark.asyncio
async def test_seed_rejects_active_nonpaper_credentials(
    oss_seeded_db_url: URL,
) -> None:
    """Reject opaque non-paper credentials before fixture writes.

    Given: A fresh seed clone containing one active non-paper credential,
    When: The internal fixture transaction validates credential safety,
    Then: It refuses the clone before inserting any instrument.
    """
    await _add_nonpaper_credential(oss_seeded_db_url)

    with pytest.raises(
        pnl_uat_fixture.PnlUatFixtureError,
        match="active non-paper credentials",
    ):
        await pnl_uat_fixture._seed_fixture_database_unchecked(
            oss_seeded_db_url,
            _ANCHOR,
        )

    assert await _instrument_count(oss_seeded_db_url) == 0


@pytest.mark.asyncio
async def test_seed_rejects_contaminated_baseline_before_backdating_symbols(
    oss_seeded_db_url: URL,
) -> None:
    """Reject unrelated prior rows before mutating canonical symbol history.

    Given: A fresh seed clone containing one unrelated paper wallet,
    When: The exact baseline guard runs,
    Then: It refuses the clone without backdating symbols or inserting instruments.
    """
    before = await _symbol_timestamps(oss_seeded_db_url)
    await _add_wallet(
        oss_seeded_db_url,
        "00000000-0000-7000-8000-00000000c201",
        "unrelated prior UAT",
    )

    with pytest.raises(
        pnl_uat_fixture.PnlUatFixtureError,
        match="fresh migrated and bundled-OSS-seeded",
    ):
        await pnl_uat_fixture._seed_fixture_database_unchecked(
            oss_seeded_db_url,
            _ANCHOR,
        )

    assert await _symbol_timestamps(oss_seeded_db_url) == before
    assert await _instrument_count(oss_seeded_db_url) == 0


@pytest.mark.asyncio
async def test_seed_rejects_fixture_namespace_collision(
    oss_seeded_db_url: URL,
) -> None:
    """Report the stable fixture collision before the generic baseline fence.

    Given: A seeded clone containing one stable fixture identity and label,
    When: The namespace collision guard runs,
    Then: It emits the specific one-shot collision and inserts no instrument.
    """
    manifest = pnl_uat_fixture.build_manifest(_ANCHOR)
    await _add_wallet(
        oss_seeded_db_url,
        manifest.ids.happy_wallet_public_id,
        manifest.happy_wallet_label,
    )

    with pytest.raises(
        pnl_uat_fixture.PnlUatFixtureError,
        match="fixture namespace is already populated",
    ):
        await pnl_uat_fixture._seed_fixture_database_unchecked(
            oss_seeded_db_url,
            _ANCHOR,
        )

    assert await _instrument_count(oss_seeded_db_url) == 0


@pytest.mark.asyncio
async def test_seed_rolls_back_symbol_backdating_on_insertion_failure(
    oss_seeded_db_url: URL,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Roll back preparatory symbol mutations when fixture construction fails.

    Given: A fresh clone whose fixture-row construction raises a database error,
    When: The one-transaction internal seed runs,
    Then: Symbol backdating and all fixture inserts are rolled back.
    """
    before = await _symbol_timestamps(oss_seeded_db_url)

    def fail_fixture_rows(
        _admin_user_public_id: str,
        _symbols: dict[str, str],
        _anchor: datetime,
    ) -> list[object]:
        raise SQLAlchemyError("forced test failure")

    monkeypatch.setattr(pnl_uat_fixture, "_fixture_rows", fail_fixture_rows)

    with pytest.raises(
        pnl_uat_fixture.PnlUatFixtureError,
        match="no transaction committed",
    ):
        await pnl_uat_fixture._seed_fixture_database_unchecked(
            oss_seeded_db_url,
            _ANCHOR,
        )

    assert await _symbol_timestamps(oss_seeded_db_url) == before
    assert await _instrument_count(oss_seeded_db_url) == 0


@pytest.mark.asyncio
async def test_activation_anchor_failure_requires_discard_without_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Require disposal after the public anchor writer fails post-commit.

    Given: A fixture manifest whose production anchor writer fails after facts commit,
    When: The six-anchor phase begins,
    Then: It emits only credential-free discard guidance and stops after first call.
    """
    writer = AsyncMock(side_effect=RuntimeError("sensitive backend detail"))
    monkeypatch.setattr(pnl_uat_fixture, "ensure_wallet_pnl_anchor", writer)
    db_url = make_url(f"sqlite+aiosqlite:///{tmp_path / 'post-commit.db'}")
    fixture_manifest = pnl_uat_fixture.build_manifest(_ANCHOR)

    with pytest.raises(
        pnl_uat_fixture.PnlUatFixtureError,
        match="discard the one-shot database",
    ) as error:
        await pnl_uat_fixture._seed_activation_anchors(
            db_url,
            fixture_manifest,
        )

    assert writer.await_count == 1
    assert "sensitive backend detail" not in str(error.value)


def _assert_complete_point(
    result: PnlWalletTimelineResult,
    expected: _ExpectedEconomics,
) -> None:
    """Assert exact latest-point economics and display decomposition."""
    point = result.series.points[-1]

    assert point.valuation_status == "complete"
    assert point.realized_pnl == expected.realized_pnl
    assert point.fee_pnl == expected.fee_pnl
    assert point.accrual_pnl == expected.accrual_pnl
    assert point.unrealized_pnl == expected.unrealized_pnl
    assert point.net_pnl == expected.net_pnl
    contributions = {row.native_symbol: row for row in point.per_instrument}
    assert set(contributions) == {"EUR-PLN", "BTC-USD"}
    assert contributions["EUR-PLN"].exchange == "walutomat"
    assert contributions["EUR-PLN"].fee_pnl == expected.fee_pnl
    assert contributions["EUR-PLN"].unrealized_pnl == expected.unrealized_pnl
    assert contributions["BTC-USD"].exchange == "kraken"
    assert contributions["BTC-USD"].fee_pnl == 0.0
    assert contributions["BTC-USD"].unrealized_pnl == 0.0
    attribution = {(row.origin, row.strategy_name): row for row in point.attribution}
    assert set(attribution) == {("manual", None), ("system", "momentum")}
    assert attribution[("manual", None)].fee_pnl == expected.fee_pnl
    assert attribution[("manual", None)].unrealized_pnl == expected.unrealized_pnl
    assert attribution[("system", "momentum")].fee_pnl == 0.0
    assert attribution[("system", "momentum")].unrealized_pnl == 0.0


def _assert_complete_point_reconciles(point: PnlTimelinePoint) -> None:
    """Reconcile every aggregate component to instrument and attribution sums."""
    assert point.realized_pnl is not None
    assert point.fee_pnl is not None
    assert point.accrual_pnl is not None
    assert point.unrealized_pnl is not None
    assert point.net_pnl is not None
    instrument_realized = 0.0
    instrument_fees = 0.0
    instrument_accruals = 0.0
    instrument_unrealized = 0.0
    for contribution in point.per_instrument:
        realized = contribution.realized_pnl
        fee = contribution.fee_pnl
        accrual = contribution.accrual_pnl
        unrealized = contribution.unrealized_pnl
        assert realized is not None
        assert fee is not None
        assert accrual is not None
        assert unrealized is not None
        instrument_realized += realized
        instrument_fees += fee
        instrument_accruals += accrual
        instrument_unrealized += unrealized
    attribution_realized = 0.0
    attribution_fees = 0.0
    attribution_accruals = 0.0
    attribution_unrealized = 0.0
    for contribution in point.attribution:
        realized = contribution.realized_pnl
        fee = contribution.fee_pnl
        accrual = contribution.accrual_pnl
        unrealized = contribution.unrealized_pnl
        assert realized is not None
        assert fee is not None
        assert accrual is not None
        assert unrealized is not None
        attribution_realized += realized
        attribution_fees += fee
        attribution_accruals += accrual
        attribution_unrealized += unrealized
    assert point.realized_pnl == instrument_realized == attribution_realized
    assert point.fee_pnl == instrument_fees == attribution_fees
    assert point.accrual_pnl == instrument_accruals == attribution_accruals
    assert point.unrealized_pnl == instrument_unrealized == attribution_unrealized
    assert point.net_pnl == (
        point.realized_pnl + point.fee_pnl + point.accrual_pnl + point.unrealized_pnl
    )


def _assert_complete_trajectory(
    result: PnlWalletTimelineResult,
    expected: _ExpectedEconomics,
    manifest: pnl_uat_fixture.PnlUatManifest,
) -> None:
    """Assert every minute from an independent linear economics contract."""
    points = result.series.points
    assert len(points) == 1_441
    for index, point in enumerate(points):
        assert point.point_time == manifest.times.window_from + timedelta(minutes=index)
        assert point.valuation_status == "complete"
        assert point.incompleteness_reasons == ()
        assert point.realized_pnl == expected.realized_pnl
        assert point.accrual_pnl == expected.accrual_pnl
        _assert_complete_point_reconciles(point)
        if index < _COMPLETE_FILL_INDEX:
            assert point.fee_pnl == 0.0
            assert point.unrealized_pnl == 0.0
            assert point.net_pnl == 0.0
            assert point.per_instrument == ()
            assert point.attribution == ()
        else:
            progress = (index - _COMPLETE_FILL_INDEX) / _COMPLETE_FILL_INDEX
            expected_unrealized = expected.unrealized_pnl * progress
            assert point.fee_pnl == expected.fee_pnl
            assert point.unrealized_pnl == pytest.approx(
                expected_unrealized,
                rel=0.0,
                abs=1e-12,
            )
            assert point.net_pnl == pytest.approx(
                expected.fee_pnl + expected_unrealized,
                rel=0.0,
                abs=1e-12,
            )
            expected_count = 1 if index < 1_440 else 2
            assert len(point.per_instrument) == expected_count
            assert len(point.attribution) == expected_count
    assert (
        points[0].fee_pnl,
        points[0].unrealized_pnl,
        points[719].fee_pnl,
        points[719].unrealized_pnl,
    ) == (0.0, 0.0, 0.0, 0.0)
    assert (
        points[720].fee_pnl,
        points[720].unrealized_pnl,
        points[1080].unrealized_pnl,
        points[1440].fee_pnl,
        points[1440].unrealized_pnl,
    ) == (
        expected.fee_pnl,
        0.0,
        0.5 * expected.unrealized_pnl,
        expected.fee_pnl,
        expected.unrealized_pnl,
    )


def _assert_markers(
    result: PnlWalletTimelineResult,
    manifest: pnl_uat_fixture.PnlUatManifest,
) -> None:
    """Assert stable marker ordering, outcomes, and public identities."""
    assert result.marker_limit == 2_000
    assert result.markers_truncated is False
    assert [(marker.kind, marker.outcome) for marker in result.markers] == [
        ("fill", "executed"),
        ("signal", "no_fill"),
        ("ai_decision", "rejected"),
        ("fill", "executed"),
        ("signal", "executed"),
    ]
    fill_markers = [marker for marker in result.markers if isinstance(marker, PnlFillMarker)]
    signal_markers = [marker for marker in result.markers if isinstance(marker, PnlSignalMarker)]
    ai_markers = [marker for marker in result.markers if isinstance(marker, PnlAiDecisionMarker)]
    assert {marker.execution_public_id for marker in fill_markers} == {
        manifest.ids.eur_pln_execution_public_id,
        manifest.ids.btc_usd_execution_public_id,
    }
    assert {marker.signal_public_id for marker in signal_markers} == {
        manifest.ids.executed_signal_public_id,
        manifest.ids.no_fill_signal_public_id,
    }
    assert len(ai_markers) == 1
    assert ai_markers[0].review_public_id == manifest.ids.ai_review_public_id
    assert ai_markers[0].event_public_id == manifest.ids.ai_event_public_id


@dataclass(frozen=True)
class _FixtureTimelines:
    """Real-service timeline results rebuilt from one seeded fixture."""

    usd: PnlWalletTimelineResult
    pln: PnlWalletTimelineResult
    eur: PnlWalletTimelineResult
    incomplete: PnlWalletTimelineResult


async def _read_fixture_activation_evidence(
    db_url: URL,
) -> tuple[list[PortfolioPnlPoint], list[VenueEvent], list[TradeCommand]]:
    """Read persisted anchors, fill observations, and initiating commands."""
    engine = create_async_engine(db_url)
    try:
        async with AsyncSession(engine) as session:
            anchors = list(
                (
                    await session.execute(
                        select(PortfolioPnlPoint).order_by(
                            PortfolioPnlPoint.wallet_public_id,
                            PortfolioPnlPoint.valuation_ccy,
                        )
                    )
                )
                .scalars()
                .all()
            )
            events = list(
                (
                    await session.execute(
                        select(VenueEvent)
                        .where(VenueEvent.event_type == "fill_observed")
                        .order_by(VenueEvent.instrument)
                    )
                )
                .scalars()
                .all()
            )
            commands = list(
                (await session.execute(select(TradeCommand).order_by(TradeCommand.instrument)))
                .scalars()
                .all()
            )
            return anchors, events, commands
    finally:
        await engine.dispose()


def _assert_seeded_anchors(
    anchors: list[PortfolioPnlPoint],
    manifest: pnl_uat_fixture.PnlUatManifest,
) -> None:
    """Assert all six durable zero anchors and their frozen v2 payloads."""
    assert {(anchor.wallet_public_id, anchor.valuation_ccy) for anchor in anchors} == {
        (manifest.ids.happy_wallet_public_id, "USD"),
        (manifest.ids.happy_wallet_public_id, "PLN"),
        (manifest.ids.happy_wallet_public_id, "EUR"),
        (manifest.ids.incomplete_wallet_public_id, "USD"),
        (manifest.ids.incomplete_wallet_public_id, "PLN"),
        (manifest.ids.incomplete_wallet_public_id, "EUR"),
    }
    assert len(anchors) == 6
    for anchor in anchors:
        assert anchor.point_time == manifest.times.window_from
        assert anchor.point_kind == "anchor"
        assert anchor.epoch_public_id == anchor.public_id
        assert anchor.calc_version == "5A.13"
        assert anchor.valuation_status == "complete"
        assert (
            anchor.realized_pnl,
            anchor.fee_pnl,
            anchor.accrual_pnl,
            anchor.unrealized_pnl,
            anchor.external_flow_adjustment,
        ) == (0.0, 0.0, 0.0, 0.0, 0.0)
        assert (
            anchor.cash_usd,
            anchor.position_value_usd,
            anchor.drawdown,
        ) == (None, None, None)
        assert anchor.mark_source == "finalized_1m_candle_close"
        assert anchor.mark_time == manifest.times.window_from
        assert anchor.watermarks_json == "{}"
        assert (
            anchor.opening_basket_json
            == '{"annulments":[],"native_basket":{},"pools":[],"schema_version":3}'
        )
        assert anchor.contributions_json == '{"pools":[],"schema_version":3}'


def _assert_seeded_fill_events(
    events: list[VenueEvent],
    commands: list[TradeCommand],
    manifest: pnl_uat_fixture.PnlUatManifest,
) -> None:
    """Assert exact fill identities and production-derived accounting shards."""
    events_by_instrument = {event.instrument: event for event in events}
    commands_by_instrument = {command.instrument: command for command in commands}
    assert len(events) == 3
    assert set(events_by_instrument) == {"EUR-PLN", "BTC-USD", "ETH-USD"}
    assert set(commands_by_instrument) == {"EUR-PLN", "BTC-USD"}
    expected = {
        "EUR-PLN": (
            manifest.ids.happy_wallet_public_id,
            "pnl-uat-manual-eur-pln",
            "pnl-uat-exchange-manual",
            "pnl-uat-exec-1-a501",
            "pnl-uat-trade-1-a501",
            None,
        ),
        "BTC-USD": (
            manifest.ids.happy_wallet_public_id,
            "pnl-uat-system-btc-usd",
            "pnl-uat-exchange-system",
            "pnl-uat-exec-2-a502",
            "pnl-uat-trade-2-a502",
            "momentum",
        ),
        "ETH-USD": (
            manifest.ids.incomplete_wallet_public_id,
            "pnl-uat-incomplete-eth-usd",
            "pnl-uat-exchange-incomplete",
            "pnl-uat-exec-1-a503",
            "pnl-uat-trade-1-a503",
            None,
        ),
    }
    for instrument, identity in expected.items():
        wallet_public_id, client_order_id, exchange_order_id, exec_id, trade_id, strategy = identity
        event = events_by_instrument[instrument]
        assert (
            event.wallet_public_id,
            event.client_order_id,
            event.exchange_order_id,
            event.exec_id,
            event.trade_id,
        ) == (
            wallet_public_id,
            client_order_id,
            exchange_order_id,
            exec_id,
            trade_id,
        )
        assert event.shard_key == compute_shard_key(
            instrument=instrument,
            exchange=ExchangeEnum.PAPER,
            mode=ExecutionModeEnum.PAPER,
            wallet_public_id=wallet_public_id,
            strategy_tag=strategy,
        )
        if instrument == "ETH-USD":
            assert event.command_public_id is None
        else:
            command = commands_by_instrument[instrument]
            assert event.command_public_id == command.public_id
            assert event.shard_key == command.shard_key


def _assert_seeded_trade_commands(
    commands: list[TradeCommand],
    manifest: pnl_uat_fixture.PnlUatManifest,
    admin_user_public_id: str,
) -> None:
    """Assert the exact seed-to-column mapping of both initiating commands.

    The builder projects one immutable seed value onto a much wider command
    row, so two transposed seed fields would still yield two well-formed rows.
    Every seeded field is therefore pinned to a distinct expected value, and
    the two rows are chosen so the surface-dependent shard tag and the optional
    signal lineage are each proven in both of their states.
    """
    commands_by_instrument = {command.instrument: command for command in commands}
    assert set(commands_by_instrument) == {"EUR-PLN", "BTC-USD"}
    manual = commands_by_instrument["EUR-PLN"]
    system = commands_by_instrument["BTC-USD"]
    assert (manual.public_id, system.public_id) == (
        pnl_uat_fixture._MANUAL_COMMAND_ID,
        pnl_uat_fixture._SYSTEM_COMMAND_ID,
    )
    assert (manual.wallet_public_id, system.wallet_public_id) == (
        manifest.ids.happy_wallet_public_id,
        manifest.ids.happy_wallet_public_id,
    )
    assert (manual.user_public_id, system.user_public_id) == (admin_user_public_id, None)
    assert (manual.client_order_id, system.client_order_id) == (
        "pnl-uat-manual-eur-pln",
        "pnl-uat-system-btc-usd",
    )
    assert (manual.venue_client_id, system.venue_client_id) == (
        "venue-pnl-uat-manual-eur-pln",
        "venue-pnl-uat-system-btc-usd",
    )
    assert (manual.idempotency_key, system.idempotency_key) == (
        "idempotency-pnl-uat-manual-eur-pln",
        "idempotency-pnl-uat-system-btc-usd",
    )
    assert (manual.exchange_order_id, system.exchange_order_id) == (
        "pnl-uat-exchange-manual",
        "pnl-uat-exchange-system",
    )
    assert (manual.quantity, system.quantity) == (20.04, 1.0)
    assert (manual.price, system.price) == (4.0, 100.0)
    assert (manual.correlation_id, system.correlation_id) == (
        pnl_uat_fixture._MANUAL_CORRELATION_ID,
        pnl_uat_fixture._SYSTEM_CORRELATION_ID,
    )
    assert (manual.sequence_id, system.sequence_id) == (601, 602)
    assert (manual.source_surface, system.source_surface) == ("rest", "strategy")
    assert (manual.strategy_id, system.strategy_id) == ("manual", "momentum")
    assert (manual.signal_public_id, system.signal_public_id) == (
        None,
        manifest.ids.executed_signal_public_id,
    )
    for trade_command, created_at in (
        (manual, manifest.times.eur_pln_fill_at),
        (system, manifest.times.anchor),
    ):
        assert (
            trade_command.created_at,
            trade_command.dispatched_at,
            trade_command.acked_at,
            trade_command.terminal_at,
            trade_command.timestamp,
        ) == (created_at, created_at, created_at, created_at, created_at)
        assert trade_command.known_to == KNOWN_TO_MAX
        assert trade_command.session_id == pnl_uat_fixture._SESSION_ID
    assert manual.shard_key == compute_shard_key(
        instrument="EUR-PLN",
        exchange=ExchangeEnum.PAPER,
        mode=ExecutionModeEnum.PAPER,
        wallet_public_id=manifest.ids.happy_wallet_public_id,
        strategy_tag=None,
    )
    assert system.shard_key == compute_shard_key(
        instrument="BTC-USD",
        exchange=ExchangeEnum.PAPER,
        mode=ExecutionModeEnum.PAPER,
        wallet_public_id=manifest.ids.happy_wallet_public_id,
        strategy_tag="momentum",
    )


async def _build_fixture_timelines(
    db_url: URL,
    manifest: pnl_uat_fixture.PnlUatManifest,
    as_of: datetime,
) -> _FixtureTimelines:
    """Rebuild every UAT valuation and prove anchor reuse is idempotent."""
    repository = SQLAlchemyRepository(db_url.render_as_string(hide_password=False))
    try:
        timelines = _FixtureTimelines(
            usd=await build_wallet_pnl_timeline(
                repository,
                manifest.ids.happy_wallet_public_id,
                "paper",
                manifest.times.window_from,
                manifest.times.anchor,
                "1m",
                as_of,
                "USD",
            ),
            pln=await build_wallet_pnl_timeline(
                repository,
                manifest.ids.happy_wallet_public_id,
                "paper",
                manifest.times.window_from,
                manifest.times.anchor,
                "1m",
                as_of,
                "PLN",
            ),
            eur=await build_wallet_pnl_timeline(
                repository,
                manifest.ids.happy_wallet_public_id,
                "paper",
                manifest.times.window_from,
                manifest.times.anchor,
                "1m",
                as_of,
                "EUR",
            ),
            incomplete=await build_wallet_pnl_timeline(
                repository,
                manifest.ids.incomplete_wallet_public_id,
                "paper",
                manifest.times.window_from,
                manifest.times.anchor,
                "1m",
                as_of,
                "USD",
            ),
        )
        existing = await ensure_wallet_pnl_anchor(
            repository,
            manifest.ids.happy_wallet_public_id,
            "paper",
            "USD",
            manifest.times.window_from,
            manifest.times.anchor,
        )
        assert existing["point_time"] == manifest.times.window_from
        return timelines
    finally:
        await repository.engine.dispose()


async def _assert_restart_stability(
    db_url: URL,
    manifest: pnl_uat_fixture.PnlUatManifest,
    as_of: datetime,
    expected_usd: PnlWalletTimelineResult,
) -> None:
    """Rebuild through a fresh repository and prove no duplicate anchor write."""
    repository = SQLAlchemyRepository(db_url.render_as_string(hide_password=False))
    try:
        restarted = await build_wallet_pnl_timeline(
            repository,
            manifest.ids.happy_wallet_public_id,
            "paper",
            manifest.times.window_from,
            manifest.times.anchor,
            "1m",
            as_of,
            "USD",
        )
    finally:
        await repository.engine.dispose()
    assert restarted == expected_usd
    engine = create_async_engine(db_url)
    try:
        async with AsyncSession(engine) as session:
            count = (
                await session.execute(select(func.count()).select_from(PortfolioPnlPoint))
            ).scalar_one()
    finally:
        await engine.dispose()
    assert count == 6


def _assert_incomplete_trajectory(
    result: PnlWalletTimelineResult,
    manifest: pnl_uat_fixture.PnlUatManifest,
) -> None:
    """Assert the exact complete prefix and mark-incomplete ETH tail."""
    points = result.series.points
    assert len(points) == 1_441
    assert (
        sum(point.valuation_status == "incomplete" for point in points)
        == _EXPECTED_INCOMPLETE_POINT_COUNT
    )
    for index, point in enumerate(points):
        assert point.point_time == manifest.times.window_from + timedelta(minutes=index)
        if index < _INCOMPLETE_FILL_INDEX:
            assert point.valuation_status == "complete"
            assert point.incompleteness_reasons == ()
            assert (
                point.realized_pnl,
                point.fee_pnl,
                point.accrual_pnl,
                point.unrealized_pnl,
                point.net_pnl,
            ) == (0.0, 0.0, 0.0, 0.0, 0.0)
            assert point.per_instrument == ()
            assert point.attribution == ()
            _assert_complete_point_reconciles(point)
        else:
            assert point.valuation_status == "incomplete"
            assert (
                point.realized_pnl,
                point.fee_pnl,
                point.accrual_pnl,
                point.unrealized_pnl,
                point.net_pnl,
            ) == (0.0, 0.0, 0.0, None, None)
            assert len(point.incompleteness_reasons) == 1
            assert point.incompleteness_reasons[0].reason == _EXPECTED_INCOMPLETE_REASON
            assert (
                point.incompleteness_reasons[0].trigger_instrument_public_id
                == _EXPECTED_INCOMPLETE_INSTRUMENT_ID
            )
            assert [(row.native_symbol, row.exchange) for row in point.per_instrument] == [
                ("ETH-USD", "kraken")
            ]
    latest = points[-1]
    assert (
        latest.realized_pnl,
        latest.fee_pnl,
        latest.accrual_pnl,
        latest.unrealized_pnl,
        latest.net_pnl,
    ) == (0.0, 0.0, 0.0, None, None)
    assert len(latest.incompleteness_reasons) == 1
    assert (
        latest.incompleteness_reasons[0].trigger_instrument_public_id
        == _EXPECTED_INCOMPLETE_INSTRUMENT_ID
    )
    assert [(row.native_symbol, row.exchange) for row in latest.per_instrument] == [
        ("ETH-USD", "kraken")
    ]


@pytest.mark.asyncio
@pytest.mark.timeout(60)
async def test_fixture_produces_exact_economics_markers_and_incompleteness(
    oss_seeded_db_url: URL,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prove the deterministic fixture through the real P&L service.

    Given: A fresh migrated and bundled-OSS-seeded SQLite clone,
    When: The fixture is seeded and all three complete valuations are rebuilt,
    Then: Every command field, minute, decomposition, marker, and incomplete point matches exactly.
    """
    engine_urls: list[URL] = []

    def tracked_create_async_engine(
        db_url: str | URL,
        *,
        poolclass: type[Pool],
    ) -> AsyncEngine:
        assert isinstance(db_url, URL)
        engine_urls.append(db_url)
        return create_async_engine(db_url, poolclass=poolclass)

    monkeypatch.setattr(
        pnl_uat_fixture,
        "create_async_engine",
        tracked_create_async_engine,
    )
    manifest = await pnl_uat_fixture._seed_fixture_database_unchecked(
        oss_seeded_db_url,
        _ANCHOR,
    )
    assert engine_urls == [oss_seeded_db_url]
    assert engine_urls[0] is oss_seeded_db_url
    anchors, events, commands = await _read_fixture_activation_evidence(oss_seeded_db_url)
    _assert_seeded_anchors(anchors, manifest)
    _assert_seeded_fill_events(events, commands, manifest)
    _assert_seeded_trade_commands(
        commands,
        manifest,
        await _require_admin_for_test(oss_seeded_db_url),
    )
    as_of = datetime.now(UTC)
    timelines = await _build_fixture_timelines(oss_seeded_db_url, manifest, as_of)
    await _assert_restart_stability(
        oss_seeded_db_url,
        manifest,
        as_of,
        timelines.usd,
    )

    _assert_complete_trajectory(
        timelines.usd,
        _EXPECTED_COMPLETE["USD"],
        manifest,
    )
    _assert_complete_trajectory(
        timelines.pln,
        _EXPECTED_COMPLETE["PLN"],
        manifest,
    )
    _assert_complete_trajectory(
        timelines.eur,
        _EXPECTED_COMPLETE["EUR"],
        manifest,
    )
    _assert_complete_point(timelines.usd, _EXPECTED_COMPLETE["USD"])
    _assert_complete_point(timelines.pln, _EXPECTED_COMPLETE["PLN"])
    _assert_complete_point(timelines.eur, _EXPECTED_COMPLETE["EUR"])
    _assert_markers(timelines.usd, manifest)
    _assert_incomplete_trajectory(timelines.incomplete, manifest)

    with pytest.raises(
        pnl_uat_fixture.PnlUatFixtureError,
        match="fixture namespace is already populated",
    ):
        await pnl_uat_fixture._seed_fixture_database_unchecked(
            oss_seeded_db_url,
            _ANCHOR,
        )
    assert engine_urls == [oss_seeded_db_url, oss_seeded_db_url]
    assert engine_urls[1] is oss_seeded_db_url
