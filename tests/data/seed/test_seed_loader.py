"""Tests for seed data loader."""

import base64
import json
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any
from typing import cast
from unittest.mock import Mock
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.pool import NullPool

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Setting
from snapper.data.seed.loader import SeedProfile
from snapper.data.seed.loader import SeedSetting
from snapper.data.seed.loader import SeedUser
from snapper.data.seed.loader import SeedWallet
from snapper.data.seed.loader import SeedWalletCredential
from snapper.data.seed.loader import _build_credential_envelope
from snapper.data.seed.loader import _hash_password
from snapper.data.seed.loader import _known_to_value
from snapper.data.seed.loader import _package_dir
from snapper.data.seed.loader import _sync_db_url
from snapper.data.seed.loader import _timestamp_value
from snapper.data.seed.loader import _validate_seed_reconciliation_method
from snapper.data.seed.loader import load_seed_profile
from snapper.data.seed.loader import resolve_seed_path
from snapper.data.seed.loader import run_seed
from snapper.data.seed.loader import seed_default_multi_tenant
from snapper.data.seed.loader import seed_settings
from snapper.data.seed.loader import seed_users
from snapper.messaging.infrastructure.publisher import SequenceTracker


class TestResolveSeedPath:
    """Tests for seed file path resolution."""

    def test_data_seed_override_takes_priority(self, tmp_path: Path) -> None:
        """Test data/seed/ override takes priority over other locations.

        Given: data/seed/test.toml and proprietary/data/seed/test.toml both exist,
        When: resolving seed path for 'test' profile,
        Then: data/seed path is returned (not proprietary).
        """
        data_dir = tmp_path / "data" / "seed"
        data_dir.mkdir(parents=True)
        (data_dir / "test.toml").write_text("")
        prop_dir = tmp_path / "proprietary" / "data" / "seed"
        prop_dir.mkdir(parents=True)
        (prop_dir / "test.toml").write_text("")
        with patch("snapper.data.seed.loader.Path.cwd", return_value=tmp_path):
            path = resolve_seed_path("test")
        assert path == data_dir / "test.toml"

    def test_package_bundled_fallback(self, tmp_path: Path) -> None:
        """Test package-bundled seed file is used as final fallback.

        Given: no seed file in CWD tiers but package dir has it,
        When: resolving seed path for 'bundled' profile,
        Then: package-bundled path is returned.
        """
        pkg_dir = tmp_path / "pkg"
        pkg_dir.mkdir()
        (pkg_dir / "bundled.toml").write_text("")
        with (
            patch("snapper.data.seed.loader.Path.cwd", return_value=tmp_path),
            patch("snapper.data.seed.loader._package_dir", return_value=pkg_dir),
        ):
            path = resolve_seed_path("bundled")
        assert path == pkg_dir / "bundled.toml"

    def test_missing_profile_raises_file_not_found(self, tmp_path: Path) -> None:
        """Test missing profile raises FileNotFoundError.

        Given: no seed file exists for profile in any tier,
        When: resolving seed path for 'nonexistent',
        Then: FileNotFoundError is raised with profile name.
        """
        pkg_dir = tmp_path / "empty_pkg"
        pkg_dir.mkdir()
        with (
            patch("snapper.data.seed.loader.Path.cwd", return_value=tmp_path),
            patch("snapper.data.seed.loader._package_dir", return_value=pkg_dir),
            pytest.raises(FileNotFoundError, match="nonexistent"),
        ):
            resolve_seed_path("nonexistent")

    def test_proprietary_overrides_package_bundled(self, tmp_path: Path) -> None:
        """Test proprietary seed file overrides package-bundled.

        Given: both proprietary and package-bundled seed files exist,
        When: resolving seed path for 'custom' profile,
        Then: proprietary path is returned instead of package-bundled.
        """
        prop_dir = tmp_path / "proprietary" / "data" / "seed"
        prop_dir.mkdir(parents=True)
        (prop_dir / "custom.toml").write_text("")
        pkg_dir = tmp_path / "pkg"
        pkg_dir.mkdir()
        (pkg_dir / "custom.toml").write_text("")
        with (
            patch("snapper.data.seed.loader.Path.cwd", return_value=tmp_path),
            patch("snapper.data.seed.loader._package_dir", return_value=pkg_dir),
        ):
            path = resolve_seed_path("custom")
        assert "proprietary" in str(path)


class TestPackageDir:
    """Tests for _package_dir helper."""

    def test_returns_loader_parent_directory(self) -> None:
        """Test _package_dir returns the directory containing loader.py.

        Given: loader.py is at snapper/data/seed/loader.py,
        When: calling _package_dir(),
        Then: returned path ends with snapper/data/seed.
        """
        result = _package_dir()
        assert result.name == "seed"
        assert (result / "loader.py").exists()


class TestLoadSeedProfile:
    """Tests for TOML seed profile loading."""

    def test_load_dev_profile(self) -> None:
        """Test loading dev seed profile.

        Given: dev.toml exists in seed directory,
        When: loading 'dev' profile,
        Then: profile contains expected users and settings.
        """
        profile = load_seed_profile("dev")
        assert len(profile.users) == 3
        assert profile.users[0].username == "admin"
        assert profile.users[0].role == "admin"
        assert profile.users[1].username == "operator"
        assert profile.users[2].username == "viewer"
        assert len(profile.settings) >= 1
        ui_origin = next(s for s in profile.settings if s.key == "ui_origin")
        assert ui_origin.value

    def test_load_profile_with_empty_sections(self, tmp_path: Path) -> None:
        """Test loading profile with empty/missing sections.

        Given: TOML file with no users or settings,
        When: loading the profile,
        Then: profile has empty lists.
        """
        toml_file = tmp_path / "seed-empty.toml"
        toml_file.write_text("")
        with patch("snapper.data.seed.loader.resolve_seed_path", return_value=toml_file):
            profile = load_seed_profile("empty")
        assert profile.users == []
        assert profile.settings == []

    def test_load_profile_users_only(self, tmp_path: Path) -> None:
        """Test loading profile with users only.

        Given: TOML file with users but no settings,
        When: loading the profile,
        Then: profile has users and empty settings.
        """
        toml_content = """
[[users]]
username = "testuser"
email = "test@test.com"
password = "TestPass123!"
role = "admin"
"""
        toml_file = tmp_path / "seed-users.toml"
        toml_file.write_text(toml_content)
        with patch("snapper.data.seed.loader.resolve_seed_path", return_value=toml_file):
            profile = load_seed_profile("users")
        assert len(profile.users) == 1
        assert profile.users[0].username == "testuser"
        assert profile.settings == []

    def test_load_profile_settings_only(self, tmp_path: Path) -> None:
        """Test loading profile with settings only.

        Given: TOML file with settings but no users,
        When: loading the profile,
        Then: profile has settings and empty users.
        """
        toml_content = """
[[settings]]
key = "test_key"
value = "test_value"
category = "test"
description = "Test setting"
"""
        toml_file = tmp_path / "seed-settings.toml"
        toml_file.write_text(toml_content)
        with patch("snapper.data.seed.loader.resolve_seed_path", return_value=toml_file):
            profile = load_seed_profile("settings")
        assert profile.users == []
        assert len(profile.settings) == 1
        assert profile.settings[0].key == "test_key"


class TestSeedDataclasses:
    """Tests for seed data dataclasses."""

    def test_seed_user_fields(self) -> None:
        """Test SeedUser dataclass fields.

        Given: SeedUser constructor arguments,
        When: creating a SeedUser instance,
        Then: all fields are set correctly.
        """
        user = SeedUser(username="admin", email="admin@test.com", password="pass", role="admin")
        assert user.username == "admin"
        assert user.email == "admin@test.com"
        assert user.password == "pass"
        assert user.role == "admin"

    def test_seed_setting_fields(self) -> None:
        """Test SeedSetting dataclass fields.

        Given: SeedSetting constructor arguments,
        When: creating a SeedSetting instance,
        Then: all fields are set correctly.
        """
        setting = SeedSetting(key="k", value="v", category="c", description="d")
        assert setting.key == "k"
        assert setting.value == "v"
        assert setting.category == "c"
        assert setting.description == "d"

    def test_seed_profile_defaults(self) -> None:
        """Test SeedProfile default values.

        Given: no arguments,
        When: creating a SeedProfile instance,
        Then: users and settings are empty lists.
        """
        profile = SeedProfile()
        assert profile.users == []
        assert profile.settings == []


class TestHashPassword:
    """Tests for password hashing."""

    def test_bcrypt_hash_format(self) -> None:
        """Test password hash has bcrypt format.

        Given: a plaintext password,
        When: hashing with _hash_password,
        Then: result starts with bcrypt prefix.
        """
        hashed = _hash_password("test-password")
        assert hashed.startswith("$2b$")

    def test_different_passwords_produce_different_hashes(self) -> None:
        """Test different passwords produce different hashes.

        Given: two different passwords,
        When: hashing each,
        Then: resulting hashes are different.
        """
        hash1 = _hash_password("password1")
        hash2 = _hash_password("password2")
        assert hash1 != hash2


class TestTimestampValue:
    """Tests for timestamp value conversion by database dialect."""

    def test_returns_iso_string_for_sqlite(self) -> None:
        """Test SQLite timestamp is serialized to UTC ISO string.

        Given: a connection dialect named "sqlite",
        When: building a seed timestamp value,
        Then: value is an ISO string with UTC offset.
        """
        conn_mock = Mock()
        conn_mock.dialect.name = "sqlite"
        value = _timestamp_value(cast(Connection, conn_mock))
        assert isinstance(value, str)
        assert value.endswith("+00:00")

    def test_returns_datetime_for_non_sqlite(self) -> None:
        """Test non-SQLite timestamp remains a timezone-aware datetime.

        Given: a connection dialect named "postgresql",
        When: building a seed timestamp value,
        Then: value is a UTC datetime object.
        """
        conn_mock = Mock()
        conn_mock.dialect.name = "postgresql"
        value = _timestamp_value(cast(Connection, conn_mock))
        assert isinstance(value, datetime)
        assert value.tzinfo == UTC


class TestKnownToValue:
    """Tests for KNOWN_TO_MAX value conversion by database dialect."""

    def test_returns_iso_string_for_sqlite(self) -> None:
        """Test SQLite known_to is serialized to ISO string.

        Given: a connection dialect named "sqlite",
        When: building a known_to value,
        Then: value is an ISO string ending with UTC offset.
        """
        conn_mock = Mock()
        conn_mock.dialect.name = "sqlite"
        value = _known_to_value(cast(Connection, conn_mock))
        assert isinstance(value, str)
        assert value.endswith("+00:00")

    def test_returns_datetime_for_non_sqlite(self) -> None:
        """Test non-SQLite known_to remains the KNOWN_TO_MAX datetime.

        Given: a connection dialect named "postgresql",
        When: building a known_to value,
        Then: value is KNOWN_TO_MAX datetime.
        """
        conn_mock = Mock()
        conn_mock.dialect.name = "postgresql"
        value = _known_to_value(cast(Connection, conn_mock))
        assert value is KNOWN_TO_MAX


class TestSyncDbUrl:
    """Tests for async-to-sync database URL conversion."""

    def test_converts_aiosqlite_to_sqlite(self) -> None:
        """Test aiosqlite URL is converted to sync sqlite.

        Given: async sqlite URL,
        When: converting to sync,
        Then: aiosqlite is replaced with plain sqlite.
        """
        result = _sync_db_url("sqlite+aiosqlite:///data/snapper.db")
        assert result == "sqlite:///data/snapper.db"

    def test_non_aiosqlite_url_unchanged(self) -> None:
        """Test non-aiosqlite URLs are returned unchanged.

        Given: a postgres URL,
        When: converting to sync,
        Then: URL is unchanged.
        """
        url = "postgresql://user:pass@localhost/db"
        assert _sync_db_url(url) == url

    def test_converts_asyncpg_to_psycopg2(self) -> None:
        """Test asyncpg URL is converted to sync psycopg2.

        Given: an async PostgreSQL URL,
        When: converting to sync,
        Then: asyncpg is replaced with psycopg2.
        """
        result = _sync_db_url("postgresql+asyncpg://user:pass@localhost/db")
        assert result == "postgresql+psycopg2://user:pass@localhost/db"

    def test_plain_sqlite_url_unchanged(self) -> None:
        """Test plain sqlite URL is returned unchanged.

        Given: a plain sqlite URL,
        When: converting to sync,
        Then: URL is unchanged.
        """
        url = "sqlite:///data/snapper.db"
        assert _sync_db_url(url) == url


class TestSeedUsers:
    """Tests for user seeding into database."""

    def test_seed_users_inserts_records(self, tmp_path: Path) -> None:
        """Test seed_users inserts user records into database.

        Given: in-memory SQLite database with users table,
        When: seeding with user entries,
        Then: users are inserted with bcrypt-hashed passwords.
        """
        db_path = tmp_path / "test.db"
        engine = create_engine(f"sqlite:///{db_path}", poolclass=NullPool)
        with engine.connect() as conn:
            conn.execute(
                text(
                    "CREATE TABLE users ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, public_id TEXT,"
                    "username TEXT UNIQUE, email TEXT,"
                    "password_hash TEXT, role TEXT, is_active INTEGER,"
                    "created_at TIMESTAMP, timestamp TIMESTAMP,"
                    "known_to DATETIME NOT NULL,"
                    "session_id TEXT NOT NULL DEFAULT '',"
                    "sequence_id INTEGER NOT NULL DEFAULT 0)"
                )
            )
            conn.commit()
            users = [
                SeedUser(
                    username="testadmin",
                    email="admin@test.com",
                    password="TestPass!",
                    role="admin",
                ),
            ]
            count = seed_users(conn, users, SequenceTracker())
            conn.commit()
            assert count == 1
            row = conn.execute(text("SELECT * FROM users WHERE username = 'testadmin'")).fetchone()
            assert row is not None
            assert row[2] == "testadmin"
            assert row[4].startswith("$2b$")
        engine.dispose()

    def test_seed_users_skips_when_any_exist(self, tmp_path: Path) -> None:
        """Test seed_users skips entirely when users table is non-empty.

        Given: database with 3 dev users,
        When: seeding with a different prod user,
        Then: all 3 original users remain unchanged, returns 0.
        """
        db_path = tmp_path / "test.db"
        engine = create_engine(f"sqlite:///{db_path}", poolclass=NullPool)
        with engine.connect() as conn:
            conn.execute(
                text(
                    "CREATE TABLE users ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, public_id TEXT,"
                    "username TEXT UNIQUE, email TEXT,"
                    "password_hash TEXT, role TEXT, is_active INTEGER,"
                    "created_at TIMESTAMP, timestamp TIMESTAMP,"
                    "known_to DATETIME NOT NULL,"
                    "session_id TEXT NOT NULL DEFAULT '',"
                    "sequence_id INTEGER NOT NULL DEFAULT 0)"
                )
            )
            conn.commit()
            dev_users = [
                SeedUser(username="admin", email="a@t.com", password="P1!", role="admin"),
                SeedUser(username="operator", email="o@t.com", password="P2!", role="operator"),
                SeedUser(username="viewer", email="v@t.com", password="P3!", role="viewer"),
            ]
            seed_users(conn, dev_users, SequenceTracker())
            conn.commit()
            assert conn.execute(text("SELECT COUNT(*) FROM users")).scalar() == 3
            original_hash = conn.execute(
                text("SELECT password_hash FROM users WHERE id = 'admin'")
            ).scalar()
            prod_users = [
                SeedUser(username="prodadmin", email="prod@t.com", password="P4!", role="admin"),
            ]
            count = seed_users(conn, prod_users, SequenceTracker())
            conn.commit()
            assert count == 0
            assert conn.execute(text("SELECT COUNT(*) FROM users")).scalar() == 3
            admin_hash = conn.execute(
                text("SELECT password_hash FROM users WHERE id = 'admin'")
            ).scalar()
            assert admin_hash == original_hash
        engine.dispose()

    def test_seed_users_idempotent_reseed(self, tmp_path: Path) -> None:
        """Test seed_users is idempotent when reseeding same profile.

        Given: database already seeded with a user,
        When: seeding the same user again,
        Then: only one user exists and second call returns 0.
        """
        db_path = tmp_path / "test.db"
        engine = create_engine(f"sqlite:///{db_path}", poolclass=NullPool)
        with engine.connect() as conn:
            conn.execute(
                text(
                    "CREATE TABLE users ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, public_id TEXT,"
                    "username TEXT UNIQUE, email TEXT,"
                    "password_hash TEXT, role TEXT, is_active INTEGER,"
                    "created_at TIMESTAMP, timestamp TIMESTAMP,"
                    "known_to DATETIME NOT NULL,"
                    "session_id TEXT NOT NULL DEFAULT '',"
                    "sequence_id INTEGER NOT NULL DEFAULT 0)"
                )
            )
            conn.commit()
            users = [
                SeedUser(username="admin", email="a@t.com", password="P1!", role="admin"),
            ]
            first_count = seed_users(conn, users, SequenceTracker())
            conn.commit()
            assert first_count == 1
            second_count = seed_users(conn, users, SequenceTracker())
            conn.commit()
            assert second_count == 0
            rows = conn.execute(text("SELECT * FROM users")).fetchall()
            assert len(rows) == 1
        engine.dispose()

    def test_seed_users_empty_list_preserves_existing(self, tmp_path: Path) -> None:
        """Test seed_users with empty list preserves existing users.

        Given: database with existing users,
        When: calling seed_users with empty list,
        Then: existing users remain and returns 0.
        """
        db_path = tmp_path / "test.db"
        engine = create_engine(f"sqlite:///{db_path}", poolclass=NullPool)
        with engine.connect() as conn:
            conn.execute(
                text(
                    "CREATE TABLE users ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, public_id TEXT,"
                    "username TEXT UNIQUE, email TEXT,"
                    "password_hash TEXT, role TEXT, is_active INTEGER,"
                    "created_at TIMESTAMP, timestamp TIMESTAMP,"
                    "known_to DATETIME NOT NULL,"
                    "session_id TEXT NOT NULL DEFAULT '',"
                    "sequence_id INTEGER NOT NULL DEFAULT 0)"
                )
            )
            conn.commit()
            users = [
                SeedUser(username="admin", email="a@t.com", password="P!", role="admin"),
            ]
            seed_users(conn, users, SequenceTracker())
            conn.commit()
            assert conn.execute(text("SELECT COUNT(*) FROM users")).scalar() == 1
            count = seed_users(conn, [], SequenceTracker())
            conn.commit()
            assert count == 0
            assert conn.execute(text("SELECT COUNT(*) FROM users")).scalar() == 1
        engine.dispose()

    def test_seed_users_skips_new_when_any_exist(self, tmp_path: Path) -> None:
        """Test seed_users does not add new users when table is non-empty.

        Given: database with one existing user,
        When: seeding with two users (one existing, one new),
        Then: no new users added, returns 0, count stays 1.
        """
        db_path = tmp_path / "test.db"
        engine = create_engine(f"sqlite:///{db_path}", poolclass=NullPool)
        with engine.connect() as conn:
            conn.execute(
                text(
                    "CREATE TABLE users ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, public_id TEXT,"
                    "username TEXT UNIQUE, email TEXT,"
                    "password_hash TEXT, role TEXT, is_active INTEGER,"
                    "created_at TIMESTAMP, timestamp TIMESTAMP,"
                    "known_to DATETIME NOT NULL,"
                    "session_id TEXT NOT NULL DEFAULT '',"
                    "sequence_id INTEGER NOT NULL DEFAULT 0)"
                )
            )
            conn.commit()
            seed_users(
                conn,
                [SeedUser(username="admin", email="a@t.com", password="P1!", role="admin")],
                SequenceTracker(),
            )
            conn.commit()
            count = seed_users(
                conn,
                [
                    SeedUser(username="admin", email="a@t.com", password="P1!", role="admin"),
                    SeedUser(username="viewer", email="v@t.com", password="P2!", role="viewer"),
                ],
                SequenceTracker(),
            )
            conn.commit()
            assert count == 0
            assert conn.execute(text("SELECT COUNT(*) FROM users")).scalar() == 1
        engine.dispose()


class TestSeedSettings:
    """Tests for settings seeding into database."""

    def test_seed_settings_inserts_records(self, tmp_path: Path) -> None:
        """Test seed_settings inserts setting records.

        Given: in-memory SQLite database with settings table,
        When: seeding with setting entries,
        Then: settings are inserted.
        """
        db_path = tmp_path / "test.db"
        engine = create_engine(f"sqlite:///{db_path}", poolclass=NullPool)
        with engine.connect() as conn:
            conn.execute(
                text(
                    "CREATE TABLE settings ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, public_id TEXT,"
                    "key TEXT UNIQUE, value TEXT, category TEXT,"
                    "description TEXT, is_encrypted INTEGER, timestamp TIMESTAMP,"
                    "known_to DATETIME NOT NULL,"
                    "session_id TEXT NOT NULL DEFAULT '',"
                    "sequence_id INTEGER NOT NULL DEFAULT 0)"
                )
            )
            conn.commit()
            settings = [
                SeedSetting(
                    key="ui_origin",
                    value="http://localhost:3000",
                    category="server",
                    description="UI origin",
                ),
            ]
            count = seed_settings(conn, settings, SequenceTracker())
            conn.commit()
            assert count == 1
            row = conn.execute(text("SELECT * FROM settings WHERE key = 'ui_origin'")).fetchone()
            assert row is not None
            assert row[3] == "http://localhost:3000"
            assert row[6] == 0
        engine.dispose()

    def test_seed_settings_encrypts_sensitive_keys(self, tmp_path: Path) -> None:
        """Test seed_settings encrypts sensitive setting values.

        Given: setting with sensitive key (api_key pattern),
        When: seeding the setting,
        Then: value is encrypted and is_encrypted flag is set.
        """
        db_path = tmp_path / "test.db"
        engine = create_engine(f"sqlite:///{db_path}", poolclass=NullPool)
        with engine.connect() as conn:
            conn.execute(
                text(
                    "CREATE TABLE settings ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, public_id TEXT,"
                    "key TEXT UNIQUE, value TEXT, category TEXT,"
                    "description TEXT, is_encrypted INTEGER, timestamp TIMESTAMP,"
                    "known_to DATETIME NOT NULL,"
                    "session_id TEXT NOT NULL DEFAULT '',"
                    "sequence_id INTEGER NOT NULL DEFAULT 0)"
                )
            )
            conn.commit()
            settings = [
                SeedSetting(
                    key="polygon_api_key",
                    value="test-api-key-value",
                    category="api",
                    description="Polygon API key",
                ),
            ]
            count = seed_settings(conn, settings, SequenceTracker())
            conn.commit()
            assert count == 1
            row = conn.execute(
                text("SELECT value, is_encrypted FROM settings WHERE key = 'polygon_api_key'")
            ).fetchone()
            assert row is not None
            assert row[0] != "test-api-key-value"
            assert row[0].startswith("gAAAAAB")
            assert row[1] == 1
        engine.dispose()

    def test_seed_settings_preserves_existing(self, tmp_path: Path) -> None:
        """Test seed_settings preserves existing setting values.

        Given: setting already exists in database,
        When: seeding same key with different value,
        Then: original value is preserved (INSERT OR IGNORE).
        """
        db_path = tmp_path / "test.db"
        engine = create_engine(f"sqlite:///{db_path}", poolclass=NullPool)
        with engine.connect() as conn:
            conn.execute(
                text(
                    "CREATE TABLE settings ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, public_id TEXT,"
                    "key TEXT UNIQUE, value TEXT, category TEXT,"
                    "description TEXT, is_encrypted INTEGER, timestamp TIMESTAMP,"
                    "known_to DATETIME NOT NULL,"
                    "session_id TEXT NOT NULL DEFAULT '',"
                    "sequence_id INTEGER NOT NULL DEFAULT 0)"
                )
            )
            conn.commit()
            settings_v1 = [
                SeedSetting(
                    key="ui_origin",
                    value="http://old:3000",
                    category="server",
                    description="Old origin",
                ),
            ]
            seed_settings(conn, settings_v1, SequenceTracker())
            conn.commit()
            settings_v2 = [
                SeedSetting(
                    key="ui_origin",
                    value="http://new:3000",
                    category="server",
                    description="New origin",
                ),
            ]
            count = seed_settings(conn, settings_v2, SequenceTracker())
            conn.commit()
            assert count == 0
            rows = conn.execute(text("SELECT * FROM settings")).fetchall()
            assert len(rows) == 1
            assert rows[0][3] == "http://old:3000"
        engine.dispose()

    def test_seed_settings_empty_list(self, tmp_path: Path) -> None:
        """Test seed_settings with empty list.

        Given: empty settings list,
        When: calling seed_settings,
        Then: returns 0 and no database changes.
        """
        db_path = tmp_path / "test.db"
        engine = create_engine(f"sqlite:///{db_path}", poolclass=NullPool)
        with engine.connect() as conn:
            conn.execute(
                text(
                    "CREATE TABLE settings ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, public_id TEXT,"
                    "key TEXT UNIQUE, value TEXT, category TEXT,"
                    "description TEXT, is_encrypted INTEGER, timestamp TIMESTAMP,"
                    "known_to DATETIME NOT NULL,"
                    "session_id TEXT NOT NULL DEFAULT '',"
                    "sequence_id INTEGER NOT NULL DEFAULT 0)"
                )
            )
            conn.commit()
            count = seed_settings(conn, [], SequenceTracker())
            assert count == 0
        engine.dispose()


class TestRunSeed:
    """Tests for the end-to-end run_seed function."""

    def test_run_seed_with_dev_profile(self, tmp_path: Path) -> None:
        """Test run_seed executes full seeding workflow.

        Given: database with required tables and dev profile,
        When: running seed with dev profile,
        Then: users and settings are seeded.
        """
        db_path = tmp_path / "test.db"
        db_url = f"sqlite:///{db_path}"
        engine = create_engine(db_url, poolclass=NullPool)
        with engine.connect() as conn:
            conn.execute(
                text(
                    "CREATE TABLE users ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, public_id TEXT,"
                    "username TEXT UNIQUE, email TEXT,"
                    "password_hash TEXT, role TEXT, is_active INTEGER,"
                    "created_at TIMESTAMP, timestamp TIMESTAMP,"
                    "known_to DATETIME NOT NULL,"
                    "session_id TEXT NOT NULL DEFAULT '',"
                    "sequence_id INTEGER NOT NULL DEFAULT 0)"
                )
            )
            conn.execute(
                text(
                    "CREATE TABLE settings ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, public_id TEXT,"
                    "key TEXT UNIQUE, value TEXT, category TEXT,"
                    "description TEXT, is_encrypted INTEGER, timestamp TIMESTAMP,"
                    "known_to DATETIME NOT NULL,"
                    "session_id TEXT NOT NULL DEFAULT '',"
                    "sequence_id INTEGER NOT NULL DEFAULT 0)"
                )
            )
            _create_multi_tenant_tables(conn)
            conn.commit()
        engine.dispose()
        with patch("snapper.data.seed.loader.BootstrapSettingsLoader") as mock_bootstrap:
            mock_bootstrap.return_value.db_url = db_url
            users_count, settings_count = run_seed("dev")
        assert users_count == 3
        assert settings_count >= 1

    def test_run_seed_missing_profile_raises(self) -> None:
        """Test run_seed raises for missing profile.

        Given: no seed file for 'nonexistent' profile,
        When: calling run_seed,
        Then: FileNotFoundError is raised.
        """
        with pytest.raises(FileNotFoundError, match="nonexistent"):
            run_seed("nonexistent")

    def test_run_seed_converts_async_url(self, tmp_path: Path) -> None:
        """Test run_seed converts async database URL to sync.

        Given: async SQLite database URL in bootstrap settings,
        When: running seed,
        Then: URL is converted to sync and seeding succeeds.
        """
        db_path = tmp_path / "test.db"
        sync_url = f"sqlite:///{db_path}"
        async_url = f"sqlite+aiosqlite:///{db_path}"
        engine = create_engine(sync_url, poolclass=NullPool)
        with engine.connect() as conn:
            conn.execute(
                text(
                    "CREATE TABLE users ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, public_id TEXT,"
                    "username TEXT UNIQUE, email TEXT,"
                    "password_hash TEXT, role TEXT, is_active INTEGER,"
                    "created_at TIMESTAMP, timestamp TIMESTAMP,"
                    "known_to DATETIME NOT NULL,"
                    "session_id TEXT NOT NULL DEFAULT '',"
                    "sequence_id INTEGER NOT NULL DEFAULT 0)"
                )
            )
            conn.execute(
                text(
                    "CREATE TABLE settings ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, public_id TEXT,"
                    "key TEXT UNIQUE, value TEXT, category TEXT,"
                    "description TEXT, is_encrypted INTEGER, timestamp TIMESTAMP,"
                    "known_to DATETIME NOT NULL,"
                    "session_id TEXT NOT NULL DEFAULT '',"
                    "sequence_id INTEGER NOT NULL DEFAULT 0)"
                )
            )
            _create_multi_tenant_tables(conn)
            conn.commit()
        engine.dispose()
        with patch("snapper.data.seed.loader.BootstrapSettingsLoader") as mock_bootstrap:
            mock_bootstrap.return_value.db_url = async_url
            users_count, settings_count = run_seed("dev")
        assert users_count == 3
        assert settings_count >= 1


class TestSeedDefaultMultiTenant:
    """Tests for the default multi-tenant bootstrap seed."""

    def _make_db(self, tmp_path: Path) -> tuple[object, Connection]:
        db_path = tmp_path / "test.db"
        engine = create_engine(f"sqlite:///{db_path}", poolclass=NullPool)
        conn = engine.connect()
        conn.execute(
            text(
                "CREATE TABLE users ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, public_id TEXT,"
                " username TEXT, email TEXT, password_hash TEXT, role TEXT,"
                " is_active INTEGER, created_at TIMESTAMP, timestamp TIMESTAMP,"
                " known_to DATETIME NOT NULL, session_id TEXT, sequence_id INTEGER)"
            )
        )
        _create_multi_tenant_tables(conn)
        conn.commit()
        return engine, conn

    def test_inserts_operator_wallet_membership_and_paper_credential(self, tmp_path: Path) -> None:
        """Bootstrap creates operator + wallet + admin membership + paper credential.

        Given: An SQLite database with an admin user already seeded and
            empty multi-tenant tables,
        When: ``seed_default_multi_tenant`` is invoked,
        Then: Default operator, paper wallet, primary admin membership,
            and a paper-mode wallet credential row are inserted (
            bootstrap so the dynamic per-wallet executor spawner finds at
            least one credential at boot).
        """
        engine, conn = self._make_db(tmp_path)
        try:
            conn.execute(
                text(
                    "INSERT INTO users (public_id, username, email, password_hash,"
                    " role, is_active, created_at, timestamp, known_to,"
                    " session_id, sequence_id)"
                    " VALUES ('user-admin', 'admin', 'a@t.com', 'hash',"
                    " 'admin', 1, :ts, :ts, :known_to, 's', 1)"
                ),
                {"ts": str(datetime.now(UTC)), "known_to": str(KNOWN_TO_MAX)},
            )
            conn.commit()

            count = seed_default_multi_tenant(conn, SequenceTracker())
            conn.commit()

            assert count == 4
            op_row = conn.execute(text("SELECT label FROM operators")).first()
            assert op_row is not None
            assert op_row[0] == "default"
            wallet_row = conn.execute(
                text("SELECT public_id, label, is_paper FROM wallets")
            ).first()
            assert wallet_row is not None
            wallet_public_id = wallet_row[0]
            assert wallet_row[1] == "default"
            assert wallet_row[2] == 1
            membership_row = conn.execute(
                text("SELECT user_public_id, is_primary FROM user_operator_memberships")
            ).first()
            assert membership_row is not None
            assert membership_row[0] == "user-admin"
            assert membership_row[1] == 1
            credential_row = conn.execute(
                text(
                    "SELECT wallet_public_id, exchange, credential_type,"
                    " encrypted_payload, label"
                    " FROM wallet_credentials"
                )
            ).first()
            assert credential_row is not None
            assert credential_row[0] == wallet_public_id
            assert credential_row[1] == "paper"
            assert credential_row[2] == "paper"
            assert credential_row[3].startswith("gAAAAAB")
            assert credential_row[4] == "default paper bootstrap"
        finally:
            conn.close()
            cast(Any, engine).dispose()

    def test_memberships_follow_read_account_state_permission(self, tmp_path: Path) -> None:
        """Bootstrap memberships follow the named account-read permission.

        Given: Active admin, operator, viewer, and AI-role users,
        When: ``seed_default_multi_tenant`` creates the default operator,
        Then: Every account-state reader receives a primary membership and
            unrelated AI roles receive none.
        """
        engine, conn = self._make_db(tmp_path)
        try:
            conn.execute(
                text(
                    "INSERT INTO users (public_id, username, email, password_hash,"
                    " role, is_active, created_at, timestamp, known_to,"
                    " session_id, sequence_id) VALUES"
                    " ('user-admin', 'admin', 'admin@t.com', 'hash', 'admin',"
                    " 1, :ts, :ts, :known_to, 's', 1),"
                    " ('user-operator', 'operator', 'operator@t.com', 'hash', 'operator',"
                    " 1, :ts, :ts, :known_to, 's', 2),"
                    " ('user-viewer', 'viewer', 'viewer@t.com', 'hash', 'viewer',"
                    " 1, :ts, :ts, :known_to, 's', 3),"
                    " ('user-ai-researcher', 'ai-researcher', NULL, 'hash', 'ai_researcher',"
                    " 1, :ts, :ts, :known_to, 's', 4),"
                    " ('user-ai-reviewer', 'ai-reviewer', NULL, 'hash', 'ai_reviewer',"
                    " 1, :ts, :ts, :known_to, 's', 5),"
                    " ('user-ai-delegate', 'ai-delegate', NULL, 'hash', 'ai_delegate',"
                    " 1, :ts, :ts, :known_to, 's', 6)"
                ),
                {"ts": str(datetime.now(UTC)), "known_to": str(KNOWN_TO_MAX)},
            )
            conn.commit()

            count = seed_default_multi_tenant(conn, SequenceTracker())
            conn.commit()

            memberships = conn.execute(
                text(
                    "SELECT user_public_id, is_primary"
                    " FROM user_operator_memberships ORDER BY user_public_id"
                )
            ).all()
            assert count == 6
            assert [(row[0], row[1]) for row in memberships] == [
                ("user-admin", 1),
                ("user-operator", 1),
                ("user-viewer", 1),
            ]
        finally:
            conn.close()
            cast(Any, engine).dispose()

    def test_skips_when_operators_already_exist(self, tmp_path: Path) -> None:
        """Bootstrap is a no-op when any operator row already exists.

        Given: A database with a pre-existing operator,
        When: ``seed_default_multi_tenant`` is invoked,
        Then: Returns 0 and leaves the existing operator intact.
        """
        engine, conn = self._make_db(tmp_path)
        try:
            conn.execute(
                text(
                    "INSERT INTO operators (public_id, label, description, timestamp,"
                    " known_to, session_id, sequence_id)"
                    " VALUES ('op-1', 'pre-existing', NULL, :ts, :known_to, 's', 1)"
                ),
                {"ts": str(datetime.now(UTC)), "known_to": str(KNOWN_TO_MAX)},
            )
            conn.commit()

            count = seed_default_multi_tenant(conn, SequenceTracker())
            conn.commit()

            assert count == 0
            assert conn.execute(text("SELECT COUNT(*) FROM operators")).scalar() == 1
            assert conn.execute(text("SELECT COUNT(*) FROM wallets")).scalar() == 0
        finally:
            conn.close()
            cast(Any, engine).dispose()

    def test_skips_when_wallets_already_exist(self, tmp_path: Path) -> None:
        """Bootstrap is a no-op when any wallet row already exists.

        Given: A database with a pre-existing wallet and no operators,
        When: ``seed_default_multi_tenant`` is invoked,
        Then: Returns 0 and does not insert the default operator.
        """
        engine, conn = self._make_db(tmp_path)
        try:
            conn.execute(
                text(
                    "INSERT INTO wallets (public_id, label, description, is_paper,"
                    " timestamp, known_to, session_id, sequence_id)"
                    " VALUES ('w-1', 'pre-existing', NULL, 0, :ts, :known_to, 's', 1)"
                ),
                {"ts": str(datetime.now(UTC)), "known_to": str(KNOWN_TO_MAX)},
            )
            conn.commit()

            count = seed_default_multi_tenant(conn, SequenceTracker())
            conn.commit()

            assert count == 0
            assert conn.execute(text("SELECT COUNT(*) FROM operators")).scalar() == 0
        finally:
            conn.close()
            cast(Any, engine).dispose()

    def test_skips_membership_when_no_entitled_user(self, tmp_path: Path) -> None:
        """Bootstrap skips membership rows when no entitled user is present.

        Given: A database with the users table empty,
        When: ``seed_default_multi_tenant`` is invoked,
        Then: Operator + wallet + paper credential are inserted but no
            membership row (count == 3 instead of 4).
        """
        engine, conn = self._make_db(tmp_path)
        try:
            count = seed_default_multi_tenant(conn, SequenceTracker())
            conn.commit()

            assert count == 3
            assert conn.execute(text("SELECT COUNT(*) FROM operators")).scalar() == 1
            assert conn.execute(text("SELECT COUNT(*) FROM wallets")).scalar() == 1
            assert (
                conn.execute(text("SELECT COUNT(*) FROM user_operator_memberships")).scalar() == 0
            )
            assert conn.execute(text("SELECT COUNT(*) FROM wallet_credentials")).scalar() == 1
        finally:
            conn.close()
            cast(Any, engine).dispose()

    def test_real_method_config_is_seeded_with_credential(self, tmp_path: Path) -> None:
        """A real method inserts its config in the bootstrap transaction."""
        engine, conn = self._make_db(tmp_path)
        wallet = SeedWallet(
            label="futures",
            is_paper=False,
            credentials=[
                SeedWalletCredential(
                    exchange="kraken_futures",
                    credential_type="api_key_secret",
                    reconciliation_method="futures_position",
                    api_key="key",
                    api_secret="secret",
                )
            ],
        )
        try:
            count = seed_default_multi_tenant(conn, SequenceTracker(), wallets=[wallet])
            conn.commit()

            assert count == 4
            config = conn.execute(
                text(
                    "SELECT wallet_public_id, exchange, mode, method"
                    " FROM portfolio_reconciliation_method_configs"
                )
            ).first()
            credential = conn.execute(
                text("SELECT wallet_public_id FROM wallet_credentials")
            ).first()
            assert config is not None
            assert credential is not None
            assert config[0] == credential[0]
            assert config[1:] == ("kraken_futures", "live", "futures_position")
        finally:
            conn.close()
            cast(Any, engine).dispose()


class TestBuildCredentialEnvelope:
    """Tests for ``_build_credential_envelope`` payload packing."""

    def test_api_key_secret_returns_expected_json_envelope(self) -> None:
        """api_key_secret credentials serialize into the DB envelope shape.

        Given: A ``SeedWalletCredential`` with ``credential_type="api_key_secret"``
            plus API key and secret values,
        When: ``_build_credential_envelope`` is called,
        Then: The returned JSON contains both values under the exact keys
            consumed later by the credential resolver.
        """
        cred = SeedWalletCredential(
            exchange="kraken",
            credential_type="api_key_secret",
            reconciliation_method="unclassified",
            api_key="public-key",
            api_secret="secret-value",
        )

        envelope = _build_credential_envelope(cred)

        assert json.loads(envelope) == {
            "api_key": "public-key",
            "api_secret": "secret-value",
        }

    def test_rsa_pem_decodes_base64_and_returns_expected_json_envelope(self) -> None:
        """rsa_pem credentials decode PEM material before building the envelope.

        Given: A ``SeedWalletCredential`` with ``credential_type="rsa_pem"``
            and a base64-encoded PEM payload,
        When: ``_build_credential_envelope`` is called,
        Then: The returned JSON contains the decoded PEM text so the
            exchange client can consume the original multi-line key.
        """
        pem_text = "-----BEGIN PRIVATE KEY-----\nabc123\n-----END PRIVATE KEY-----\n"
        cred = SeedWalletCredential(
            exchange="walutomat",
            credential_type="rsa_pem",
            reconciliation_method="spot_execution_replay",
            api_key="wallet-key",
            private_key_pem_base64=base64.b64encode(pem_text.encode("utf-8")).decode("ascii"),
        )

        envelope = _build_credential_envelope(cred)

        assert json.loads(envelope) == {
            "api_key": "wallet-key",
            "private_key_pem": pem_text,
        }

    def test_unknown_credential_type_raises_value_error(self) -> None:
        """Unknown credential_type surfaces a fail-fast ValueError.

        Given: A ``SeedWalletCredential`` with a ``credential_type``
            that does not match any of the supported envelopes
            (``api_key_secret`` / ``rsa_pem`` / ``paper``),
        When: ``_build_credential_envelope`` is called,
        Then: A ``ValueError`` is raised listing the exchange name and
            the set of allowed credential types — so a typo in the
            seed TOML fails at seed time instead of storing an empty
            envelope that crashes later at executor startup.
        """
        cred = SeedWalletCredential(
            exchange="kraken",
            credential_type="oauth",
            reconciliation_method="unclassified",
        )
        with pytest.raises(ValueError, match="Unknown seed credential_type 'oauth'"):
            _build_credential_envelope(cred)

    def test_invalid_base64_in_rsa_pem_raises_value_error_with_exchange_name(self) -> None:
        """Malformed base64 in ``private_key_pem_base64`` surfaces a contextual error.

        Given: A ``SeedWalletCredential`` with ``credential_type="rsa_pem"``
            whose ``private_key_pem_base64`` is not valid base64,
        When: ``_build_credential_envelope`` is called,
        Then: A ``ValueError`` is raised naming the exchange so the
            operator can locate the typo in a multi-credential seed
            file instead of chasing a cryptic ``binascii.Error``.
        """
        cred = SeedWalletCredential(
            exchange="walutomat",
            credential_type="rsa_pem",
            reconciliation_method="spot_execution_replay",
            api_key="k",
            private_key_pem_base64="!!!not-base64!!!",
        )
        with pytest.raises(ValueError, match="Invalid base64.*walutomat"):
            _build_credential_envelope(cred)


class TestSeedReconciliationPolicy:
    """Seed classifications obey the exact concrete-adapter policy."""

    def test_accepts_each_reviewed_adapter_method_and_unclassified(self) -> None:
        """Reviewed real methods and explicit unclassified values pass."""
        credentials = [
            SeedWalletCredential(
                exchange="kraken_futures",
                credential_type="api_key_secret",
                reconciliation_method="futures_position",
            ),
            SeedWalletCredential(
                exchange="kraken",
                credential_type="api_key_secret",
                reconciliation_method="spot_execution_replay",
            ),
            SeedWalletCredential(
                exchange="kraken",
                credential_type="api_key_secret",
                reconciliation_method="margin_ledger_replay",
            ),
            SeedWalletCredential(
                exchange="walutomat",
                credential_type="rsa_pem",
                reconciliation_method="spot_execution_replay",
            ),
            SeedWalletCredential(
                exchange="polygon",
                credential_type="api_key_secret",
                reconciliation_method="unclassified",
            ),
            SeedWalletCredential(
                exchange="paper",
                credential_type="paper",
                reconciliation_method="unclassified",
            ),
        ]

        for credential in credentials:
            _validate_seed_reconciliation_method(credential)

    def test_rejects_uppercase_exchange(self) -> None:
        """Seed exchange identity must retain its lowercase DB invariant."""
        credential = SeedWalletCredential(
            exchange="Kraken",
            credential_type="api_key_secret",
            reconciliation_method="unclassified",
        )

        with pytest.raises(ValueError, match="must be lowercase"):
            _validate_seed_reconciliation_method(credential)

    def test_rejects_paper_type_venue_mismatch(self) -> None:
        """Paper credential type cannot be paired with a live venue."""
        credential = SeedWalletCredential(
            exchange="kraken",
            credential_type="paper",
            reconciliation_method="unclassified",
        )

        with pytest.raises(ValueError, match="paper credential and venue"):
            _validate_seed_reconciliation_method(credential)

    def test_rejects_real_method_for_paper(self) -> None:
        """Paper provisioning never creates a live method-config row."""
        credential = SeedWalletCredential(
            exchange="paper",
            credential_type="paper",
            reconciliation_method="futures_position",
        )

        with pytest.raises(ValueError, match="Paper seed credentials"):
            _validate_seed_reconciliation_method(credential)

    def test_rejects_method_outside_concrete_adapter_allowed_set(self) -> None:
        """An exchange-looking venue cannot select another adapter's method."""
        credential = SeedWalletCredential(
            exchange="kraken_futures_preview",
            credential_type="api_key_secret",
            reconciliation_method="futures_position",
        )

        with pytest.raises(ValueError, match="is not allowed"):
            _validate_seed_reconciliation_method(credential)


def _create_multi_tenant_tables(conn: Connection) -> None:
    """Create the minimal operators / wallets / user_operator_memberships tables.

    Used by both ``TestRunSeed`` and ``TestSeedDefaultMultiTenant`` to
    exercise the new bootstrap without pulling in the full Alembic
    migration history.
    """
    conn.execute(
        text(
            "CREATE TABLE operators ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, public_id TEXT,"
            " label TEXT, description TEXT,"
            " timestamp TIMESTAMP, known_to DATETIME NOT NULL,"
            " session_id TEXT NOT NULL DEFAULT '',"
            " sequence_id INTEGER NOT NULL DEFAULT 0)"
        )
    )
    conn.execute(
        text(
            "CREATE TABLE wallets ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, public_id TEXT,"
            " label TEXT, description TEXT, is_paper INTEGER NOT NULL DEFAULT 0,"
            " timestamp TIMESTAMP, known_to DATETIME NOT NULL,"
            " session_id TEXT NOT NULL DEFAULT '',"
            " sequence_id INTEGER NOT NULL DEFAULT 0)"
        )
    )
    conn.execute(
        text(
            "CREATE TABLE user_operator_memberships ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, public_id TEXT,"
            " user_public_id TEXT, operator_public_id TEXT,"
            " is_primary INTEGER NOT NULL DEFAULT 0,"
            " timestamp TIMESTAMP, known_to DATETIME NOT NULL,"
            " session_id TEXT NOT NULL DEFAULT '',"
            " sequence_id INTEGER NOT NULL DEFAULT 0)"
        )
    )
    conn.execute(
        text(
            "CREATE TABLE wallet_credentials ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, public_id TEXT,"
            " wallet_public_id TEXT, exchange TEXT, credential_type TEXT,"
            " encrypted_payload TEXT, label TEXT,"
            " timestamp TIMESTAMP, known_to DATETIME NOT NULL,"
            " session_id TEXT NOT NULL DEFAULT '',"
            " sequence_id INTEGER NOT NULL DEFAULT 0)"
        )
    )
    conn.execute(
        text(
            "CREATE TABLE portfolio_reconciliation_method_configs ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, public_id TEXT,"
            " wallet_public_id TEXT, exchange TEXT, mode TEXT, method TEXT,"
            " timestamp TIMESTAMP, known_to DATETIME NOT NULL,"
            " session_id TEXT NOT NULL DEFAULT '',"
            " sequence_id INTEGER NOT NULL DEFAULT 0)"
        )
    )


class TestSeedProfilesFitColumnLimits:
    """Every resolvable seed profile must fit the Setting column widths.

    Regression guard for the 2026-07-05 prod incident: db-seed died with
    StringDataRightTruncation because ONE setting description exceeded
    settings.description varchar(1024). Postgres validates the bind cast
    in the INSERT..SELECT..WHERE NOT EXISTS statement even for rows the
    NOT EXISTS clause will skip, so a single over-limit field breaks the
    otherwise idempotent seed pass for the whole profile. Limits are
    introspected from the ORM model so a future column resize keeps this
    test honest without edits.
    """

    @pytest.mark.parametrize("profile", ["dev", "prod"])
    def test_settings_fit_column_widths(self, profile: str) -> None:
        """All settings in each resolvable profile fit the model columns."""
        try:
            seed = load_seed_profile(profile)
        except FileNotFoundError:
            pytest.skip(f"profile {profile!r} not present in this checkout")
        limits = {
            "key": Setting.__table__.c.key.type.length,
            "category": Setting.__table__.c.category.type.length,
            "description": Setting.__table__.c.description.type.length,
        }
        for setting in seed.settings:
            for field, limit in limits.items():
                assert limit is not None
                value = getattr(setting, field, None) or ""
                assert len(value) <= limit, (
                    f"{profile}: setting {setting.key!r} field {field!r}"
                    f" is {len(value)} chars > varchar({limit})"
                )
