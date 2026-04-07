"""Tests for seed data loader."""

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
from snapper.data.seed.loader import SeedProfile
from snapper.data.seed.loader import SeedSetting
from snapper.data.seed.loader import SeedUser
from snapper.data.seed.loader import _hash_password
from snapper.data.seed.loader import _known_to_value
from snapper.data.seed.loader import _package_dir
from snapper.data.seed.loader import _sync_db_url
from snapper.data.seed.loader import _timestamp_value
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

    def test_inserts_operator_wallet_and_membership(self, tmp_path: Path) -> None:
        """Bootstrap creates an operator, a paper wallet, and an admin membership.

        Given: An SQLite database with an admin user already seeded and
            empty multi-tenant tables,
        When: ``seed_default_multi_tenant`` is invoked,
        Then: Default operator, paper wallet, and primary admin membership
            rows are inserted.
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

            assert count == 3
            op_row = conn.execute(text("SELECT label FROM operators")).first()
            assert op_row is not None
            assert op_row[0] == "default"
            wallet_row = conn.execute(text("SELECT label, is_paper FROM wallets")).first()
            assert wallet_row is not None
            assert wallet_row[0] == "default-paper"
            assert wallet_row[1] == 1
            membership_row = conn.execute(
                text("SELECT user_public_id, is_primary FROM user_operator_memberships")
            ).first()
            assert membership_row is not None
            assert membership_row[0] == "user-admin"
            assert membership_row[1] == 1
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

    def test_skips_membership_when_no_admin_user(self, tmp_path: Path) -> None:
        """Bootstrap skips the membership row when no admin user is present.

        Given: A database with the users table empty,
        When: ``seed_default_multi_tenant`` is invoked,
        Then: Operator and wallet are inserted but no membership row
            (count == 2 instead of 3).
        """
        engine, conn = self._make_db(tmp_path)
        try:
            count = seed_default_multi_tenant(conn, SequenceTracker())
            conn.commit()

            assert count == 2
            assert conn.execute(text("SELECT COUNT(*) FROM operators")).scalar() == 1
            assert conn.execute(text("SELECT COUNT(*) FROM wallets")).scalar() == 1
            assert (
                conn.execute(text("SELECT COUNT(*) FROM user_operator_memberships")).scalar() == 0
            )
        finally:
            conn.close()
            cast(Any, engine).dispose()


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
