"""Tests for seed data loader."""

from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import cast
from unittest.mock import Mock
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.pool import NullPool

from snapper.data.seed.loader import SeedProfile
from snapper.data.seed.loader import SeedSetting
from snapper.data.seed.loader import SeedUser
from snapper.data.seed.loader import _hash_password
from snapper.data.seed.loader import _sync_db_url
from snapper.data.seed.loader import _timestamp_value
from snapper.data.seed.loader import load_seed_profile
from snapper.data.seed.loader import resolve_seed_path
from snapper.data.seed.loader import run_seed
from snapper.data.seed.loader import seed_settings
from snapper.data.seed.loader import seed_users


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
        with patch("snapper.data.seed.loader._project_root", return_value=tmp_path):
            path = resolve_seed_path("test")
        assert path == data_dir / "test.toml"

    def test_open_source_fallback(self, tmp_path: Path) -> None:
        """Test open-source seed file is found as fallback.

        Given: no data/seed or proprietary override exists,
        When: resolving seed path for 'dev' profile,
        Then: src/snapper/data/seed/dev.toml is returned.
        """
        src_dir = tmp_path / "src" / "snapper" / "data" / "seed"
        src_dir.mkdir(parents=True)
        (src_dir / "dev.toml").write_text("")
        with patch("snapper.data.seed.loader._project_root", return_value=tmp_path):
            path = resolve_seed_path("dev")
        assert path == src_dir / "dev.toml"

    def test_missing_profile_raises_file_not_found(self) -> None:
        """Test missing profile raises FileNotFoundError.

        Given: no seed file exists for profile,
        When: resolving seed path for 'nonexistent',
        Then: FileNotFoundError is raised with profile name.
        """
        with pytest.raises(FileNotFoundError, match="nonexistent"):
            resolve_seed_path("nonexistent")

    def test_proprietary_overrides_open_source(self, tmp_path: Path) -> None:
        """Test proprietary seed file overrides open-source.

        Given: both proprietary and open-source seed files exist,
        When: resolving seed path with _project_root pointing to tmp_path,
        Then: proprietary path is returned instead of open-source.
        """
        prop_dir = tmp_path / "proprietary" / "data" / "seed"
        prop_dir.mkdir(parents=True)
        (prop_dir / "custom.toml").write_text("")
        src_dir = tmp_path / "src" / "snapper" / "data" / "seed"
        src_dir.mkdir(parents=True)
        (src_dir / "custom.toml").write_text("")
        with patch("snapper.data.seed.loader._project_root", return_value=tmp_path):
            path = resolve_seed_path("custom")
        assert "proprietary" in str(path)


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
                    "id TEXT PRIMARY KEY, username TEXT, email TEXT,"
                    "password_hash TEXT, role TEXT, is_active INTEGER,"
                    "created_at TIMESTAMP)"
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
            count = seed_users(conn, users)
            conn.commit()
            assert count == 1
            row = conn.execute(text("SELECT * FROM users WHERE id = 'testadmin'")).fetchone()
            assert row is not None
            assert row[1] == "testadmin"
            assert row[3].startswith("$2b$")
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
                    "id TEXT PRIMARY KEY, username TEXT, email TEXT,"
                    "password_hash TEXT, role TEXT, is_active INTEGER,"
                    "created_at TIMESTAMP)"
                )
            )
            conn.commit()
            dev_users = [
                SeedUser(username="admin", email="a@t.com", password="P1!", role="admin"),
                SeedUser(username="operator", email="o@t.com", password="P2!", role="operator"),
                SeedUser(username="viewer", email="v@t.com", password="P3!", role="viewer"),
            ]
            seed_users(conn, dev_users)
            conn.commit()
            assert conn.execute(text("SELECT COUNT(*) FROM users")).scalar() == 3
            original_hash = conn.execute(
                text("SELECT password_hash FROM users WHERE id = 'admin'")
            ).scalar()
            prod_users = [
                SeedUser(username="prodadmin", email="prod@t.com", password="P4!", role="admin"),
            ]
            count = seed_users(conn, prod_users)
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
                    "id TEXT PRIMARY KEY, username TEXT, email TEXT,"
                    "password_hash TEXT, role TEXT, is_active INTEGER,"
                    "created_at TIMESTAMP)"
                )
            )
            conn.commit()
            users = [
                SeedUser(username="admin", email="a@t.com", password="P1!", role="admin"),
            ]
            first_count = seed_users(conn, users)
            conn.commit()
            assert first_count == 1
            second_count = seed_users(conn, users)
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
                    "id TEXT PRIMARY KEY, username TEXT, email TEXT,"
                    "password_hash TEXT, role TEXT, is_active INTEGER,"
                    "created_at TIMESTAMP)"
                )
            )
            conn.commit()
            users = [
                SeedUser(username="admin", email="a@t.com", password="P!", role="admin"),
            ]
            seed_users(conn, users)
            conn.commit()
            assert conn.execute(text("SELECT COUNT(*) FROM users")).scalar() == 1
            count = seed_users(conn, [])
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
                    "id TEXT PRIMARY KEY, username TEXT, email TEXT,"
                    "password_hash TEXT, role TEXT, is_active INTEGER,"
                    "created_at TIMESTAMP)"
                )
            )
            conn.commit()
            seed_users(
                conn,
                [SeedUser(username="admin", email="a@t.com", password="P1!", role="admin")],
            )
            conn.commit()
            count = seed_users(
                conn,
                [
                    SeedUser(username="admin", email="a@t.com", password="P1!", role="admin"),
                    SeedUser(username="viewer", email="v@t.com", password="P2!", role="viewer"),
                ],
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
                    "key TEXT PRIMARY KEY, value TEXT, category TEXT,"
                    "description TEXT, is_encrypted INTEGER, updated_at TIMESTAMP)"
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
            count = seed_settings(conn, settings)
            conn.commit()
            assert count == 1
            row = conn.execute(text("SELECT * FROM settings WHERE key = 'ui_origin'")).fetchone()
            assert row is not None
            assert row[1] == "http://localhost:3000"
            assert row[4] == 0
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
                    "key TEXT PRIMARY KEY, value TEXT, category TEXT,"
                    "description TEXT, is_encrypted INTEGER, updated_at TIMESTAMP)"
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
            count = seed_settings(conn, settings)
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
                    "key TEXT PRIMARY KEY, value TEXT, category TEXT,"
                    "description TEXT, is_encrypted INTEGER, updated_at TIMESTAMP)"
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
            seed_settings(conn, settings_v1)
            conn.commit()
            settings_v2 = [
                SeedSetting(
                    key="ui_origin",
                    value="http://new:3000",
                    category="server",
                    description="New origin",
                ),
            ]
            count = seed_settings(conn, settings_v2)
            conn.commit()
            assert count == 0
            rows = conn.execute(text("SELECT * FROM settings")).fetchall()
            assert len(rows) == 1
            assert rows[0][1] == "http://old:3000"
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
                    "key TEXT PRIMARY KEY, value TEXT, category TEXT,"
                    "description TEXT, is_encrypted INTEGER, updated_at TIMESTAMP)"
                )
            )
            conn.commit()
            count = seed_settings(conn, [])
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
                    "id TEXT PRIMARY KEY, username TEXT, email TEXT,"
                    "password_hash TEXT, role TEXT, is_active INTEGER,"
                    "created_at TIMESTAMP)"
                )
            )
            conn.execute(
                text(
                    "CREATE TABLE settings ("
                    "key TEXT PRIMARY KEY, value TEXT, category TEXT,"
                    "description TEXT, is_encrypted INTEGER, updated_at TIMESTAMP)"
                )
            )
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
                    "id TEXT PRIMARY KEY, username TEXT, email TEXT,"
                    "password_hash TEXT, role TEXT, is_active INTEGER,"
                    "created_at TIMESTAMP)"
                )
            )
            conn.execute(
                text(
                    "CREATE TABLE settings ("
                    "key TEXT PRIMARY KEY, value TEXT, category TEXT,"
                    "description TEXT, is_encrypted INTEGER, updated_at TIMESTAMP)"
                )
            )
            conn.commit()
        engine.dispose()
        with patch("snapper.data.seed.loader.BootstrapSettingsLoader") as mock_bootstrap:
            mock_bootstrap.return_value.db_url = async_url
            users_count, settings_count = run_seed("dev")
        assert users_count == 3
        assert settings_count >= 1
