"""Seed data loader for environment-specific database seeding.

Loads seed profiles from TOML files using a three-tier lookup:
``data/seed/{profile}.toml`` (CWD, e.g. Docker volume mount)
-> ``proprietary/data/seed/{profile}.toml`` (CWD, local dev)
-> package-bundled ``snapper/data/seed/{profile}.toml`` (installed wheel).

Provides idempotent semantics so ``db-seed`` can be run repeatedly
without duplicating data.  Users are skipped entirely when any account
already exists; settings use INSERT OR IGNORE to preserve manually
configured values.

Example:
    >>> from snapper.data.seed.loader import run_seed
    >>> users, settings = run_seed("dev")
"""

import tomllib
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from pathlib import Path
from uuid import uuid7

import bcrypt
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.pool import NullPool

from snapper.config.bootstrap import BootstrapSettingsLoader
from snapper.data.models import KNOWN_TO_MAX
from snapper.infrastructure.security.encryption import SettingsEncryptionService
from snapper.infrastructure.security.encryption import get_encryption_service
from snapper.messaging.infrastructure.publisher import SequenceTracker


@dataclass
class SeedUser:
    """Seed data for a user account.

    Attributes:
        username: Login username (also used as primary key id).
        email: User email address.
        password: Plaintext password (will be bcrypt-hashed before insert).
        role: User role (admin, operator, viewer).
    """

    username: str
    email: str
    password: str
    role: str


@dataclass
class SeedSetting:
    """Seed data for an application setting.

    Attributes:
        key: Setting key name.
        value: Setting value (sensitive values encrypted before insert).
        category: Setting category (api, server, etc.).
        description: Human-readable description.
    """

    key: str
    value: str
    category: str
    description: str


@dataclass
class SeedProfile:
    """Complete seed profile parsed from TOML.

    Attributes:
        users: List of user seed entries.
        settings: List of setting seed entries.
    """

    users: list[SeedUser] = field(default_factory=list)
    settings: list[SeedSetting] = field(default_factory=list)


def _package_dir() -> Path:
    """Return the directory containing this module (snapper/data/seed/)."""
    return Path(__file__).resolve().parent


def resolve_seed_path(profile: str) -> Path:
    """Resolve the seed TOML file path using three-tier lookup.

    Lookup order (CWD-relative for tiers 1-2, then package-bundled):
        1. ``{cwd}/data/seed/{profile}.toml`` (deployment/volume override)
        2. ``{cwd}/proprietary/data/seed/{profile}.toml`` (local dev)
        3. Package-bundled ``snapper/data/seed/{profile}.toml`` (installed wheel)

    In Docker, CWD is ``/app`` (WORKDIR) and ``data/`` is a volume mount,
    so tier 1 picks up overrides.  Locally, CWD is the project root.

    Args:
        profile: Seed profile name (e.g. "dev", "prod").

    Returns:
        Path to the resolved TOML file.

    Raises:
        FileNotFoundError: If no seed file found in any location.
    """
    cwd = Path.cwd()
    candidates = [
        cwd / "data" / "seed" / f"{profile}.toml",
        cwd / "proprietary" / "data" / "seed" / f"{profile}.toml",
        _package_dir() / f"{profile}.toml",
    ]
    for path in candidates:
        if path.exists():
            logger.info(f"Seed profile resolved: {path}")
            return path
    searched = ", ".join(str(p) for p in candidates)
    raise FileNotFoundError(f"Seed profile '{profile}' not found. Searched: {searched}")


def load_seed_profile(profile: str) -> SeedProfile:
    """Load and parse a seed profile from TOML.

    Args:
        profile: Seed profile name.

    Returns:
        Parsed SeedProfile with users and settings.
    """
    path = resolve_seed_path(profile)
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    users = [SeedUser(**entry) for entry in data.get("users", [])]
    settings = [SeedSetting(**entry) for entry in data.get("settings", [])]
    return SeedProfile(users=users, settings=settings)


def _hash_password(password: str) -> str:
    """Hash a plaintext password using bcrypt.

    Args:
        password: Plaintext password string.

    Returns:
        Bcrypt hash string suitable for database storage.
    """
    hashed: bytes = bcrypt.hashpw(password.encode(), bcrypt.gensalt())
    return hashed.decode()


def _known_to_value(conn: Connection) -> datetime | str:
    """Build a KNOWN_TO_MAX value compatible with the current SQL driver.

    Args:
        conn: Active SQLAlchemy connection.

    Returns:
        KNOWN_TO_MAX datetime for non-SQLite engines, ISO string for SQLite.
    """
    if conn.dialect.name == "sqlite":
        return str(KNOWN_TO_MAX)
    return KNOWN_TO_MAX


def _timestamp_value(conn: Connection) -> datetime | str:
    """Build a UTC timestamp value compatible with the current SQL driver.

    sqlite3 no longer provides an implicit datetime adapter. Returning
    an ISO-8601 string for SQLite avoids errors while keeping UTC data.

    Args:
        conn: Active SQLAlchemy connection.

    Returns:
        UTC datetime for non-SQLite engines, ISO string for SQLite.
    """
    now = datetime.now(tz=UTC)
    if conn.dialect.name == "sqlite":
        return str(now)
    return now


def seed_users(conn: Connection, users: list[SeedUser], tracker: SequenceTracker) -> int:
    """Seed user accounts into the database.

    If any user already exists in the database the entire seed is
    skipped.  This prevents mixing accounts from different profiles
    (e.g. an accidental ``migrate-dev`` on a production database).
    Passwords are bcrypt-hashed before insertion.

    Args:
        conn: Active SQLAlchemy connection.
        users: List of user seed entries.
        tracker: SequenceTracker for stamping session_id and sequence_id.

    Returns:
        Number of users inserted (0 when table is non-empty or list is empty).
    """
    if not users:
        return 0
    existing = conn.execute(text("SELECT COUNT(*) FROM users")).scalar() or 0
    if existing > 0:
        logger.info(f"Users table has {existing} rows, skipping user seed")
        return 0
    now = _timestamp_value(conn)
    for user in users:
        password_hash = _hash_password(user.password)
        conn.execute(
            text(
                "INSERT INTO users"
                " (public_id, username, email, password_hash, role, is_active,"
                "  created_at, timestamp, known_to, session_id, sequence_id)"
                " VALUES (:public_id, :username, :email, :password_hash, :role, 1,"
                "  :created_at, :timestamp, :known_to, :session_id, :sequence_id)"
            ),
            {
                "public_id": str(uuid7()),
                "username": user.username,
                "email": user.email,
                "password_hash": password_hash,
                "role": user.role,
                "created_at": now,
                "timestamp": now,
                "known_to": _known_to_value(conn),
                "session_id": tracker.session_id,
                "sequence_id": tracker.next_sequence("users"),
            },
        )
    logger.info(f"Seeded {len(users)} users")
    return len(users)


def seed_settings(conn: Connection, settings: list[SeedSetting], tracker: SequenceTracker) -> int:
    """Seed application settings into the database.

    Sensitive settings (detected by key pattern) are encrypted
    via ``get_encryption_service()`` before insertion.
    Uses INSERT OR IGNORE so manually configured values are never
    overwritten by a subsequent seed run.

    Args:
        conn: Active SQLAlchemy connection.
        settings: List of setting seed entries.
        tracker: SequenceTracker for stamping session_id and sequence_id.

    Returns:
        Number of new settings inserted (skips existing keys).
    """
    encryption = get_encryption_service()
    now = _timestamp_value(conn)
    inserted = 0
    for setting in settings:
        stored_value = setting.value
        is_encrypted = 0
        if SettingsEncryptionService.is_sensitive_setting(setting.key):
            stored_value = encryption.encrypt(setting.value)
            is_encrypted = 1
        result = conn.execute(
            text(
                "INSERT INTO settings"
                " (public_id, key, value, category, description, is_encrypted,"
                "  timestamp, known_to, session_id, sequence_id)"
                " SELECT :public_id, :key, :value, :category, :description, :is_encrypted,"
                "  :timestamp, :known_to, :session_id, :sequence_id"
                " WHERE NOT EXISTS (SELECT 1 FROM settings WHERE key = :key)"
            ),
            {
                "public_id": str(uuid7()),
                "key": setting.key,
                "value": stored_value,
                "category": setting.category,
                "description": setting.description,
                "is_encrypted": is_encrypted,
                "timestamp": now,
                "known_to": _known_to_value(conn),
                "session_id": tracker.session_id,
                "sequence_id": tracker.next_sequence("settings"),
            },
        )
        inserted += result.rowcount
    logger.info(f"Seeded {inserted} new settings ({len(settings) - inserted} already existed)")
    return inserted


def seed_default_multi_tenant(conn: Connection, tracker: SequenceTracker) -> int:
    """Seed the default Operator, Wallet, and UserOperatorMembership rows.

    Plan 0 Phase 0a (single-user deployment bootstrap). Creates:

    1. Operator ``label="default"`` — the seed trading identity used by
       the single-user deployment until an admin introduces additional
       operators.
    2. Wallet ``label="default-paper"`` with ``is_paper=True`` — a paper
       sandbox wallet that does not require live exchange credentials.
    3. UserOperatorMembership linking the first admin user in the
       ``users`` table to the default operator as ``is_primary=TRUE``.

    Idempotent: the function checks each target table and inserts only
    when the table is empty. A re-seed on an established DB is a no-op,
    matching ``seed_users`` / ``seed_settings`` semantics.

    Per Plan 0 D2 no default scope grants are inserted — the empty
    ``users`` table case (single-user bootstrap) has nothing to grant
    from, and UnderlyingAsset-based seed grants are deferred until the
    UnderlyingAsset seed lands in a later phase.

    Args:
        conn: Active SQLAlchemy connection (same transaction as the
            users/settings seed).
        tracker: ``SequenceTracker`` for stamping provenance columns.

    Returns:
        Count of rows inserted across operators + wallets + memberships.
    """
    inserted = 0
    now = _timestamp_value(conn)
    known_to = _known_to_value(conn)

    existing_ops = conn.execute(text("SELECT COUNT(*) FROM operators")).scalar() or 0
    existing_wallets = conn.execute(text("SELECT COUNT(*) FROM wallets")).scalar() or 0
    if existing_ops > 0 or existing_wallets > 0:
        logger.info(
            f"Multi-tenant bootstrap skipped: operators={existing_ops} wallets={existing_wallets}"
        )
        return 0

    operator_public_id = str(uuid7())
    conn.execute(
        text(
            "INSERT INTO operators"
            " (public_id, label, description, timestamp, known_to, session_id, sequence_id)"
            " VALUES"
            " (:public_id, :label, :description, :timestamp, :known_to, :session_id, :sequence_id)"
        ),
        {
            "public_id": operator_public_id,
            "label": "default",
            "description": "Default seed operator for single-user deployment",
            "timestamp": now,
            "known_to": known_to,
            "session_id": tracker.session_id,
            "sequence_id": tracker.next_sequence("operators"),
        },
    )
    inserted += 1

    wallet_public_id = str(uuid7())
    conn.execute(
        text(
            "INSERT INTO wallets"
            " (public_id, label, description, is_paper,"
            "  timestamp, known_to, session_id, sequence_id)"
            " VALUES"
            " (:public_id, :label, :description, :is_paper,"
            "  :timestamp, :known_to, :session_id, :sequence_id)"
        ),
        {
            "public_id": wallet_public_id,
            "label": "default-paper",
            "description": "Default paper-mode wallet seeded for single-user deployment",
            "is_paper": 1,
            "timestamp": now,
            "known_to": known_to,
            "session_id": tracker.session_id,
            "sequence_id": tracker.next_sequence("wallets"),
        },
    )
    inserted += 1

    admin_row = conn.execute(
        text(
            "SELECT public_id FROM users"
            " WHERE role = 'admin' AND known_to = :known_to"
            " ORDER BY id ASC LIMIT 1"
        ),
        {"known_to": known_to},
    ).first()
    if admin_row is not None:
        conn.execute(
            text(
                "INSERT INTO user_operator_memberships"
                " (public_id, user_public_id, operator_public_id, is_primary,"
                "  timestamp, known_to, session_id, sequence_id)"
                " VALUES"
                " (:public_id, :user_public_id, :operator_public_id, :is_primary,"
                "  :timestamp, :known_to, :session_id, :sequence_id)"
            ),
            {
                "public_id": str(uuid7()),
                "user_public_id": admin_row[0],
                "operator_public_id": operator_public_id,
                "is_primary": 1,
                "timestamp": now,
                "known_to": known_to,
                "session_id": tracker.session_id,
                "sequence_id": tracker.next_sequence("user_operator_memberships"),
            },
        )
        inserted += 1

    logger.info(
        f"Seeded multi-tenant bootstrap: 1 operator, 1 wallet, "
        f"{1 if admin_row is not None else 0} membership"
    )
    return inserted


def _sync_db_url(db_url: str) -> str:
    """Convert an async database URL to sync for direct engine use.

    Args:
        db_url: SQLAlchemy database URL (possibly async).

    Returns:
        Synchronous database URL.
    """
    if "aiosqlite" in db_url:
        return db_url.replace("sqlite+aiosqlite://", "sqlite://")
    return db_url


def run_seed(profile: str) -> tuple[int, int]:
    """Load a seed profile and apply it to the database.

    Creates a synchronous SQLAlchemy engine, loads the TOML profile,
    and seeds users and settings in a single transaction.

    Args:
        profile: Seed profile name (e.g. "dev", "prod").

    Returns:
        Tuple of (users_count, settings_count).
    """
    seed_data = load_seed_profile(profile)
    db_url = _sync_db_url(BootstrapSettingsLoader().db_url)
    engine = create_engine(db_url, poolclass=NullPool)
    tracker = SequenceTracker()
    with engine.connect() as conn:
        users_count = seed_users(conn, seed_data.users, tracker)
        settings_count = seed_settings(conn, seed_data.settings, tracker)
        seed_default_multi_tenant(conn, tracker)
        conn.commit()
    engine.dispose()
    return users_count, settings_count
