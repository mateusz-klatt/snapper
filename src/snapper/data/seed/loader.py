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

import base64
import json
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
class SeedWalletCredential:
    """Seed data for one wallet credential row (nested under a SeedWallet).

    Attributes:
        exchange: Exchange name (e.g. ``"kraken"``, ``"walutomat"``,
            ``"paper"``). Must pass the ``ck_wallet_credentials_exchange_lower``
            CHECK constraint — always lowercase.
        credential_type: One of ``"api_key_secret"``, ``"rsa_pem"``,
            ``"oauth"``, ``"paper"``. Determines which fields of this
            dataclass are packed into the encrypted envelope.
        api_key: Exchange API key (for ``api_key_secret`` / ``rsa_pem``).
        api_secret: Exchange API secret (for ``api_key_secret``).
        private_key_pem_base64: Base64-encoded PEM private key
            (for ``rsa_pem``, e.g. Walutomat).
        initial_balance: Starting cash balance (for ``paper``, as a
            string so the TOML type is unambiguous).
        label: Optional human-readable description of this credential
            row. Stored as-is in ``wallet_credentials.label``.
    """

    exchange: str
    credential_type: str
    api_key: str = ""
    api_secret: str = ""
    private_key_pem_base64: str = ""
    initial_balance: str = ""
    label: str | None = None


@dataclass
class SeedWallet:
    """Seed data for one wallet + its per-exchange credential rows.

    Represents one ``[[wallets]]`` entry in the profile TOML. The
    ``credentials`` list maps to nested ``[[wallets.credentials]]``
    sub-entries. Seed loader upserts the wallet by the
    ``(label, is_paper)`` unique key and
    inserts each credential row against that wallet's ``public_id``.

    Attributes:
        label: Human-readable identity for the wallet (no mode / no
            exchange suffix — those are separate columns).
        is_paper: ``True`` for simulated paper wallets, ``False`` for
            real-money live wallets.
        description: Optional free-text description.
        credentials: Per-exchange credential envelopes nested under
            this wallet.
    """

    label: str
    is_paper: bool
    description: str | None = None
    credentials: list[SeedWalletCredential] = field(default_factory=list)


@dataclass
class SeedProfile:
    """Complete seed profile parsed from TOML.

    Attributes:
        users: List of user seed entries.
        settings: List of setting seed entries.
        wallets: List of wallet seed entries (with nested credentials).
    """

    users: list[SeedUser] = field(default_factory=list)
    settings: list[SeedSetting] = field(default_factory=list)
    wallets: list[SeedWallet] = field(default_factory=list)


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
    wallets: list[SeedWallet] = []
    for wallet_entry in data.get("wallets", []):
        credential_entries = wallet_entry.get("credentials", [])
        credentials = [SeedWalletCredential(**cred) for cred in credential_entries]
        wallets.append(
            SeedWallet(
                label=wallet_entry["label"],
                is_paper=bool(wallet_entry.get("is_paper", False)),
                description=wallet_entry.get("description"),
                credentials=credentials,
            )
        )
    return SeedProfile(users=users, settings=settings, wallets=wallets)


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


def _build_credential_envelope(cred: SeedWalletCredential) -> str:
    """Pack a SeedWalletCredential into the JSON envelope stored in DB.

    The shape of the envelope depends on ``credential_type`` and must
    match what ``CredentialResolver`` + the concrete
    ``_create_exchange_client`` methods expect to read back:

    - ``api_key_secret`` → ``{"api_key": ..., "api_secret": ...}``
    - ``rsa_pem`` → ``{"api_key": ..., "private_key_pem": ...}`` where
      the PEM value is passed in the seed as base64 (so a multi-line
      PEM fits cleanly in a TOML string literal) and decoded here.
    - ``paper`` → ``{"initial_balance": "..."}``

    Any unknown ``credential_type`` raises ``ValueError`` so a typo in
    the seed file fails fast instead of storing an empty envelope.
    """
    if cred.credential_type == "api_key_secret":
        return json.dumps({"api_key": cred.api_key, "api_secret": cred.api_secret})
    if cred.credential_type == "rsa_pem":

        try:
            pem_bytes = base64.b64decode(cred.private_key_pem_base64)
        except Exception as exc:
            raise ValueError(
                f"Invalid base64 in private_key_pem_base64 for exchange "
                f"'{cred.exchange}': {exc}"
            ) from exc
        return json.dumps({"api_key": cred.api_key, "private_key_pem": pem_bytes.decode("utf-8")})
    if cred.credential_type == "paper":
        balance = cred.initial_balance or "10000.0"
        return json.dumps({"initial_balance": balance})
    raise ValueError(
        f"Unknown seed credential_type '{cred.credential_type}' for exchange "
        f"'{cred.exchange}'. Expected one of: api_key_secret, rsa_pem, paper."
    )


def _seed_wallet_with_credentials(
    conn: Connection,
    wallet: SeedWallet,
    tracker: SequenceTracker,
    now: datetime | str,
    known_to: datetime | str,
) -> int:
    """Insert one wallet row and its nested credential rows.

    Called per ``[[wallets]]`` entry in the seed TOML. Encrypts every
    credential payload with the master-password Fernet key before
    insert. Returns the number of rows inserted (always ``1 + len(
    credentials)`` unless the wallet already exists — see the unique
    index on ``(label, is_paper)``).

    Raises ``IntegrityError`` if two seed entries collide on
    ``(label, is_paper)`` so the operator fixes the TOML instead of
    getting a silently-merged wallet.
    """
    encryption = get_encryption_service()
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
            "label": wallet.label,
            "description": wallet.description,
            "is_paper": 1 if wallet.is_paper else 0,
            "timestamp": now,
            "known_to": known_to,
            "session_id": tracker.session_id,
            "sequence_id": tracker.next_sequence("wallets"),
        },
    )
    inserted = 1
    for cred in wallet.credentials:
        envelope = _build_credential_envelope(cred)
        encrypted_payload = encryption.encrypt(envelope)
        conn.execute(
            text(
                "INSERT INTO wallet_credentials"
                " (public_id, wallet_public_id, exchange, credential_type,"
                "  encrypted_payload, label,"
                "  timestamp, known_to, session_id, sequence_id)"
                " VALUES"
                " (:public_id, :wallet_public_id, :exchange, :credential_type,"
                "  :encrypted_payload, :label,"
                "  :timestamp, :known_to, :session_id, :sequence_id)"
            ),
            {
                "public_id": str(uuid7()),
                "wallet_public_id": wallet_public_id,
                "exchange": cred.exchange,
                "credential_type": cred.credential_type,
                "encrypted_payload": encrypted_payload,
                "label": cred.label,
                "timestamp": now,
                "known_to": known_to,
                "session_id": tracker.session_id,
                "sequence_id": tracker.next_sequence("wallet_credentials"),
            },
        )
        inserted += 1
    return inserted


def seed_default_multi_tenant(
    conn: Connection,
    tracker: SequenceTracker,
    wallets: list[SeedWallet] | None = None,
) -> int:
    """Seed the default Operator, Wallets (with credentials), and memberships.

    Creates:

    1. Operator ``label="default"`` — the seed trading identity used
       by the single-user deployment until an admin introduces
       additional operators.
    2. One ``Wallet`` + nested ``WalletCredential`` rows per entry in
       the ``wallets`` argument. Each wallet is identified by the
       ``(label, is_paper)`` unique key so a ``default``/paper and a
       ``default``/live wallet can coexist. When ``wallets`` is None
       or empty, a single hardcoded ``default``/paper wallet is
       created as the bootstrap fallback so fresh ``make migrate-dev``
       runs against seed profiles that predate the ``[[wallets]]``
       TOML format still produce a working paper sandbox.
    3. UserOperatorMembership linking the first admin user in the
       ``users`` table to the default operator as ``is_primary=TRUE``.

    Idempotent: the function checks the ``operators`` and ``wallets``
    tables and skips the entire bootstrap when either is non-empty.
    A re-seed on an established DB is a no-op, matching the
    ``seed_users`` / ``seed_settings`` semantics.

    No default scope grants are inserted.

    Args:
        conn: Active SQLAlchemy connection (same transaction as the
            users/settings seed).
        tracker: ``SequenceTracker`` for stamping provenance columns.
        wallets: Optional list of ``SeedWallet`` entries from the
            profile TOML. When ``None`` or empty, a single hardcoded
            ``default``/paper wallet with an ``{"initial_balance":
            "10000.0"}`` credential envelope is inserted as the
            legacy bootstrap path.

    Returns:
        Count of rows inserted across operators + wallets + memberships
        + wallet_credentials.
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

    wallet_list: list[SeedWallet] = wallets or []
    if not wallet_list:
        wallet_list = [
            SeedWallet(
                label="default",
                is_paper=True,
                description="Default paper-mode wallet seeded for single-user deployment",
                credentials=[
                    SeedWalletCredential(
                        exchange="paper",
                        credential_type="paper",
                        initial_balance="10000.0",
                        label="default paper bootstrap",
                    )
                ],
            )
        ]

    wallet_count = 0
    credential_count = 0
    for wallet in wallet_list:
        wallet_rows = _seed_wallet_with_credentials(conn, wallet, tracker, now, known_to)
        wallet_count += 1
        credential_count += wallet_rows - 1
        inserted += wallet_rows

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
        f"Seeded multi-tenant bootstrap: 1 operator, {wallet_count} wallet(s), "
        f"{1 if admin_row is not None else 0} membership, "
        f"{credential_count} wallet credential(s)"
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
        seed_default_multi_tenant(conn, tracker, wallets=seed_data.wallets)
        conn.commit()
    engine.dispose()
    return users_count, settings_count
