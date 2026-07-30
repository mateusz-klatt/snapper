"""Seed data loader for environment-specific database seeding.

Loads seed profiles from TOML files using a three-tier lookup:
``data/seed/{profile}.toml`` (CWD, e.g. Docker volume mount)
-> ``proprietary/data/seed/{profile}.toml`` (CWD, local dev)
-> package-bundled ``snapper/data/seed/{profile}.toml`` (installed wheel).

A seed profile is a self-sufficient declaration: format version 2 owns
operators and per-user operator memberships instead of leaving them to
be synthesized from roles in code. Wallet read grants and operator scope
grants are parsed and validated but are not written by ``db-seed``.
Every file stands alone — there is no cross-file inheritance and no
base/overlay merging.

Provides idempotent semantics so ``db-seed`` can be run repeatedly
without duplicating data. User creation and the complete multi-tenant
bootstrap run as one fresh-only unit: any historical row in users,
operators, memberships, wallets, credentials, or reconciliation-method
configs skips both. Settings retain per-key insert-if-absent semantics.

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
from typing import Literal
from typing import cast
from uuid import uuid7

import bcrypt
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.pool import NullPool

from snapper.application.portfolio.reconciliation_methods import PortfolioReconciliationMethod
from snapper.auth.domain.permissions import ROLE_PERMISSIONS
from snapper.auth.domain.permissions import Permission
from snapper.config.bootstrap import BootstrapSettingsLoader
from snapper.core.json_types import JsonObject
from snapper.core.json_types import JsonValue
from snapper.data.models import KNOWN_TO_MAX
from snapper.infrastructure.exchanges.reconciliation_policy import account_mode_for_exchange
from snapper.infrastructure.exchanges.reconciliation_policy import is_reconciliation_method_allowed
from snapper.infrastructure.security.encryption import SettingsEncryptionService
from snapper.infrastructure.security.encryption import get_encryption_service
from snapper.messaging.infrastructure.publisher import SequenceTracker

SUPPORTED_SEED_FORMAT_VERSIONS: frozenset[int] = frozenset({2})
"""Seed profile format versions this loader accepts.

Version 1 profiles described only settings, users and wallets, leaving
operators, memberships and scope grants to code-side synthesis. They are
rejected at parse time so a deployment fails loud and is hand-migrated
instead of silently seeding a database state no profile describes.
"""

_TOP_LEVEL_SECTIONS: frozenset[str] = frozenset(
    {"profile", "runtime_owned", "operators", "users", "settings", "wallets", "scope_grants"}
)
"""Sections a version-2 seed document is allowed to declare.

An unrecognised top-level section is either a typo or a fact the loader
would silently drop. Both contradict the premise that a profile is a
complete description of a database state, so they are rejected instead
of ignored.
"""

_READ_GRANT_FORBIDDEN_PERMISSIONS: frozenset[Permission] = frozenset(
    {Permission.CREATE_ORDERS, Permission.IMPERSONATE_OPERATOR}
)
"""Permissions whose holders must never be modelled as read-only principals.

A named permission set holding either permission can place orders or act
as an operator, so a read-only wallet grant on such a user is a modelling
error rather than a narrower entitlement.
"""


@dataclass
class SeedOperator:
    """Seed data for one operator (trading identity).

    Attributes:
        label: Operator label, unique within the profile.
        description: Human-readable purpose of the operator.
    """

    label: str
    description: str


@dataclass
class SeedReadGrant:
    """Seed data for one read-only wallet grant held by a user.

    Attributes:
        wallet: Label of the readable wallet.
        wallet_is_paper: Paper flag completing the wallet natural key.
        note: Operator-facing rationale for the read grant.
    """

    wallet: str
    wallet_is_paper: bool
    note: str


@dataclass
class SeedUser:
    """Seed data for a user account.

    Attributes:
        username: Login username (also used as primary key id).
        email: User email address.
        password: Plaintext password (will be bcrypt-hashed before insert).
        role: User role (admin, operator, viewer).
        operators: Labels of the operators this user is a member of.
        primary_operator: Label of the primary membership, or ``""``
            when the user holds no operator membership at all.
        readable_wallets: Read-only wallet grants held by this user.
    """

    username: str
    email: str
    password: str
    role: str
    operators: list[str] = field(default_factory=list)
    primary_operator: str = ""
    readable_wallets: list[SeedReadGrant] = field(default_factory=list)


@dataclass
class SeedScopeGrant:
    """Seed data for one operator market-scope grant on a wallet.

    The wallet is addressed by the natural ``(wallet, wallet_is_paper)``
    key because the wallets table is unique on ``(label, is_paper)``.

    Attributes:
        operator: Label of the granted operator, declared in ``[[operators]]``.
        wallet: Label of the wallet the scope is granted on.
        wallet_is_paper: Paper flag completing the wallet natural key.
        granted_by: Username of the declaring principal.
        scope_kind: Either ``"underlying"`` or ``"instrument"``.
        underlying: Underlying symbol for an underlying-kind grant.
        instrument: Instrument symbol for an instrument-kind grant.
        note: Operator-facing rationale for the grant.
    """

    operator: str
    wallet: str
    wallet_is_paper: bool
    granted_by: str
    scope_kind: Literal["underlying", "instrument"]
    underlying: str | None
    instrument: str | None
    note: str


@dataclass
class SeedRuntimeOwned:
    """Facts a profile deliberately leaves to runtime provisioning.

    An empty ``user_roles`` list is a deliberate statement that the
    profile owns every user row, not an omission.

    Attributes:
        user_roles: Role names whose users, memberships and caps are
            minted at runtime rather than declared by any profile.
    """

    user_roles: list[str] = field(default_factory=list)


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
            ``"paper"``. Determines which fields of this dataclass are
            packed into the encrypted envelope.
        reconciliation_method: Required explicit classification. Real methods
            create a live config row; ``unclassified`` creates no config row.
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
    reconciliation_method: PortfolioReconciliationMethod
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
        name: Profile identity declared in ``[profile]``.
        format_version: Declared seed format version.
        tier: Lookup tier the profile is authored for (1 = ``data/seed``,
            2 = ``proprietary/data/seed``, 3 = package-bundled).
        operators: Operators the profile declares.
        scope_grants: Operator market-scope grants the profile declares.
        runtime_owned: Facts the profile deliberately leaves to runtime.
    """

    users: list[SeedUser] = field(default_factory=list)
    settings: list[SeedSetting] = field(default_factory=list)
    wallets: list[SeedWallet] = field(default_factory=list)
    name: str = ""
    format_version: int = 0
    tier: int = 0
    operators: list[SeedOperator] = field(default_factory=list)
    scope_grants: list[SeedScopeGrant] = field(default_factory=list)
    runtime_owned: SeedRuntimeOwned = field(default_factory=SeedRuntimeOwned)


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
    for tier, path in enumerate(candidates, start=1):
        if path.exists():
            logger.info(f"Seed profile resolved: {path} (lookup tier {tier})")
            return path
    searched = ", ".join(str(p) for p in candidates)
    raise FileNotFoundError(f"Seed profile '{profile}' not found. Searched: {searched}")


def _as_int(value: JsonValue) -> int:
    """Return a decoded TOML integer scalar typed as ``int``."""
    return cast(int, value)


def _as_tables(value: JsonValue) -> list[JsonObject]:
    """Return a decoded TOML array-of-tables typed as a list of mappings."""
    return cast(list[JsonObject], value)


def _as_texts(value: JsonValue) -> list[str]:
    """Return a decoded TOML string array typed as a list of ``str``."""
    return [str(item) for item in cast(list[JsonValue], value)]


def _optional_text(entry: JsonObject, key: str) -> str | None:
    """Return a TOML string field, or ``None`` when absent or empty."""
    value = entry.get(key)
    if value is None or value == "":
        return None
    return str(value)


def _check_keys(
    entry: JsonObject, required: tuple[str, ...], optional: tuple[str, ...], context: str
) -> None:
    """Validate one seed table's key set against the version-2 format.

    Args:
        entry: Decoded TOML table to validate.
        required: Keys the format demands, in the order they are reported.
        optional: Keys the format allows but does not demand.
        context: Location prefix naming the offending table in messages.

    Raises:
        ValueError: When a required key is absent, or when the table
            declares a key outside the required and optional sets. An
            unrecognised key is a typo or a fact the loader would drop
            without a word, so it is rejected rather than ignored.
    """
    missing = [key for key in required if key not in entry]
    if missing:
        raise ValueError(f"{context} is missing required key(s): {', '.join(missing)}")
    unknown = sorted(key for key in entry if key not in required and key not in optional)
    if unknown:
        raise ValueError(f"{context} declares unknown key(s): {', '.join(unknown)}")


def _reject_unknown_sections(document: JsonObject, source: str) -> None:
    """Fail closed when a seed document declares a section the loader ignores."""
    unknown = sorted(key for key in document if key not in _TOP_LEVEL_SECTIONS)
    if unknown:
        raise ValueError(
            f"Seed profile '{source}' declares unknown section(s): {', '.join(unknown)}; "
            f"supported sections are {sorted(_TOP_LEVEL_SECTIONS)}"
        )


def _parse_profile_header(document: JsonObject, source: str) -> tuple[str, int, int]:
    """Parse the ``[profile]`` identity block and enforce the format version."""
    if "profile" not in document:
        raise ValueError(
            f"Seed profile '{source}' has no [profile] section; a v2 profile must declare "
            f"name, format_version and tier"
        )
    header = cast(JsonObject, document["profile"])
    _check_keys(
        header,
        ("name", "format_version", "tier"),
        (),
        f"[profile] in seed profile '{source}'",
    )
    format_version = _as_int(header["format_version"])
    if format_version not in SUPPORTED_SEED_FORMAT_VERSIONS:
        raise ValueError(
            f"Seed profile '{source}' declares unsupported format_version {format_version}; "
            f"this loader supports {sorted(SUPPORTED_SEED_FORMAT_VERSIONS)}"
        )
    return str(header["name"]), format_version, _as_int(header["tier"])


def _known_role_names() -> frozenset[str]:
    """Return every role name the permission model defines."""
    return frozenset(role.value for role in ROLE_PERMISSIONS)


def _parse_runtime_owned(document: JsonObject, source: str) -> SeedRuntimeOwned:
    """Parse the ``[runtime_owned]`` block naming runtime-minted facts."""
    if "runtime_owned" not in document:
        raise ValueError(
            f"Seed profile '{source}' has no [runtime_owned] section; declare an empty "
            f"user_roles list when the profile owns every user"
        )
    section = cast(JsonObject, document["runtime_owned"])
    _check_keys(section, ("user_roles",), (), f"[runtime_owned] in seed profile '{source}'")
    user_roles = _as_texts(section["user_roles"])
    known = _known_role_names()
    unknown = sorted(set(user_roles) - known)
    if unknown:
        raise ValueError(
            f"Seed profile '{source}' [runtime_owned] names unknown role(s): "
            f"{', '.join(unknown)}; known roles are {sorted(known)}"
        )
    return SeedRuntimeOwned(user_roles=user_roles)


def _parse_operators(document: JsonObject, source: str) -> list[SeedOperator]:
    """Parse ``[[operators]]`` entries and reject duplicate labels."""
    operators: list[SeedOperator] = []
    seen: set[str] = set()
    for index, entry in enumerate(_as_tables(document.get("operators", [])), start=1):
        _check_keys(
            entry,
            ("label", "description"),
            (),
            f"[[operators]] entry {index} in seed profile '{source}'",
        )
        label = str(entry["label"])
        if label in seen:
            raise ValueError(f"Seed profile '{source}' declares duplicate operator label '{label}'")
        seen.add(label)
        operators.append(SeedOperator(label=label, description=str(entry["description"])))
    return operators


def _read_grant_forbidden_roles() -> frozenset[str]:
    """Return role names whose permission set forbids declared read grants."""
    return frozenset(
        role.value
        for role, permissions in ROLE_PERMISSIONS.items()
        if _READ_GRANT_FORBIDDEN_PERMISSIONS & permissions
    )


def _parse_read_grants(entry: JsonObject, username: str, source: str) -> list[SeedReadGrant]:
    """Parse one user's ``[[users.readable_wallets]]`` entries."""
    grants: list[SeedReadGrant] = []
    seen: set[tuple[str, bool]] = set()
    for index, raw in enumerate(_as_tables(entry["readable_wallets"]), start=1):
        context = (
            f"[[users.readable_wallets]] entry {index} for user '{username}' "
            f"in seed profile '{source}'"
        )
        _check_keys(raw, ("wallet", "wallet_is_paper", "note"), (), context)
        wallet = str(raw["wallet"])
        wallet_is_paper = bool(raw["wallet_is_paper"])
        if (wallet, wallet_is_paper) in seen:
            raise ValueError(
                f"Seed profile '{source}' user '{username}' declares readable wallet "
                f"('{wallet}', is_paper={wallet_is_paper}) more than once"
            )
        seen.add((wallet, wallet_is_paper))
        grants.append(
            SeedReadGrant(
                wallet=wallet,
                wallet_is_paper=wallet_is_paper,
                note=str(raw["note"]),
            )
        )
    return grants


def _parse_users(document: JsonObject, source: str) -> list[SeedUser]:
    """Parse ``[[users]]`` entries with their membership and read-grant blocks."""
    forbidden_roles = _read_grant_forbidden_roles()
    users: list[SeedUser] = []
    seen: set[str] = set()
    for index, entry in enumerate(_as_tables(document.get("users", [])), start=1):
        _check_keys(
            entry,
            (
                "username",
                "email",
                "password",
                "role",
                "operators",
                "primary_operator",
                "readable_wallets",
            ),
            (),
            f"[[users]] entry {index} in seed profile '{source}'",
        )
        username = str(entry["username"])
        if username in seen:
            raise ValueError(f"Seed profile '{source}' declares duplicate username '{username}'")
        seen.add(username)
        role = str(entry["role"])
        operators = _as_texts(entry["operators"])
        if len(operators) != len(set(operators)):
            raise ValueError(
                f"Seed profile '{source}' user '{username}' declares duplicate "
                f"operator memberships {operators}"
            )
        primary_operator = str(entry["primary_operator"])
        if operators and not primary_operator:
            raise ValueError(
                f"Seed profile '{source}' user '{username}' declares operator "
                f"memberships {operators} but no primary_operator"
            )
        if primary_operator and primary_operator not in operators:
            raise ValueError(
                f"Seed profile '{source}' user '{username}' declares primary_operator "
                f"'{primary_operator}' outside its own operators list {operators}"
            )
        readable_wallets = _parse_read_grants(entry, username, source)
        if readable_wallets and role in forbidden_roles:
            raise ValueError(
                f"Seed profile '{source}' user '{username}' holds trade-capable role '{role}' "
                f"and must not declare readable_wallets; read grants model read-only principals"
            )
        users.append(
            SeedUser(
                username=username,
                email=str(entry["email"]),
                password=str(entry["password"]),
                role=role,
                operators=operators,
                primary_operator=primary_operator,
                readable_wallets=readable_wallets,
            )
        )
    return users


def _scope_kind_of(value: str, context: str) -> Literal["underlying", "instrument"]:
    """Return the validated scope-kind literal for one seed scope grant."""
    if value == "underlying":
        return "underlying"
    if value == "instrument":
        return "instrument"
    raise ValueError(
        f"{context} declares unknown scope_kind '{value}'; expected 'underlying' or 'instrument'"
    )


def _parse_scope_grants(document: JsonObject, source: str) -> list[SeedScopeGrant]:
    """Parse ``[[scope_grants]]`` entries and enforce the scope-kind XOR rule."""
    grants: list[SeedScopeGrant] = []
    for index, entry in enumerate(_as_tables(document.get("scope_grants", [])), start=1):
        context = f"[[scope_grants]] entry {index} in seed profile '{source}'"
        _check_keys(
            entry,
            ("operator", "wallet", "wallet_is_paper", "granted_by", "scope_kind", "note"),
            ("underlying", "instrument"),
            context,
        )
        scope_kind = _scope_kind_of(str(entry["scope_kind"]), context)
        underlying = _optional_text(entry, "underlying")
        instrument = _optional_text(entry, "instrument")
        target = f"operator '{entry['operator']}' wallet '{entry['wallet']}'"
        if scope_kind == "underlying" and (underlying is None or instrument is not None):
            raise ValueError(
                f"{context} for {target} uses scope_kind='underlying' and must set underlying "
                f"while leaving instrument unset"
            )
        if scope_kind == "instrument" and (instrument is None or underlying is not None):
            raise ValueError(
                f"{context} for {target} uses scope_kind='instrument' and must set instrument "
                f"while leaving underlying unset"
            )
        grants.append(
            SeedScopeGrant(
                operator=str(entry["operator"]),
                wallet=str(entry["wallet"]),
                wallet_is_paper=bool(entry["wallet_is_paper"]),
                granted_by=str(entry["granted_by"]),
                scope_kind=scope_kind,
                underlying=underlying,
                instrument=instrument,
                note=str(entry["note"]),
            )
        )
    return grants


def _parse_settings(document: JsonObject, source: str) -> list[SeedSetting]:
    """Parse ``[[settings]]`` entries into their dataclass."""
    settings: list[SeedSetting] = []
    for index, entry in enumerate(_as_tables(document.get("settings", [])), start=1):
        _check_keys(
            entry,
            ("key", "value", "category", "description"),
            (),
            f"[[settings]] entry {index} in seed profile '{source}'",
        )
        settings.append(
            SeedSetting(
                key=str(entry["key"]),
                value=str(entry["value"]),
                category=str(entry["category"]),
                description=str(entry["description"]),
            )
        )
    return settings


def _parse_wallet_credential(entry: JsonObject, context: str) -> SeedWalletCredential:
    """Parse one ``[[wallets.credentials]]`` table into its dataclass."""
    _check_keys(
        entry,
        ("exchange", "credential_type", "reconciliation_method"),
        ("api_key", "api_secret", "private_key_pem_base64", "initial_balance", "label"),
        context,
    )
    return SeedWalletCredential(
        exchange=str(entry["exchange"]),
        credential_type=str(entry["credential_type"]),
        reconciliation_method=cast(
            PortfolioReconciliationMethod, str(entry["reconciliation_method"])
        ),
        api_key=str(entry.get("api_key", "")),
        api_secret=str(entry.get("api_secret", "")),
        private_key_pem_base64=str(entry.get("private_key_pem_base64", "")),
        initial_balance=str(entry.get("initial_balance", "")),
        label=_optional_text(entry, "label"),
    )


def _parse_wallets(document: JsonObject, source: str) -> list[SeedWallet]:
    """Parse ``[[wallets]]`` entries with their nested credential tables.

    ``is_paper`` is required rather than defaulted: it completes the
    ``(label, is_paper)`` natural key every cross-reference validator
    matches on, and a silent default would quietly promote an
    under-specified entry to a live-money wallet.
    """
    wallets: list[SeedWallet] = []
    seen: set[tuple[str, bool]] = set()
    for index, entry in enumerate(_as_tables(document.get("wallets", [])), start=1):
        context = f"[[wallets]] entry {index} in seed profile '{source}'"
        _check_keys(entry, ("label", "is_paper"), ("description", "credentials"), context)
        label = str(entry["label"])
        is_paper = bool(entry["is_paper"])
        if (label, is_paper) in seen:
            raise ValueError(
                f"Seed profile '{source}' declares wallet "
                f"('{label}', is_paper={is_paper}) more than once"
            )
        seen.add((label, is_paper))
        wallets.append(
            SeedWallet(
                label=label,
                is_paper=is_paper,
                description=_optional_text(entry, "description"),
                credentials=[
                    _parse_wallet_credential(
                        raw, f"[[wallets.credentials]] entry {position} under {context}"
                    )
                    for position, raw in enumerate(
                        _as_tables(entry.get("credentials", [])), start=1
                    )
                ],
            )
        )
    return wallets


def _validate_user_references(profile: SeedProfile, source: str) -> None:
    """Fail closed when a user names an operator or wallet the profile omits."""
    operator_labels = {operator.label for operator in profile.operators}
    wallet_keys = {(wallet.label, wallet.is_paper) for wallet in profile.wallets}
    for user in profile.users:
        for label in user.operators:
            if label not in operator_labels:
                raise ValueError(
                    f"Seed profile '{source}' user '{user.username}' names operator '{label}' "
                    f"which is not declared in [[operators]]"
                )
        for grant in user.readable_wallets:
            if (grant.wallet, grant.wallet_is_paper) not in wallet_keys:
                raise ValueError(
                    f"Seed profile '{source}' user '{user.username}' names readable wallet "
                    f"('{grant.wallet}', is_paper={grant.wallet_is_paper}) which is not "
                    f"declared in [[wallets]]"
                )


def _validate_scope_grant_references(profile: SeedProfile, source: str) -> None:
    """Fail closed when a scope grant names an undeclared operator, user or wallet."""
    operator_labels = {operator.label for operator in profile.operators}
    wallet_keys = {(wallet.label, wallet.is_paper) for wallet in profile.wallets}
    usernames = {user.username for user in profile.users}
    for grant in profile.scope_grants:
        if grant.operator not in operator_labels:
            raise ValueError(
                f"Seed profile '{source}' scope grant names operator '{grant.operator}' "
                f"which is not declared in [[operators]]"
            )
        if grant.granted_by not in usernames:
            raise ValueError(
                f"Seed profile '{source}' scope grant names granted_by '{grant.granted_by}' "
                f"which is not declared in [[users]]"
            )
        if (grant.wallet, grant.wallet_is_paper) not in wallet_keys:
            raise ValueError(
                f"Seed profile '{source}' scope grant names wallet "
                f"('{grant.wallet}', is_paper={grant.wallet_is_paper}) which is not "
                f"declared in [[wallets]]"
            )


def _validate_runtime_owned_users(profile: SeedProfile, source: str) -> None:
    """Fail closed when a declared user holds a role stated to be runtime-owned."""
    runtime_roles = set(profile.runtime_owned.user_roles)
    for user in profile.users:
        if user.role in runtime_roles:
            raise ValueError(
                f"Seed profile '{source}' declares user '{user.username}' with role "
                f"'{user.role}', which [runtime_owned] states is minted at runtime"
            )


def _validate_profile_identity(profile: SeedProfile, requested: str, source: str) -> None:
    """Fail closed when the declared name contradicts the name it was loaded as.

    The declared ``tier`` is deliberately NOT cross-checked against the
    tier the file was resolved from: the container image copies the
    proprietary tier-2 profiles into the package-bundled tier-3 slot
    (``Dockerfile``), so one authored file legitimately resolves from
    different tiers depending on packaging. ``tier`` records where the
    profile is authored to live; the name is the identity that must hold
    everywhere.

    Args:
        profile: Parsed profile carrying the declared identity block.
        requested: Profile name the caller asked ``load_seed_profile`` for.
        source: Resolved file path, quoted in failure messages.

    Raises:
        ValueError: When the declared name is not the requested name. An
            unchecked name is decoration: a renamed or mis-copied file
            could otherwise claim to describe a state it does not seed.
    """
    if profile.name != requested:
        raise ValueError(
            f"Seed profile '{source}' declares name '{profile.name}' but was loaded as "
            f"'{requested}'; the identity block must name the profile it is resolved as"
        )


def _parse_seed_profile(document: JsonObject, source: str) -> SeedProfile:
    """Parse a decoded seed document into a fully validated ``SeedProfile``."""
    _reject_unknown_sections(document, source)
    name, format_version, tier = _parse_profile_header(document, source)
    profile = SeedProfile(
        users=_parse_users(document, source),
        settings=_parse_settings(document, source),
        wallets=_parse_wallets(document, source),
        name=name,
        format_version=format_version,
        tier=tier,
        operators=_parse_operators(document, source),
        scope_grants=_parse_scope_grants(document, source),
        runtime_owned=_parse_runtime_owned(document, source),
    )
    _validate_user_references(profile, source)
    _validate_scope_grant_references(profile, source)
    _validate_runtime_owned_users(profile, source)
    return profile


def load_seed_profile(profile: str) -> SeedProfile:
    """Load, parse and validate a seed profile from TOML.

    Args:
        profile: Seed profile name.

    Returns:
        Parsed SeedProfile with users, settings, wallets, operators,
        scope grants and the runtime-owned declaration.

    Raises:
        ValueError: When the resolved file is not a complete, internally
            consistent profile at a supported format version, or when its
            identity block declares a different profile name.
    """
    path = resolve_seed_path(profile)
    document: JsonObject = tomllib.loads(path.read_text(encoding="utf-8"))
    parsed = _parse_seed_profile(document, str(path))
    _validate_profile_identity(parsed, profile, str(path))
    return parsed


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
        KNOWN_TO_MAX datetime for non-SQLite engines. SQLite receives the
        microsecond-precise naive UTC spelling used by ``TZDateTime`` and
        the active-row partial indexes.
    """
    if conn.dialect.name == "sqlite":
        return KNOWN_TO_MAX.replace(tzinfo=None).isoformat(
            sep=" ",
            timespec="microseconds",
        )
    return KNOWN_TO_MAX


def _timestamp_value(conn: Connection) -> datetime | str:
    """Build a UTC timestamp value compatible with the current SQL driver.

    sqlite3 no longer provides an implicit datetime adapter. Returning
    the same naive UTC spelling as ``TZDateTime`` keeps direct seed SQL
    comparable with ORM-bound timestamps.

    Args:
        conn: Active SQLAlchemy connection.

    Returns:
        UTC datetime for non-SQLite engines, canonical naive UTC string
        for SQLite.
    """
    now = datetime.now(tz=UTC)
    if conn.dialect.name == "sqlite":
        return now.replace(tzinfo=None).isoformat(
            sep=" ",
            timespec="microseconds",
        )
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
                " VALUES (:public_id, :username, :email, :password_hash, :role, :is_active,"
                "  :created_at, :timestamp, :known_to, :session_id, :sequence_id)"
            ),
            {
                "public_id": str(uuid7()),
                "username": user.username,
                "email": user.email,
                "password_hash": password_hash,
                "role": user.role,
                "is_active": True,
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
        is_encrypted = False
        if SettingsEncryptionService.is_sensitive_setting(setting.key):
            stored_value = encryption.encrypt(setting.value)
            is_encrypted = True
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
                f"Invalid base64 in private_key_pem_base64 for exchange '{cred.exchange}': {exc}"
            ) from exc
        return json.dumps({"api_key": cred.api_key, "private_key_pem": pem_bytes.decode("utf-8")})
    if cred.credential_type == "paper":
        balance = cred.initial_balance or "10000.0"
        return json.dumps({"initial_balance": balance})
    raise ValueError(
        f"Unknown seed credential_type '{cred.credential_type}' for exchange "
        f"'{cred.exchange}'. Expected one of: api_key_secret, rsa_pem, paper."
    )


def _validate_seed_reconciliation_method(cred: SeedWalletCredential) -> None:
    """Fail closed when a seed classification contradicts its adapter policy."""
    if cred.exchange != cred.exchange.lower():
        raise ValueError(f"Seed credential exchange must be lowercase: '{cred.exchange}'")
    mode = account_mode_for_exchange(cred.exchange)
    paper_credential = cred.credential_type == "paper"
    paper_venue = mode == "paper"
    if paper_credential != paper_venue:
        raise ValueError(
            f"Seed paper credential and venue must agree for exchange '{cred.exchange}'"
        )
    if paper_venue:
        if cred.reconciliation_method != "unclassified":
            raise ValueError("Paper seed credentials must use reconciliation_method='unclassified'")
        return
    if cred.reconciliation_method == "unclassified":
        return
    if not is_reconciliation_method_allowed(cred.exchange, cred.reconciliation_method):
        raise ValueError(
            f"Reconciliation method '{cred.reconciliation_method}' is not allowed "
            f"for the registered '{cred.exchange}' adapter"
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
    insert. A real reconciliation method creates its config row in the same
    transaction. Returns the total number of inserted rows.

    Raises ``IntegrityError`` if two seed entries collide on
    ``(label, is_paper)`` so the operator fixes the TOML instead of
    getting a silently-merged wallet.
    """
    encryption = get_encryption_service()
    for credential in wallet.credentials:
        _validate_seed_reconciliation_method(credential)
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
            "is_paper": bool(wallet.is_paper),
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
        if cred.reconciliation_method != "unclassified":
            conn.execute(
                text(
                    "INSERT INTO portfolio_reconciliation_method_configs"
                    " (wallet_public_id, exchange, mode, method, public_id,"
                    "  session_id, sequence_id, timestamp, known_to)"
                    " VALUES"
                    " (:wallet_public_id, :exchange, :mode, :method, :public_id,"
                    "  :session_id, :sequence_id, :timestamp, :known_to)"
                ),
                {
                    "wallet_public_id": wallet_public_id,
                    "exchange": cred.exchange,
                    "mode": "live",
                    "method": cred.reconciliation_method,
                    "public_id": str(uuid7()),
                    "session_id": tracker.session_id,
                    "sequence_id": tracker.next_sequence("portfolio_reconciliation_method_configs"),
                    "timestamp": now,
                    "known_to": known_to,
                },
            )
            inserted += 1
    return inserted


def _seed_declared_operators(
    conn: Connection,
    operators: list[SeedOperator],
    tracker: SequenceTracker,
    now: datetime | str,
) -> dict[str, str]:
    """Insert exactly the operators declared by a fresh seed profile."""
    known_to = _known_to_value(conn)
    public_ids: dict[str, str] = {}
    for operator in operators:
        public_id = str(uuid7())
        conn.execute(
            text(
                "INSERT INTO operators"
                " (public_id, label, description, timestamp, known_to, session_id, sequence_id)"
                " VALUES"
                " (:public_id, :label, :description, :timestamp, :known_to,"
                "  :session_id, :sequence_id)"
            ),
            {
                "public_id": public_id,
                "label": operator.label,
                "description": operator.description,
                "timestamp": now,
                "known_to": known_to,
                "session_id": tracker.session_id,
                "sequence_id": tracker.next_sequence("operators"),
            },
        )
        public_ids[operator.label] = public_id
    return public_ids


def _seed_declared_memberships(
    conn: Connection,
    users: list[SeedUser],
    operator_public_ids: dict[str, str],
    tracker: SequenceTracker,
    now: datetime | str,
) -> int:
    """Insert the profile's exact user-to-operator membership relation."""
    known_to = _known_to_value(conn)
    user_rows = conn.execute(
        text("SELECT public_id, username FROM users WHERE known_to = :known_to ORDER BY id ASC"),
        {"known_to": known_to},
    ).all()
    user_public_ids = {str(row[1]): str(row[0]) for row in user_rows}
    inserted = 0
    for user in users:
        if not user.operators:
            continue
        user_public_id = user_public_ids.get(user.username)
        if user_public_id is None:
            raise ValueError(
                f"Cannot seed memberships for declared user '{user.username}': "
                "no active user row exists"
            )
        for operator_label in user.operators:
            operator_public_id = operator_public_ids.get(operator_label)
            if operator_public_id is None:
                raise ValueError(
                    f"Cannot seed membership for user '{user.username}': "
                    f"operator '{operator_label}' was not declared"
                )
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
                    "user_public_id": user_public_id,
                    "operator_public_id": operator_public_id,
                    "is_primary": operator_label == user.primary_operator,
                    "timestamp": now,
                    "known_to": known_to,
                    "session_id": tracker.session_id,
                    "sequence_id": tracker.next_sequence("user_operator_memberships"),
                },
            )
            inserted += 1
    return inserted


def seed_default_multi_tenant(
    conn: Connection,
    tracker: SequenceTracker,
    wallets: list[SeedWallet] | None = None,
    operators: list[SeedOperator] | None = None,
    users: list[SeedUser] | None = None,
) -> int:
    """Seed declared operators, wallets, credentials, and memberships.

    Creates:

    1. Exactly the operators declared by the profile, preserving each
       label and description.
    2. One ``Wallet`` + nested ``WalletCredential`` rows and requested
       real reconciliation-method configs per entry in the ``wallets``
       argument. Each wallet is identified by the
       ``(label, is_paper)`` unique key so a ``default``/paper and a
       ``default``/live wallet can coexist. When ``wallets`` is None
       or empty, a single hardcoded ``default``/paper wallet is
       created as the bootstrap fallback so fresh ``make migrate-dev``
       runs against seed profiles that predate the ``[[wallets]]``
       TOML format still produce a working paper sandbox.
    3. Exactly the memberships declared by each user's ``operators``
       and ``primary_operator`` fields. Roles do not imply membership.

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
        operators: Operator declarations from the profile. ``None``
            inserts no operators.
        users: User declarations carrying the exact membership relation.
            ``None`` inserts no memberships.

    Returns:
        Count of rows inserted across operators + wallets + memberships
        + wallet_credentials + reconciliation-method configs.
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

    operator_public_ids = _seed_declared_operators(conn, operators or [], tracker, now)
    inserted += len(operator_public_ids)

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
                        reconciliation_method="unclassified",
                        initial_balance="10000.0",
                        label="default paper bootstrap",
                    )
                ],
            )
        ]

    wallet_count = 0
    credential_count = 0
    method_config_count = 0
    for wallet in wallet_list:
        wallet_rows = _seed_wallet_with_credentials(conn, wallet, tracker, now, known_to)
        wallet_count += 1
        credential_count += len(wallet.credentials)
        method_config_count += sum(
            credential.reconciliation_method != "unclassified" for credential in wallet.credentials
        )
        inserted += wallet_rows

    membership_count = _seed_declared_memberships(
        conn,
        users or [],
        operator_public_ids,
        tracker,
        now,
    )
    inserted += membership_count

    logger.info(
        f"Seeded multi-tenant bootstrap: {len(operator_public_ids)} operator(s), "
        f"{wallet_count} wallet(s), "
        f"{membership_count} membership(s), "
        f"{credential_count} wallet credential(s), "
        f"{method_config_count} reconciliation method config(s)"
    )
    return inserted


def _sync_db_url(db_url: str) -> str:
    """Convert an async database URL to sync for direct engine use.

    Async drivers cannot drive ``create_engine``; seed + migration
    paths need the matching sync driver:

    * ``sqlite+aiosqlite://`` → ``sqlite://``
    * ``postgresql+asyncpg://`` → ``postgresql+psycopg2://``

    Both sync drivers (``sqlite3`` from stdlib, ``psycopg2-binary``)
    are runtime-required so the seed loader can run without a
    separate async event loop.

    Args:
        db_url: SQLAlchemy database URL (possibly async).

    Returns:
        Synchronous database URL.
    """
    if "aiosqlite" in db_url:
        return db_url.replace("sqlite+aiosqlite://", "sqlite://")
    if "asyncpg" in db_url:
        return db_url.replace("postgresql+asyncpg://", "postgresql+psycopg2://")
    return db_url


def run_seed(profile: str) -> tuple[int, int]:
    """Load a seed profile and apply it to the database.

    Creates a synchronous SQLAlchemy engine, loads the TOML profile,
    and seeds users, settings, and declared multi-tenant rows in a
    single transaction.

    Args:
        profile: Seed profile name (e.g. "dev", "prod").

    Returns:
        Tuple of (users_count, settings_count). Wallet/operator
        bootstrap counts are intentionally not part of the return
        value.
    """
    seed_data = load_seed_profile(profile)
    db_url = _sync_db_url(BootstrapSettingsLoader().db_url)
    engine = create_engine(db_url, poolclass=NullPool)
    tracker = SequenceTracker()
    with engine.connect() as conn:
        existing_bootstrap_rows = {
            "users": conn.execute(text("SELECT COUNT(*) FROM users")).scalar_one(),
            "operators": conn.execute(text("SELECT COUNT(*) FROM operators")).scalar_one(),
            "wallets": conn.execute(text("SELECT COUNT(*) FROM wallets")).scalar_one(),
            "memberships": conn.execute(
                text("SELECT COUNT(*) FROM user_operator_memberships")
            ).scalar_one(),
            "credentials": conn.execute(
                text("SELECT COUNT(*) FROM wallet_credentials")
            ).scalar_one(),
            "method_configs": conn.execute(
                text("SELECT COUNT(*) FROM portfolio_reconciliation_method_configs")
            ).scalar_one(),
        }
        bootstrap_is_fresh = not any(existing_bootstrap_rows.values())
        if bootstrap_is_fresh:
            users_count = seed_users(conn, seed_data.users, tracker)
        else:
            users_count = 0
            logger.info(
                "Tenant bootstrap tables were non-empty before db-seed "
                f"({existing_bootstrap_rows}); skipping users and multi-tenant bootstrap"
            )
        settings_count = seed_settings(conn, seed_data.settings, tracker)
        if bootstrap_is_fresh:
            seed_default_multi_tenant(
                conn,
                tracker,
                wallets=seed_data.wallets,
                operators=seed_data.operators,
                users=seed_data.users,
            )
        conn.commit()
    engine.dispose()
    return users_count, settings_count
