"""Tests for seed data loader."""

import base64
import json
import tomllib
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
from snapper.data.seed.loader import SUPPORTED_SEED_FORMAT_VERSIONS
from snapper.data.seed.loader import SeedOperator
from snapper.data.seed.loader import SeedProfile
from snapper.data.seed.loader import SeedReadGrant
from snapper.data.seed.loader import SeedRuntimeOwned
from snapper.data.seed.loader import SeedScopeGrant
from snapper.data.seed.loader import SeedSetting
from snapper.data.seed.loader import SeedUser
from snapper.data.seed.loader import SeedWallet
from snapper.data.seed.loader import SeedWalletCredential
from snapper.data.seed.loader import _build_credential_envelope
from snapper.data.seed.loader import _hash_password
from snapper.data.seed.loader import _known_to_value
from snapper.data.seed.loader import _package_dir
from snapper.data.seed.loader import _parse_seed_profile
from snapper.data.seed.loader import _seed_declared_memberships
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

_SQLITE_KNOWN_TO_MAX = "9999-12-31 23:59:59.000000"


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


_V2_PREAMBLE = """[profile]
name = "unit"
format_version = 2
tier = 1

[runtime_owned]
user_roles = []

[[operators]]
label = "default"
description = "Unit-test seed operator"
"""
"""Minimal valid version-2 header shared by the inline parser tests."""

_V2_PAPER_WALLET = """
[[wallets]]
label = "paper"
is_paper = true
description = "Unit-test paper wallet"

[[wallets.credentials]]
exchange = "paper"
credential_type = "paper"
reconciliation_method = "unclassified"
initial_balance = "10000.0"
label = "paper bootstrap"
"""
"""One paper wallet so read/scope grants have a declared target."""

_V2_ADMIN_USER = """
[[users]]
username = "klattm"
email = "klattm@test.local"
password = "unit-test-password"
role = "admin"
operators = ["default"]
primary_operator = "default"
readable_wallets = []
"""
"""One trade-capable user usable as a scope grant's granted_by."""

_V2_UNDERLYING_GRANT = """
[[scope_grants]]
operator = "default"
wallet = "paper"
wallet_is_paper = true
granted_by = "klattm"
scope_kind = "underlying"
underlying = "BTC"
note = "heartbeat_consult_btc_1h AI reviews"
"""
"""One valid underlying-kind scope grant against the paper wallet."""


def _load_inline_profile(tmp_path: Path, body: str, *, requested: str = "unit") -> SeedProfile:
    """Parse an inline profile body through the real loader entry point.

    Args:
        tmp_path: Pytest temporary directory for the profile file.
        body: Complete TOML document text.
        requested: Profile name the loader is asked to resolve; defaults
            to the name declared by :data:`_V2_PREAMBLE` so the identity
            check passes.

    Returns:
        The parsed and validated profile.
    """
    toml_file = tmp_path / "inline.toml"
    toml_file.write_text(body, encoding="utf-8")
    with patch("snapper.data.seed.loader.resolve_seed_path", return_value=toml_file):
        return load_seed_profile(requested)


class TestLoadSeedProfile:
    """Tests for TOML seed profile loading."""

    def test_load_dev_profile(self) -> None:
        """Test loading dev seed profile.

        Given: dev.toml exists in seed directory,
        When: loading 'dev' profile,
        Then: profile contains expected users, settings and v2 identity.
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
        assert profile.name == "dev"
        assert profile.format_version == 2
        assert [operator.label for operator in profile.operators] == ["default"]
        assert all(user.primary_operator == "default" for user in profile.users)

    def test_load_profile_with_empty_optional_sections(self, tmp_path: Path) -> None:
        """Test loading a header-only profile.

        Given: a v2 profile declaring no users, settings or wallets,
        When: loading the profile,
        Then: the optional collections are empty and the header is parsed.
        """
        profile = _load_inline_profile(tmp_path, _V2_PREAMBLE)
        assert profile.users == []
        assert profile.settings == []
        assert profile.wallets == []
        assert profile.scope_grants == []
        assert profile.runtime_owned.user_roles == []
        assert profile.tier == 1

    def test_load_profile_users_only(self, tmp_path: Path) -> None:
        """Test loading profile with users only.

        Given: TOML file with users but no settings,
        When: loading the profile,
        Then: profile has users and empty settings.
        """
        body = _V2_PREAMBLE + """
[[users]]
username = "testuser"
email = "test@test.com"
password = "TestPass123!"
role = "admin"
operators = ["default"]
primary_operator = "default"
readable_wallets = []
"""
        profile = _load_inline_profile(tmp_path, body)
        assert len(profile.users) == 1
        assert profile.users[0].username == "testuser"
        assert profile.users[0].operators == ["default"]
        assert profile.settings == []

    def test_load_profile_settings_only(self, tmp_path: Path) -> None:
        """Test loading profile with settings only.

        Given: TOML file with settings but no users,
        When: loading the profile,
        Then: profile has settings and empty users.
        """
        body = _V2_PREAMBLE + """
[[settings]]
key = "test_key"
value = "test_value"
category = "test"
description = "Test setting"
"""
        profile = _load_inline_profile(tmp_path, body)
        assert profile.users == []
        assert len(profile.settings) == 1
        assert profile.settings[0].key == "test_key"


class TestSeedProfileFormatV2:
    """Parse-time validation of the self-sufficient version-2 profile format.

    A seed profile must describe a complete database state on its own.
    Every rejection here is raised while parsing, so an under-specified
    profile can never reach the write path and silently seed a state that
    no file describes.
    """

    def test_missing_profile_section_is_rejected(self, tmp_path: Path) -> None:
        """A document without the [profile] section is rejected.

        Given: a legacy version-1 document declaring only users,
        When: the profile is loaded,
        Then: a ValueError names the missing [profile] section.
        """
        body = """
[[users]]
username = "admin"
email = "admin@test.local"
password = "unit-test-password"
role = "admin"
operators = []
primary_operator = ""
readable_wallets = []
"""
        with pytest.raises(ValueError, match=r"has no \[profile\] section"):
            _load_inline_profile(tmp_path, body)

    def test_missing_profile_header_keys_are_rejected(self, tmp_path: Path) -> None:
        """The [profile] identity keys have no defaults.

        Given: a [profile] section declaring only name,
        When: the profile is loaded,
        Then: a ValueError names format_version and tier as missing.
        """
        body = '[profile]\nname = "unit"\n\n[runtime_owned]\nuser_roles = []\n'
        with pytest.raises(ValueError, match=r"missing required key\(s\): format_version, tier"):
            _load_inline_profile(tmp_path, body)

    def test_unsupported_format_version_is_rejected(self, tmp_path: Path) -> None:
        """Only the supported format versions parse.

        Given: a document declaring format_version 1,
        When: the profile is loaded,
        Then: a ValueError names the unsupported version.
        """
        assert 2 in SUPPORTED_SEED_FORMAT_VERSIONS
        assert 1 not in SUPPORTED_SEED_FORMAT_VERSIONS
        body = _V2_PREAMBLE.replace("format_version = 2", "format_version = 1")
        with pytest.raises(ValueError, match="unsupported format_version 1"):
            _load_inline_profile(tmp_path, body)

    def test_missing_runtime_owned_section_is_rejected(self, tmp_path: Path) -> None:
        """An absent [runtime_owned] section is an omission, not a statement.

        Given: a v2 header without the [runtime_owned] section,
        When: the profile is loaded,
        Then: a ValueError names the missing section.
        """
        body = _V2_PREAMBLE.replace("[runtime_owned]\nuser_roles = []\n\n", "")
        with pytest.raises(ValueError, match=r"has no \[runtime_owned\] section"):
            _load_inline_profile(tmp_path, body)

    def test_missing_user_membership_keys_are_rejected(self, tmp_path: Path) -> None:
        """Per-user membership keys may be empty but never absent.

        Given: a user table omitting operators and readable_wallets,
        When: the profile is loaded,
        Then: a ValueError names both missing keys.
        """
        body = _V2_PREAMBLE + """
[[users]]
username = "admin"
email = "admin@test.local"
password = "unit-test-password"
role = "admin"
primary_operator = "default"
"""
        with pytest.raises(
            ValueError, match=r"missing required key\(s\): operators, readable_wallets"
        ):
            _load_inline_profile(tmp_path, body)

    def test_primary_operator_outside_own_operators_is_rejected(self, tmp_path: Path) -> None:
        """A primary membership must be one of the user's own memberships.

        Given: a user whose primary_operator is absent from its operators list,
        When: the profile is loaded,
        Then: a ValueError names the offending primary_operator.
        """
        body = _V2_PREAMBLE + """
[[users]]
username = "admin"
email = "admin@test.local"
password = "unit-test-password"
role = "admin"
operators = []
primary_operator = "default"
readable_wallets = []
"""
        with pytest.raises(ValueError, match="declares primary_operator 'default' outside"):
            _load_inline_profile(tmp_path, body)

    def test_non_empty_operators_require_primary_operator(self, tmp_path: Path) -> None:
        """Every declared membership set has exactly one primary.

        Given: a user with one declared operator but an empty primary,
        When: the profile is loaded,
        Then: parsing fails before the write path can create zero primaries.
        """
        body = _V2_PREAMBLE + """
[[users]]
username = "admin"
email = "admin@test.local"
password = "unit-test-password"
role = "admin"
operators = ["default"]
primary_operator = ""
readable_wallets = []
"""
        with pytest.raises(ValueError, match="but no primary_operator"):
            _load_inline_profile(tmp_path, body)

    def test_duplicate_user_operator_membership_is_rejected(self, tmp_path: Path) -> None:
        """A user cannot declare the same desk membership twice.

        Given: a user whose operators list repeats one label,
        When: the profile is loaded,
        Then: parsing fails instead of reaching a database uniqueness error.
        """
        body = _V2_PREAMBLE + """
[[users]]
username = "admin"
email = "admin@test.local"
password = "unit-test-password"
role = "admin"
operators = ["default", "default"]
primary_operator = "default"
readable_wallets = []
"""
        with pytest.raises(ValueError, match="declares duplicate operator memberships"):
            _load_inline_profile(tmp_path, body)

    def test_user_operator_must_be_declared(self, tmp_path: Path) -> None:
        """A membership can only name an operator the profile declares.

        Given: a user that is a member of an undeclared operator label,
        When: the profile is loaded,
        Then: a ValueError names the undeclared operator.
        """
        body = _V2_PREAMBLE + """
[[users]]
username = "admin"
email = "admin@test.local"
password = "unit-test-password"
role = "admin"
operators = ["ghost"]
primary_operator = "ghost"
readable_wallets = []
"""
        with pytest.raises(ValueError, match="names operator 'ghost' which is not declared"):
            _load_inline_profile(tmp_path, body)

    def test_duplicate_operator_label_is_rejected(self, tmp_path: Path) -> None:
        """Operator labels are unique within a profile.

        Given: two [[operators]] entries sharing one label,
        When: the profile is loaded,
        Then: a ValueError names the duplicated label.
        """
        body = _V2_PREAMBLE + """
[[operators]]
label = "default"
description = "Duplicate of the first operator"
"""
        with pytest.raises(ValueError, match="duplicate operator label 'default'"):
            _load_inline_profile(tmp_path, body)

    def test_scope_grant_operator_must_be_declared(self, tmp_path: Path) -> None:
        """A scope grant can only name a declared operator.

        Given: a scope grant naming an undeclared operator label,
        When: the profile is loaded,
        Then: a ValueError names the undeclared operator.
        """
        body = (
            _V2_PREAMBLE
            + _V2_PAPER_WALLET
            + _V2_ADMIN_USER
            + _V2_UNDERLYING_GRANT.replace('operator = "default"', 'operator = "ghost"')
        )
        with pytest.raises(ValueError, match="scope grant names operator 'ghost'"):
            _load_inline_profile(tmp_path, body)

    def test_scope_grant_granted_by_must_be_declared(self, tmp_path: Path) -> None:
        """A scope grant can only be attributed to a declared user.

        Given: a scope grant whose granted_by names no declared user,
        When: the profile is loaded,
        Then: a ValueError names the unknown username.
        """
        body = (
            _V2_PREAMBLE
            + _V2_PAPER_WALLET
            + _V2_ADMIN_USER
            + _V2_UNDERLYING_GRANT.replace('granted_by = "klattm"', 'granted_by = "ghost"')
        )
        with pytest.raises(ValueError, match="scope grant names granted_by 'ghost'"):
            _load_inline_profile(tmp_path, body)

    def test_scope_grant_wallet_must_be_declared(self, tmp_path: Path) -> None:
        """A scope grant addresses a wallet by its natural key.

        Given: a scope grant whose paper flag matches no declared wallet,
        When: the profile is loaded,
        Then: a ValueError reports the unknown wallet key.
        """
        body = (
            _V2_PREAMBLE
            + _V2_PAPER_WALLET
            + _V2_ADMIN_USER
            + _V2_UNDERLYING_GRANT.replace("wallet_is_paper = true", "wallet_is_paper = false")
        )
        with pytest.raises(ValueError, match="scope grant names wallet"):
            _load_inline_profile(tmp_path, body)

    def test_read_grant_wallet_must_be_declared(self, tmp_path: Path) -> None:
        """A read grant addresses a wallet by its natural key.

        Given: a viewer read grant naming an undeclared wallet,
        When: the profile is loaded,
        Then: a ValueError reports the unknown wallet key.
        """
        body = _V2_PREAMBLE + _V2_PAPER_WALLET + """
[[users]]
username = "viewer"
email = "viewer@test.local"
password = "unit-test-password"
role = "viewer"
operators = []
primary_operator = ""

  [[users.readable_wallets]]
  wallet = "main"
  wallet_is_paper = false
  note = "read-only oversight"
"""
        with pytest.raises(ValueError, match="names readable wallet"):
            _load_inline_profile(tmp_path, body)

    def test_duplicate_read_grant_is_rejected(self, tmp_path: Path) -> None:
        """One user cannot declare the same readable wallet twice.

        Given: a viewer declaring the paper wallet twice,
        When: the profile is loaded,
        Then: a ValueError reports the repeated wallet key.
        """
        body = _V2_PREAMBLE + _V2_PAPER_WALLET + """
[[users]]
username = "viewer"
email = "viewer@test.local"
password = "unit-test-password"
role = "viewer"
operators = []
primary_operator = ""

  [[users.readable_wallets]]
  wallet = "paper"
  wallet_is_paper = true
  note = "read-only oversight of the paper book"

  [[users.readable_wallets]]
  wallet = "paper"
  wallet_is_paper = true
  note = "duplicate of the same wallet"
"""
        with pytest.raises(ValueError, match="more than once"):
            _load_inline_profile(tmp_path, body)

    def test_trade_capable_role_cannot_hold_read_grants(self, tmp_path: Path) -> None:
        """Read grants model read-only principals only.

        Given: an operator-role user declaring a readable wallet,
        When: the profile is loaded,
        Then: a ValueError rejects the trade-capable read grant.
        """
        body = _V2_PREAMBLE + _V2_PAPER_WALLET + """
[[users]]
username = "trader"
email = "trader@test.local"
password = "unit-test-password"
role = "operator"
operators = ["default"]
primary_operator = "default"

  [[users.readable_wallets]]
  wallet = "paper"
  wallet_is_paper = true
  note = "invalid read grant for a trade-capable role"
"""
        with pytest.raises(ValueError, match="must not declare readable_wallets"):
            _load_inline_profile(tmp_path, body)

    def test_unknown_scope_kind_is_rejected(self, tmp_path: Path) -> None:
        """Scope kinds are a closed set.

        Given: a scope grant declaring an unrecognised scope_kind,
        When: the profile is loaded,
        Then: a ValueError names the unknown scope_kind.
        """
        body = (
            _V2_PREAMBLE
            + _V2_PAPER_WALLET
            + _V2_ADMIN_USER
            + _V2_UNDERLYING_GRANT.replace('scope_kind = "underlying"', 'scope_kind = "venue"')
        )
        with pytest.raises(ValueError, match="unknown scope_kind 'venue'"):
            _load_inline_profile(tmp_path, body)

    def test_underlying_scope_kind_rejects_instrument_field(self, tmp_path: Path) -> None:
        """An underlying-kind grant must leave instrument unset.

        Given: an underlying-kind scope grant that also sets instrument,
        When: the profile is loaded,
        Then: a ValueError states the underlying-only requirement.
        """
        body = (
            _V2_PREAMBLE
            + _V2_PAPER_WALLET
            + _V2_ADMIN_USER
            + _V2_UNDERLYING_GRANT.replace(
                'underlying = "BTC"', 'underlying = "BTC"\ninstrument = "BTC/USD"'
            )
        )
        with pytest.raises(ValueError, match="must set underlying"):
            _load_inline_profile(tmp_path, body)

    def test_instrument_scope_kind_requires_instrument_field(self, tmp_path: Path) -> None:
        """An instrument-kind grant must set instrument and leave underlying unset.

        Given: an instrument-kind scope grant carrying only an underlying,
        When: the profile is loaded,
        Then: a ValueError states the instrument-only requirement.
        """
        body = (
            _V2_PREAMBLE
            + _V2_PAPER_WALLET
            + _V2_ADMIN_USER
            + _V2_UNDERLYING_GRANT.replace('scope_kind = "underlying"', 'scope_kind = "instrument"')
        )
        with pytest.raises(ValueError, match="must set instrument"):
            _load_inline_profile(tmp_path, body)

    def test_complete_profile_parses_every_declared_fact(self, tmp_path: Path) -> None:
        """A complete profile round-trips every v2 fact it declares.

        Given: a profile with runtime-owned roles, an operator, a wallet, a
            trade-capable member, a read-only viewer and both scope kinds,
        When: the profile is loaded,
        Then: every declared fact is present on the parsed profile.
        """
        body = (
            _V2_PREAMBLE.replace("user_roles = []", 'user_roles = ["ai_delegate"]')
            + _V2_PAPER_WALLET
            + _V2_ADMIN_USER
            + """
[[users]]
username = "viewer"
email = "viewer@test.local"
password = "unit-test-password"
role = "viewer"
operators = []
primary_operator = ""

  [[users.readable_wallets]]
  wallet = "paper"
  wallet_is_paper = true
  note = "read-only oversight of the paper book"
"""
            + _V2_UNDERLYING_GRANT
            + """
[[scope_grants]]
operator = "default"
wallet = "paper"
wallet_is_paper = true
granted_by = "klattm"
scope_kind = "instrument"
instrument = "PI_XBTUSD"
note = "single instrument delegation"
"""
        )
        profile = _load_inline_profile(tmp_path, body)
        assert profile.runtime_owned == SeedRuntimeOwned(user_roles=["ai_delegate"])
        assert profile.operators == [
            SeedOperator(label="default", description="Unit-test seed operator")
        ]
        assert profile.users[1].primary_operator == ""
        assert profile.users[1].readable_wallets == [
            SeedReadGrant(
                wallet="paper",
                wallet_is_paper=True,
                note="read-only oversight of the paper book",
            )
        ]
        assert profile.scope_grants == [
            SeedScopeGrant(
                operator="default",
                wallet="paper",
                wallet_is_paper=True,
                granted_by="klattm",
                scope_kind="underlying",
                underlying="BTC",
                instrument=None,
                note="heartbeat_consult_btc_1h AI reviews",
            ),
            SeedScopeGrant(
                operator="default",
                wallet="paper",
                wallet_is_paper=True,
                granted_by="klattm",
                scope_kind="instrument",
                underlying=None,
                instrument="PI_XBTUSD",
                note="single instrument delegation",
            ),
        ]
        assert profile.wallets[0].credentials[0].initial_balance == "10000.0"
        assert profile.wallets[0].description == "Unit-test paper wallet"

    def test_missing_runtime_owned_user_roles_is_rejected(self, tmp_path: Path) -> None:
        """A present [runtime_owned] section still has to state its roles.

        Given: a [runtime_owned] section declaring no user_roles key,
        When: the profile is loaded,
        Then: a ValueError names the missing key and its section.
        """
        body = _V2_PREAMBLE.replace("user_roles = []", "")
        with pytest.raises(
            ValueError, match=r"\[runtime_owned\] in seed profile .* missing required key\(s\): "
        ):
            _load_inline_profile(tmp_path, body)

    def test_unknown_runtime_owned_role_is_rejected(self, tmp_path: Path) -> None:
        """Runtime-owned roles are checked against the permission model.

        Given: a [runtime_owned] section naming a misspelled role,
        When: the profile is loaded,
        Then: a ValueError names the unknown role.
        """
        body = _V2_PREAMBLE.replace("user_roles = []", 'user_roles = ["ai_delegat"]')
        with pytest.raises(ValueError, match=r"names unknown role\(s\): ai_delegat"):
            _load_inline_profile(tmp_path, body)

    def test_runtime_owned_role_cannot_also_be_declared(self, tmp_path: Path) -> None:
        """A file cannot both declare a user and disown its role.

        Given: a profile listing 'admin' as runtime-owned while declaring
            an admin user,
        When: the profile is loaded,
        Then: a ValueError names the self-contradicting user and role.
        """
        body = _V2_PREAMBLE.replace("user_roles = []", 'user_roles = ["admin"]') + _V2_ADMIN_USER
        with pytest.raises(ValueError, match="which \\[runtime_owned\\] states is minted"):
            _load_inline_profile(tmp_path, body)

    def test_unknown_top_level_section_is_rejected(self, tmp_path: Path) -> None:
        """A section the loader would ignore is an error, not a no-op.

        Given: a profile declaring a misspelled [[scope_grant]] section,
        When: the profile is loaded,
        Then: a ValueError names the unknown section.
        """
        body = _V2_PREAMBLE + '\n[[scope_grant]]\noperator = "default"\n'
        with pytest.raises(ValueError, match=r"declares unknown section\(s\): scope_grant"):
            _load_inline_profile(tmp_path, body)

    def test_unknown_key_in_a_table_is_rejected(self, tmp_path: Path) -> None:
        """A key the loader would drop is an error, not a no-op.

        Given: a user table carrying a key outside the v2 format,
        When: the profile is loaded,
        Then: a ValueError names the unknown key and the offending entry.
        """
        body = _V2_PREAMBLE + _V2_ADMIN_USER + "is_superuser = true\n"
        with pytest.raises(
            ValueError,
            match=r"\[\[users\]\] entry 1 .* declares unknown key\(s\): is_superuser",
        ):
            _load_inline_profile(tmp_path, body)

    def test_duplicate_username_is_rejected(self, tmp_path: Path) -> None:
        """Usernames are unique within a profile.

        Given: two [[users]] entries sharing one username,
        When: the profile is loaded,
        Then: a ValueError names the duplicated username.
        """
        body = _V2_PREAMBLE + _V2_ADMIN_USER + _V2_ADMIN_USER
        with pytest.raises(ValueError, match="duplicate username 'klattm'"):
            _load_inline_profile(tmp_path, body)

    def test_missing_setting_keys_are_rejected(self, tmp_path: Path) -> None:
        """A settings table states all four columns or none of them.

        Given: a settings entry omitting description,
        When: the profile is loaded,
        Then: a ValueError names the entry and the missing key.
        """
        body = _V2_PREAMBLE + '\n[[settings]]\nkey = "k"\nvalue = "v"\ncategory = "c"\n'
        with pytest.raises(
            ValueError,
            match=r"\[\[settings\]\] entry 1 .* missing required key\(s\): description",
        ):
            _load_inline_profile(tmp_path, body)

    def test_wallet_must_declare_is_paper(self, tmp_path: Path) -> None:
        """The paper flag completes the wallet key and has no default.

        Given: a wallet entry declaring only its label,
        When: the profile is loaded,
        Then: a ValueError names the missing is_paper key.
        """
        body = _V2_PREAMBLE + '\n[[wallets]]\nlabel = "paper"\n'
        with pytest.raises(
            ValueError, match=r"\[\[wallets\]\] entry 1 .* missing required key\(s\): is_paper"
        ):
            _load_inline_profile(tmp_path, body)

    def test_wallet_credential_missing_keys_are_rejected(self, tmp_path: Path) -> None:
        """Credential envelopes state their exchange, type and method.

        Given: a nested credential entry omitting reconciliation_method,
        When: the profile is loaded,
        Then: a ValueError names the nested entry and the missing key.
        """
        body = _V2_PREAMBLE + _V2_PAPER_WALLET.replace(
            'reconciliation_method = "unclassified"\n', ""
        )
        with pytest.raises(
            ValueError,
            match=(
                r"\[\[wallets\.credentials\]\] entry 1 under \[\[wallets\]\] entry 1 .* "
                r"missing required key\(s\): reconciliation_method"
            ),
        ):
            _load_inline_profile(tmp_path, body)

    def test_duplicate_wallet_key_is_rejected(self, tmp_path: Path) -> None:
        """Wallets are unique on their (label, is_paper) natural key.

        Given: two [[wallets]] entries sharing one label and paper flag,
        When: the profile is loaded,
        Then: a ValueError names the duplicated wallet key.
        """
        body = _V2_PREAMBLE + _V2_PAPER_WALLET + _V2_PAPER_WALLET
        with pytest.raises(
            ValueError, match=r"declares wallet \('paper', is_paper=True\) more than once"
        ):
            _load_inline_profile(tmp_path, body)

    def test_declared_name_must_match_requested_profile(self, tmp_path: Path) -> None:
        """The identity block cannot claim to be a different profile.

        Given: a profile declaring name 'unit' resolved as 'prod',
        When: the profile is loaded,
        Then: a ValueError reports both names.
        """
        with pytest.raises(ValueError, match="declares name 'unit' but was loaded as 'prod'"):
            _load_inline_profile(tmp_path, _V2_PREAMBLE, requested="prod")

    def test_declared_tier_is_not_cross_checked_against_the_lookup_tier(
        self, tmp_path: Path
    ) -> None:
        """The declared tier records authorship, not the resolved slot.

        The container image copies the proprietary tier-2 profiles into
        the package-bundled tier-3 slot, so one authored file resolves
        from different tiers depending on packaging and the declared tier
        cannot be an equality check.

        Given: a profile declaring tier 2 loaded from an arbitrary path,
        When: the profile is loaded,
        Then: it parses and keeps the declared tier verbatim.
        """
        body = _V2_PREAMBLE.replace("tier = 1", "tier = 2")
        assert _load_inline_profile(tmp_path, body).tier == 2


class TestDocumentedSeedProfileExample:
    """The documented version-2 example must parse with the shipped loader.

    ``docs/configuration.md`` carries the canonical profile operators copy
    when authoring a new seed file. A documented example the loader
    rejects is worse than no example at all, so it is parsed here.
    """

    def test_documented_v2_examples_parse(self) -> None:
        """Every documented v2 TOML example is a loadable profile.

        Given: the fenced TOML blocks in docs/configuration.md that
            declare format_version 2,
        When: each block is parsed by the loader,
        Then: it parses and declares a supported format version.
        """
        docs = _package_dir().parents[3] / "docs" / "configuration.md"
        blocks = [
            block.split("\n", 1)[1]
            for block in docs.read_text(encoding="utf-8").split("```")[1::2]
            if block.startswith("toml\n") and "format_version = 2" in block
        ]
        assert blocks
        for block in blocks:
            profile = _parse_seed_profile(tomllib.loads(block), "docs/configuration.md")
            assert profile.format_version in SUPPORTED_SEED_FORMAT_VERSIONS


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
        Then: every collection is empty and the identity fields are unset.
        """
        profile = SeedProfile()
        assert profile.users == []
        assert profile.settings == []
        assert profile.operators == []
        assert profile.scope_grants == []
        assert profile.runtime_owned == SeedRuntimeOwned()
        assert profile.name == ""
        assert profile.format_version == 0
        assert profile.tier == 0

    def test_seed_user_membership_defaults(self) -> None:
        """Test SeedUser membership fields default to declared-nothing.

        Given: only the legacy identity arguments,
        When: creating a SeedUser instance,
        Then: the membership and read-grant fields are empty.
        """
        user = SeedUser(username="viewer", email="v@t.com", password="pass", role="viewer")
        assert user.operators == []
        assert user.primary_operator == ""
        assert user.readable_wallets == []


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
        """Test SQLite timestamp matches SQLAlchemy's naive UTC storage.

        Given: a connection dialect named "sqlite",
        When: building a seed timestamp value,
        Then: value is a microsecond-precise string with no offset suffix.
        """
        conn_mock = Mock()
        conn_mock.dialect.name = "sqlite"
        value = _timestamp_value(cast(Connection, conn_mock))
        assert isinstance(value, str)
        assert datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f").tzinfo is None

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
        """Test SQLite known_to matches the active-index sentinel exactly.

        Given: a connection dialect named "sqlite",
        When: building a known_to value,
        Then: value equals SQLAlchemy's microsecond-precise naive UTC binding.
        """
        conn_mock = Mock()
        conn_mock.dialect.name = "sqlite"
        value = _known_to_value(cast(Connection, conn_mock))
        assert value == "9999-12-31 23:59:59.000000"

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
        engine = create_engine(db_url, poolclass=NullPool)
        with engine.connect() as conn:
            operator = conn.execute(text("SELECT label, description FROM operators")).one()
            memberships = conn.execute(
                text(
                    "SELECT users.username, operators.label, memberships.is_primary"
                    " FROM user_operator_memberships AS memberships"
                    " JOIN users ON users.public_id = memberships.user_public_id"
                    " JOIN operators"
                    " ON operators.public_id = memberships.operator_public_id"
                    " ORDER BY users.username"
                )
            ).all()
            read_grant_count: int = conn.execute(
                text("SELECT COUNT(*) FROM wallet_user_read_grants")
            ).scalar_one()
            active_sentinel = "9999-12-31 23:59:59.000000"
            sentinel_counts: dict[str, tuple[int, int]] = {
                table: (
                    conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar_one(),
                    conn.execute(
                        text(f"SELECT COUNT(*) FROM {table} WHERE known_to = :known_to"),
                        {"known_to": active_sentinel},
                    ).scalar_one(),
                )
                for table in (
                    "users",
                    "operators",
                    "user_operator_memberships",
                    "wallets",
                    "wallet_credentials",
                    "portfolio_reconciliation_method_configs",
                    "settings",
                )
            }
        engine.dispose()
        assert operator == ("default", "Default seed operator for single-user deployment")
        assert memberships == [
            ("admin", "default", 1),
            ("operator", "default", 1),
            ("viewer", "default", 1),
        ]
        assert read_grant_count == 0
        assert all(total == active for total, active in sentinel_counts.values())

    def test_run_seed_missing_profile_raises(self) -> None:
        """Test run_seed raises for missing profile.

        Given: no seed file for 'nonexistent' profile,
        When: calling run_seed,
        Then: FileNotFoundError is raised.
        """
        with pytest.raises(FileNotFoundError, match="nonexistent"):
            run_seed("nonexistent")

    @pytest.mark.parametrize(
        "preexisting_table",
        [
            "users",
            "operators",
            "wallets",
            "user_operator_memberships",
            "wallet_credentials",
            "portfolio_reconciliation_method_configs",
        ],
    )
    def test_existing_tenant_state_prevents_partial_bootstrap(
        self,
        tmp_path: Path,
        preexisting_table: str,
    ) -> None:
        """Established tenant state cannot be partially merged with a profile.

        Given: one row in any seed-owned tenant-bootstrap table,
        When: the dev profile is seeded,
        Then: settings may seed but users, operators, wallets, and memberships
            remain unchanged.
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
            now = str(datetime.now(UTC))
            known_to = _SQLITE_KNOWN_TO_MAX
            existing_rows = {
                "users": (
                    "INSERT INTO users (public_id, username, email, password_hash,"
                    " role, is_active, created_at, timestamp, known_to, session_id, sequence_id)"
                    " VALUES ('existing-user', 'admin', 'existing@test.local', 'hash',"
                    " 'admin', 1, :now, :now, :known_to, 'existing', 1)"
                ),
                "operators": (
                    "INSERT INTO operators (public_id, label, description, timestamp,"
                    " known_to, session_id, sequence_id)"
                    " VALUES ('existing-operator', 'existing', 'existing desk', :now,"
                    " :known_to, 'existing', 1)"
                ),
                "wallets": (
                    "INSERT INTO wallets (public_id, label, description, is_paper,"
                    " timestamp, known_to, session_id, sequence_id)"
                    " VALUES ('existing-wallet', 'existing', 'existing wallet', 1,"
                    " :now, :known_to, 'existing', 1)"
                ),
                "user_operator_memberships": (
                    "INSERT INTO user_operator_memberships"
                    " (public_id, user_public_id, operator_public_id, is_primary,"
                    " timestamp, known_to, session_id, sequence_id)"
                    " VALUES ('existing-membership', 'orphan-user', 'orphan-operator', 1,"
                    " :now, :known_to, 'existing', 1)"
                ),
                "wallet_credentials": (
                    "INSERT INTO wallet_credentials"
                    " (public_id, wallet_public_id, exchange, credential_type,"
                    " encrypted_payload, label, timestamp, known_to, session_id, sequence_id)"
                    " VALUES ('existing-credential', 'orphan-wallet', 'paper', 'paper',"
                    " '{}', 'orphan credential', :now, :known_to, 'existing', 1)"
                ),
                "portfolio_reconciliation_method_configs": (
                    "INSERT INTO portfolio_reconciliation_method_configs"
                    " (public_id, wallet_public_id, exchange, mode, method,"
                    " timestamp, known_to, session_id, sequence_id)"
                    " VALUES ('existing-method', 'orphan-wallet', 'paper', 'paper',"
                    " 'unclassified', :now, :known_to, 'existing', 1)"
                ),
            }
            conn.execute(
                text(existing_rows[preexisting_table]),
                {"now": now, "known_to": known_to},
            )
            conn.commit()
        engine.dispose()

        with patch("snapper.data.seed.loader.BootstrapSettingsLoader") as mock_bootstrap:
            mock_bootstrap.return_value.db_url = db_url
            users_count, settings_count = run_seed("dev")

        assert users_count == 0
        assert settings_count >= 1
        engine = create_engine(db_url, poolclass=NullPool)
        with engine.connect() as conn:
            actual_counts: dict[str, int] = {
                "users": conn.execute(text("SELECT COUNT(*) FROM users")).scalar_one(),
                "operators": conn.execute(text("SELECT COUNT(*) FROM operators")).scalar_one(),
                "wallets": conn.execute(text("SELECT COUNT(*) FROM wallets")).scalar_one(),
                "user_operator_memberships": conn.execute(
                    text("SELECT COUNT(*) FROM user_operator_memberships")
                ).scalar_one(),
                "wallet_credentials": conn.execute(
                    text("SELECT COUNT(*) FROM wallet_credentials")
                ).scalar_one(),
                "portfolio_reconciliation_method_configs": conn.execute(
                    text("SELECT COUNT(*) FROM portfolio_reconciliation_method_configs")
                ).scalar_one(),
            }
            assert actual_counts == {
                table: int(table == preexisting_table)
                for table in (
                    "users",
                    "operators",
                    "wallets",
                    "user_operator_memberships",
                    "wallet_credentials",
                    "portfolio_reconciliation_method_configs",
                )
            }
        engine.dispose()

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

    def test_inserts_declared_operators_memberships_and_paper_credential(
        self, tmp_path: Path
    ) -> None:
        """Bootstrap preserves declared operators and the primary membership.

        Given: An SQLite database with an admin user already seeded and
            empty multi-tenant tables,
        When: ``seed_default_multi_tenant`` is invoked,
        Then: The declared desk, paper wallet, primary admin membership,
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
                {"ts": str(datetime.now(UTC)), "known_to": _SQLITE_KNOWN_TO_MAX},
            )
            conn.commit()

            operators = [
                SeedOperator(
                    label="desk-alpha",
                    description="Alpha desk declared by the seed profile",
                ),
                SeedOperator(
                    label="desk-beta",
                    description="Beta desk declared by the seed profile",
                ),
            ]
            user = SeedUser(
                username="admin",
                email="a@t.com",
                password="unused",
                role="admin",
                operators=["desk-alpha", "desk-beta"],
                primary_operator="desk-beta",
            )
            count = seed_default_multi_tenant(
                conn,
                SequenceTracker(),
                operators=operators,
                users=[user],
            )
            conn.commit()

            assert count == 6
            op_rows = conn.execute(
                text("SELECT label, description FROM operators ORDER BY label")
            ).all()
            assert op_rows == [
                ("desk-alpha", "Alpha desk declared by the seed profile"),
                ("desk-beta", "Beta desk declared by the seed profile"),
            ]
            wallet_row = conn.execute(
                text("SELECT public_id, label, is_paper FROM wallets")
            ).first()
            assert wallet_row is not None
            wallet_public_id = wallet_row[0]
            assert wallet_row[1] == "default"
            assert wallet_row[2] == 1
            membership_rows = conn.execute(
                text(
                    "SELECT operators.label, memberships.user_public_id,"
                    " memberships.is_primary"
                    " FROM user_operator_memberships AS memberships"
                    " JOIN operators"
                    " ON operators.public_id = memberships.operator_public_id"
                    " ORDER BY operators.label"
                )
            ).all()
            assert membership_rows == [
                ("desk-alpha", "user-admin", 0),
                ("desk-beta", "user-admin", 1),
            ]
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

    def test_declared_membership_requires_an_active_seeded_user(self, tmp_path: Path) -> None:
        """Membership seeding refuses a declaration with no active user row.

        Given: A declared viewer membership but an empty users table,
        When: The exact membership relation is materialized,
        Then: The missing active user fails loudly before any membership insert.
        """
        engine, conn = self._make_db(tmp_path)
        viewer = SeedUser(
            username="viewer",
            email="viewer@t.com",
            password="unused",
            role="viewer",
            operators=["default"],
            primary_operator="default",
        )
        try:
            s5778_value_1 = SequenceTracker()
            s5778_value_2 = str(datetime.now(UTC))
            with pytest.raises(ValueError, match="no active user row exists"):
                _seed_declared_memberships(
                    conn,
                    [viewer],
                    {"default": "operator-default"},
                    s5778_value_1,
                    s5778_value_2,
                )
            assert (
                conn.execute(text("SELECT COUNT(*) FROM user_operator_memberships")).scalar_one()
                == 0
            )
        finally:
            conn.close()
            cast(Any, engine).dispose()

    def test_declared_membership_requires_a_seeded_operator(self, tmp_path: Path) -> None:
        """Membership seeding refuses a declaration whose operator is absent.

        Given: An active viewer row and a membership naming an unmaterialized desk,
        When: The exact membership relation is materialized,
        Then: The missing operator fails loudly before any membership insert.
        """
        engine, conn = self._make_db(tmp_path)
        now = str(datetime.now(UTC))
        viewer = SeedUser(
            username="viewer",
            email="viewer@t.com",
            password="unused",
            role="viewer",
            operators=["default"],
            primary_operator="default",
        )
        try:
            conn.execute(
                text(
                    "INSERT INTO users (public_id, username, email, password_hash,"
                    " role, is_active, created_at, timestamp, known_to,"
                    " session_id, sequence_id)"
                    " VALUES ('user-viewer', 'viewer', 'viewer@t.com', 'hash',"
                    " 'viewer', 1, :now, :now, :known_to, 's', 1)"
                ),
                {"now": now, "known_to": _SQLITE_KNOWN_TO_MAX},
            )
            s5778_value_1 = SequenceTracker()
            with pytest.raises(ValueError, match="operator 'default' was not declared"):
                _seed_declared_memberships(
                    conn,
                    [viewer],
                    {},
                    s5778_value_1,
                    now,
                )
            assert (
                conn.execute(text("SELECT COUNT(*) FROM user_operator_memberships")).scalar_one()
                == 0
            )
        finally:
            conn.close()
            cast(Any, engine).dispose()

    def test_memberships_follow_declared_profile_exactly(self, tmp_path: Path) -> None:
        """Bootstrap memberships follow declarations rather than role permissions.

        Given: Active admin, operator, viewer, and AI-role users,
        When: The profile declares one primary default-desk membership for
            admin, operator, and viewer only,
        Then: Exactly those three memberships exist and no personal wallet
            read grants are synthesized.
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
                {"ts": str(datetime.now(UTC)), "known_to": _SQLITE_KNOWN_TO_MAX},
            )
            conn.commit()

            operator = SeedOperator(
                label="default",
                description="Default seed operator for single-user deployment",
            )
            declared_users = [
                SeedUser(
                    username=username,
                    email=f"{username}@t.com",
                    password="unused",
                    role=role,
                    operators=["default"] if username in {"admin", "operator", "viewer"} else [],
                    primary_operator=(
                        "default" if username in {"admin", "operator", "viewer"} else ""
                    ),
                )
                for username, role in (
                    ("admin", "admin"),
                    ("operator", "operator"),
                    ("viewer", "viewer"),
                    ("ai-researcher", "ai_researcher"),
                    ("ai-reviewer", "ai_reviewer"),
                    ("ai-delegate", "ai_delegate"),
                )
            ]
            count = seed_default_multi_tenant(
                conn,
                SequenceTracker(),
                operators=[operator],
                users=declared_users,
            )
            conn.commit()

            memberships = conn.execute(
                text(
                    "SELECT users.username, operators.label, memberships.is_primary"
                    " FROM user_operator_memberships AS memberships"
                    " JOIN users ON users.public_id = memberships.user_public_id"
                    " JOIN operators"
                    " ON operators.public_id = memberships.operator_public_id"
                    " ORDER BY users.username"
                )
            ).all()
            assert count == 6
            assert memberships == [
                ("admin", "default", 1),
                ("operator", "default", 1),
                ("viewer", "default", 1),
            ]
            assert conn.execute(text("SELECT COUNT(*) FROM wallet_user_read_grants")).scalar() == 0
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
                {"ts": str(datetime.now(UTC)), "known_to": _SQLITE_KNOWN_TO_MAX},
            )
            conn.commit()

            count = seed_default_multi_tenant(
                conn,
                SequenceTracker(),
                operators=[SeedOperator(label="new", description="must not be merged")],
            )
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
                {"ts": str(datetime.now(UTC)), "known_to": _SQLITE_KNOWN_TO_MAX},
            )
            conn.commit()

            count = seed_default_multi_tenant(
                conn,
                SequenceTracker(),
                operators=[SeedOperator(label="new", description="must not be merged")],
            )
            conn.commit()

            assert count == 0
            assert conn.execute(text("SELECT COUNT(*) FROM operators")).scalar() == 0
        finally:
            conn.close()
            cast(Any, engine).dispose()

    def test_viewer_without_declared_operator_gets_no_membership(self, tmp_path: Path) -> None:
        """A viewer role alone does not imply desk membership.

        Given: A viewer exists but its profile entry declares ``operators=[]``,
        When: ``seed_default_multi_tenant`` is invoked with a declared desk,
        Then: Operator + wallet + paper credential are inserted but no
            membership row (count == 3 instead of 4).
        """
        engine, conn = self._make_db(tmp_path)
        try:
            conn.execute(
                text(
                    "INSERT INTO users (public_id, username, email, password_hash,"
                    " role, is_active, created_at, timestamp, known_to,"
                    " session_id, sequence_id)"
                    " VALUES ('user-viewer', 'viewer', 'v@t.com', 'hash',"
                    " 'viewer', 1, :ts, :ts, :known_to, 's', 1)"
                ),
                {"ts": str(datetime.now(UTC)), "known_to": _SQLITE_KNOWN_TO_MAX},
            )
            viewer = SeedUser(
                username="viewer",
                email="v@t.com",
                password="unused",
                role="viewer",
                operators=[],
                primary_operator="",
            )
            count = seed_default_multi_tenant(
                conn,
                SequenceTracker(),
                operators=[SeedOperator(label="default", description="Default desk")],
                users=[viewer],
            )
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
            count = seed_default_multi_tenant(
                conn,
                SequenceTracker(),
                wallets=[wallet],
                operators=[SeedOperator(label="default", description="Default desk")],
            )
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
            "CREATE TABLE wallet_user_read_grants ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, public_id TEXT,"
            " user_public_id TEXT, wallet_public_id TEXT,"
            " note TEXT, granted_by_user_public_id TEXT,"
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
        """All settings in each resolvable profile fit the model columns.

        A profile that fails v2 validation is not loadable at all — its
        ``db-seed`` run fails loud before any INSERT — so the width guard
        has nothing to inspect and skips with the parse error. Tracked
        profiles keep unconditional coverage in
        :class:`TestTrackedSeedProfilesUseFormatV2`.

        Given: a seed profile name resolvable in this checkout,
        When: every parsed setting is measured against the ORM columns,
        Then: no key, category or description exceeds its varchar width.
        """
        try:
            seed = load_seed_profile(profile)
        except FileNotFoundError:
            pytest.skip(f"profile {profile!r} not present in this checkout")
        except ValueError as exc:
            pytest.skip(f"profile {profile!r} is not a loadable v2 seed profile: {exc}")
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


class TestTrackedSeedProfilesUseFormatV2:
    """Every git-tracked seed profile parses as a complete v2 description.

    Tier-1 ``data/seed`` files are deployment-local and untracked, so they
    can shadow a tracked profile name during resolution. These checks read
    the tracked files by path instead, keeping the format contract and the
    settings column-width guard honest regardless of local overrides.
    """

    def _tracked_profile_paths(self) -> list[tuple[str, Path]]:
        """Return the tracked (label, path) seed profiles present on disk.

        Returns:
            One entry per tracked profile file that exists in this checkout.
        """
        repo_root = _package_dir().parents[3]
        candidates = [
            ("bundled dev", _package_dir() / "dev.toml"),
            ("proprietary dev", repo_root / "proprietary" / "data" / "seed" / "dev.toml"),
            ("proprietary prod", repo_root / "proprietary" / "data" / "seed" / "prod.toml"),
        ]
        return [(name, path) for name, path in candidates if path.exists()]

    def test_tracked_profiles_declare_supported_format(self) -> None:
        """Tracked profiles parse and declare a supported format version.

        Given: the tracked seed profiles present in this checkout,
        When: each file is parsed directly by path,
        Then: each declares a supported format_version and a named profile.
        """
        tracked = self._tracked_profile_paths()
        assert tracked
        for name, path in tracked:
            profile = _parse_seed_profile(
                tomllib.loads(path.read_text(encoding="utf-8")), str(path)
            )
            assert profile.format_version in SUPPORTED_SEED_FORMAT_VERSIONS, name
            assert profile.name, name
            assert profile.tier in {2, 3}, name

    def test_tracked_dev_profiles_seed_default_human_desk_members(self) -> None:
        """Every tracked dev profile declares the complete default human desk graph."""
        tracked = [
            item
            for item in self._tracked_profile_paths()
            if item[0] in {"bundled dev", "proprietary dev"}
        ]
        assert tracked
        expected_roles = {
            "admin": "admin",
            "operator": "operator",
            "viewer": "viewer",
        }
        for name, path in tracked:
            profile = _parse_seed_profile(
                tomllib.loads(path.read_text(encoding="utf-8")), str(path)
            )
            users = {user.username: user for user in profile.users}
            for username, role in expected_roles.items():
                assert users[username].role == role, name
                assert users[username].operators == ["default"], name
                assert users[username].primary_operator == "default", name
                assert users[username].readable_wallets == [], name

    def test_tracked_profile_settings_fit_column_widths(self) -> None:
        """Tracked profile settings fit the Setting column widths.

        Given: the tracked seed profiles present in this checkout,
        When: each parsed setting is measured against the ORM column limits,
        Then: no key, category or description exceeds its varchar width.
        """
        limits = {
            "key": Setting.__table__.c.key.type.length,
            "category": Setting.__table__.c.category.type.length,
            "description": Setting.__table__.c.description.type.length,
        }
        for name, path in self._tracked_profile_paths():
            profile = _parse_seed_profile(
                tomllib.loads(path.read_text(encoding="utf-8")), str(path)
            )
            for setting in profile.settings:
                for field, limit in limits.items():
                    assert limit is not None
                    value = getattr(setting, field, None) or ""
                    assert len(value) <= limit, (
                        f"{name}: setting {setting.key!r} field {field!r}"
                        f" is {len(value)} chars > varchar({limit})"
                    )
