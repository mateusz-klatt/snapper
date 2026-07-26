"""Tests for ``snapper token preflight``.

The command exists so an operator can find out BEFORE a deploy whether
a credential that cannot be re-minted still authenticates. Its value
rests on two properties, and these tests pin both: the verdict comes
from the same verifier the running server uses, and nothing replayable
ever reaches the output.
"""

import json
from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from snapper.application.services.settings import SettingsService
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.tokens import TOKEN_TYPE_ACCESS
from snapper.auth.tokens import TOKEN_TYPE_REFRESH
from snapper.auth.tokens import TokenManager
from snapper.auth.tokens import hash_token
from snapper.cli.app import app
from snapper.cli.token_preflight import BRIDGE_CONFIG_TOKEN_KEY
from snapper.data.repository import Repository
from snapper.data.repository_types import UserActiveTokenVerificationRow

_USER_ID = "user-preflight"


@pytest.fixture
def runner() -> CliRunner:
    """Provide an isolated Typer CLI runner."""
    return CliRunner()


@pytest.fixture(autouse=True)
def settings_service() -> Iterator[MagicMock]:
    """Stand in for the DB-backed settings service the command must bind.

    Returns the caller's default for every key, so a test that opts out
    of the autouse settings mock (``@pytest.mark.real_settings``)
    resolves ``auth_algorithm`` through the genuine ``AppSettings``
    rather than through a test double. That is what makes the binding
    regression test able to fail.
    """
    service = MagicMock(spec=SettingsService)
    service.get_setting = lambda key, default=None: default
    bootstrap = MagicMock()
    bootstrap.db_url = "sqlite+aiosqlite:///:memory:"
    bootstrap.zmq_broker_xsub = "tcp://127.0.0.1:7500"
    with (
        patch(
            "snapper.cli.token_preflight.get_settings_service",
            new=AsyncMock(return_value=service),
        ),
        patch(
            "snapper.cli.token_preflight.get_bootstrap_settings",
            return_value=bootstrap,
        ),
        patch(
            "snapper.cli.token_preflight.dispose_repositories",
            new=AsyncMock(return_value=None),
        ),
    ):
        yield service


@pytest.fixture
def repository() -> Iterator[MagicMock]:
    """Bind the command to a typed repository double for one test."""
    double = MagicMock(spec=Repository)
    with patch(
        "snapper.cli.token_preflight.get_repository",
        return_value=cast(Repository, double),
    ):
        yield double


def _manager() -> TokenManager:
    """Clean-state token manager for minting a real signed credential."""
    TokenManager._initialized = False
    manager = TokenManager()
    manager._blacklisted_tokens.clear()
    manager._blacklist_cleanup_heap.clear()
    manager._next_blacklist_cleanup_ts = float("inf")
    manager._verify_cache.clear()
    manager._user_cache_generations.clear()
    return manager


def _mint(manager: TokenManager) -> str:
    """Mint one real access credential."""
    principal = AuthPrincipal(
        username="preflight-user",
        role=UserRole.AI_DELEGATE,
        user_public_id=_USER_ID,
    )
    return manager.create_tokens(principal).access_token


def _row(
    manager: TokenManager,
    token: str,
    *,
    token_type: str = TOKEN_TYPE_ACCESS,
    revoked: bool = False,
    user_is_active: bool = True,
) -> UserActiveTokenVerificationRow:
    """Inventory projection matching ``token``'s signed ``jti``."""
    claims = manager.verify_token(token)
    assert claims is not None
    now = datetime.now(UTC)
    return UserActiveTokenVerificationRow(
        user_public_id=_USER_ID,
        revoked_at=now if revoked else None,
        expires_at=now + timedelta(days=60),
        user_is_active=user_is_active,
        token_type=token_type,
        jti=claims.jti,
    )


def _write_bridge_config(tmp_path: Path, token: str) -> Path:
    """Write the MCP bridge ``--config=PATH`` envelope shape."""
    path = tmp_path / "bridge.json"
    path.write_text(
        json.dumps({"SNAPPER_BASE_URL": "http://x/api/mcp", BRIDGE_CONFIG_TOKEN_KEY: token}),
        encoding="utf-8",
    )
    return path


class TestPreflightVerdict:
    """The verdict must be the running server's verdict, not a re-derivation."""

    def test_a_live_access_credential_is_accepted(
        self,
        runner: CliRunner,
        repository: MagicMock,
        tmp_path: Path,
    ) -> None:
        """A credential the server would admit exits 0 and reports ACCEPTED.

        Given: a real signed access JWT whose hash resolves to an
            unrevoked, unexpired ``access`` row naming the same ``jti``,
        When: preflight runs against a raw-JWT file,
        Then: it exits 0, prints ACCEPTED, and publishes the row's
            purpose so the operator can see WHY.
        """
        manager = _manager()
        token = _mint(manager)
        repository.get_active_token_by_hash = AsyncMock(return_value=_row(manager, token))
        path = tmp_path / "raw.jwt"
        path.write_text(token, encoding="utf-8")
        result = runner.invoke(app, ["token", "preflight", "--token-file", str(path)])
        assert result.exit_code == 0
        assert "verdict: ACCEPTED" in result.stdout
        assert f"row token_type: {TOKEN_TYPE_ACCESS}" in result.stdout
        assert "jti agreement: yes" in result.stdout

    def test_it_reads_the_bridge_config_envelope(
        self,
        runner: CliRunner,
        repository: MagicMock,
        tmp_path: Path,
    ) -> None:
        """The operator can point it at the file the integration actually uses.

        Given: the JSON envelope an MCP bridge is configured with,
        When: preflight runs against it,
        Then: the embedded credential is the one checked.
        """
        manager = _manager()
        token = _mint(manager)
        repository.get_active_token_by_hash = AsyncMock(return_value=_row(manager, token))
        path = _write_bridge_config(tmp_path, token)
        result = runner.invoke(app, ["token", "preflight", "--token-file", str(path)])
        assert result.exit_code == 0
        assert "verdict: ACCEPTED" in result.stdout

    def test_a_refresh_row_is_rejected(
        self,
        runner: CliRunner,
        repository: MagicMock,
        tmp_path: Path,
    ) -> None:
        """A credential inventoried as refresh is not an access bearer.

        Given: a live row typed ``refresh`` for the presented JWT,
        When: preflight runs,
        Then: it exits 1 with REJECTED, having shown the row's type.
        """
        manager = _manager()
        token = _mint(manager)
        repository.get_active_token_by_hash = AsyncMock(
            return_value=_row(manager, token, token_type=TOKEN_TYPE_REFRESH)
        )
        path = tmp_path / "raw.jwt"
        path.write_text(token, encoding="utf-8")
        result = runner.invoke(app, ["token", "preflight", "--token-file", str(path)])
        assert result.exit_code == 1
        assert "verdict: REJECTED" in result.output
        assert f"row token_type: {TOKEN_TYPE_REFRESH}" in result.output

    def test_a_revoked_row_for_an_inactive_user_is_reported_and_rejected(
        self,
        runner: CliRunner,
        repository: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Dead lifecycle state is printed, not merely refused.

        Given: a revoked row owned by a deactivated user,
        When: preflight runs,
        Then: it exits 1 and the diagnostics name both conditions.
        """
        manager = _manager()
        token = _mint(manager)
        repository.get_active_token_by_hash = AsyncMock(
            return_value=_row(manager, token, revoked=True, user_is_active=False)
        )
        path = tmp_path / "raw.jwt"
        path.write_text(token, encoding="utf-8")
        result = runner.invoke(app, ["token", "preflight", "--token-file", str(path)])
        assert result.exit_code == 1
        assert "user active: NO" in result.output
        assert "revoked: NO" not in result.output

    def test_a_row_naming_a_different_jti_is_reported_and_rejected(
        self,
        runner: CliRunner,
        repository: MagicMock,
        tmp_path: Path,
    ) -> None:
        """A row that does not name the credential is surfaced explicitly.

        Given: an ``access`` row whose ``jti`` is not the signed one,
        When: preflight runs,
        Then: it exits 1 and reports the jti disagreement.
        """
        manager = _manager()
        token = _mint(manager)
        row = _row(manager, token)
        row["jti"] = "some-other-credential"
        repository.get_active_token_by_hash = AsyncMock(return_value=row)
        path = tmp_path / "raw.jwt"
        path.write_text(token, encoding="utf-8")
        result = runner.invoke(app, ["token", "preflight", "--token-file", str(path)])
        assert result.exit_code == 1
        assert "jti agreement: NO" in result.output

    def test_a_missing_inventory_row_is_reported_and_rejected(
        self,
        runner: CliRunner,
        repository: MagicMock,
        tmp_path: Path,
    ) -> None:
        """No row at all is the pre-deploy signal that matters most.

        Given: a hash that resolves to nothing,
        When: preflight runs,
        Then: it exits 1 and says the row was not found.
        """
        manager = _manager()
        token = _mint(manager)
        repository.get_active_token_by_hash = AsyncMock(return_value=None)
        path = tmp_path / "raw.jwt"
        path.write_text(token, encoding="utf-8")
        result = runner.invoke(app, ["token", "preflight", "--token-file", str(path)])
        assert result.exit_code == 1
        assert "inventory row: NOT FOUND" in result.output

    def test_it_never_prints_the_credential_or_its_hash(
        self,
        runner: CliRunner,
        repository: MagicMock,
        tmp_path: Path,
    ) -> None:
        """The output must be safe to paste into a ticket.

        Given: an accepted credential,
        When: preflight runs,
        Then: neither the JWT nor its SHA-256 hash appears anywhere in
            the output — publishing either would hand over a replayable
            secret or the exact inventory lookup key.
        """
        manager = _manager()
        token = _mint(manager)
        repository.get_active_token_by_hash = AsyncMock(return_value=_row(manager, token))
        path = _write_bridge_config(tmp_path, token)
        result = runner.invoke(app, ["token", "preflight", "--token-file", str(path)])
        assert result.exit_code == 0
        assert token not in result.output
        assert hash_token(token) not in result.output


class TestPreflightRefusals:
    """Unusable input refuses loudly rather than guessing."""

    def test_an_unreadable_file_refuses(
        self, runner: CliRunner, settings_service: MagicMock, tmp_path: Path
    ) -> None:
        """A path that cannot be read exits 1.

        Given: a path that does not exist,
        When: preflight runs,
        Then: it exits 1 with a refusal naming the path, without ever
            starting the settings service — a typo in ``--token-file``
            must not look like a deployment that is down.
        """
        missing = tmp_path / "absent.json"
        result = runner.invoke(app, ["token", "preflight", "--token-file", str(missing)])
        assert result.exit_code == 1
        assert "refused: cannot read" in result.output
        settings_service.shutdown.assert_not_awaited()

    def test_an_empty_file_refuses(self, runner: CliRunner, tmp_path: Path) -> None:
        """An empty file exits 1.

        Given: a file with no content,
        When: preflight runs,
        Then: it exits 1 saying the file is empty.
        """
        path = tmp_path / "empty.json"
        path.write_text("   \n", encoding="utf-8")
        result = runner.invoke(app, ["token", "preflight", "--token-file", str(path)])
        assert result.exit_code == 1
        assert "is empty" in result.output

    def test_json_that_is_not_an_object_refuses(self, runner: CliRunner, tmp_path: Path) -> None:
        """A JSON array is not a bridge envelope.

        Given: a file holding a JSON list,
        When: preflight runs,
        Then: it exits 1 rather than treating the text as a JWT.
        """
        path = tmp_path / "list.json"
        path.write_text("[1, 2]", encoding="utf-8")
        result = runner.invoke(app, ["token", "preflight", "--token-file", str(path)])
        assert result.exit_code == 1
        assert "not an object" in result.output

    def test_an_envelope_without_the_token_key_refuses(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """A JSON object lacking the credential key exits 1.

        Given: an envelope with no ``SNAPPER_ACCESS_TOKEN``,
        When: preflight runs,
        Then: it exits 1 naming the key it needed.
        """
        path = tmp_path / "bad.json"
        path.write_text(json.dumps({"SNAPPER_BASE_URL": "http://x"}), encoding="utf-8")
        result = runner.invoke(app, ["token", "preflight", "--token-file", str(path)])
        assert result.exit_code == 1
        assert BRIDGE_CONFIG_TOKEN_KEY in result.output

    def test_an_unverifiable_jwt_refuses_before_the_inventory_read(
        self, runner: CliRunner, repository: MagicMock, tmp_path: Path
    ) -> None:
        """A JWT that does not verify has nothing to look up.

        Given: a file holding text that is not a valid signed JWT,
        When: preflight runs,
        Then: it exits 1 and the inventory is never queried.
        """
        repository.get_active_token_by_hash = AsyncMock(return_value=None)
        path = tmp_path / "raw.jwt"
        path.write_text("not.a.jwt", encoding="utf-8")
        result = runner.invoke(app, ["token", "preflight", "--token-file", str(path)])
        assert result.exit_code == 1
        assert "failed signature/expiry verification" in result.output
        repository.get_active_token_by_hash.assert_not_awaited()


class TestPreflightConfiguration:
    """The command must load configuration the way the server does."""

    @pytest.mark.real_settings
    def test_it_binds_a_settings_service_before_verifying_anything(
        self,
        runner: CliRunner,
        repository: MagicMock,
        settings_service: MagicMock,
        tmp_path: Path,
    ) -> None:
        """An unbound settings service turns a live credential into a lockout.

        Regression: the command called ``verify_token`` against a token
        manager that had never been given a :class:`SettingsService`.
        The signing algorithm is a DB-backed setting, so the very first
        step raised ``RuntimeError`` and the command exited 1 — which
        its own contract defines as "this credential would be
        rejected". The one operator action the tool exists for would
        have reported a false lockout on a credential that works.

        Given: a live access credential and a token manager with
            nothing bound, exactly as a fresh CLI process starts,
        When: preflight runs under real settings, so the autouse mock
            cannot resolve ``auth_algorithm`` on the command's behalf,
        Then: it binds the service itself, raises nothing, and reports
            ACCEPTED.
        """
        manager = _manager()
        manager.set_settings_service(cast(SettingsService, settings_service))
        token = _mint(manager)
        repository.get_active_token_by_hash = AsyncMock(return_value=_row(manager, token))
        manager._settings = None
        path = tmp_path / "raw.jwt"
        path.write_text(token, encoding="utf-8")
        result = runner.invoke(app, ["token", "preflight", "--token-file", str(path)])
        assert result.exception is None
        assert result.exit_code == 0
        assert "verdict: ACCEPTED" in result.stdout
        assert manager._settings is not None

    def test_it_shuts_the_settings_service_down_on_every_path(
        self,
        runner: CliRunner,
        repository: MagicMock,
        settings_service: MagicMock,
        tmp_path: Path,
    ) -> None:
        """A read-only check must not leak the socket it opened.

        Given: a credential the server rejects, so the command exits
            down the refusal path,
        When: preflight runs,
        Then: the settings service is still shut down.
        """
        manager = _manager()
        token = _mint(manager)
        repository.get_active_token_by_hash = AsyncMock(return_value=None)
        path = tmp_path / "raw.jwt"
        path.write_text(token, encoding="utf-8")
        result = runner.invoke(app, ["token", "preflight", "--token-file", str(path)])
        assert result.exit_code == 1
        settings_service.shutdown.assert_awaited_once()
