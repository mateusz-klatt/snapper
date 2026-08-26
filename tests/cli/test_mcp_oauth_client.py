"""Integration tests for the MCP OAuth client provisioning CLI."""

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from typing import NoReturn

import bcrypt
import pytest
from typer.testing import CliRunner

import snapper.cli.mcp_oauth as mcp_oauth_cli
import snapper.mcp.oauth.client_provisioning as provisioning
from snapper.cli.app import app
from snapper.data.models import OAuthClient
from snapper.data.repository import SQLAlchemyRepository
from snapper.mcp.oauth.scopes import OFFLINE_ACCESS
from snapper.mcp.oauth.scopes import SNAPPER_ACCOUNT_READ
from snapper.mcp.oauth.scopes import SNAPPER_READ
from snapper.mcp.oauth.scopes import SNAPPER_RESEARCH_WRITE
from snapper.mcp.oauth.scopes import SNAPPER_REVIEW_WRITE
from snapper.mcp.oauth.scopes import SNAPPER_TRADE
from snapper.mcp.oauth.store import MCPOAuthStore

_REDIRECT_URI = "https://chatgpt.com/connector/oauth/callback"


async def _create_database(database_url: str) -> None:
    """Create and close one isolated CLI database."""
    repository = SQLAlchemyRepository(database_url)
    await repository.create_all()
    await repository.engine.dispose()


async def _load_client(database_url: str, client_id: str) -> OAuthClient | None:
    """Load and detach one provisioned row from an isolated CLI database."""
    repository = SQLAlchemyRepository(database_url)
    row = await MCPOAuthStore(repository).get_client(client_id)
    await repository.engine.dispose()
    return row


@pytest.fixture
def runner() -> CliRunner:
    """Provide an isolated Typer runner."""
    return CliRunner()


def test_cli_prints_secret_once_after_durable_hash_only_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    runner: CliRunner,
) -> None:
    """Verify the CLI reveals one credential only after hash-only persistence.

    Given: A migrated isolated database and valid provisioning options.
    When: The operator invokes the MCP OAuth provisioning command.
    Then: One JSON secret is printed and only its bcrypt hash exists in SQLite.
    """
    database_path = tmp_path / "oauth-cli.db"
    database_url = f"sqlite+aiosqlite:///{database_path}"
    asyncio.run(_create_database(database_url))
    monkeypatch.setattr(
        mcp_oauth_cli,
        "get_bootstrap_settings",
        lambda: SimpleNamespace(db_url=database_url),
    )

    def _token_urlsafe(byte_count: int) -> str:
        return {32: "cli-client-id", 48: "cli-secret-value"}[byte_count]

    monkeypatch.setattr(provisioning, "token_urlsafe", _token_urlsafe)
    result = runner.invoke(
        app,
        [
            "mcp-oauth",
            "provision-client",
            "--name",
            "Private ChatGPT",
            "--redirect-uri",
            _REDIRECT_URI,
        ],
    )

    assert result.exit_code == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload == {
        "allowed_scopes": [SNAPPER_READ, OFFLINE_ACCESS],
        "client_id": "snapper-mcp-cli-client-id",
        "client_name": "Private ChatGPT",
        "client_secret": "cli-secret-value",
        "redirect_uris": [_REDIRECT_URI],
        "token_endpoint_auth_method": "client_secret_basic",
    }
    assert result.stdout.count("cli-secret-value") == 1
    assert "cli-secret-value" not in result.stderr
    row = asyncio.run(_load_client(database_url, payload["client_id"]))
    assert row is not None
    assert bcrypt.checkpw(payload["client_secret"].encode(), row.client_secret_hash.encode())
    assert payload["client_secret"].encode() not in database_path.read_bytes()


def test_cli_refuses_duplicate_without_printing_second_secret(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    runner: CliRunner,
) -> None:
    """Verify a CLI collision refuses without revealing the failed secret.

    Given: A client already stored under the deterministically generated identifier.
    When: The operator invokes the same provisioning command again.
    Then: The command exits one and emits no raw credential on either output stream.
    """
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'oauth-cli-duplicate.db'}"
    asyncio.run(_create_database(database_url))
    monkeypatch.setattr(
        mcp_oauth_cli,
        "get_bootstrap_settings",
        lambda: SimpleNamespace(db_url=database_url),
    )

    def _token_urlsafe(byte_count: int) -> str:
        return {32: "duplicate-client", 48: "duplicate-secret"}[byte_count]

    monkeypatch.setattr(provisioning, "token_urlsafe", _token_urlsafe)
    arguments = [
        "mcp-oauth",
        "provision-client",
        "--name",
        "Private ChatGPT",
        "--redirect-uri",
        _REDIRECT_URI,
    ]
    first = runner.invoke(app, arguments)
    second = runner.invoke(app, arguments)

    assert first.exit_code == 0
    assert second.exit_code == 1
    assert second.stdout == ""
    assert "already exists" in second.stderr
    assert "duplicate-secret" not in second.stderr


@pytest.mark.parametrize(
    ("scope", "error_message"),
    [
        ("snapper.admin", "Unknown OAuth scope: snapper.admin"),
        (SNAPPER_ACCOUNT_READ, "initial read-only connector"),
        (SNAPPER_TRADE, "initial read-only connector"),
        (SNAPPER_REVIEW_WRITE, "initial read-only connector"),
        (SNAPPER_RESEARCH_WRITE, "initial read-only connector"),
    ],
)
def test_cli_rejects_unavailable_scope_before_opening_database(
    monkeypatch: pytest.MonkeyPatch,
    runner: CliRunner,
    scope: str,
    error_message: str,
) -> None:
    """Verify unknown and deferred scopes fail before database construction.

    Given: A requested scope outside the initial connector ceiling.
    When: The operator invokes client provisioning.
    Then: The command exits one without constructing a repository or gaining authority.
    """
    monkeypatch.setattr(
        mcp_oauth_cli,
        "get_bootstrap_settings",
        lambda: SimpleNamespace(db_url="sqlite+aiosqlite:///unused.db"),
    )
    opened = False

    def _unexpected_repository(_database_url: str) -> NoReturn:
        nonlocal opened
        opened = True
        raise AssertionError("repository must not open for invalid metadata")

    monkeypatch.setattr(mcp_oauth_cli, "get_repository", _unexpected_repository)
    result = runner.invoke(
        app,
        [
            "mcp-oauth",
            "provision-client",
            "--name",
            "Private ChatGPT",
            "--redirect-uri",
            _REDIRECT_URI,
            "--scope",
            scope,
        ],
    )

    assert result.exit_code == 1
    assert error_message in result.stderr
    assert opened is False
