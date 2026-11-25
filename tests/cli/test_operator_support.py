"""Tests for the helpers shared by operator commands."""

from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
import typer
from sqlalchemy.ext.asyncio import AsyncEngine

from snapper.cli import operator_support
from snapper.data.repository import dispose_repositories


def test_refusal_goes_to_stderr_with_a_non_zero_status(capsys: pytest.CaptureFixture[str]) -> None:
    """A refusal is one line on the error stream and a failing exit.

    Given: A refusal message.
    When: The command refuses.
    Then: The message is written to stderr only and the exit status is 1.
    """
    with pytest.raises(typer.Exit) as exit_info:
        operator_support.refuse("refused: nothing was written")

    captured = capsys.readouterr()
    assert exit_info.value.exit_code == 1
    assert captured.err == "refused: nothing was written\n"
    assert captured.out == ""


@pytest.mark.parametrize(
    ("db_url", "identity"),
    [
        (
            "postgresql+asyncpg://operator:secret@prod-db:5433/snapper_prod?ssl=require",
            "dialect=postgresql host=prod-db database=snapper_prod",
        ),
        ("sqlite+aiosqlite:///./data/dev.db", "dialect=sqlite host=(local) database=./data/dev.db"),
        ("sqlite+aiosqlite://", "dialect=sqlite host=(local) database=(none)"),
    ],
)
def test_database_identity_names_the_target_without_credentials(db_url: str, identity: str) -> None:
    """The printed target never carries a user, password, port or query.

    Given: A database URL with or without a host, database and credentials.
    When: Its identity is rendered for the operator.
    Then: Only the dialect, host and database appear, with placeholders for
        a missing host or database.
    """
    assert operator_support.database_identity(db_url) == identity


def test_repository_resolution_uses_the_configured_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """Repository resolution passes through the exact configured URL.

    Given: A patched repository factory and one database URL.
    When: The shared helper resolves the repository.
    Then: It returns the factory result and passes the URL unchanged.
    """
    resolved = MagicMock()
    factory = MagicMock(return_value=resolved)
    monkeypatch.setattr(operator_support, "get_repository", factory)

    result = operator_support.configured_repository("sqlite+aiosqlite:///fixture.db")

    assert result is resolved
    factory.assert_called_once_with("sqlite+aiosqlite:///fixture.db")


@pytest.mark.asyncio
async def test_named_repository_is_uncached_and_disposed_with_the_others(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Named SQLite repositories ignore connection naming and remain tracked for disposal.

    Given: One SQLite URL and multiple application names.
    When: Build named and unnamed repositories, then dispose the configuration.
    Then: The unnamed repository is cached and the tracked named repositories are distinct and
        disposed.
    """
    url = f"sqlite+aiosqlite:///{tmp_path / 'operator.db'}"
    cached = operator_support.configured_repository(url)
    named = operator_support.configured_repository(url, application_name="snapper:repair-candles")
    another = operator_support.configured_repository(url, application_name="snapper:repair-candles")
    assert cached is operator_support.configured_repository(url)
    assert cached is not named and named is not another
    await named.create_all()
    dispose = AsyncMock(wraps=AsyncEngine.dispose)

    async def dispose_engine(engine: AsyncEngine, close: bool = True) -> None:
        """Track the actual disposal, including the named engine and its pool."""
        await dispose(engine, close=close)

    monkeypatch.setattr(type(named.engine), "dispose", dispose_engine)
    await dispose_repositories()
    assert {cached.engine, named.engine, another.engine} <= {
        call.args[0] for call in dispose.await_args_list
    }
