"""Tests for the helpers shared by operator commands."""

from unittest.mock import MagicMock

import pytest
import typer

from snapper.cli import operator_support


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
