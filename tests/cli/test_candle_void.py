"""Check that candle-void argument refusals never disclose raw credential-shaped tokens."""

from pathlib import Path

import pytest
from typer.testing import CliRunner

from snapper.cli import candle_void
from snapper.cli.app import app

_SECRET = "synthetic-C5-password"
_URL = f"postgresql+asyncpg://synthetic-user:{_SECRET}@db.example/synthetic"


@pytest.mark.parametrize(
    "tail",
    [
        ["--unknown-option", _URL],
        ["--evidence", _URL],
    ],
)
def test_invalid_arguments_refuse_without_printing_raw_input(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tail: list[str]
) -> None:
    """Malformed arguments refuse before bootstrap and redact stdout and stderr.

    Given invalid command arguments containing sensitive synthetic text.
    When the command parses the arguments.
    Then it refuses without printing the raw input.
    """

    def forbidden_bootstrap() -> None:
        """Fail if invalid input reaches configuration or any database."""
        pytest.fail("invalid arguments must refuse before bootstrap")

    monkeypatch.setattr(candle_void, "get_bootstrap_settings", forbidden_bootstrap)
    result = CliRunner().invoke(
        app,
        [
            "void-candles",
            "--instrument",
            "synthetic-instrument",
            "--output-dir",
            str(tmp_path / "output"),
            *tail,
        ],
    )
    assert result.exit_code == 1
    combined = result.stdout + result.stderr
    assert "refused" in combined
    assert _URL not in combined
    assert _SECRET not in combined
    assert "synthetic-user" not in combined
    assert not (tmp_path / "output").exists()
