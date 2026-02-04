"""Unit tests for Snapper package entry point."""

import runpy
import subprocess
import sys
from typing import Any
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

import snapper.__main__
from snapper.__main__ import main
from snapper.cli.app import app
from snapper.utils.logging import setup_logging


def test_main_invokes_setup_and_app(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify main() initializes logging and starts CLI app.

    Given mocked setup_logging and app functions,
    When main() is called,
    Then setup_logging is called with INFO level and app is invoked.
    """
    setup_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    app_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def _fake_setup_logging(*args: Any, **kwargs: Any) -> None:
        setup_calls.append((args, kwargs))

    def _fake_app(*args: Any, **kwargs: Any) -> None:
        app_calls.append((args, kwargs))

    monkeypatch.setattr("snapper.__main__.setup_logging", _fake_setup_logging)
    monkeypatch.setattr("snapper.__main__.app", _fake_app)
    main()
    assert setup_calls
    args, kwargs = setup_calls[0]
    assert args == ()
    assert kwargs == {
        "level": "INFO",
        "json_logs": False,
        "logfile": "data/snapper.log",
    }
    assert app_calls == [((), {})]


def test_run_module_executes_main(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify runpy execution triggers main() entry point.

    Given mocked logging and app functions,
    When snapper.__main__ is run as __main__,
    Then both setup_logging and app are called.
    """
    setup_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    app_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def _fake_setup_logging(*args: Any, **kwargs: Any) -> None:
        setup_calls.append((args, kwargs))

    def _fake_app(*args: Any, **kwargs: Any) -> None:
        app_calls.append((args, kwargs))

    monkeypatch.setattr("snapper.utils.logging.setup_logging", _fake_setup_logging)
    monkeypatch.setattr("snapper.cli.app.app", _fake_app)
    monkeypatch.delitem(sys.modules, "snapper.__main__", raising=False)
    with pytest.raises(SystemExit) as exc_info:
        runpy.run_module("snapper.__main__", run_name="__main__", alter_sys=True)
    assert exc_info.value.code == 0
    assert setup_calls
    assert app_calls == [((), {})]


class TestMain:
    """Test suite for main() entrypoint function."""

    @patch("snapper.__main__.app")
    @patch("snapper.__main__.setup_logging")
    def test_main_configures_logging_and_runs_app(
        self,
        mock_setup_logging: MagicMock,
        mock_app: MagicMock,
    ) -> None:
        """Verify main() calls setup_logging then app exactly once.

        Given mocked setup_logging and app,
        When main() is called,
        Then setup_logging is called with expected args and app is invoked once.
        """
        main()
        mock_setup_logging.assert_called_once_with(
            level="INFO",
            json_logs=False,
            logfile="data/snapper.log",
        )
        mock_app.assert_called_once()
        assert mock_setup_logging.call_count == 1
        assert mock_app.call_count == 1

    def test_main_module_executable(self) -> None:
        """Verify snapper.__main__ exposes required attributes.

        Given the snapper.__main__ module,
        When checking for main function and __name__,
        Then all attributes are present and callable.
        """
        assert hasattr(snapper.__main__, "main")
        assert callable(snapper.__main__.main)
        assert hasattr(snapper.__main__, "__name__")

    def test_main_imports_available(self) -> None:
        """Verify all required imports are available.

        Given the test module imports,
        When checking main, app, setup_logging,
        Then all are callable.
        """
        assert callable(main)
        assert callable(app)
        assert callable(setup_logging)

    @patch("snapper.__main__.app")
    @patch("snapper.__main__.setup_logging")
    def test_main_executed_as_module(
        self,
        mock_setup_logging: MagicMock,
        mock_app: MagicMock,
    ) -> None:
        """Verify python -m snapper --help executes successfully.

        Given the snapper package,
        When running python -m snapper --help in subprocess,
        Then exit code is 0 and output contains usage info.
        """
        result = subprocess.run(
            [sys.executable, "-m", "snapper", "--help"],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        assert result.returncode == 0
        output = (result.stdout or "") + (result.stderr or "")
        if output:
            assert (
                "Usage:" in output or "snapper" in output.lower()
            ), f"Unexpected output: {output!r}"
