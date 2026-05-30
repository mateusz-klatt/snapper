"""Unit tests for Snapper package entry point."""

import os
import runpy
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

import snapper.__main__
from snapper.__main__ import main
from snapper.cli.app import app
from snapper.utils.logging import LOGFILE_ENV_VAR
from snapper.utils.logging import setup_logging


def test_main_invokes_setup_and_app(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify main() initializes logging and starts CLI app.

    Given mocked setup_logging and app functions,
    And argv with no sub-command (i.e. plain ``snapper`` invocation),
    When main() is called,
    Then setup_logging is called with INFO level + default API
    container logfile and app is invoked.
    """
    setup_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    app_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def _fake_setup_logging(*args: object, **kwargs: object) -> None:
        setup_calls.append((args, kwargs))

    def _fake_app(*args: object, **kwargs: object) -> None:
        app_calls.append((args, kwargs))

    monkeypatch.setattr("snapper.__main__.setup_logging", _fake_setup_logging)
    monkeypatch.setattr("snapper.__main__.log_kraken_sdk_patches_status", lambda: None)
    monkeypatch.setattr("snapper.__main__.app", _fake_app)
    monkeypatch.setattr(sys, "argv", ["snapper"])
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


def test_main_egress_subcommand_uses_egress_logfile(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify ``snapper egress`` switches the logfile to ``data/snapper-egress.log``.

    Given mocked setup_logging and app,
    And argv whose first positional arg is ``"egress"`` (matching
        the docker-compose CMD for the sidecar service),
    When main() is called,
    Then setup_logging receives ``logfile="data/snapper-egress.log"``.

    This prevents the sidecar (which runs as ``root`` for
    ``CAP_NET_ADMIN``) from taking ownership of the API container's
    log file. Before this dispatch the API container (running as
    ``snapper:snapper`` uid 888) could not append to its own log if
    the sidecar had already opened it first.
    """
    setup_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def _fake_setup_logging(*args: object, **kwargs: object) -> None:
        setup_calls.append((args, kwargs))

    monkeypatch.setattr("snapper.__main__.setup_logging", _fake_setup_logging)
    monkeypatch.setattr("snapper.__main__.log_kraken_sdk_patches_status", lambda: None)
    monkeypatch.setattr("snapper.__main__.app", lambda: None)
    monkeypatch.setattr(sys, "argv", ["snapper", "egress"])
    main()
    assert setup_calls
    _, kwargs = setup_calls[0]
    assert kwargs["logfile"] == "data/snapper-egress.log"


def test_main_egress_with_extra_args_still_uses_egress_logfile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Spec — ``snapper egress --instance-id foo`` still goes to the egress log.

    Given argv whose first positional arg is ``"egress"`` followed by
        additional trailing flags forwarded to the sidecar entrypoint,
    When main() is called,
    Then setup_logging still receives ``logfile="data/snapper-egress.log"``
        — only the first positional arg is inspected by the dispatch
        in ``_resolve_logfile``.
    """
    setup_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def _fake_setup_logging(*args: object, **kwargs: object) -> None:
        setup_calls.append((args, kwargs))

    monkeypatch.setattr("snapper.__main__.setup_logging", _fake_setup_logging)
    monkeypatch.setattr("snapper.__main__.log_kraken_sdk_patches_status", lambda: None)
    monkeypatch.setattr("snapper.__main__.app", lambda: None)
    monkeypatch.setattr(sys, "argv", ["snapper", "egress", "--instance-id", "snapper-egress"])
    main()
    _, kwargs = setup_calls[0]
    assert kwargs["logfile"] == "data/snapper-egress.log"


def test_main_calls_log_patches_status_after_setup_logging(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Spec — setup_logging MUST run before log_kraken_sdk_patches_status.

    Given mocked setup_logging, log_kraken_sdk_patches_status and app,
    When main() is called,
    Then the call order is setup_logging → log_kraken_sdk_patches_status
        → app — the patch-status reporter depends on loguru's file sink
        being live, which only happens once setup_logging has installed
        it. Reversing this order would recreate the original bug where
        patch confirmations were written to stderr before the file sink
        existed and never reached ``data/snapper.log`` (see
        ``proprietary/plans/plan_2026_05_25_log_noise_followups.md``
        item #3).
    """
    call_order: list[str] = []

    def _fake_setup_logging(*args: object, **kwargs: object) -> None:
        call_order.append("setup_logging")

    def _fake_log_patches_status() -> None:
        call_order.append("log_kraken_sdk_patches_status")

    def _fake_app(*args: object, **kwargs: object) -> None:
        call_order.append("app")

    monkeypatch.setattr("snapper.__main__.setup_logging", _fake_setup_logging)
    monkeypatch.setattr("snapper.__main__.log_kraken_sdk_patches_status", _fake_log_patches_status)
    monkeypatch.setattr("snapper.__main__.app", _fake_app)
    monkeypatch.setattr(sys, "argv", ["snapper"])
    main()
    assert call_order == ["setup_logging", "log_kraken_sdk_patches_status", "app"]


def test_main_non_egress_subcommand_uses_default_logfile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Spec — a non-mapped sub-command keeps the shared ``data/snapper.log``.

    Given argv whose first positional arg is a command without a
        dedicated logfile (e.g. ``"server"``, the ``snapper-api``
        container),
    When main() is called,
    Then setup_logging receives ``logfile="data/snapper.log"`` —
        ``feed-engine`` and ``egress`` map to dedicated files while
        ``server``, ``broker`` etc. share the API container's log
        because they run under the same ``snapper:snapper`` uid.
    """
    setup_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def _fake_setup_logging(*args: object, **kwargs: object) -> None:
        setup_calls.append((args, kwargs))

    monkeypatch.setattr("snapper.__main__.setup_logging", _fake_setup_logging)
    monkeypatch.setattr("snapper.__main__.log_kraken_sdk_patches_status", lambda: None)
    monkeypatch.setattr("snapper.__main__.app", lambda: None)
    monkeypatch.setattr(sys, "argv", ["snapper", "server"])
    main()
    _, kwargs = setup_calls[0]
    assert kwargs["logfile"] == "data/snapper.log"


def test_main_feed_engine_subcommand_uses_feed_logfile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Spec — ``snapper feed-engine`` logs to ``data/snapper-feed.log``.

    Given argv whose first positional arg is ``"feed-engine"`` (the
        docker-compose CMD for the ``snapper-feed`` container),
    When main() is called,
    Then setup_logging receives ``logfile="data/snapper-feed.log"`` so
        the feed container does not write to the API container's
        ``data/snapper.log`` on the shared ``./data`` bind mount.
    """
    setup_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def _fake_setup_logging(*args: object, **kwargs: object) -> None:
        setup_calls.append((args, kwargs))

    monkeypatch.setattr("snapper.__main__.setup_logging", _fake_setup_logging)
    monkeypatch.setattr("snapper.__main__.log_kraken_sdk_patches_status", lambda: None)
    monkeypatch.setattr("snapper.__main__.app", lambda: None)
    monkeypatch.setattr(sys, "argv", ["snapper", "feed-engine"])
    monkeypatch.setenv(LOGFILE_ENV_VAR, "sentinel")
    main()
    _, kwargs = setup_calls[0]
    assert kwargs["logfile"] == "data/snapper-feed.log"


def test_main_exports_resolved_logfile_to_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Spec — main() exports the resolved logfile in ``SNAPPER_LOG_FILE``.

    Given argv for the feed container (``feed-engine``),
    When main() is called,
    Then ``os.environ[LOGFILE_ENV_VAR]`` holds ``data/snapper-feed.log``
        so subprocesses spawned by the container inherit it and log to
        the SAME per-container file (see
        :func:`snapper.utils.logging.resolve_subprocess_logfile`).
    """
    monkeypatch.setattr("snapper.__main__.setup_logging", lambda **kwargs: None)
    monkeypatch.setattr("snapper.__main__.log_kraken_sdk_patches_status", lambda: None)
    monkeypatch.setattr("snapper.__main__.app", lambda: None)
    monkeypatch.setattr(sys, "argv", ["snapper", "feed-engine"])
    monkeypatch.setenv(LOGFILE_ENV_VAR, "sentinel")
    main()
    assert os.environ[LOGFILE_ENV_VAR] == "data/snapper-feed.log"


def test_run_module_executes_main(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify runpy execution triggers main() entry point.

    Given mocked logging and app functions,
    When snapper.__main__ is run as __main__,
    Then both setup_logging and app are called.
    """
    setup_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    app_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def _fake_setup_logging(*args: object, **kwargs: object) -> None:
        setup_calls.append((args, kwargs))

    def _fake_app(self: object, *args: object, **kwargs: object) -> None:
        app_calls.append((args, kwargs))

    monkeypatch.setattr("snapper.utils.logging.setup_logging", _fake_setup_logging)
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.kraken_sdk_patches.log_kraken_sdk_patches_status",
        lambda: None,
    )
    monkeypatch.setattr(type(app), "__call__", _fake_app)
    monkeypatch.delitem(sys.modules, "snapper.__main__", raising=False)
    with pytest.raises(SystemExit) as exc_info:
        runpy.run_module("snapper.__main__", run_name="__main__", alter_sys=True)
    assert exc_info.value.code == 0
    assert setup_calls
    assert app_calls == [((), {})]


class TestMain:
    """Test suite for main() entrypoint function."""

    @patch("snapper.__main__.app")
    @patch("snapper.__main__.log_kraken_sdk_patches_status")
    @patch("snapper.__main__.setup_logging")
    def test_main_configures_logging_and_runs_app(
        self,
        mock_setup_logging: MagicMock,
        mock_log_patches_status: MagicMock,
        mock_app: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Verify main() calls setup_logging then app exactly once.

        Given mocked setup_logging, log_kraken_sdk_patches_status and app,
        And argv with no sub-command (default API container path),
        When main() is called,
        Then setup_logging is called with expected args, the kraken-sdk
            patch-status reporter is invoked once, and app is invoked once.
        """
        monkeypatch.setattr(sys, "argv", ["snapper"])
        main()
        mock_setup_logging.assert_called_once_with(
            level="INFO",
            json_logs=False,
            logfile="data/snapper.log",
        )
        mock_log_patches_status.assert_called_once_with()
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
    @patch("snapper.__main__.log_kraken_sdk_patches_status")
    @patch("snapper.__main__.setup_logging")
    def test_main_executed_as_module(
        self,
        mock_setup_logging: MagicMock,
        mock_log_patches_status: MagicMock,
        mock_app: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Verify python -m snapper --help executes successfully.

        Given the snapper package,
        When running python -m snapper --help in subprocess,
        Then exit code is 0 and output contains usage info.
        """
        env = os.environ.copy()
        env["DB_URL"] = f"sqlite+aiosqlite:///{(tmp_path / 'subprocess.db').as_posix()}"
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        env["PYTHONPATH"] = os.pathsep.join(
            filter(
                None,
                [
                    str(Path(__file__).resolve().parents[1] / "src"),
                    env.get("PYTHONPATH", ""),
                ],
            )
        )
        run_cwd = tmp_path / "runroot"
        run_cwd.mkdir()
        result = subprocess.run(
            [sys.executable, "-m", "snapper", "--help"],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
            cwd=run_cwd,
            env=env,
        )
        assert result.returncode == 0
        output = (result.stdout or "") + (result.stderr or "")
        if output:
            assert (
                "Usage:" in output or "snapper" in output.lower()
            ), f"Unexpected output: {output!r}"
