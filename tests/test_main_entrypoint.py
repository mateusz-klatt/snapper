"""Unit tests for Snapper package entry point."""

import asyncio
import json
import os
import runpy
import subprocess
import sys
from datetime import UTC
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

import snapper.__main__
from snapper.__main__ import main
from snapper.cli.app import app
from snapper.data.models import Symbol
from snapper.data.repository import SQLAlchemyRepository
from snapper.infrastructure.symbols.functions import resolve_symbol_public_id
from snapper.utils.logging import LOGFILE_ENV_VAR
from snapper.utils.logging import setup_logging


@pytest.fixture(autouse=True)
def _clear_logfile_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep entry-point logfile selection isolated from the host environment.

    Given a test process that may inherit ``SNAPPER_LOG_FILE``,
    When an entry-point test begins,
    Then the variable is absent until that test explicitly supplies it.
    """
    monkeypatch.delenv(LOGFILE_ENV_VAR, raising=False)


async def _seed_active_aapl(database_url: str, active_at: datetime) -> None:
    """Create one active Polygon target without candles for a subprocess audit."""
    repository = SQLAlchemyRepository(database_url)
    await repository.create_all()
    try:
        async with repository.session() as session:
            session.add(
                Symbol(
                    native_symbol="AAPL",
                    base="AAPL",
                    quote="USD",
                    asset_type="equity",
                    created_at=active_at,
                    timestamp=active_at,
                    session_id="entrypoint-test",
                    sequence_id=1,
                )
            )
            await session.commit()
        symbol_public_id = await resolve_symbol_public_id(
            repository,
            "AAPL",
            as_of=active_at,
        )
        assert symbol_public_id is not None
        await repository.ensure_instrument(
            symbol_public_id=symbol_public_id,
            exchange="polygon",
            session_id="entrypoint-test",
            sequence_id=1,
            timestamp=active_at,
        )
    finally:
        await repository.engine.dispose()


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
        "logfile": "data/log/snapper/snapper.log",
        "console_to_stderr": False,
    }
    assert app_calls == [((), {})]


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        (["snapper", "audit-candles", "--json"], True),
        (["snapper", "audit-candles"], False),
        (["snapper", "server", "--json"], False),
    ],
)
def test_main_reserves_machine_stdout_only_for_json_candle_audits(
    monkeypatch: pytest.MonkeyPatch,
    argv: list[str],
    expected: bool,
) -> None:
    """Only the machine candle report routes console logs to stderr.

    Given: A JSON candle invocation or a nearby human/non-candle invocation.
    When: The package entry point configures logging.
    Then: The stderr-console flag matches the exact machine-output contract.
    """
    captured: dict[str, object] = {}

    def fake_setup_logging(**kwargs: object) -> None:
        captured.update(kwargs)

    monkeypatch.setattr("snapper.__main__.setup_logging", fake_setup_logging)
    monkeypatch.setattr("snapper.__main__.log_kraken_sdk_patches_status", lambda: None)
    monkeypatch.setattr("snapper.__main__.app", lambda: None)
    monkeypatch.setattr(sys, "argv", argv)
    main()
    assert captured["console_to_stderr"] is expected


def test_main_egress_subcommand_uses_egress_logfile(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify ``snapper egress`` selects its per-service logfile.

    Given mocked setup_logging and app,
    And argv whose first positional arg is ``"egress"`` (matching
        the docker-compose CMD for the sidecar service),
    When main() is called,
    Then setup_logging receives the ``snapper-egress`` service logfile.

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
    assert kwargs["logfile"] == "data/log/snapper-egress/snapper-egress.log"


def test_main_egress_with_extra_args_still_uses_egress_logfile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Spec — ``snapper egress --instance-id foo`` still goes to the egress log.

    Given argv whose first positional arg is ``"egress"`` followed by
        additional trailing flags forwarded to the sidecar entrypoint,
    When main() is called,
    Then setup_logging still receives the ``snapper-egress`` service logfile
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
    assert kwargs["logfile"] == "data/log/snapper-egress/snapper-egress.log"


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
        existed and never reached ``data/log/snapper/snapper.log``.
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
    """Spec — a non-mapped sub-command keeps the API service logfile.

    Given argv whose first positional arg is a command without a
        dedicated logfile (e.g. ``"server"``, the ``snapper-api``
        container),
    When main() is called,
    Then setup_logging receives ``data/log/snapper/snapper.log`` —
        ``feed-engine``, ``strategies-engine``, ``broker`` and
        ``egress`` map to dedicated files while ``server`` (and any
        other unmapped command) keeps the ``snapper-api`` container's
        dedicated API logfile.
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
    assert kwargs["logfile"] == "data/log/snapper/snapper.log"


def test_main_feed_engine_subcommand_uses_feed_logfile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Spec — ``snapper feed-engine`` logs to its per-service path.

    Given argv whose first positional arg is ``"feed-engine"`` (the
        docker-compose CMD for the ``snapper-feed`` container),
    When main() is called,
    Then setup_logging receives the ``snapper-feed`` service logfile so
        the feed container does not write to the API container's
        logfile on the shared ``./data`` bind mount.
    """
    setup_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def _fake_setup_logging(*args: object, **kwargs: object) -> None:
        setup_calls.append((args, kwargs))

    monkeypatch.setattr("snapper.__main__.setup_logging", _fake_setup_logging)
    monkeypatch.setattr("snapper.__main__.log_kraken_sdk_patches_status", lambda: None)
    monkeypatch.setattr("snapper.__main__.app", lambda: None)
    monkeypatch.setattr(sys, "argv", ["snapper", "feed-engine"])
    main()
    _, kwargs = setup_calls[0]
    assert kwargs["logfile"] == "data/log/snapper-feed/snapper-feed.log"


def test_main_strategies_engine_subcommand_uses_strategies_logfile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Spec — ``snapper strategies-engine`` logs to its per-service path.

    Given argv whose first positional arg is ``"strategies-engine"`` (the
        docker-compose CMD for the ``snapper-strategies`` container),
    When main() is called,
    Then setup_logging receives the ``snapper-strategies`` service logfile
        so the strategies container does not interleave its lines with the
        API container's logfile on the shared ``./data`` bind mount.
    """
    setup_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def _fake_setup_logging(*args: object, **kwargs: object) -> None:
        setup_calls.append((args, kwargs))

    monkeypatch.setattr("snapper.__main__.setup_logging", _fake_setup_logging)
    monkeypatch.setattr("snapper.__main__.log_kraken_sdk_patches_status", lambda: None)
    monkeypatch.setattr("snapper.__main__.app", lambda: None)
    monkeypatch.setattr(sys, "argv", ["snapper", "strategies-engine"])
    main()
    _, kwargs = setup_calls[0]
    assert kwargs["logfile"] == "data/log/snapper-strategies/snapper-strategies.log"


def test_main_broker_subcommand_uses_broker_logfile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Spec — ``snapper broker`` logs to its per-service path.

    Given argv whose first positional arg is ``"broker"`` (the
        docker-compose CMD for the ``snapper-broker`` container),
    When main() is called,
    Then setup_logging receives the ``snapper-broker`` service logfile.
        The ``snapper-broker`` service mounts ``./data`` so this dedicated
        file is host-visible; without the mapping the broker fell through
        to the API logfile and, running as an unprivileged uid over
        the image's ``/app/data`` with no bind mount, could only emit a
        swallowed PermissionError instead of logging.
    """
    setup_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def _fake_setup_logging(*args: object, **kwargs: object) -> None:
        setup_calls.append((args, kwargs))

    monkeypatch.setattr("snapper.__main__.setup_logging", _fake_setup_logging)
    monkeypatch.setattr("snapper.__main__.log_kraken_sdk_patches_status", lambda: None)
    monkeypatch.setattr("snapper.__main__.app", lambda: None)
    monkeypatch.setattr(sys, "argv", ["snapper", "broker"])
    main()
    _, kwargs = setup_calls[0]
    assert kwargs["logfile"] == "data/log/snapper-broker/snapper-broker.log"


def test_main_notify_subcommand_uses_notify_logfile(monkeypatch: pytest.MonkeyPatch) -> None:
    """Spec — ``snapper notify`` does not share the API logfile.

    Given argv whose first positional argument is ``notify``,
    When main configures logging,
    Then setup_logging receives the ``snapper-notify`` service logfile.
    """
    setup_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def _fake_setup_logging(*args: object, **kwargs: object) -> None:
        setup_calls.append((args, kwargs))

    monkeypatch.setattr("snapper.__main__.setup_logging", _fake_setup_logging)
    monkeypatch.setattr("snapper.__main__.log_kraken_sdk_patches_status", lambda: None)
    monkeypatch.setattr("snapper.__main__.app", lambda: None)
    monkeypatch.setattr(sys, "argv", ["snapper", "notify"])
    main()
    _, kwargs = setup_calls[0]
    assert kwargs["logfile"] == "data/log/snapper-notify/snapper-notify.log"


def test_main_exports_resolved_logfile_to_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Spec — main() exports the resolved logfile in ``SNAPPER_LOG_FILE``.

    Given argv for the feed container (``feed-engine``),
    When main() is called,
    Then ``os.environ[LOGFILE_ENV_VAR]`` holds the feed service logfile
        so subprocesses spawned by the container inherit it and log to
        the SAME per-container file (see
        :func:`snapper.utils.logging.resolve_subprocess_logfile`).
    """
    monkeypatch.setattr("snapper.__main__.setup_logging", lambda **kwargs: None)
    monkeypatch.setattr("snapper.__main__.log_kraken_sdk_patches_status", lambda: None)
    monkeypatch.setattr("snapper.__main__.app", lambda: None)
    monkeypatch.setattr(sys, "argv", ["snapper", "feed-engine"])
    main()
    assert os.environ[LOGFILE_ENV_VAR] == "data/log/snapper-feed/snapper-feed.log"


@pytest.mark.parametrize(
    "configured_logfile",
    ("custom/logs/override.log", "/var/log/snapper/override.log"),
)
def test_main_valid_logfile_environment_overrides_command_default(
    monkeypatch: pytest.MonkeyPatch,
    configured_logfile: str,
) -> None:
    """A valid explicit logfile takes precedence over the command mapping.

    Given a relative or absolute ``SNAPPER_LOG_FILE`` and a broker command,
    When main resolves and exports the logfile,
    Then logging and child inheritance both use the explicit path.
    """
    captured: dict[str, object] = {}

    def _fake_setup_logging(*args: object, **kwargs: object) -> None:
        captured.update(kwargs)

    monkeypatch.setattr("snapper.__main__.setup_logging", _fake_setup_logging)
    monkeypatch.setattr("snapper.__main__.log_kraken_sdk_patches_status", lambda: None)
    monkeypatch.setattr("snapper.__main__.app", lambda: None)
    monkeypatch.setattr(sys, "argv", ["snapper", "broker"])
    monkeypatch.setenv(LOGFILE_ENV_VAR, configured_logfile)
    main()
    assert captured["logfile"] == configured_logfile
    assert os.environ[LOGFILE_ENV_VAR] == configured_logfile


@pytest.mark.parametrize(
    "configured_logfile",
    (
        "",
        "data/log/snapper",
        ".log",
        "../outside.log",
        "data/log/snapper/../outside.log",
        r"data\log\..\outside.log",
        " data/log/snapper/snapper.log",
        "data/log/snapper/snapper.log/",
        "https://logs.invalid/snapper.log",
    ),
)
def test_main_invalid_logfile_environment_falls_back_to_command_default(
    monkeypatch: pytest.MonkeyPatch,
    configured_logfile: str,
) -> None:
    """Invalid explicit log paths cannot escape the command mapping.

    Given an invalid or traversal-bearing ``SNAPPER_LOG_FILE``,
    When main resolves the logfile for ``feed-engine``,
    Then logging and the exported environment use the safe feed default.
    """
    captured: dict[str, object] = {}

    def _fake_setup_logging(*args: object, **kwargs: object) -> None:
        captured.update(kwargs)

    expected = "data/log/snapper-feed/snapper-feed.log"
    monkeypatch.setattr("snapper.__main__.setup_logging", _fake_setup_logging)
    monkeypatch.setattr("snapper.__main__.log_kraken_sdk_patches_status", lambda: None)
    monkeypatch.setattr("snapper.__main__.app", lambda: None)
    monkeypatch.setattr(sys, "argv", ["snapper", "feed-engine"])
    monkeypatch.setenv(LOGFILE_ENV_VAR, configured_logfile)
    main()
    assert captured["logfile"] == expected
    assert os.environ[LOGFILE_ENV_VAR] == expected


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

    @pytest.mark.timeout(45)
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
            logfile="data/log/snapper/snapper.log",
            console_to_stderr=False,
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
            timeout=30,
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


def test_json_candle_audit_subprocess_reserves_stdout_for_one_document(tmp_path: Path) -> None:
    """The real package entry point never mixes log lines into machine output.

    Given: An active SQLite-backed target with no candles in a closed window.
    When: ``python -m snapper audit-candles --json`` completes with an anomaly.
    Then: Standard output is exactly one parseable JSON document despite INFO logs.
    """
    database_url = f"sqlite+aiosqlite:///{(tmp_path / 'audit.db').as_posix()}"
    active_at = datetime(2026, 6, 30, tzinfo=UTC)
    asyncio.run(_seed_active_aapl(database_url, active_at))
    env = os.environ.copy()
    env["DB_URL"] = database_url
    env["SNAPPER_ENV"] = "test"
    env[LOGFILE_ENV_VAR] = str(tmp_path / "audit.log")
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
    run_cwd = tmp_path / "audit-runroot"
    run_cwd.mkdir()
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "snapper",
            "audit-candles",
            "--target",
            "polygon",
            "AAPL",
            "--timeframe",
            "1m",
            "--window",
            "2026-07-01T13:30:00+00:00",
            "2026-07-01T13:31:00+00:00",
            "--json",
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
        cwd=run_cwd,
        env=env,
    )
    assert result.returncode == 1, result.stderr
    assert result.stdout.count("\n") == 1
    payload = json.loads(result.stdout)
    assert payload["status"] == "anomalies"
    assert payload["anomalies"][0]["type"] == "empty_window"
