"""Unit tests for the ``python -m snapper.egress`` entrypoint.

SC.4 entrypoint tests verify CLI argv parsing, environment-variable
preconditions, and the asyncio handoff to run_sidecar. All async
work + SettingsService access is mocked so the tests are hermetic.
"""

from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.egress import __main__ as egress_main


class TestParseArgs:
    """CLI argv parsing."""

    def test_default_instance_id(self) -> None:
        """Spec — empty argv produces the default instance_id.

        Given argv=[],
        When _parse_args runs,
        Then args.instance_id == "snapper-egress".
        """
        args = egress_main._parse_args([])
        assert args.instance_id == "snapper-egress"

    def test_explicit_instance_id(self) -> None:
        """Spec — --instance-id overrides the default.

        Given argv=["--instance-id", "test-egress"],
        When _parse_args runs,
        Then args.instance_id reflects the override.
        """
        args = egress_main._parse_args(["--instance-id", "test-egress"])
        assert args.instance_id == "test-egress"


class TestAsyncMain:
    """async _async_main bootstrap + handoff."""

    @pytest.mark.asyncio
    async def test_returns_2_when_db_url_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Spec — missing DB_URL → exit code 2 with a clear log.

        Given env without DB_URL,
        When _async_main runs,
        Then 2 is returned and the orchestrator is NOT called.
        """
        monkeypatch.delenv("DB_URL", raising=False)
        monkeypatch.setenv("ZMQ_BROKER_XSUB", "tcp://localhost:5555")
        args = MagicMock()
        run_mock = AsyncMock()
        with patch.object(egress_main, "run_sidecar", new=run_mock):
            result = await egress_main._async_main(args)
        assert result == 2
        run_mock.assert_not_called()

    @pytest.mark.asyncio
    async def test_returns_2_when_zmq_xsub_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Spec — missing ZMQ_BROKER_XSUB → exit code 2.

        Given DB_URL set but ZMQ_BROKER_XSUB missing,
        When _async_main runs,
        Then 2 is returned.
        """
        monkeypatch.setenv("DB_URL", "postgres://x/y")
        monkeypatch.delenv("ZMQ_BROKER_XSUB", raising=False)
        args = MagicMock()
        run_mock = AsyncMock()
        with patch.object(egress_main, "run_sidecar", new=run_mock):
            result = await egress_main._async_main(args)
        assert result == 2
        run_mock.assert_not_called()

    @pytest.mark.asyncio
    async def test_happy_path_calls_run_sidecar(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Spec — env present → SettingsService + run_sidecar called.

        Given DB_URL + ZMQ_BROKER_XSUB present,
        When _async_main runs,
        Then get_settings_service is awaited with the env values,
            install_signal_handlers is invoked, and run_sidecar
            returns its result code.
        """
        monkeypatch.setenv("DB_URL", "postgres://x/y")
        monkeypatch.setenv("ZMQ_BROKER_XSUB", "tcp://localhost:5555")
        args = MagicMock()
        args.instance_id = "test-egress"
        fake_service = MagicMock()
        run_mock = AsyncMock(return_value=0)
        get_service_mock = AsyncMock(return_value=fake_service)
        install_mock = MagicMock()
        with (
            patch.object(egress_main, "get_settings_service", new=get_service_mock),
            patch.object(egress_main, "install_signal_handlers", new=install_mock),
            patch.object(egress_main, "run_sidecar", new=run_mock),
        ):
            result = await egress_main._async_main(args)
        assert result == 0
        get_service_mock.assert_awaited_once_with("postgres://x/y", "tcp://localhost:5555")
        install_mock.assert_called_once()
        run_mock.assert_awaited_once()


class TestMain:
    """Sync wrapper called by Docker CMD."""

    def test_main_calls_probe_before_asyncio_run(self) -> None:
        """Spec — probe_kernel_wireguard runs BEFORE asyncio.run.

        Given the sync main wrapper,
        When invoked,
        Then wg_control.probe_kernel_wireguard is called BEFORE
            asyncio.run starts the event loop. pyroute2 0.9.x cannot
            construct IPRoute from inside a running asyncio loop, so
            the probe must run in sync context.
        """
        observed_order: list[str] = []

        def fake_probe() -> None:
            observed_order.append("probe")

        def fake_run(coro: Any) -> int:
            observed_order.append("asyncio.run")
            coro.close()
            return 0

        with (
            patch.object(
                egress_main.wg_control,
                "probe_kernel_wireguard",
                side_effect=fake_probe,
            ),
            patch.object(egress_main, "_async_main", new=AsyncMock(return_value=0)),
            patch.object(egress_main.asyncio, "run", side_effect=fake_run),
        ):
            egress_main.main(["--instance-id", "x"])
        assert observed_order == ["probe", "asyncio.run"]

    def test_main_propagates_probe_systemexit(self) -> None:
        """Spec — probe SystemExit propagates from main() without asyncio.run.

        Given probe raises SystemExit(1) (kernel WG missing),
        When main runs,
        Then SystemExit propagates AND asyncio.run is NOT called
            (we never enter the event loop).
        """
        run_mock = MagicMock()
        with (
            patch.object(
                egress_main.wg_control,
                "probe_kernel_wireguard",
                side_effect=SystemExit(1),
            ),
            patch.object(egress_main.asyncio, "run", side_effect=run_mock),
            pytest.raises(SystemExit) as excinfo,
        ):
            egress_main.main([])
        assert excinfo.value.code == 1
        run_mock.assert_not_called()

    def test_main_dispatches_to_asyncio_run(self) -> None:
        """Spec — main() parses argv + dispatches via asyncio.run.

        Given argv=["--instance-id", "x"],
        When main runs,
        Then asyncio.run is invoked with the coroutine returned by
            _async_main and the result is returned verbatim.

        ``asyncio.run`` is mocked to actually drive the coroutine
        through a fresh event loop so the AsyncMock-backed
        ``_async_main`` finishes (otherwise the unawaited coroutine
        emits a RuntimeWarning during gc).
        """

        def fake_run(coro: Any) -> int:
            try:
                loop = egress_main.asyncio.new_event_loop()
                try:
                    return int(loop.run_until_complete(coro))
                finally:
                    loop.close()
            except StopIteration:
                return 42

        async_main_mock = AsyncMock(return_value=42)
        with (
            patch.object(egress_main.wg_control, "probe_kernel_wireguard"),
            patch.object(egress_main, "_async_main", new=async_main_mock),
            patch.object(egress_main.asyncio, "run", side_effect=fake_run),
        ):
            result = egress_main.main(["--instance-id", "x"])
        assert result == 42

    def test_main_uses_sys_argv_when_argv_none(self) -> None:
        """Spec — argv=None falls through to sys.argv[1:].

        Given main(argv=None) and sys.argv=["prog", "--instance-id", "z"],
        When main runs,
        Then the resulting args.instance_id == "z".
        """
        observed: dict[str, Any] = {}

        async def fake_async_main(args: Any) -> int:
            observed["instance_id"] = args.instance_id
            return 0

        with (
            patch.object(egress_main.wg_control, "probe_kernel_wireguard"),
            patch.object(egress_main, "_async_main", new=fake_async_main),
            patch.object(egress_main.sys, "argv", ["prog", "--instance-id", "z"]),
        ):
            result = egress_main.main(None)
        assert result == 0
        assert observed["instance_id"] == "z"
