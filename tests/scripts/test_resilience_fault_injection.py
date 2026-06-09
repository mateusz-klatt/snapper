"""Tests for the resilience fault-injection harness."""

from collections.abc import Iterator
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from scripts.resilience_fault_injection import _parse_args
from scripts.resilience_fault_injection import _psql_connection
from scripts.resilience_fault_injection import await_recovery
from scripts.resilience_fault_injection import build_iptables_command
from scripts.resilience_fault_injection import main
from scripts.resilience_fault_injection import query_seconds_since_fresh
from scripts.resilience_fault_injection import run_command
from scripts.resilience_fault_injection import run_outage_cycle

_MODULE = "scripts.resilience_fault_injection"


class TestBuildIptablesCommand:
    """Tests for the iptables command builder."""

    def test_append_drop_rule(self) -> None:
        """Verify the append form targets the container and HTTPS port.

        Given: A container name and the append action,
        When: build_iptables_command is called,
        Then: A docker-exec iptables -A OUTPUT DROP on 443 is returned.
        """
        argv = build_iptables_command("snapper-egress", "-A")
        assert argv == [
            "docker",
            "exec",
            "snapper-egress",
            "iptables",
            "-A",
            "OUTPUT",
            "-p",
            "tcp",
            "--dport",
            "443",
            "-j",
            "DROP",
        ]

    def test_delete_rule_with_custom_port(self) -> None:
        """Verify the delete form honours a custom port.

        Given: The delete action and a custom port,
        When: build_iptables_command is called,
        Then: The -D action and the custom port appear in the argv.
        """
        argv = build_iptables_command("c", "-D", port=8443)
        assert argv[4] == "-D"
        assert argv[9] == "8443"


class TestRunCommand:
    """Tests for the subprocess command runner."""

    def test_returns_exit_code(self) -> None:
        """Verify run_command returns the subprocess exit code.

        Given: A subprocess that exits non-zero,
        When: run_command is called,
        Then: The exit code is returned.
        """
        with patch(f"{_MODULE}.subprocess.run", return_value=MagicMock(returncode=3)) as run_mock:
            assert run_command(["echo", "hi"]) == 3
        run_mock.assert_called_once()


class TestPsqlConnection:
    """Tests for the psql connection builder."""

    def test_raises_without_db_url(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify a missing DB_URL raises.

        Given: DB_URL absent from the environment,
        When: _psql_connection is called,
        Then: RuntimeError is raised.
        """
        monkeypatch.delenv("DB_URL", raising=False)
        with pytest.raises(RuntimeError, match="DB_URL"):
            _psql_connection()

    def test_parses_url_and_rewrites_bridge_host(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify driver suffix strip, bridge-host rewrite, and password env.

        Given: A DB_URL with +asyncpg and the docker bridge host,
        When: _psql_connection is called,
        Then: The host is loopback, password is in env not argv, and the
            connection fields are parsed.
        """
        monkeypatch.setenv("DB_URL", "postgresql+asyncpg://snapper:secret@172.17.0.1:5433/snapper")
        argv, env = _psql_connection()
        assert "172.17.0.1" not in argv
        assert "secret" not in argv
        assert env["PGPASSWORD"] == "secret"
        assert "127.0.0.1" in argv
        assert "5433" in argv
        assert "snapper" in argv

    def test_falls_back_to_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify host/port/db defaults when the URL omits them.

        Given: A DB_URL with no host, port, or path,
        When: _psql_connection is called,
        Then: Loopback host, default port, and default db are used.
        """
        monkeypatch.setenv("DB_URL", "postgresql://user:pw@")
        argv, _env = _psql_connection()
        assert "127.0.0.1" in argv
        assert "5432" in argv
        assert "snapper" in argv


class TestQuerySecondsSinceFresh:
    """Tests for the candle-freshness query."""

    def test_parses_float(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify a numeric result is parsed to seconds.

        Given: psql returns a numeric staleness,
        When: query_seconds_since_fresh runs,
        Then: The float value is returned.
        """
        monkeypatch.setenv("DB_URL", "postgresql://u:p@h/snapper")
        with patch(f"{_MODULE}.subprocess.run", return_value=MagicMock(stdout="42.5\n")):
            assert query_seconds_since_fresh("kraken") == 42.5

    def test_empty_result_is_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify an empty result yields None.

        Given: psql returns no rows,
        When: query_seconds_since_fresh runs,
        Then: None is returned.
        """
        monkeypatch.setenv("DB_URL", "postgresql://u:p@h/snapper")
        with patch(f"{_MODULE}.subprocess.run", return_value=MagicMock(stdout="\n")):
            assert query_seconds_since_fresh("kraken") is None

    def test_unparseable_result_is_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify a non-numeric result yields None.

        Given: psql returns garbage,
        When: query_seconds_since_fresh runs,
        Then: None is returned.
        """
        monkeypatch.setenv("DB_URL", "postgresql://u:p@h/snapper")
        with patch(f"{_MODULE}.subprocess.run", return_value=MagicMock(stdout="oops")):
            assert query_seconds_since_fresh("kraken") is None


class TestAwaitRecovery:
    """Tests for the recovery measurement loop."""

    def test_recovers_immediately(self) -> None:
        """Verify fresh data on the first poll reports recovery.

        Given: A query reporting fresh data within the SLA,
        When: await_recovery runs,
        Then: The exchange is marked recovered and no sleep occurs.
        """
        sleep_mock = MagicMock()
        result = await_recovery(
            ["kraken"],
            sla_s=10.0,
            poll_s=1.0,
            query=lambda _e: 5.0,
            sleep=sleep_mock,
            now=lambda: 0.0,
        )
        assert result == {"kraken": 0.0}
        sleep_mock.assert_not_called()

    def test_recovers_after_one_poll(self) -> None:
        """Verify recovery detected on a later poll.

        Given: A query that reports stale then fresh,
        When: await_recovery runs,
        Then: The exchange recovers after one sleep.
        """
        sleep_mock = MagicMock()
        readings: Iterator[float | None] = iter([None, 5.0])
        result = await_recovery(
            ["kraken"],
            sla_s=10.0,
            poll_s=1.0,
            query=lambda _e: next(readings),
            sleep=sleep_mock,
            now=lambda: 0.0,
        )
        assert result == {"kraken": 0.0}
        sleep_mock.assert_called_once_with(1.0)

    def test_sla_exceeded_reports_none(self) -> None:
        """Verify no recovery within the SLA reports None.

        Given: A query that never reports fresh data and a clock that
            advances past the deadline,
        When: await_recovery runs,
        Then: The exchange is reported as not recovered.
        """
        clock: Iterator[float] = iter([0.0, 0.0, 5.0, 100.0])
        result = await_recovery(
            ["kraken"],
            sla_s=10.0,
            poll_s=1.0,
            query=lambda _e: None,
            sleep=MagicMock(),
            now=lambda: next(clock),
        )
        assert result == {"kraken": None}


class TestRunOutageCycle:
    """Tests for the inject/hold/restore/measure cycle."""

    def test_drops_then_restores_and_measures(self) -> None:
        """Verify the cycle drops, holds, restores, and returns recovery.

        Given: A stubbed runner and recovery measurement,
        When: run_outage_cycle runs,
        Then: The drop rule precedes the restore rule and the recovery
            mapping is returned.
        """
        run_mock = MagicMock(return_value=0)
        sleep_mock = MagicMock()
        with patch(f"{_MODULE}.await_recovery", return_value={"kraken": 2.0}) as await_mock:
            result = run_outage_cycle(
                container="snapper-egress",
                exchanges=["kraken"],
                hold_s=300.0,
                sla_s=120.0,
                poll_s=5.0,
                run=run_mock,
                sleep=sleep_mock,
            )
        assert result == {"kraken": 2.0}
        assert run_mock.call_args_list[0].args[0][4] == "-A"
        assert run_mock.call_args_list[1].args[0][4] == "-D"
        sleep_mock.assert_called_once_with(300.0)
        await_mock.assert_called_once()

    def test_restores_even_when_hold_raises(self) -> None:
        """Verify connectivity is restored if the hold raises.

        Given: A sleep that raises during the hold,
        When: run_outage_cycle runs,
        Then: The restore rule is still issued and the error propagates.
        """
        run_mock = MagicMock(return_value=0)
        sleep_mock = MagicMock(side_effect=RuntimeError("interrupted"))
        with pytest.raises(RuntimeError, match="interrupted"):
            run_outage_cycle(
                container="snapper-egress",
                exchanges=["kraken"],
                hold_s=300.0,
                sla_s=120.0,
                poll_s=5.0,
                run=run_mock,
                sleep=sleep_mock,
            )
        assert run_mock.call_args_list[0].args[0][4] == "-A"
        assert run_mock.call_args_list[1].args[0][4] == "-D"


class TestParseArgs:
    """Tests for argument parsing."""

    def test_defaults(self) -> None:
        """Verify default arguments.

        Given: No arguments,
        When: _parse_args runs,
        Then: The public path and default timings are used.
        """
        args = _parse_args([])
        assert args.path == "public"
        assert args.hold_s == 300.0
        assert args.sla_s == 120.0

    def test_private_path_overrides(self) -> None:
        """Verify private-path overrides.

        Given: Private path and container arguments,
        When: _parse_args runs,
        Then: The parsed namespace reflects them.
        """
        args = _parse_args(["--path", "private", "--private-container", "snapper-feed"])
        assert args.path == "private"
        assert args.private_container == "snapper-feed"


class TestMain:
    """Tests for the harness entry point."""

    def test_public_all_recovered_returns_zero(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify a clean public-path run reports PASS and exits zero.

        Given: Every public exchange recovers,
        When: main runs the public path,
        Then: It returns 0 and prints PASS lines.
        """
        with patch(
            f"{_MODULE}.run_outage_cycle",
            return_value={"kraken": 5.0, "kraken_futures": 10.0},
        ) as cycle_mock:
            code = main(["--path", "public"])
        assert code == 0
        assert cycle_mock.call_args.kwargs["container"] == "snapper-egress"
        assert "PASS kraken" in capsys.readouterr().out

    def test_private_failure_returns_one(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify a non-recovering private-path run reports FAIL and exits one.

        Given: The private exchange does not recover,
        When: main runs the private path with a custom container,
        Then: It returns 1, prints FAIL, and targets the custom container.
        """
        with patch(
            f"{_MODULE}.run_outage_cycle",
            return_value={"kraken_equities": None},
        ) as cycle_mock:
            code = main(["--path", "private", "--private-container", "snapper-feed"])
        assert code == 1
        assert cycle_mock.call_args.kwargs["container"] == "snapper-feed"
        assert "FAIL kraken_equities" in capsys.readouterr().out
