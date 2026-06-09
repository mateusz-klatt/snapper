"""Tests for the resilience fault-injection harness."""

from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Sequence
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from scripts.resilience_fault_injection import _COMPOSE_FILE
from scripts.resilience_fault_injection import _HELPER_DOCKERFILE
from scripts.resilience_fault_injection import _build_helper_image
from scripts.resilience_fault_injection import _parse_args
from scripts.resilience_fault_injection import _psql_connection
from scripts.resilience_fault_injection import await_recovery
from scripts.resilience_fault_injection import build_netns_iptables_check_command
from scripts.resilience_fault_injection import build_netns_iptables_command
from scripts.resilience_fault_injection import build_recreate_command
from scripts.resilience_fault_injection import ensure_helper_image
from scripts.resilience_fault_injection import inject
from scripts.resilience_fault_injection import main
from scripts.resilience_fault_injection import query_seconds_since_fresh
from scripts.resilience_fault_injection import restore
from scripts.resilience_fault_injection import run_helper
from scripts.resilience_fault_injection import run_outage_cycle
from scripts.resilience_fault_injection import verify_rule_present

_MODULE = "scripts.resilience_fault_injection"


def _runner(
    *,
    inspect: tuple[int, str] = (0, ""),
    inject_rc: tuple[int, str] = (0, ""),
    delete: tuple[int, str] = (0, ""),
    recreate: tuple[int, str] = (0, ""),
    checks: Sequence[tuple[int, str]] | None = None,
) -> Callable[[Sequence[str]], tuple[int, str]]:
    """Build a fake command runner keyed by command shape (``-C`` is a queue)."""
    pending_checks = list(checks or [])

    def run(argv: Sequence[str]) -> tuple[int, str]:
        a = list(argv)
        if "image" in a and "inspect" in a:
            return inspect
        if "-A" in a:
            return inject_rc
        if "-D" in a:
            return delete
        if "-C" in a:
            return pending_checks.pop(0)
        if "compose" in a:
            return recreate
        return (0, "")

    return run


class TestRunHelper:
    """Tests for the subprocess runner."""

    def test_returns_rc_and_stdout(self) -> None:
        """Verify run_helper returns the exit code and stdout.

        Given: A subprocess that exits non-zero with output,
        When: run_helper is called,
        Then: The (returncode, stdout) pair is returned.
        """
        with patch(
            f"{_MODULE}.subprocess.run",
            return_value=MagicMock(returncode=3, stdout="out\n"),
        ):
            assert run_helper(["echo", "hi"]) == (3, "out\n")


class TestBuildHelperImage:
    """Tests for the prebuilt-image builder."""

    def test_builds_from_inline_dockerfile(self) -> None:
        """Verify the image is built from the inline iptables Dockerfile.

        Given: A successful docker build,
        When: _build_helper_image runs,
        Then: It returns 0 and feeds the iptables Dockerfile on stdin.
        """
        with patch(f"{_MODULE}.subprocess.run", return_value=MagicMock(returncode=0)) as run_mock:
            assert _build_helper_image() == 0
        assert run_mock.call_args.kwargs["input"] == _HELPER_DOCKERFILE


class TestEnsureHelperImage:
    """Tests for helper-image presence/build orchestration."""

    def test_skips_build_when_present(self) -> None:
        """Verify no build when the image already exists.

        Given: docker image inspect succeeds,
        When: ensure_helper_image runs,
        Then: The builder is not called.
        """
        build_mock = MagicMock(return_value=0)
        ensure_helper_image(run=_runner(inspect=(0, "")), build=build_mock)
        build_mock.assert_not_called()

    def test_builds_when_absent(self) -> None:
        """Verify a build when the image is absent.

        Given: docker image inspect fails,
        When: ensure_helper_image runs,
        Then: The builder is invoked.
        """
        build_mock = MagicMock(return_value=0)
        ensure_helper_image(run=_runner(inspect=(1, "")), build=build_mock)
        build_mock.assert_called_once()

    def test_raises_when_build_fails(self) -> None:
        """Verify a failed build raises.

        Given: The image is absent and the build fails,
        When: ensure_helper_image runs,
        Then: RuntimeError is raised.
        """
        with pytest.raises(RuntimeError, match="helper image"):
            ensure_helper_image(run=_runner(inspect=(1, "")), build=MagicMock(return_value=1))


class TestBuildCommands:
    """Tests for the command builders."""

    def test_append_command(self) -> None:
        """Verify the append command joins the netns and adds the DROP rule."""
        argv = build_netns_iptables_command("snapper-feed", "-A")
        assert "container:snapper-feed" in argv
        assert "NET_ADMIN" in argv
        assert "--pull=never" in argv
        assert argv[-9:] == [
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

    def test_delete_command_custom_port_image(self) -> None:
        """Verify the delete command honours a custom port and image."""
        argv = build_netns_iptables_command("c", "-D", port=8443, image="img")
        assert "img" in argv
        assert argv[-8:] == ["-D", "OUTPUT", "-p", "tcp", "--dport", "8443", "-j", "DROP"]

    def test_check_command(self) -> None:
        """Verify the check command uses iptables -C for existence."""
        argv = build_netns_iptables_check_command("snapper-feed")
        assert "container:snapper-feed" in argv
        assert argv[-9:] == [
            "iptables",
            "-C",
            "OUTPUT",
            "-p",
            "tcp",
            "--dport",
            "443",
            "-j",
            "DROP",
        ]

    def test_recreate_command(self) -> None:
        """Verify the recreate backstop uses an explicit compose file (cwd-free)."""
        assert build_recreate_command("snapper-feed") == [
            "docker",
            "compose",
            "-f",
            _COMPOSE_FILE,
            "up",
            "-d",
            "--no-deps",
            "--force-recreate",
            "snapper-feed",
        ]


class TestVerifyRulePresent:
    """Tests for rule-existence verification."""

    def test_present(self) -> None:
        """Verify exit 0 reports the rule present."""
        assert verify_rule_present("c", run=_runner(checks=[(0, "")])) is True

    def test_absent(self) -> None:
        """Verify exit 1 reports the rule absent."""
        assert verify_rule_present("c", run=_runner(checks=[(1, "")])) is False

    def test_unknown(self) -> None:
        """Verify any other exit code reports unknown (unsafe)."""
        assert verify_rule_present("c", run=_runner(checks=[(2, "err")])) is None


class TestInject:
    """Tests for inject."""

    def test_adds_rule(self) -> None:
        """Verify inject issues the append command when it succeeds."""
        run_mock = MagicMock(return_value=(0, ""))
        inject("snapper-feed", run=run_mock)
        assert "-A" in run_mock.call_args.args[0]

    def test_raises_on_failure(self) -> None:
        """Verify inject fails loudly when the append exits non-zero.

        Given: iptables -A exits non-zero,
        When: inject runs,
        Then: RuntimeError is raised so the harness never runs a no-op outage.
        """
        with pytest.raises(RuntimeError, match="inject failed"):
            inject("snapper-feed", run=_runner(inject_rc=(1, "")))


class TestRestore:
    """Tests for the safety-critical restore."""

    def test_removed_when_delete_proves_absent(self) -> None:
        """Verify restore reports removed when -C proves the rule gone."""
        assert restore("c", run=_runner(delete=(0, ""), checks=[(1, "")])) == "removed"

    def test_recreates_when_still_present(self) -> None:
        """Verify restore recreates when the rule survives the delete."""
        run = _runner(checks=[(0, ""), (1, "")], recreate=(0, ""))
        assert restore("c", run=run) == "recreated"

    def test_recreates_when_verification_unknown(self) -> None:
        """Verify an unverifiable delete escalates to recreate."""
        run = _runner(checks=[(2, "err"), (1, "")], recreate=(0, ""))
        assert restore("c", run=run) == "recreated"

    def test_raises_when_recreate_fails(self) -> None:
        """Verify restore raises loudly when the recreate backstop fails."""
        with pytest.raises(RuntimeError, match="recreate"):
            restore("c", run=_runner(checks=[(0, "")], recreate=(1, "boom")))

    def test_raises_when_rule_present_after_recreate(self) -> None:
        """Verify restore raises if the rule is still present post-recreate."""
        with pytest.raises(RuntimeError, match="unproven"):
            restore("c", run=_runner(checks=[(0, ""), (0, "")], recreate=(0, "")))

    def test_raises_when_unproven_after_recreate(self) -> None:
        """Verify restore raises when post-recreate verification is unknown.

        Given: The rule is present, recreate succeeds, but the post-recreate
            check returns unknown (not provably absent),
        When: restore runs,
        Then: It raises rather than reporting an uncertain success.
        """
        with pytest.raises(RuntimeError, match="unproven"):
            restore("c", run=_runner(checks=[(0, ""), (2, "err")], recreate=(0, "")))

    def test_recreate_uses_distinct_service(self) -> None:
        """Verify the recreate backstop targets the compose service, not container.

        Given: A container whose name differs from the compose service,
        When: restore escalates to the recreate backstop,
        Then: The compose recreate targets the service name.
        """
        run_spy = MagicMock(side_effect=_runner(checks=[(0, ""), (1, "")], recreate=(0, "")))
        assert restore("cont", run=run_spy, service="svc") == "recreated"
        compose = [c.args[0] for c in run_spy.call_args_list if "compose" in c.args[0]]
        assert compose and compose[0][-1] == "svc"


class TestPsqlConnection:
    """Tests for the psql connection builder."""

    def test_raises_without_db_url(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify a missing DB_URL raises."""
        monkeypatch.delenv("DB_URL", raising=False)
        with pytest.raises(RuntimeError, match="DB_URL"):
            _psql_connection()

    def test_parses_url_and_rewrites_bridge_host(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify driver-suffix strip, bridge-host rewrite, and password env."""
        monkeypatch.setenv("DB_URL", "postgresql+asyncpg://snapper:secret@172.17.0.1:5433/snapper")
        argv, env = _psql_connection()
        assert "172.17.0.1" not in argv
        assert "secret" not in argv
        assert env["PGPASSWORD"] == "secret"
        assert "127.0.0.1" in argv
        assert "5433" in argv
        assert "snapper" in argv

    def test_falls_back_to_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify host/port/db defaults when the URL omits them."""
        monkeypatch.setenv("DB_URL", "postgresql://user:pw@")
        argv, _env = _psql_connection()
        assert "127.0.0.1" in argv
        assert "5432" in argv
        assert "snapper" in argv


class TestQuerySecondsSinceFresh:
    """Tests for the candle-freshness query."""

    def test_parses_float(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify a numeric result is parsed to seconds."""
        monkeypatch.setenv("DB_URL", "postgresql://u:p@h/snapper")
        with patch(f"{_MODULE}.subprocess.run", return_value=MagicMock(stdout="42.5\n")):
            assert query_seconds_since_fresh("kraken") == 42.5

    def test_empty_result_is_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify an empty result yields None."""
        monkeypatch.setenv("DB_URL", "postgresql://u:p@h/snapper")
        with patch(f"{_MODULE}.subprocess.run", return_value=MagicMock(stdout="\n")):
            assert query_seconds_since_fresh("kraken") is None

    def test_unparseable_result_is_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify a non-numeric result yields None."""
        monkeypatch.setenv("DB_URL", "postgresql://u:p@h/snapper")
        with patch(f"{_MODULE}.subprocess.run", return_value=MagicMock(stdout="oops")):
            assert query_seconds_since_fresh("kraken") is None


class TestAwaitRecovery:
    """Tests for the recovery measurement loop."""

    def test_recovers_immediately(self) -> None:
        """Verify fresh data on the first poll reports recovery without sleeping."""
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
        """Verify recovery detected on a later poll."""
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
        """Verify no recovery within the SLA reports None."""
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

    def test_injects_holds_restores_measures(self) -> None:
        """Verify the cycle ensures the image, injects, holds, restores, measures."""
        run = _runner(inspect=(0, ""), inject_rc=(0, ""), delete=(0, ""), checks=[(1, "")])
        run_spy = MagicMock(side_effect=run)
        sleep_mock = MagicMock()
        with patch(f"{_MODULE}.await_recovery", return_value={"kraken": 2.0}) as await_mock:
            result = run_outage_cycle(
                container="snapper-feed",
                port=443,
                exchanges=["kraken"],
                hold_s=180.0,
                sla_s=180.0,
                poll_s=10.0,
                run=run_spy,
                sleep=sleep_mock,
            )
        assert result == {"kraken": 2.0}
        commands = [c.args[0] for c in run_spy.call_args_list]
        assert any("-A" in c for c in commands)
        assert any("-D" in c for c in commands)
        sleep_mock.assert_called_once_with(180.0)
        await_mock.assert_called_once()

    def test_restores_even_when_hold_raises(self) -> None:
        """Verify restore runs (and the error propagates) when the hold raises."""
        run = _runner(inspect=(0, ""), delete=(0, ""), checks=[(1, "")])
        run_spy = MagicMock(side_effect=run)
        sleep_mock = MagicMock(side_effect=RuntimeError("interrupted"))
        with pytest.raises(RuntimeError, match="interrupted"):
            run_outage_cycle(
                container="snapper-feed",
                port=443,
                exchanges=["kraken"],
                hold_s=180.0,
                sla_s=180.0,
                poll_s=10.0,
                run=run_spy,
                sleep=sleep_mock,
            )
        commands = [c.args[0] for c in run_spy.call_args_list]
        assert any("-D" in c for c in commands)

    def test_restores_when_inject_raises(self) -> None:
        """Verify a failed inject still triggers restore via the finally.

        Given: iptables -A exits non-zero (inject raises),
        When: run_outage_cycle runs,
        Then: The finally still issues the delete and the error propagates.
        """
        run = _runner(inspect=(0, ""), inject_rc=(1, ""), delete=(0, ""), checks=[(1, "")])
        run_spy = MagicMock(side_effect=run)
        with pytest.raises(RuntimeError, match="inject failed"):
            run_outage_cycle(
                container="snapper-feed",
                port=443,
                exchanges=["kraken"],
                hold_s=1.0,
                sla_s=1.0,
                poll_s=1.0,
                run=run_spy,
                sleep=MagicMock(),
            )
        commands = [c.args[0] for c in run_spy.call_args_list]
        assert any("-D" in c for c in commands)


class TestParseArgs:
    """Tests for argument parsing."""

    def test_defaults(self) -> None:
        """Verify default arguments."""
        args = _parse_args([])
        assert args.container == "snapper-feed"
        assert args.service is None
        assert args.port == 443
        assert args.exchanges == "kraken,kraken_futures"
        assert args.hold_s == 180.0

    def test_overrides(self) -> None:
        """Verify argument overrides."""
        args = _parse_args(
            ["--container", "snapper-egress", "--port", "8443", "--exchanges", "kraken"]
        )
        assert args.container == "snapper-egress"
        assert args.port == 8443
        assert args.exchanges == "kraken"


class TestMain:
    """Tests for the harness entry point."""

    def test_all_recovered_returns_zero(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify a clean run reports PASS, splits exchanges, and exits zero."""
        with patch(
            f"{_MODULE}.run_outage_cycle",
            return_value={"kraken": 5.0, "kraken_futures": 10.0},
        ) as cycle_mock:
            code = main(["--exchanges", "kraken,kraken_futures"])
        assert code == 0
        assert cycle_mock.call_args.kwargs["exchanges"] == ("kraken", "kraken_futures")
        assert cycle_mock.call_args.kwargs["service"] == "snapper-feed"
        assert "PASS kraken" in capsys.readouterr().out

    def test_service_override(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify --service is passed through distinct from --container.

        Given: --container and a distinct --service,
        When: main runs,
        Then: The cycle receives the explicit service name.
        """
        with patch(f"{_MODULE}.run_outage_cycle", return_value={"kraken": 1.0}) as cycle_mock:
            code = main(["--container", "x", "--service", "y", "--exchanges", "kraken"])
        assert code == 0
        assert cycle_mock.call_args.kwargs["container"] == "x"
        assert cycle_mock.call_args.kwargs["service"] == "y"
        capsys.readouterr()

    def test_failure_returns_one(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify a non-recovering run reports FAIL and exits one."""
        with patch(f"{_MODULE}.run_outage_cycle", return_value={"kraken": None}) as cycle_mock:
            code = main(["--container", "snapper-egress", "--exchanges", "kraken"])
        assert code == 1
        assert cycle_mock.call_args.kwargs["container"] == "snapper-egress"
        assert "FAIL kraken" in capsys.readouterr().out

    def test_restore_only_skips_outage(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify --restore-only restores a stranded rule and runs no outage.

        Given: The --restore-only flag,
        When: main runs,
        Then: It ensures the helper image, restores, runs no outage cycle, and
            exits zero.
        """
        with (
            patch(f"{_MODULE}.ensure_helper_image") as ensure_mock,
            patch(f"{_MODULE}.restore", return_value="removed") as restore_mock,
            patch(f"{_MODULE}.run_outage_cycle") as cycle_mock,
        ):
            code = main(["--restore-only", "--container", "snapper-feed"])
        assert code == 0
        ensure_mock.assert_called_once()
        restore_mock.assert_called_once()
        cycle_mock.assert_not_called()
        assert "restore-only" in capsys.readouterr().out


class TestFreshnessDecoupling:
    """Tests for the fresh-vs-SLA decoupling and deadline-honest reporting."""

    def test_age_under_sla_but_over_fresh_is_not_recovery(self) -> None:
        """A candle age under the SLA but over fresh_s is NOT a recovery.

        Given: A query reporting a constant 200s age, SLA 240s, fresh 120s,
        When: await_recovery polls until the deadline,
        Then: The exchange reports None — the previous conflated predicate
            (age <= sla) declared instant recovery while the venue was dark.
        """
        clock: Iterator[float] = iter([0.0, 0.0, 5.0, 300.0])
        result = await_recovery(
            ["kraken_futures"],
            sla_s=240.0,
            poll_s=1.0,
            fresh_s=120.0,
            query=lambda _e: 200.0,
            sleep=MagicMock(),
            now=lambda: next(clock),
        )
        assert result == {"kraken_futures": None}

    def test_pass_not_recorded_after_deadline(self) -> None:
        """Freshness observed only after the deadline is not a pass.

        Given: A slow poll whose fresh reading lands past the SLA deadline,
        When: await_recovery evaluates it,
        Then: The exchange reports None — elapsed times can no longer exceed
            the SLA in a PASS line (the 423s-on-a-240s-SLA report).
        """
        clock: Iterator[float] = iter([0.0, 5.0, 250.0, 251.0])
        result = await_recovery(
            ["kraken"],
            sla_s=240.0,
            poll_s=1.0,
            fresh_s=120.0,
            query=lambda _e: 60.0,
            sleep=MagicMock(),
            now=lambda: next(clock),
        )
        assert result == {"kraken": None}

    def test_parse_args_accepts_fresh_s(self) -> None:
        """--fresh-s parses into args.fresh_s.

        Given: A command line with --fresh-s 60,
        When: _parse_args runs,
        Then: The parsed namespace carries fresh_s=60.0.
        """
        args = _parse_args(["--fresh-s", "60"])
        assert args.fresh_s == 60.0
