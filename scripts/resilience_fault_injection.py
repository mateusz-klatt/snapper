"""Operator harness: fault-inject a market-data outage and assert feed recovery.

Severs the feed's exchange connectivity, holds the outage, restores it, and
verifies each Kraken feed recovers fresh 1m candles within an SLA. Exercises
the publisher liveness-recovery loop, transport keepalive, and dark-feed
watchdog end to end. Run it in a low-impact window — it darkens live market
data for the hold duration.

Injection blocks outbound HTTPS (:443) INSIDE THE FEED CONTAINER'S network
namespace via a throwaway privileged helper
(``docker run --net=container:<feed> --cap-add=NET_ADMIN``). Two earlier
approaches do not work here and informed this one: the runtime image ships no
``iptables`` (so ``docker exec <feed> iptables`` fails), and the feed connects
DIRECTLY to the exchanges rather than through the egress proxy (so pausing or
firewalling egress is a no-op). Only :443 is dropped, so the ZMQ bus and the
Postgres write path (other ports) keep working — the outage is market-data
only; order execution (a separate container) is unaffected.

Restore is treated as safety-critical: the DROP rule lives in the feed's netns
independently of the helper container, and a stranded rule keeps the feed dark.
Every step is exit-code-checked; the helper runs a PREBUILT image with
``iptables`` baked in (no runtime package install while the link is down);
removal is proven with ``iptables -C`` (not substring scraping); and if removal
cannot be PROVEN, the harness recreates the feed container (fresh netns) and
re-verifies, raising loudly if it still cannot prove the rule is gone. It never
silently reports success on an uncertain restore.

Recovery is measured against the ``candles`` table (seconds since the newest
current 1m candle per exchange), derived from ``DB_URL``; the password is never
printed.
"""

import argparse
import os
import subprocess
import time
import urllib.parse
from collections.abc import Callable
from collections.abc import Sequence
from pathlib import Path

_COMPOSE_FILE = str(Path(__file__).resolve().parent.parent / "docker-compose.yml")
_FEED_CONTAINER = "snapper-feed"
_HELPER_IMAGE = "snapper-fault-helper"
_HELPER_DOCKERFILE = "FROM alpine\nRUN apk add --no-cache iptables\n"
_HTTPS_PORT = 443
_DEFAULT_HOLD_S = 180.0
_DEFAULT_SLA_S = 180.0
_DEFAULT_POLL_S = 10.0
_DEFAULT_EXCHANGES = "kraken,kraken_futures"
_DEFAULT_FRESH_S = 120.0
"""Default freshness threshold: a venue counts as recovered once its newest
1m candle is at most this old. Decoupled from ``sla_s`` — the SLA is the
WALL-CLOCK budget for reaching freshness, not the freshness bar itself.
Conflating the two let a run with a generous SLA "pass" while a venue was
still dark (candle age happened to sit under the SLA at restore time)."""
_FRESHNESS_WINDOW_MIN = 30
"""Recent ``open_at`` window (minutes) the freshness probe scans."""
_FRESHNESS_SQL = (
    "SELECT EXTRACT(EPOCH FROM now() - max(c.open_at)) FROM candles c "
    "JOIN instruments i ON i.public_id = c.instrument_public_id "
    "AND i.known_to >= TIMESTAMP '9999-12-31' "
    "WHERE c.timeframe = '1m' AND c.known_to >= TIMESTAMP '9999-12-31' "
    f"AND c.open_at > now() - INTERVAL '{_FRESHNESS_WINDOW_MIN} minutes' "
    "AND i.exchange = '{exchange}'"
)
"""Freshness probe bounded to a recent ``open_at`` window.

Without the bound this was an unbounded ``max(open_at)`` over the
multi-hundred-million-row candles table — the same scan class as the feed
startup-cache incident — and under post-blackout write load a single poll
took long enough to blow past the SLA deadline check, producing reports
like ``PASS ... recovered in 423s`` on a 240 s-SLA run. Bounded, the probe
rides the ``(instrument, open_at)`` index; when no candle exists inside the
window the query returns NULL, which the caller treats as not-yet-fresh and
keeps polling — exactly the wanted semantics."""

_Runner = Callable[[Sequence[str]], tuple[int, str]]


def run_helper(argv: Sequence[str]) -> tuple[int, str]:
    """Run a command, returning its exit code and stdout.

    Args:
        argv: The command and arguments to execute.

    Returns:
        A ``(returncode, stdout)`` pair.
    """
    completed = subprocess.run(list(argv), check=False, capture_output=True, text=True)
    return completed.returncode, completed.stdout


def _build_helper_image() -> int:
    """Build the prebuilt iptables helper image; return the build exit code.

    Returns:
        The ``docker build`` exit code.
    """
    completed = subprocess.run(
        ["docker", "build", "-t", _HELPER_IMAGE, "-"],
        input=_HELPER_DOCKERFILE,
        text=True,
        check=False,
    )
    return completed.returncode


def ensure_helper_image(
    *,
    run: _Runner = run_helper,
    build: Callable[[], int] = _build_helper_image,
) -> None:
    """Ensure the iptables helper image exists, building it if absent.

    Building happens before any outage (network up), so the image has
    ``iptables`` baked in and the restore path never needs a package install
    while :443 is dropped.

    Args:
        run: Command runner returning ``(rc, stdout)``.
        build: Builder returning the build exit code.

    Returns:
        None.

    Raises:
        RuntimeError: If the image is absent and the build fails.
    """
    rc, _out = run(["docker", "image", "inspect", _HELPER_IMAGE])
    if rc == 0:
        return
    if build() != 0:
        raise RuntimeError(f"failed to build helper image {_HELPER_IMAGE}")


def build_netns_iptables_command(
    container: str, action: str, port: int = _HTTPS_PORT, image: str = _HELPER_IMAGE
) -> list[str]:
    """Return a privileged-helper command to add/remove a netns DROP rule.

    The helper joins ``container``'s network namespace and adds (``-A``) or
    removes (``-D``) an ``OUTPUT`` DROP on ``port``. ``--pull=never`` forces
    the local prebuilt image.

    Args:
        container: Container whose netns is firewalled.
        action: ``"-A"`` to start the outage or ``"-D"`` to restore.
        port: Destination TCP port to block.
        image: Prebuilt helper image with ``iptables``.

    Returns:
        The argv list for the command.
    """
    return [
        "docker",
        "run",
        "--rm",
        "--pull=never",
        "--net",
        f"container:{container}",
        "--cap-add",
        "NET_ADMIN",
        image,
        "iptables",
        action,
        "OUTPUT",
        "-p",
        "tcp",
        "--dport",
        str(port),
        "-j",
        "DROP",
    ]


def build_netns_iptables_check_command(
    container: str, port: int = _HTTPS_PORT, image: str = _HELPER_IMAGE
) -> list[str]:
    """Return a helper command that checks the DROP rule's existence.

    ``iptables -C`` exits 0 when the exact rule exists and 1 when it does not,
    which is a reliable existence test (no output scraping).

    Args:
        container: Container whose netns is checked.
        port: Destination TCP port of the rule.
        image: Prebuilt helper image with ``iptables``.

    Returns:
        The argv list for the command.
    """
    return [
        "docker",
        "run",
        "--rm",
        "--pull=never",
        "--net",
        f"container:{container}",
        "--cap-add",
        "NET_ADMIN",
        image,
        "iptables",
        "-C",
        "OUTPUT",
        "-p",
        "tcp",
        "--dport",
        str(port),
        "-j",
        "DROP",
    ]


def build_recreate_command(service: str) -> list[str]:
    """Return the compose command that recreates a service (resets its netns).

    Uses an explicit ``-f <repo>/docker-compose.yml`` so the backstop does not
    depend on the harness's working directory. ``service`` is the compose
    SERVICE name (which the caller keeps distinct from the docker container
    name, defaulting them equal for ``snapper-feed``); a service the compose
    file does not define makes the recreate fail loudly rather than silently
    strand the rule.

    Args:
        service: Compose service to force-recreate.

    Returns:
        The argv list for the command.
    """
    return [
        "docker",
        "compose",
        "-f",
        _COMPOSE_FILE,
        "up",
        "-d",
        "--no-deps",
        "--force-recreate",
        service,
    ]


def verify_rule_present(
    container: str,
    port: int = _HTTPS_PORT,
    *,
    run: _Runner = run_helper,
) -> bool | None:
    """Return whether the DROP rule exists in the container's netns.

    Args:
        container: Container whose netns to check.
        port: Destination TCP port of the rule.
        run: Command runner returning ``(rc, stdout)``.

    Returns:
        ``True`` if the rule exists (``iptables -C`` exit 0), ``False`` if it
        provably does not (exit 1), or ``None`` if existence could not be
        determined (any other exit code — treat as unsafe/unknown).
    """
    rc, _out = run(build_netns_iptables_check_command(container, port))
    if rc == 0:
        return True
    if rc == 1:
        return False
    return None


def inject(container: str, port: int = _HTTPS_PORT, *, run: _Runner = run_helper) -> None:
    """Start the outage by adding the netns DROP rule, failing loudly on error.

    Args:
        container: Container whose netns is firewalled.
        port: Destination TCP port to block.
        run: Command runner returning ``(rc, stdout)``.

    Returns:
        None.

    Raises:
        RuntimeError: If the iptables append exits non-zero (so the harness
            never proceeds with a silent no-op outage).
    """
    rc, _out = run(build_netns_iptables_command(container, "-A", port))
    if rc != 0:
        raise RuntimeError(f"inject failed: iptables -A exited {rc} for {container}:{port}")


def restore(
    container: str,
    port: int = _HTTPS_PORT,
    *,
    run: _Runner = run_helper,
    service: str | None = None,
) -> str:
    """Restore connectivity, proving the rule is gone with a recreate backstop.

    Issues ``iptables -D`` (best effort — the rule may not exist), then PROVES
    absence with ``iptables -C``. If the rule is still present or its absence
    cannot be proven, the compose ``service`` is force-recreated (fresh netns)
    and absence is re-verified. Raises loudly rather than reporting an
    uncertain success.

    Args:
        container: Container whose netns DROP rule to remove.
        port: Destination TCP port that was blocked.
        run: Command runner returning ``(rc, stdout)``.
        service: Compose service to recreate as the backstop; defaults to
            ``container`` (correct when the container name is also the service
            name, as for ``snapper-feed``).

    Returns:
        ``"removed"`` when the explicit delete provably cleared the rule, or
        ``"recreated"`` when the service had to be recreated as a backstop.

    Raises:
        RuntimeError: If recreate fails, or the rule is still present (or its
            absence is unprovable) after the recreate backstop.
    """
    recreate_service = service or container
    run(build_netns_iptables_command(container, "-D", port))
    if verify_rule_present(container, port, run=run) is False:
        return "removed"
    rc, _out = run(build_recreate_command(recreate_service))
    if rc != 0:
        raise RuntimeError(
            f"restore failed: rule present/unverifiable and recreate of "
            f"{recreate_service} exited {rc}"
        )
    if verify_rule_present(container, port, run=run) is not False:
        raise RuntimeError(
            f"restore failed: rule absence unproven after recreating {recreate_service} "
            "(still present or unverifiable)"
        )
    return "recreated"


def _psql_connection() -> tuple[list[str], dict[str, str]]:
    """Build a psql base argv and environment from ``DB_URL``.

    Strips the SQLAlchemy ``+asyncpg`` driver suffix and rewrites the
    docker-bridge host to loopback so the harness can reach a host-native
    Postgres. The password is carried in ``PGPASSWORD`` and never placed on
    the command line.

    Returns:
        A ``(argv_prefix, env)`` pair; ``argv_prefix`` lacks the ``-c SQL``
        the caller appends.

    Raises:
        RuntimeError: If ``DB_URL`` is not set in the environment.
    """
    raw = os.environ.get("DB_URL")
    if not raw:
        raise RuntimeError("DB_URL is not set")
    parsed = urllib.parse.urlparse(raw.replace("+asyncpg", "").replace("+aiosqlite", ""))
    host = parsed.hostname or "127.0.0.1"
    if host == "172.17.0.1":
        host = "127.0.0.1"
    env = dict(os.environ)
    env["PGPASSWORD"] = urllib.parse.unquote(parsed.password or "")
    argv = [
        "psql",
        "-h",
        host,
        "-p",
        str(parsed.port or 5432),
        "-U",
        urllib.parse.unquote(parsed.username or ""),
        "-d",
        (parsed.path or "/snapper").lstrip("/"),
        "-tAc",
    ]
    return argv, env


def query_seconds_since_fresh(exchange: str) -> float | None:
    """Return seconds since the most recent current 1m candle for an exchange.

    Args:
        exchange: The instrument exchange to measure (e.g. ``"kraken"``).

    Returns:
        Seconds since the newest current 1m candle, or ``None`` when the
        query returns no rows or an unparseable value.
    """
    argv, env = _psql_connection()
    result = subprocess.run(
        [*argv, _FRESHNESS_SQL.format(exchange=exchange)],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    value = result.stdout.strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def await_recovery(
    exchanges: Sequence[str],
    *,
    sla_s: float,
    poll_s: float,
    fresh_s: float = _DEFAULT_FRESH_S,
    query: Callable[[str], float | None] = query_seconds_since_fresh,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], float] = time.monotonic,
) -> dict[str, float | None]:
    """Poll each exchange until fresh data returns or the SLA elapses.

    Freshness (``fresh_s``: how recent the newest candle must be) is a
    separate axis from the SLA (``sla_s``: the wall-clock budget for
    reaching that freshness). The pass condition is recorded only while the
    deadline holds, so a slow poll can never report a "recovery" that
    happened after the SLA already expired.

    Args:
        exchanges: Exchanges to verify recovery for.
        sla_s: Maximum seconds to wait for fresh data after restoration.
        poll_s: Seconds between freshness polls.
        fresh_s: Maximum age (seconds) of the newest candle that counts as
            fresh data.
        query: Callable returning seconds-since-fresh for an exchange.
        sleep: Callable used to pace polling.
        now: Monotonic clock callable.

    Returns:
        Mapping of exchange to the seconds-to-recovery measured, or ``None``
        if the SLA elapsed before fresh data returned.
    """
    start = now()
    deadline = start + sla_s
    pending = list(exchanges)
    recovered: dict[str, float | None] = {}
    while pending and now() < deadline:
        still_pending: list[str] = []
        for exchange in pending:
            seconds_since = query(exchange)
            if seconds_since is not None and seconds_since <= fresh_s and now() < deadline:
                recovered[exchange] = now() - start
            else:
                still_pending.append(exchange)
        pending = still_pending
        if pending:
            sleep(poll_s)
    for exchange in pending:
        recovered[exchange] = None
    return recovered


def run_outage_cycle(
    *,
    container: str,
    port: int,
    exchanges: Sequence[str],
    hold_s: float,
    sla_s: float,
    poll_s: float,
    fresh_s: float = _DEFAULT_FRESH_S,
    service: str | None = None,
    run: _Runner = run_helper,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, float | None]:
    """Inject an outage, hold, restore (proven), then measure recovery.

    The helper image is ensured before any outage starts. Injection and the
    hold run inside a ``try`` whose ``finally`` always restores — and
    :func:`restore` raises rather than reporting an uncertain success — so an
    outage can never silently outlive the run.

    Args:
        container: Container whose netns is firewalled.
        port: Destination TCP port to block.
        exchanges: Exchanges expected to recover after restoration.
        hold_s: Seconds to hold the outage.
        sla_s: Recovery SLA passed to :func:`await_recovery`.
        fresh_s: Freshness threshold passed to :func:`await_recovery`.
        poll_s: Poll cadence passed to :func:`await_recovery`.
        service: Compose service to recreate as the restore backstop;
            defaults to ``container``.
        run: Command runner returning ``(rc, stdout)``.
        sleep: Callable used to hold the outage.

    Returns:
        The per-exchange recovery mapping from :func:`await_recovery`.
    """
    ensure_helper_image(run=run)
    try:
        inject(container, port, run=run)
        sleep(hold_s)
    finally:
        restore(container, port, run=run, service=service)
    return await_recovery(exchanges, sla_s=sla_s, poll_s=poll_s, fresh_s=fresh_s)


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    """Parse command-line arguments for the harness.

    Args:
        argv: Argument vector, or ``None`` to read from ``sys.argv``.

    Returns:
        The parsed namespace.
    """
    parser = argparse.ArgumentParser(description="Fault-inject a feed market-data outage.")
    parser.add_argument(
        "--container",
        default=_FEED_CONTAINER,
        help="Docker container whose netns to firewall (must allow a NET_ADMIN helper).",
    )
    parser.add_argument(
        "--service",
        default=None,
        help="Compose service to recreate as the restore backstop (default: --container).",
    )
    parser.add_argument("--port", type=int, default=_HTTPS_PORT)
    parser.add_argument(
        "--exchanges",
        default=_DEFAULT_EXCHANGES,
        help="Comma-separated exchanges to verify recovery for.",
    )
    parser.add_argument("--hold-s", type=float, default=_DEFAULT_HOLD_S)
    parser.add_argument("--sla-s", type=float, default=_DEFAULT_SLA_S)
    parser.add_argument("--poll-s", type=float, default=_DEFAULT_POLL_S)
    parser.add_argument(
        "--fresh-s",
        type=float,
        default=_DEFAULT_FRESH_S,
        help="Max age (s) of the newest 1m candle that counts as fresh data.",
    )
    parser.add_argument(
        "--restore-only",
        action="store_true",
        help="Remove a stranded DROP rule and verify; run no outage (SIGKILL remedy).",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run one fault-injection cycle and report recovery, returning an exit code.

    Args:
        argv: Argument vector, or ``None`` to read from ``sys.argv``.

    Returns:
        ``0`` when every exchange recovered within the SLA, else ``1``.
    """
    args = _parse_args(argv)
    service = args.service or args.container
    if args.restore_only:
        ensure_helper_image()
        status = restore(args.container, args.port, service=service)
        print(f"restore-only: {status} on {args.container} :{args.port}")
        return 0
    exchanges = tuple(e.strip() for e in args.exchanges.split(",") if e.strip())
    print(f"Injecting outage in {args.container} netns (:{args.port}) for {args.hold_s:.0f}s")
    recovered = run_outage_cycle(
        container=args.container,
        port=args.port,
        exchanges=exchanges,
        hold_s=args.hold_s,
        sla_s=args.sla_s,
        poll_s=args.poll_s,
        fresh_s=args.fresh_s,
        service=service,
    )
    ok = True
    for exchange, seconds in recovered.items():
        if seconds is None:
            print(
                f"FAIL {exchange}: no candle fresher than {args.fresh_s:.0f}s "
                f"within {args.sla_s:.0f}s SLA"
            )
            ok = False
        else:
            print(
                f"PASS {exchange}: candle age <= {args.fresh_s:.0f}s "
                f"after {seconds:.0f}s (SLA {args.sla_s:.0f}s)"
            )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
