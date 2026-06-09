"""Operator harness: fault-inject network outages and assert feed recovery.

STAGING ONLY — never run against production. Simulates multi-minute network
outages on the public (egress) and private (direct) paths, then verifies each
Kraken feed recovers to fresh market data within an SLA. Exercises the
publisher liveness-recovery loop, transport keepalive, and dark-feed watchdog
end to end.

Injection blocks outbound HTTPS in the target container via
``docker exec <container> iptables`` so the internal ZMQ bus (other ports)
keeps working and only the exchange link is severed. The DROP rule is always
removed in a ``finally`` block so a failed or interrupted run cannot leave a
container partitioned. The public path targets ``snapper-egress`` (which holds
``NET_ADMIN``); the private/direct path targets the container the operator
passes via ``--private-container`` (it must be started with ``NET_ADMIN`` for
the test).

Recovery is measured against the ``candles`` table: seconds since the most
recent current 1m candle per exchange, derived from ``DB_URL`` in the
environment. The password is never printed.
"""

import argparse
import os
import subprocess
import time
import urllib.parse
from collections.abc import Callable
from collections.abc import Sequence

_EGRESS_CONTAINER = "snapper-egress"
_HTTPS_PORT = 443
_DEFAULT_HOLD_S = 300.0
_DEFAULT_SLA_S = 120.0
_DEFAULT_POLL_S = 5.0
_PUBLIC_EXCHANGES = ("kraken", "kraken_futures")
_PRIVATE_EXCHANGES = ("kraken_equities",)
_FRESHNESS_SQL = (
    "SELECT EXTRACT(EPOCH FROM now() - max(c.open_at)) FROM candles c "
    "JOIN instruments i ON i.public_id = c.instrument_public_id "
    "AND i.known_to >= TIMESTAMP '9999-12-31' "
    "WHERE c.timeframe = '1m' AND c.known_to >= TIMESTAMP '9999-12-31' "
    "AND i.exchange = '{exchange}'"
)


def build_iptables_command(container: str, action: str, port: int = _HTTPS_PORT) -> list[str]:
    """Return a docker-exec iptables command to drop or restore egress.

    Args:
        container: Target container name.
        action: ``"-A"`` to append the DROP rule (start the outage) or
            ``"-D"`` to remove it (restore connectivity).
        port: Destination TCP port to block (HTTPS/WSS by default).

    Returns:
        The argv list for the command.
    """
    return [
        "docker",
        "exec",
        container,
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


def run_command(argv: Sequence[str]) -> int:
    """Run a command and return its exit code.

    Args:
        argv: The command and arguments to execute.

    Returns:
        The process exit code.
    """
    return subprocess.run(list(argv), check=False).returncode


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
    query: Callable[[str], float | None] = query_seconds_since_fresh,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], float] = time.monotonic,
) -> dict[str, float | None]:
    """Poll each exchange until fresh data returns or the SLA elapses.

    Args:
        exchanges: Exchanges to verify recovery for.
        sla_s: Maximum seconds to wait for fresh data after restoration.
        poll_s: Seconds between freshness polls.
        query: Callable returning seconds-since-fresh for an exchange.
        sleep: Callable used to pace polling.
        now: Monotonic clock callable.

    Returns:
        Mapping of exchange to the seconds-to-recovery measured, or ``None``
        if the SLA elapsed before fresh data returned.
    """
    deadline = now() + sla_s
    pending = list(exchanges)
    recovered: dict[str, float | None] = {}
    start = now()
    while pending and now() < deadline:
        still_pending: list[str] = []
        for exchange in pending:
            seconds_since = query(exchange)
            if seconds_since is not None and seconds_since <= sla_s:
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
    exchanges: Sequence[str],
    hold_s: float,
    sla_s: float,
    poll_s: float,
    run: Callable[[Sequence[str]], int] = run_command,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, float | None]:
    """Inject an outage in ``container``, hold, restore, then measure recovery.

    Connectivity is always restored in a ``finally`` block even if the hold
    or measurement raises, so a partition can never outlive the run.

    Args:
        container: Container whose outbound HTTPS is dropped.
        exchanges: Exchanges expected to recover after restoration.
        hold_s: Seconds to hold the outage.
        sla_s: Recovery SLA passed to :func:`await_recovery`.
        poll_s: Poll cadence passed to :func:`await_recovery`.
        run: Command runner returning an exit code.
        sleep: Callable used to hold the outage.

    Returns:
        The per-exchange recovery mapping from :func:`await_recovery`.
    """
    run(build_iptables_command(container, "-A"))
    try:
        sleep(hold_s)
    finally:
        run(build_iptables_command(container, "-D"))
    return await_recovery(exchanges, sla_s=sla_s, poll_s=poll_s)


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    """Parse command-line arguments for the harness.

    Args:
        argv: Argument vector, or ``None`` to read from ``sys.argv``.

    Returns:
        The parsed namespace.
    """
    parser = argparse.ArgumentParser(description="Fault-inject feed outages (staging only).")
    parser.add_argument(
        "--path",
        choices=("public", "private"),
        default="public",
        help="Which network path to sever.",
    )
    parser.add_argument(
        "--private-container",
        default=_EGRESS_CONTAINER,
        help="Container to target for the private/direct path (needs NET_ADMIN).",
    )
    parser.add_argument("--hold-s", type=float, default=_DEFAULT_HOLD_S)
    parser.add_argument("--sla-s", type=float, default=_DEFAULT_SLA_S)
    parser.add_argument("--poll-s", type=float, default=_DEFAULT_POLL_S)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run one fault-injection cycle and report recovery, returning an exit code.

    Args:
        argv: Argument vector, or ``None`` to read from ``sys.argv``.

    Returns:
        ``0`` when every exchange recovered within the SLA, else ``1``.
    """
    args = _parse_args(argv)
    if args.path == "public":
        container = _EGRESS_CONTAINER
        exchanges: Sequence[str] = _PUBLIC_EXCHANGES
    else:
        container = args.private_container
        exchanges = _PRIVATE_EXCHANGES
    print(f"Injecting {args.path} outage in {container} for {args.hold_s:.0f}s")
    recovered = run_outage_cycle(
        container=container,
        exchanges=exchanges,
        hold_s=args.hold_s,
        sla_s=args.sla_s,
        poll_s=args.poll_s,
    )
    ok = True
    for exchange, seconds in recovered.items():
        if seconds is None:
            print(f"FAIL {exchange}: no fresh data within {args.sla_s:.0f}s SLA")
            ok = False
        else:
            print(f"PASS {exchange}: recovered in {seconds:.0f}s")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
