"""Read-only standalone monitor with a bounded existing host-alert adapter.

Invoke with ``python -m snapper.cli.market_snapshot_freshness_monitor``.
The alert hook must be an executable wrapper accepting one safe reason code;
the host's sourced ``_alert.sh`` helper is not itself such an executable.
No response body, token, exception payload or credential path is printed.
"""

import argparse
import asyncio
import os
import subprocess
import sys
from pathlib import Path

from snapper.application.market_snapshot_refresh.monitor import FreshnessMonitorConfig
from snapper.application.market_snapshot_refresh.monitor import FreshnessMonitorError
from snapper.application.market_snapshot_refresh.monitor import check_market_snapshot_freshness


def _parser() -> argparse.ArgumentParser:
    """Build the explicit monitor command without implicit instrument or FX defaults.

    Returns:
        Argument parser whose credential input is a file path only.
    """
    parser = argparse.ArgumentParser(description="Observe a frozen market-snapshot scope.")
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--token-file", type=Path, required=True)
    parser.add_argument("--instrument-public-id", action="append", required=True)
    parser.add_argument("--fx-exchange", action="append", required=True)
    parser.add_argument("--scope-digest", required=True)
    parser.add_argument("--alert-hook", type=Path, required=True)
    return parser


def _alert(hook: Path, reason: str) -> None:
    """Attempt the existing executable host adapter without exposing child output.

    Args:
        hook: Explicit executable wrapper, never sourced as shell text here.
        reason: Fixed monitor failure code without remote response text.
    """
    print("ALERT requested: market-snapshot-freshness", file=sys.stderr)
    if not hook.is_absolute():
        print("ALERT-UNDELIVERED: hook path is not absolute", file=sys.stderr)
        return
    try:
        result = subprocess.run(
            [str(hook), reason],
            check=False,
            timeout=5,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except (OSError, subprocess.TimeoutExpired):
        print("ALERT-UNDELIVERED: hook execution failed", file=sys.stderr)
        return
    if result.returncode != 0:
        print("ALERT-UNDELIVERED: hook returned failure", file=sys.stderr)


def main(argv: list[str] | None = None) -> int:
    """Perform one observation and make failures visible to the host alert path.

    Args:
        argv: Optional command arguments for isolated CLI tests.

    Returns:
        Zero for ready evidence, one for an invalid or failed observation.
    """
    args = _parser().parse_args(argv)
    try:
        if (
            not args.alert_hook.is_absolute()
            or not args.alert_hook.is_file()
            or not os.access(args.alert_hook, os.X_OK)
        ):
            raise FreshnessMonitorError("alert_unavailable")
        config = FreshnessMonitorConfig(
            endpoint=args.endpoint,
            token_file=args.token_file,
            instrument_public_ids=tuple(args.instrument_public_id),
            fx_exchanges=tuple(args.fx_exchange),
            expected_scope_digest=args.scope_digest,
        )
        asyncio.run(check_market_snapshot_freshness(config))
    except FreshnessMonitorError as error:
        print(f"REFUSE market-snapshot-freshness: {error}", file=sys.stderr)
        _alert(args.alert_hook, str(error))
        return 1
    print("OK market-snapshot-freshness: ready")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
