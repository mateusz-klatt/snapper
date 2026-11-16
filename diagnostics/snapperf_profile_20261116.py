"""Bound one reviewed read-only profile worker before importing any application code."""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from contextlib import suppress
from pathlib import Path


def _stop_worker(process: subprocess.Popen[bytes]) -> None:
    """Terminate and reap the one owned worker group with a bounded kill escalation."""
    with suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=3)
    except subprocess.TimeoutExpired:
        with suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=7)


def main() -> int:
    """Start the deadline before application imports, database binding or profile work."""
    os.umask(0o077)
    started = time.monotonic()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--scope-hash", default="")
    parser.add_argument("--venues", default="")
    parser.add_argument("--list-scopes", action="store_true")
    args, unsupported = parser.parse_known_args()
    if unsupported:
        raise ValueError("Unsupported command-line options")
    args.output.mkdir(mode=0o700, parents=True, exist_ok=False)
    worker = Path(__file__).with_name("snapperf_profile_worker_20261116.py").resolve()
    environment = dict(os.environ, SNAPPER_PROFILE_PARENT_PID=str(os.getpid()))
    with (args.output / "worker-output.log").open("xb") as stream:
        process = subprocess.Popen(
            [sys.executable, str(worker), *sys.argv[1:], "--_worker"],
            env=environment,
            stdout=stream,
            stderr=stream,
            start_new_session=True,
        )
        try:
            code = process.wait(timeout=max(0.001, 580 - (time.monotonic() - started)))
        except subprocess.TimeoutExpired:
            _stop_worker(process)
            code = 124
            (args.output / "parent-timeout.json").write_text(
                json.dumps(dict(status="hard_timeout", worker_reaped=True)) + "\n"
            )
        finally:
            if process.poll() is None:
                _stop_worker(process)
    print(
        json.dumps(dict(status="finished" if code == 0 else "failed_or_timed_out", exit_code=code))
    )
    return code


if __name__ == "__main__":
    sys.excepthook = lambda error_type, error, traceback: print(
        json.dumps(dict(status="failed", error_type=error_type.__name__)), file=sys.stderr
    )
    raise SystemExit(main())
