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


def _group_exists(group: int) -> bool:
    """Observe whether the owned process group still has any members."""
    try:
        os.killpg(group, 0)
    except ProcessLookupError:
        return False
    return True


def _stop_worker(process: subprocess.Popen[bytes], deadline: float) -> dict[str, bool]:
    """Clean the owned group once, including descendants after its leader exits."""
    group = process.pid
    with suppress(ProcessLookupError):
        os.killpg(group, signal.SIGTERM)
    term_deadline = min(deadline, time.monotonic() + 3)
    while _group_exists(group) and time.monotonic() < term_deadline:
        process.poll()
        time.sleep(min(0.02, max(0, term_deadline - time.monotonic())))
    with suppress(ProcessLookupError):
        os.killpg(group, signal.SIGKILL)
    remaining = max(0, deadline - time.monotonic())
    with suppress(subprocess.TimeoutExpired):
        process.wait(timeout=remaining)
    while _group_exists(group) and time.monotonic() < deadline:
        with suppress(ChildProcessError):
            os.waitpid(-group, os.WNOHANG)
        time.sleep(min(0.02, max(0, deadline - time.monotonic())))
    return dict(leader_reaped=process.poll() is not None, group_empty=not _group_exists(group))


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
    bootstrap = Path(__file__).with_name("snapperf_profile_bootstrap_20261116.py").resolve()
    environment = dict(
        os.environ,
        SNAPPER_PROFILE_PARENT_PID=str(os.getpid()),
        SNAPPER_PROFILE_OUTPUT=str(args.output.resolve()),
    )
    with (args.output / "worker-output.log").open("xb") as stream:
        process = subprocess.Popen(
            [sys.executable, str(bootstrap), str(worker), *sys.argv[1:], "--_worker"],
            env=environment,
            stdout=stream,
            stderr=stream,
            start_new_session=True,
        )
        try:
            code = process.wait(timeout=max(0.001, 580 - (time.monotonic() - started)))
        except subprocess.TimeoutExpired:
            code = 124
        finally:
            cleanup = _stop_worker(process, min(started + 590, time.monotonic() + 10))
            (args.output / "parent-cleanup.json").write_text(json.dumps(cleanup) + "\n")
        if not all(cleanup.values()):
            code = 125
    print(
        json.dumps(dict(status="finished" if code == 0 else "failed_or_timed_out", exit_code=code))
    )
    return code


if __name__ == "__main__":
    sys.excepthook = lambda error_type, error, traceback: print(
        json.dumps(dict(status="failed", error_type=error_type.__name__)), file=sys.stderr
    )
    raise SystemExit(main())
