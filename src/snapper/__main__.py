"""Snapper package entry point for command-line execution.

This module provides the main entry point when Snapper is invoked as a package
via ``python -m snapper``. It initializes the logging subsystem and launches
the Typer CLI application.

Example:
    Run Snapper from the command line::

        $ python -m snapper --help
        $ python -m snapper server start
        $ python -m snapper broker kraken

Note:
    Log output is written per container so services sharing the
    ``./data`` bind mount never write to the same file. Each service owns
    ``data/log/<service>/<service>.log``; the API/default service owns
    ``data/log/snapper/snapper.log``. A validated explicit
    ``SNAPPER_LOG_FILE`` overrides the command mapping, and subprocesses
    inherit the selected path. JSON logging is disabled for human-readable
    console output.
"""

import os
import sys

from snapper.cli.app import app
from snapper.infrastructure.exchanges.kraken_sdk_patches import log_kraken_sdk_patches_status
from snapper.utils.logging import DEFAULT_LOGFILE
from snapper.utils.logging import LOGFILE_ENV_VAR
from snapper.utils.logging import resolve_logfile_from_environment
from snapper.utils.logging import setup_logging

_COMMAND_LOGFILES: dict[str, str] = {
    "egress": "data/log/snapper-egress/snapper-egress.log",
    "feed-engine": "data/log/snapper-feed/snapper-feed.log",
    "strategies-engine": "data/log/snapper-strategies/snapper-strategies.log",
    "broker": "data/log/snapper-broker/snapper-broker.log",
    "notify": "data/log/snapper-notify/snapper-notify.log",
}


def _machine_stdout_requested(argv: list[str]) -> bool:
    """Return whether this invocation reserves stdout for one data document."""
    return len(argv) > 1 and argv[1] == "audit-candles" and "--json" in argv[2:]


def _resolve_logfile(argv: list[str]) -> str:
    """Return the logfile path appropriate for the current CLI invocation.

    Sub-commands run in different containers that share the ``./data`` bind
    mount, so each command maps to a dedicated per-service directory via
    :data:`_COMMAND_LOGFILES`:

    - ``feed-engine`` (the ``snapper-feed`` container) ->
      ``data/log/snapper-feed/snapper-feed.log``.
    - ``strategies-engine`` (the ``snapper-strategies`` container) ->
      ``data/log/snapper-strategies/snapper-strategies.log``.
    - ``broker`` (the ``snapper-broker`` container) ->
      ``data/log/snapper-broker/snapper-broker.log``.
    - ``egress`` (the ``snapper-egress`` sidecar, running as ``root``
      for ``CAP_NET_ADMIN``) ->
      ``data/log/snapper-egress/snapper-egress.log``.
    - ``notify`` (the ``snapper-notify`` sidecar) ->
      ``data/log/snapper-notify/snapper-notify.log``.
    - every other command (the ``snapper-api`` container) ->
      :data:`DEFAULT_LOGFILE`.

    Args:
        argv: ``sys.argv``-shaped list. The first positional argument
            (``argv[1]``, if present) is the sub-command name.

    Returns:
        Command-specific default logfile path.
    """
    if len(argv) > 1:
        return _COMMAND_LOGFILES.get(argv[1], DEFAULT_LOGFILE)
    return DEFAULT_LOGFILE


def main() -> int:
    """Initialize logging and launch the Snapper CLI application.

    Sets up the logging subsystem with sensible defaults (INFO level,
    human-readable format, file output) and then invokes the Typer
    CLI application to process command-line arguments.

    The function configures:
        - Log level: INFO
        - JSON format: disabled (human-readable)
        - Log file: validated :data:`LOGFILE_ENV_VAR`, or the per-service
          command path from :func:`_resolve_logfile` when the variable is
          absent or invalid.

    The resolved logfile is exported in :data:`LOGFILE_ENV_VAR` before
    the CLI runs so subprocesses spawned by this container (via
    ``process_runner``) inherit it and write to the SAME per-container
    file instead of the shared default. See
    :func:`snapper.utils.logging.resolve_subprocess_logfile`.

    Returns:
        Exit code (always 0 on success).
    """
    logfile = resolve_logfile_from_environment(_resolve_logfile(sys.argv))
    os.environ[LOGFILE_ENV_VAR] = logfile
    setup_logging(
        level="INFO",
        json_logs=False,
        logfile=logfile,
        console_to_stderr=_machine_stdout_requested(sys.argv),
    )
    log_kraken_sdk_patches_status()
    app()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
