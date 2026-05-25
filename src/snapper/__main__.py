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
    Log output is written to ``data/snapper.log`` by default with INFO level.
    The ``egress`` sub-command writes to ``data/snapper-egress.log`` instead
    so the sidecar (running as ``root`` for ``CAP_NET_ADMIN``) does not
    take ownership of the API container's log file (which runs as the
    unprivileged ``snapper`` user and would otherwise fail to write).
    JSON logging is disabled for human-readable console output.
"""

import sys

from snapper.cli.app import app
from snapper.infrastructure.exchanges.kraken_sdk_patches import log_kraken_sdk_patches_status
from snapper.utils.logging import setup_logging


def _resolve_logfile(argv: list[str]) -> str:
    """Return the logfile path appropriate for the current CLI invocation.

    Sub-commands run in different containers with different uids, so they
    must not share a log file. Today only the ``egress`` sidecar needs a
    dedicated path (it runs as ``root``); every other command writes to
    the API container's shared ``data/snapper.log``.

    Args:
        argv: ``sys.argv``-shaped list. The first positional argument
            (``argv[1]``, if present) is the sub-command name.

    Returns:
        Absolute-style relative path to the logfile.
    """
    if len(argv) > 1 and argv[1] == "egress":
        return "data/snapper-egress.log"
    return "data/snapper.log"


def main() -> int:
    """Initialize logging and launch the Snapper CLI application.

    Sets up the logging subsystem with sensible defaults (INFO level,
    human-readable format, file output) and then invokes the Typer
    CLI application to process command-line arguments.

    The function configures:
        - Log level: INFO
        - JSON format: disabled (human-readable)
        - Log file: ``data/snapper-egress.log`` when invoked as
          ``snapper egress``, ``data/snapper.log`` otherwise.

    Returns:
        Exit code (always 0 on success).
    """
    setup_logging(level="INFO", json_logs=False, logfile=_resolve_logfile(sys.argv))
    log_kraken_sdk_patches_status()
    app()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
