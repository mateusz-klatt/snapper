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
    ``./data`` bind mount never write to the same file: ``feed-engine``
    (the ``snapper-feed`` container) logs to ``data/snapper-feed.log``,
    ``strategies-engine`` (``snapper-strategies``) to
    ``data/snapper-strategies.log``, ``broker`` (``snapper-broker``) to
    ``data/snapper-broker.log``, the ``egress`` sidecar (running as
    ``root`` for ``CAP_NET_ADMIN``) to ``data/snapper-egress.log``, and
    every other command (the ``snapper-api`` container) to
    ``data/snapper.log`` at INFO level. Subprocesses inherit their
    container's file via the ``SNAPPER_LOG_FILE`` environment variable.
    JSON logging is disabled for human-readable console output.
"""

import os
import sys

from snapper.cli.app import app
from snapper.infrastructure.exchanges.kraken_sdk_patches import log_kraken_sdk_patches_status
from snapper.utils.logging import DEFAULT_LOGFILE
from snapper.utils.logging import LOGFILE_ENV_VAR
from snapper.utils.logging import setup_logging

_COMMAND_LOGFILES: dict[str, str] = {
    "egress": "data/snapper-egress.log",
    "feed-engine": "data/snapper-feed.log",
    "strategies-engine": "data/snapper-strategies.log",
    "broker": "data/snapper-broker.log",
}


def _resolve_logfile(argv: list[str]) -> str:
    """Return the logfile path appropriate for the current CLI invocation.

    Sub-commands run in different containers that share the ``./data``
    bind mount, so they must not write to the same log file. Each
    container's command maps to a dedicated file via
    :data:`_COMMAND_LOGFILES`:

    - ``feed-engine`` (the ``snapper-feed`` container) ->
      ``data/snapper-feed.log``.
    - ``strategies-engine`` (the ``snapper-strategies`` container) ->
      ``data/snapper-strategies.log``.
    - ``broker`` (the ``snapper-broker`` container) ->
      ``data/snapper-broker.log``.
    - ``egress`` (the ``snapper-egress`` sidecar, running as ``root``
      for ``CAP_NET_ADMIN``) -> ``data/snapper-egress.log``.
    - every other command (the ``snapper-api`` container) ->
      :data:`DEFAULT_LOGFILE`.

    Args:
        argv: ``sys.argv``-shaped list. The first positional argument
            (``argv[1]``, if present) is the sub-command name.

    Returns:
        Absolute-style relative path to the logfile.
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
        - Log file: per container via :func:`_resolve_logfile`
          (``data/snapper-feed.log`` for ``feed-engine``,
          ``data/snapper-strategies.log`` for ``strategies-engine``,
          ``data/snapper-broker.log`` for ``broker``,
          ``data/snapper-egress.log`` for ``egress``,
          :data:`DEFAULT_LOGFILE` otherwise).

    The resolved logfile is exported in :data:`LOGFILE_ENV_VAR` before
    the CLI runs so subprocesses spawned by this container (via
    ``process_runner``) inherit it and write to the SAME per-container
    file instead of the shared default. See
    :func:`snapper.utils.logging.resolve_subprocess_logfile`.

    Returns:
        Exit code (always 0 on success).
    """
    logfile = _resolve_logfile(sys.argv)
    os.environ[LOGFILE_ENV_VAR] = logfile
    setup_logging(level="INFO", json_logs=False, logfile=logfile)
    log_kraken_sdk_patches_status()
    app()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
