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
    JSON logging is disabled for human-readable console output.
"""

from snapper.cli.app import app
from snapper.utils.logging import setup_logging


def main() -> int:
    """Initialize logging and launch the Snapper CLI application.

    Sets up the logging subsystem with sensible defaults (INFO level,
    human-readable format, file output) and then invokes the Typer
    CLI application to process command-line arguments.

    The function configures:
        - Log level: INFO
        - JSON format: disabled (human-readable)
        - Log file: data/snapper.log

    Returns:
        Exit code (always 0 on success).
    """
    setup_logging(level="INFO", json_logs=False, logfile="data/snapper.log")
    app()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
