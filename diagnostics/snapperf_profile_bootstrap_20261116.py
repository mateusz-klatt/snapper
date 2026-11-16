"""Sanitize startup diagnostics before loading any third-party or application code."""

import importlib
import json
import logging
import os
import runpy
import sys
import warnings
from pathlib import Path
from typing import Protocol
from typing import cast


class _LogSinkControl(Protocol):
    """The single logging control needed before application imports."""

    def remove(self) -> None:
        """Remove default third-party sinks before they can print diagnostic values."""


def _safe_exception(
    error_type: type[BaseException], error: BaseException, traceback: object
) -> None:
    """Report only a class name, never traceback frames or exception text."""
    print(
        json.dumps(dict(status="startup_failed", error_type=error_type.__name__)), file=sys.stderr
    )


def main() -> int:
    """Apply logging policy before imports and preserve a sanitized startup receipt."""
    os.umask(0o077)
    sys.excepthook = _safe_exception
    logging.disable(logging.CRITICAL)
    warnings.simplefilter("error")
    try:
        logger = cast(_LogSinkControl, importlib.import_module("loguru").logger)
        logger.remove()
        worker = sys.argv[1]
        sys.argv = sys.argv[1:]
        runpy.run_path(worker, run_name="__main__")
    except SystemExit as error:
        return error.code if isinstance(error.code, int) else 1
    except BaseException as error:
        payload = dict(
            status="startup_failed", error_type=type(error).__name__, sql_statements=None
        )
        output = Path(os.environ["SNAPPER_PROFILE_OUTPUT"])
        receipt = output / "status.json"
        if receipt.exists():
            receipt = output / "bootstrap-error.json"
        receipt.write_text(json.dumps(payload) + "\n")
        _safe_exception(type(error), error, None)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
