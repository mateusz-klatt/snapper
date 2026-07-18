"""Pin charset-normalizer to the poetry-locked version before poetry install.

``make setup`` runs this into the fresh virtualenv BEFORE upgrading poetry so
poetry's own dependency tree cannot pull a newer charset-normalizer and then
downgrade it during ``poetry install``. On Windows that downgrade fails with
WinError 5 because it deletes charset-normalizer's loaded mypyc ``.pyd``, which
the interpreter holds open; pre-pinning the locked version skips the reinstall.

The step is a cross-platform no-op when the lock file or the package is absent,
and it is a plain Python script (not shell) so it runs identically under the
Windows ``cmd.exe`` and POSIX ``sh`` shells ``make`` uses.
"""

import re
import subprocess
import sys
from pathlib import Path
from typing import Final

LOCKFILE_NAME: Final = "poetry.lock"
PACKAGE_NAME: Final = "charset-normalizer"
_VERSION_RE: Final[re.Pattern[str]] = re.compile(
    r'name = "charset-normalizer"\nversion = "(?P<version>[^"]+)"'
)


def _default_root() -> Path:
    """Return the repository root (this script lives in ``<root>/scripts``)."""
    return Path(__file__).resolve().parent.parent


def locked_charset_version(lock_text: str) -> str | None:
    """Return the charset-normalizer version pinned in a poetry.lock, or None.

    Args:
        lock_text: The full text of a ``poetry.lock`` file.

    Returns:
        The pinned version string, or ``None`` when the package is not locked.
    """
    match = _VERSION_RE.search(lock_text)
    return match.group("version") if match else None


def pin_locked_charset_normalizer(root: Path) -> int:
    """Install charset-normalizer at the locked version; a no-op when absent.

    Args:
        root: The repository root holding ``poetry.lock``.

    Returns:
        The ``pip install`` return code, or ``0`` when there is nothing to pin.
    """
    lock_path = root / LOCKFILE_NAME
    if not lock_path.exists():
        return 0
    version = locked_charset_version(lock_path.read_text(encoding="utf-8"))
    if version is None:
        return 0
    completed = subprocess.run(
        [sys.executable, "-m", "pip", "install", f"{PACKAGE_NAME}=={version}"],
        check=False,
    )
    return completed.returncode


def main() -> int:
    """Pin the locked charset-normalizer version into the active interpreter.

    Returns:
        The pin's return code (``0`` when there is nothing to pin).
    """
    return pin_locked_charset_normalizer(_default_root())


if __name__ == "__main__":
    raise SystemExit(main())
