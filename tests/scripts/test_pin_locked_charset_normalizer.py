"""Tests for the charset-normalizer lock-pinning setup helper."""

from pathlib import Path
from unittest.mock import MagicMock
from unittest.mock import patch

from scripts.pin_locked_charset_normalizer import _default_root
from scripts.pin_locked_charset_normalizer import locked_charset_version
from scripts.pin_locked_charset_normalizer import main
from scripts.pin_locked_charset_normalizer import pin_locked_charset_normalizer

_LOCK = '[[package]]\nname = "charset-normalizer"\nversion = "3.4.7"\ndescription = "x"\n'


def test_default_root_is_repository_root() -> None:
    """Given: the script under ``<root>/scripts``.

    When: the default root is resolved,
    Then: it is the grandparent directory that contains the script.
    """
    assert (_default_root() / "scripts" / "pin_locked_charset_normalizer.py").exists()


def test_locked_charset_version_found() -> None:
    """Given: a lock pinning charset-normalizer.

    When: the version is read,
    Then: the exact pinned version is returned.
    """
    assert locked_charset_version(_LOCK) == "3.4.7"


def test_locked_charset_version_absent() -> None:
    """Given: a lock without charset-normalizer.

    When: the version is read,
    Then: None is returned.
    """
    assert locked_charset_version('name = "other"\nversion = "1.0"') is None


def test_pin_is_noop_without_a_lock(tmp_path: Path) -> None:
    """Given: no poetry.lock in the root.

    When: the pin runs,
    Then: it is a no-op returning 0.
    """
    assert pin_locked_charset_normalizer(tmp_path) == 0


def test_pin_is_noop_when_package_absent(tmp_path: Path) -> None:
    """Given: a lock that does not pin charset-normalizer.

    When: the pin runs,
    Then: it is a no-op returning 0.
    """
    (tmp_path / "poetry.lock").write_text('name = "other"\nversion = "1.0"', encoding="utf-8")
    assert pin_locked_charset_normalizer(tmp_path) == 0


def test_pin_installs_the_locked_version(tmp_path: Path) -> None:
    """Given: a lock pinning charset-normalizer.

    When: the pin runs,
    Then: pip installs exactly the locked version and its return code propagates.
    """
    (tmp_path / "poetry.lock").write_text(_LOCK, encoding="utf-8")
    with patch("scripts.pin_locked_charset_normalizer.subprocess.run") as run:
        run.return_value = MagicMock(returncode=7)
        result = pin_locked_charset_normalizer(tmp_path)
    assert result == 7
    command = run.call_args[0][0]
    assert command[1:] == ["-m", "pip", "install", "charset-normalizer==3.4.7"]


def test_main_pins_from_the_default_root() -> None:
    """Given: the script invoked as a program.

    When: main runs,
    Then: it pins from the resolved repository root and returns the pin's code.
    """
    with patch(
        "scripts.pin_locked_charset_normalizer.pin_locked_charset_normalizer",
        return_value=0,
    ) as pin:
        assert main() == 0
    pin.assert_called_once()
