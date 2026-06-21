"""Tests for runtime dependency declarations required by production images."""

import tomllib
from pathlib import Path
from typing import cast


def test_pysocks_declared_for_ccxt_socks_proxy_support() -> None:
    """Spec — PySocks is a locked runtime dependency for CCXT REST SOCKS.

    Given: production installs the Poetry main dependency group,
    When: dependency declarations are inspected,
    Then: PySocks is declared and locked as a main dependency while the
        existing python-socks WebSocket dependency remains present.
    """
    pyproject = tomllib.loads(Path("pyproject.toml").read_text())
    tool = cast(dict[str, object], pyproject["tool"])
    poetry = cast(dict[str, object], tool["poetry"])
    dependencies = cast(dict[str, object], poetry["dependencies"])
    lock_text = Path("poetry.lock").read_text()

    assert dependencies["PySocks"] == "^1.7.1"
    assert "python-socks" in dependencies
    assert 'name = "pysocks"' in lock_text
    assert 'groups = ["main"]' in lock_text
    assert "PySocks-1.7.1-py3-none-any.whl" in lock_text
