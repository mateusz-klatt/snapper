"""FastAPI dependency tests for :func:`get_caps_enforcer_dependency`.

Covers the narrow surface added in   that
``tests/application/trade/test_caps_enforcer.py`` cannot reach
(FastAPI-layer singleton cache + reset hook + RuntimeError when
the repo is not SQLAlchemyRepository).
"""

from typing import Any
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.data.repository import SQLAlchemyRepository
from snapper.server.dependencies import get_caps_enforcer_dependency
from snapper.server.dependencies import reset_caps_enforcer_singleton


def test_non_sqlalchemy_repository_raises_runtime_error() -> None:
    """Non-SQLAlchemy repository at bootstrap raises ``RuntimeError``.

    Given: a configured repository that is NOT a
        :class:`SQLAlchemyRepository` (e.g., a test fixture that
        returned a MagicMock from ``get_repository``),
    When: :func:`get_caps_enforcer_dependency` is invoked,
    Then: :class:`RuntimeError` is raised with a message naming
        the actual type — guards against silent fall-through to
        a ``.session()`` attribute error deep inside the cap
        evaluator.
    """
    reset_caps_enforcer_singleton()
    fake_repo = MagicMock()
    with (
        patch("snapper.server.dependencies.get_repository", return_value=fake_repo),
        pytest.raises(RuntimeError, match="SQLAlchemyRepository"),
    ):
        get_caps_enforcer_dependency()


def test_reset_singleton_clears_cache() -> None:
    """``reset_caps_enforcer_singleton`` evicts the cached enforcer.

    Given: a :func:`get_caps_enforcer_dependency` call that cached
        a real enforcer,
    When: :func:`reset_caps_enforcer_singleton` runs,
    Then: the cache entry is popped so a subsequent call
        reconstructs a fresh enforcer (verified via id() inequality).
    """
    reset_caps_enforcer_singleton()
    fake_repo = MagicMock(spec=SQLAlchemyRepository)

    def _fake_get_repository(_db_url: Any) -> Any:
        return fake_repo

    with patch("snapper.server.dependencies.get_repository", _fake_get_repository):
        first = get_caps_enforcer_dependency()
        cached = get_caps_enforcer_dependency()
        assert first is cached
        reset_caps_enforcer_singleton()
        fresh = get_caps_enforcer_dependency()
        assert fresh is not first
    reset_caps_enforcer_singleton()
