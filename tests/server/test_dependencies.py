"""Tests for the shared ``server.dependencies`` helpers.

The shared module deduplicates the ``get_repository_dependency``
callable that every new Phase 0d route file uses. These tests pin
its contract so a future refactor of the caching shape is caught
immediately.
"""

from unittest.mock import MagicMock
from unittest.mock import patch

from snapper.data.repository import Repository
from snapper.server.dependencies import get_repository_dependency


class TestGetRepositoryDependency:
    """Behaviour of ``get_repository_dependency``."""

    def test_returns_repository_for_configured_db_url(self) -> None:
        """The helper consults ``get_settings`` and ``get_repository``.

        Given: A patched ``get_settings`` returning a sentinel DB URL
            and a patched ``get_repository`` returning a stub,
        When: ``get_repository_dependency`` is called,
        Then: ``get_repository`` is invoked with the sentinel URL and
            the stub repository is returned verbatim.
        """
        stub_repo = MagicMock(spec=Repository)
        fake_settings = MagicMock(db_url="sqlite+aiosqlite:///:memory:")
        with (
            patch(
                "snapper.server.dependencies.get_settings",
                return_value=fake_settings,
            ),
            patch(
                "snapper.server.dependencies.get_repository",
                return_value=stub_repo,
            ) as mock_get_repo,
        ):
            result = get_repository_dependency()

        assert result is stub_repo
        mock_get_repo.assert_called_once_with("sqlite+aiosqlite:///:memory:")
