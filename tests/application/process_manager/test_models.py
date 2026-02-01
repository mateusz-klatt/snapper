"""Tests for process manager models."""

from datetime import UTC
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from snapper.application.process_manager.models import ProcessInstanceInfo
from snapper.application.process_manager.models import RegisterableProcess


class TestRegisterableProcess:
    """Test suite for RegisterableProcess abstract base class."""

    def test_get_default_kwargs_returns_empty_dict(self) -> None:
        """Verify get_default_kwargs returns empty dict as base implementation.

        Given: A RegisterableProcess class,
        When: get_default_kwargs is called with any settings object,
        Then: An empty dictionary is returned as the default behavior.
        """
        mock_settings = MagicMock()
        result = RegisterableProcess.get_default_kwargs(mock_settings)
        assert result == {}

    @pytest.mark.asyncio
    async def test_stop_default_implementation(self) -> None:
        """Verify stop method has no-op default implementation.

        Given: A minimal RegisterableProcess subclass instance,
        When: stop is called on the instance,
        Then: The method completes without error as a no-op.
        """

        class MinimalProcess(RegisterableProcess):
            async def start(self) -> None:
                """No-op start for MinimalProcess test stub."""
                pass

        process = MinimalProcess()
        await process.stop()

    def test_get_status_default_implementation(self) -> None:
        """Verify get_status returns empty dict as base implementation.

        Given: A minimal RegisterableProcess subclass instance,
        When: get_status is called on the instance,
        Then: An empty dictionary is returned as the default status.
        """

        class MinimalProcess(RegisterableProcess):
            async def start(self) -> None:
                """No-op start for MinimalProcess test stub."""
                pass

        process = MinimalProcess()
        result = process.get_status()
        assert result == {}


class TestProcessInstanceInfo:
    """Test suite for ProcessInstanceInfo data class."""

    @pytest.mark.asyncio
    async def test_start_does_nothing(self) -> None:
        """Verify start method is a no-op for already running process info.

        Given: A ProcessInstanceInfo representing an already started process,
        When: start is called on the instance,
        Then: The method completes without performing any action.
        """
        proc_info = ProcessInstanceInfo(
            name="test",
            pid=123,
            started_at=datetime.now(UTC),
            config={},
            process=MagicMock(),
        )
        await proc_info.start()

    @pytest.mark.asyncio
    async def test_stop_when_already_stopped_returns_early(self) -> None:
        """Verify stop returns early when process is already stopped.

        Given: A ProcessInstanceInfo with _stopped flag set to True,
        When: stop is called on the instance,
        Then: Spawner terminate and cleanup are not called.
        """
        spawner_mock = MagicMock()
        proc_info = ProcessInstanceInfo(
            name="test",
            pid=123,
            started_at=datetime.now(UTC),
            config={},
            process=MagicMock(),
            spawner=spawner_mock,
        )
        proc_info._stopped = True
        await proc_info.stop()
        spawner_mock.terminate.assert_not_called()
        spawner_mock.cleanup.assert_not_called()

    @pytest.mark.asyncio
    async def test_stop_without_spawner_does_not_call_terminate(self) -> None:
        """Verify stop sets stopped flag even without spawner.

        Given: A ProcessInstanceInfo with spawner set to None,
        When: stop is called on the instance,
        Then: The _stopped flag is set to True without errors.
        """
        proc_info = ProcessInstanceInfo(
            name="test",
            pid=123,
            started_at=datetime.now(UTC),
            config={},
            process=MagicMock(),
            spawner=None,
        )
        await proc_info.stop()
        assert proc_info._stopped is True

    def test_get_status_returns_process_info(self) -> None:
        """Verify get_status returns dict with pid, started_at and exit_code.

        Given: A ProcessInstanceInfo with pid, started_at and exit_code set,
        When: get_status is called on the instance,
        Then: A dictionary with pid, ISO formatted started_at and exit_code is returned.
        """
        started_at = datetime.now(UTC)
        proc_info = ProcessInstanceInfo(
            name="test",
            pid=123,
            started_at=started_at,
            config={},
            process=MagicMock(),
            exit_code=0,
        )
        status = proc_info.get_status()
        assert status == {
            "pid": 123,
            "started_at": started_at.isoformat(),
            "exit_code": 0,
        }
