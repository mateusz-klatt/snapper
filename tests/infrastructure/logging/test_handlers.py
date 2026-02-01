"""Unit tests for logging handlers."""

import logging
from unittest.mock import MagicMock
from unittest.mock import patch

from snapper.infrastructure.logging.handlers import InterceptStdLogHandler


class TestInterceptStdLogHandler:
    """Tests for InterceptStdLogHandler logging integration."""

    def test_emit_with_valid_record(self) -> None:
        """Verify handler emits valid log records with correct module binding.

        Given a valid logging.LogRecord at INFO level,
        When handler.emit() is called,
        Then logger.bind is called with record.module and opt(depth=6, exception=None).
        """
        handler = InterceptStdLogHandler()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        with patch("snapper.infrastructure.logging.handlers.logger") as mock_logger:
            mock_bound = MagicMock()
            mock_logger.bind.return_value = mock_bound
            handler.emit(record)
            mock_logger.bind.assert_called_once_with(module=record.module)
            mock_bound.opt.assert_called_once_with(depth=6, exception=None)

    def test_emit_with_exception_info(self) -> None:
        """Verify handler passes exception info to Loguru.

        Given a LogRecord with exc_info from a caught ValueError,
        When handler.emit() is called,
        Then opt() receives the exception tuple.
        """
        handler = InterceptStdLogHandler()
        try:
            raise ValueError("Test exception")
        except ValueError:
            import sys

            exc_info = sys.exc_info()
        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="test.py",
            lineno=1,
            msg="Error occurred",
            args=(),
            exc_info=exc_info,
        )
        with patch("snapper.infrastructure.logging.handlers.logger") as mock_logger:
            mock_bound = MagicMock()
            mock_logger.bind.return_value = mock_bound
            handler.emit(record)
            mock_logger.bind.assert_called_once_with(module=record.module)
            mock_bound.opt.assert_called_once_with(depth=6, exception=exc_info)

    def test_emit_handles_missing_levelname(self) -> None:
        """Verify handler defaults to INFO when levelname raises exception.

        Given a broken record where levelname property raises,
        When handler.emit() is called,
        Then it falls back to INFO level and emits the message.
        """
        handler = InterceptStdLogHandler()

        class BrokenRecord:
            def __init__(self) -> None:
                self.module = "test_module"
                self.exc_info = None

            @property
            def levelname(self) -> str:
                raise RuntimeError("Broken levelname")

            def getMessage(self) -> str:
                return "Test message"

        record = BrokenRecord()
        with patch("snapper.infrastructure.logging.handlers.logger") as mock_logger:
            mock_bound = MagicMock()
            mock_logger.bind.return_value = mock_bound
            mock_opt = MagicMock()
            mock_bound.opt.return_value = mock_opt
            handler.emit(record)
            mock_opt.log.assert_called_once()
            args = mock_opt.log.call_args[0]
            assert args[0] == "INFO"
            assert args[1] == "Test message"
