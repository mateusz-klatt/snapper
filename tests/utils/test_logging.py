"""Unit tests for Snapper logging utilities."""

import asyncio
import json
import logging as stdlib_logging
from collections.abc import Callable
from collections.abc import Mapping
from datetime import UTC
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from typing import cast

import pytest
from loguru import logger

from snapper.infrastructure.logging.handlers import InterceptStdLogHandler
from snapper.utils import logging
from snapper.utils import logging as log_utils


def test_filter_cancelled_errors() -> None:
    """Test _filter_cancelled_errors filters CancelledError.

    Given: log records with and without CancelledError,
    When: applying _filter_cancelled_errors,
    Then: CancelledError records return False, others return True.
    """
    filter_fn = cast(
        Callable[[dict[str, object]], bool],
        logging.__dict__["_filter_cancelled_errors"],
    )
    record_with_cancelled: dict[str, object] = {
        "exception": type("ExcInfo", (), {"type": asyncio.CancelledError}),
    }
    assert not filter_fn(record_with_cancelled)
    record_without_cancelled: dict[str, object] = {"exception": None}
    assert filter_fn(record_without_cancelled)


def test_intercept_handler_emits(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test InterceptStdLogHandler emits to Loguru.

    Given: stdlib log record with INFO level,
    When: handler.emit is called,
    Then: Loguru receives message with correct level and module.
    """

    class DummyLogger:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str]] = []
            self.depth: int | None = None
            self.exception: Any = None
            self.module: str | None = None

        def bind(self, **kwargs: Any) -> DummyLogger:
            self.module = kwargs.get("module")
            return self

        def opt(self, *, depth: int, exception: Any) -> DummyLogger:
            self.depth = depth
            self.exception = exception
            return self

        def log(self, level: str, message: str) -> None:
            self.calls.append((level, message))

    dummy_logger = DummyLogger()
    monkeypatch.setattr("snapper.infrastructure.logging.handlers.logger", dummy_logger)
    handler = InterceptStdLogHandler()
    record = stdlib_logging.makeLogRecord({"msg": "hello", "levelname": "INFO", "module": "tests"})
    handler.emit(record)
    assert dummy_logger.calls == [("INFO", "hello")]
    assert dummy_logger.module == "tests"
    assert dummy_logger.depth == 6


def test_setup_logging_json_and_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Test setup_logging with JSON mode and file output.

    Given: mocked logger and tmp_path for log file,
    When: calling setup_logging with json_logs=True and logfile,
    Then: sinks configured for JSON format and file output.
    """

    class DummyLogger:
        def __init__(self) -> None:
            self.remove_called = False
            self.add_calls: list[dict[str, Any]] = []
            self.extra: dict[str, Any] = {}

        def remove(self) -> None:
            self.remove_called = True

        def configure(self, **kwargs: Any) -> None:
            if "extra" in kwargs:
                self.extra.update(kwargs["extra"])

        def add(self, sink: Any, *args: Any, **kwargs: Any) -> int:
            call = {"sink": sink}
            call.update(kwargs)
            self.add_calls.append(call)
            return 1

    dummy_logger = DummyLogger()
    monkeypatch.setattr(logging, "logger", dummy_logger)
    basic_config_called: dict[str, Any] = {}

    def fake_basic_config(**kwargs: Any) -> None:
        basic_config_called.update(kwargs)

    monkeypatch.setattr(stdlib_logging, "basicConfig", fake_basic_config)
    made_directories: list[str] = []

    def fake_makedirs(path: str, *, exist_ok: bool) -> None:
        made_directories.append(path)
        assert exist_ok

    monkeypatch.setattr("snapper.utils.logging.os.makedirs", fake_makedirs)
    log_file: Path = tmp_path / "logs" / "snapper.log"
    logging.setup_logging(level="DEBUG", json_logs=True, logfile=str(log_file))
    handler_constructor = basic_config_called["handlers"][0]
    assert isinstance(handler_constructor, InterceptStdLogHandler)
    assert dummy_logger.remove_called
    assert dummy_logger.extra == {"context": "main"}
    assert len(dummy_logger.add_calls) == 2
    first_call = dummy_logger.add_calls[0]
    assert callable(first_call["sink"])
    assert first_call["level"] == "DEBUG"
    filter_callable = first_call["filter"]
    assert filter_callable is logging.__dict__["_filter_cancelled_errors"]
    assert callable(first_call["format"])
    format_callable = cast(Callable[[Mapping[str, Any]], str], first_call["format"])
    sample_record: dict[str, Any] = {
        "time": datetime(2024, 1, 1, tzinfo=UTC),
        "level": SimpleNamespace(name="INFO"),
        "message": "sample-message",
        "module": "tests",
        "function": "format_check",
        "line": 1,
    }
    logging.set_log_context("json_sample")
    serialized = format_callable(sample_record)
    parsed_serialized = json.loads(serialized)
    assert parsed_serialized["message"] == "sample-message"
    assert parsed_serialized["level"] == "INFO"
    assert parsed_serialized["context"] == "json_sample"
    logging.set_log_context("main")
    second_call = dummy_logger.add_calls[1]
    assert callable(second_call["sink"])
    assert "rotation" not in second_call or second_call.get("rotation") is None
    assert made_directories == [str(log_file.parent)]


def test_setup_logging_plain_text(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test setup_logging with plain text mode.

    Given: mocked logger,
    When: calling setup_logging with json_logs=False,
    Then: single sink configured with message format.
    """

    class DummyLogger:
        def __init__(self) -> None:
            self.remove_called = False
            self.add_calls: list[dict[str, Any]] = []
            self.extra: dict[str, Any] = {}

        def remove(self) -> None:
            self.remove_called = True

        def configure(self, **kwargs: Any) -> None:
            if "extra" in kwargs:
                self.extra.update(kwargs["extra"])

        def add(self, sink: Any, *args: Any, **kwargs: Any) -> int:
            call = {"sink": sink}
            call.update(kwargs)
            self.add_calls.append(call)
            return 1

    dummy_logger = DummyLogger()
    monkeypatch.setattr(logging, "logger", dummy_logger)

    def fake_basic_config(**_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(stdlib_logging, "basicConfig", fake_basic_config)
    logging.setup_logging(level="INFO", json_logs=False, logfile=None)
    assert dummy_logger.remove_called
    assert dummy_logger.extra == {"context": "main"}
    assert len(dummy_logger.add_calls) == 1
    first_call = dummy_logger.add_calls[0]
    assert callable(first_call["sink"])
    assert first_call["level"] == "INFO"
    assert first_call["format"] == "{message}" or callable(first_call["format"])


def test_format_message_appends_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test format_message appends exception traceback.

    Given: log record with RuntimeError exception,
    When: sink processes record,
    Then: output contains RuntimeError and message text.
    """

    class DummyLogger:
        def __init__(self) -> None:
            self.remove_called = False
            self.add_calls: list[dict[str, Any]] = []
            self.extra: dict[str, Any] = {}

        def remove(self) -> None:
            self.remove_called = True

        def configure(self, **kwargs: Any) -> None:
            if "extra" in kwargs:
                self.extra.update(kwargs["extra"])

        def add(self, sink: Any, *args: Any, **kwargs: Any) -> int:
            call = {"sink": sink}
            call.update(kwargs)
            self.add_calls.append(call)
            return 1

    class DummyStdout:
        def isatty(self) -> bool:
            return False

    dummy_logger = DummyLogger()
    monkeypatch.setattr(logging, "logger", dummy_logger)

    def fake_basic_config(**_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(stdlib_logging, "basicConfig", fake_basic_config)
    monkeypatch.setattr("sys.stdout", DummyStdout())
    captured: list[str] = []

    def fake_print(text: str, *, end: str = "") -> None:
        captured.append(text)

    monkeypatch.setattr("builtins.print", fake_print)
    logging.setup_logging(level="INFO", json_logs=False, logfile=None)
    assert dummy_logger.add_calls, "Expected at least one sink to be registered"
    sink_callable = dummy_logger.add_calls[0]["sink"]
    exception_info: SimpleNamespace | None = None
    try:
        raise RuntimeError("format-failure")
    except RuntimeError as exc:
        exception_info = SimpleNamespace(type=type(exc), value=exc, traceback=exc.__traceback__)
    assert exception_info is not None
    record: dict[str, Any] = {
        "time": datetime.now(UTC),
        "level": SimpleNamespace(name="ERROR"),
        "process": 5678,
        "module": "tests",
        "function": "test_format_message_appends_exception",
        "line": 123,
        "message": "something broke",
        "exception": exception_info,
    }
    sink_callable(SimpleNamespace(record=record))
    assert any("RuntimeError" in entry for entry in captured)
    assert any("format-failure" in entry for entry in captured)


def test_format_message_exception_without_type_value(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test format_message handles exception without type/value attrs.

    Given: log record with string exception (no type/value),
    When: sink processes record,
    Then: output contains message without crash.
    """

    class DummyLogger:
        def __init__(self) -> None:
            self.remove_called = False
            self.add_calls: list[dict[str, Any]] = []
            self.extra: dict[str, Any] = {}

        def remove(self) -> None:
            self.remove_called = True

        def configure(self, **kwargs: Any) -> None:
            if "extra" in kwargs:
                self.extra.update(kwargs["extra"])

        def add(self, sink: Any, *args: Any, **kwargs: Any) -> int:
            call = {"sink": sink}
            call.update(kwargs)
            self.add_calls.append(call)
            return 1

    class DummyStdout:
        def isatty(self) -> bool:
            return False

    dummy_logger = DummyLogger()
    monkeypatch.setattr(logging, "logger", dummy_logger)

    def fake_basic_config(**_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(stdlib_logging, "basicConfig", fake_basic_config)
    monkeypatch.setattr("sys.stdout", DummyStdout())
    captured: list[str] = []

    def fake_print(text: str, *, end: str = "") -> None:
        captured.append(text)

    monkeypatch.setattr("builtins.print", fake_print)
    logging.setup_logging(level="INFO", json_logs=False, logfile=None)
    assert dummy_logger.add_calls, "Expected at least one sink to be registered"
    sink_callable = dummy_logger.add_calls[0]["sink"]
    record: dict[str, Any] = {
        "time": datetime.now(UTC),
        "level": SimpleNamespace(name="ERROR"),
        "process": 5678,
        "module": "tests",
        "function": "test_format_message_exception_without_type_value",
        "line": 123,
        "message": "error without proper exception",
        "exception": "just a string, no type/value",
    }
    sink_callable(SimpleNamespace(record=record))
    assert any("error without proper exception" in entry for entry in captured)


def test_set_log_context(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test set_log_context and get_log_context.

    Given: logging module,
    When: setting context to various values,
    Then: get_log_context returns the set value.
    """
    logging.set_log_context("test_service")
    assert logging.get_log_context() == "test_service"
    logging.set_log_context("another_service")
    assert logging.get_log_context() == "another_service"


def test_setup_logging_color_sink_when_tty(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test colorized sink when terminal is TTY.

    Given: mocked stdout with isatty=True,
    When: calling setup_logging and logging messages,
    Then: captured output contains colorized log entries.
    """

    class DummyStdout:
        def isatty(self) -> bool:
            return True

    captured: list[str] = []

    def fake_print(*args: Any, **kwargs: Any) -> None:
        text = "".join(cast(str, arg) for arg in args)
        if text:
            captured.append(text)

    monkeypatch.setattr("sys.stdout", DummyStdout())
    monkeypatch.setattr("builtins.print", fake_print)
    logger.remove()
    logging.setup_logging(level="INFO", json_logs=False, logfile=None)
    logger.info("tty-color-test")
    logger.warning("tty-warning")
    assert captured, "Expected colorized sink to emit output when terminal supports TTY"
    assert any("tty-warning" in entry for entry in captured)
    assert any("WARNING" in entry for entry in captured)
    logger.remove()


def test_colorize_record_includes_exception_traceback() -> None:
    """Test _colorize_record includes exception traceback.

    Given: log record with ValueError exception,
    When: calling _colorize_record,
    Then: output contains exception type, message, and context.
    """
    colorize = cast(
        Callable[[Mapping[str, Any]], str],
        logging.__dict__["_colorize_record"],
    )
    logging.set_log_context("exception_test")
    exception_info: SimpleNamespace | None = None
    try:
        raise ValueError("boom")
    except ValueError as exc:
        exception_info = SimpleNamespace(
            type=type(exc),
            value=exc,
            traceback=exc.__traceback__,
        )
    assert exception_info is not None
    record: dict[str, Any] = {
        "time": datetime.now(UTC),
        "level": SimpleNamespace(name="ERROR"),
        "process": 1234,
        "module": "tests",
        "function": "test_colorize",
        "line": 42,
        "message": "boom",
        "exception": exception_info,
    }
    output = colorize(record)
    assert "ValueError" in output
    assert "boom" in output
    assert "exception_test" in output
    logging.set_log_context("main")


def test_colorize_record_exception_without_type_value_attrs() -> None:
    """Test _colorize_record with exception lacking type/value attrs.

    Given: log record with string exception (no attrs),
    When: calling _colorize_record,
    Then: output contains message and context without crash.
    """
    colorize = cast(
        Callable[[Mapping[str, Any]], str],
        logging.__dict__["_colorize_record"],
    )
    logging.set_log_context("branch_test")
    record: dict[str, Any] = {
        "time": datetime.now(UTC),
        "level": SimpleNamespace(name="ERROR"),
        "process": 1234,
        "module": "tests",
        "function": "test_colorize",
        "line": 42,
        "message": "error occurred",
        "exception": "some exception text without attrs",
    }
    output = colorize(record)
    assert "error occurred" in output
    assert "branch_test" in output
    logging.set_log_context("main")


def test_get_context_bg_color_cached() -> None:
    """Test _get_context_bg_color caches results.

    Given: context string,
    When: calling _get_context_bg_color twice,
    Then: same ANSI color code returned both times.
    """
    first = log_utils._get_context_bg_color("ctx-test")
    second = log_utils._get_context_bg_color("ctx-test")
    assert first == second
    assert first.startswith("\033[48;2;")


def test_filter_cancelled_errors_filters_cancelled() -> None:
    """Test _filter_cancelled_errors with explicit types.

    Given: records with CancelledError and RuntimeError,
    When: applying filter function,
    Then: CancelledError filtered out, RuntimeError passes.
    """
    cancelled_record = {
        "exception": SimpleNamespace(type=asyncio.CancelledError, value=None, traceback=None)
    }
    other_record = {"exception": SimpleNamespace(type=RuntimeError, value=None, traceback=None)}
    assert log_utils._filter_cancelled_errors(cancelled_record) is False
    assert log_utils._filter_cancelled_errors(other_record) is True


def test_get_context_bg_color_covers_branches(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test _get_context_bg_color covers CRC32 hash branches.

    Given: mocked zlib.crc32 returning different values,
    When: calling _get_context_bg_color with different contexts,
    Then: different color codes generated for each branch.
    """
    log_utils._CONTEXT_COLOR_CACHE.clear()

    def _crc32_factory(value: int) -> Callable[[bytes], int]:
        return lambda _: value

    zlib_mod = log_utils.zlib
    monkeypatch.setattr(zlib_mod, "crc32", _crc32_factory(10))
    color1 = log_utils._get_context_bg_color("ctx-10")
    monkeypatch.setattr(zlib_mod, "crc32", _crc32_factory(150))
    color2 = log_utils._get_context_bg_color("ctx-150")
    monkeypatch.setattr(zlib_mod, "crc32", _crc32_factory(270))
    color3 = log_utils._get_context_bg_color("ctx-270")
    assert color1 != color2 != color3


def test_setup_logging_json_logs_executes_serializer(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test setup_logging JSON mode executes serializer.

    Given: mocked logger.add that invokes sink,
    When: calling setup_logging with json_logs=True,
    Then: JSON serializer executes without error.
    """

    def _fake_add(sink: object, **_: object) -> None:
        if callable(sink):
            cast(Callable[[str], object], sink)("{}")

    logger_obj = log_utils.logger
    monkeypatch.setattr(logger_obj, "add", _fake_add)
    log_utils.setup_logging(json_logs=True)
    logger_obj.info("json-log-test")


def test_setup_logging_file_sink_executes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Test setup_logging file sink writes to file.

    Given: tmp_path for log file and mocked logger,
    When: calling setup_logging with logfile and invoking sink,
    Then: log file created with message content.
    """
    log_file = tmp_path / "test.log"
    sinks_captured: list[object] = []

    def _fake_add(sink: object, **_: object) -> int:
        sinks_captured.append(sink)
        return 1

    class FakeRecord:
        def __init__(self) -> None:
            self.record = {
                "time": datetime(2024, 1, 1, tzinfo=UTC),
                "level": SimpleNamespace(name="INFO"),
                "message": "test-file-log",
                "module": "tests",
                "function": "test_fn",
                "line": 42,
                "process": 12345,
                "exception": None,
            }

    logger_obj = log_utils.logger
    monkeypatch.setattr(logger_obj, "add", _fake_add)
    monkeypatch.setattr(logger_obj, "remove", lambda: None)
    monkeypatch.setattr(logger_obj, "configure", lambda **_: None)
    monkeypatch.setattr(stdlib_logging, "basicConfig", lambda **_: None)

    log_utils.setup_logging(level="INFO", json_logs=False, logfile=str(log_file))

    file_sink = sinks_captured[-1]
    assert callable(file_sink)

    file_sink(FakeRecord())

    assert log_file.exists()
    content = log_file.read_text()
    assert "test-file-log" in content
