"""Run one delegate directly as a container PID1 without Snapper coordination.

The entrypoint owns no database, message-bus, broker, process-launcher, or
vendor-client lifecycle. It accepts one exact model route, vendor endpoint,
and Snapper control origin from environment references, verifies that both
credential file references are present and readable, and only then
instantiates ``DelegateRunner``.
Incomplete or malformed configuration stays alive in a signal-aware idle
state so a profile-enabled canary fails closed without a restart loop.
"""

import asyncio
import os
import re
import signal
from collections.abc import Mapping
from pathlib import Path
from typing import Final
from urllib.parse import urlsplit

from loguru import logger
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import ValidationError
from pydantic import field_validator

from snapper_delegate.runner import DelegateRunner

_ENV_PREFIX: Final[str] = "SNAPPER_DELEGATE_"
_MODEL_ALIAS_ENV: Final[str] = f"{_ENV_PREFIX}MODEL_ALIAS"
_BASE_URL_ENV: Final[str] = f"{_ENV_PREFIX}BASE_URL"
_ENDPOINT_PATH_ENV: Final[str] = f"{_ENV_PREFIX}ENDPOINT_PATH"
_SNAPPER_URL_ENV: Final[str] = f"{_ENV_PREFIX}SNAPPER_URL"
_API_KEY_FILE_ENV: Final[str] = f"{_ENV_PREFIX}API_KEY_FILE"
_TOKEN_FILE_ENV: Final[str] = f"{_ENV_PREFIX}TOKEN_FILE"
_MAX_TOOL_ROUNDS_ENV: Final[str] = f"{_ENV_PREFIX}MAX_TOOL_ROUNDS"
_CONFIG_ENV_NAMES: Final[frozenset[str]] = frozenset(
    {
        _MODEL_ALIAS_ENV,
        _BASE_URL_ENV,
        _ENDPOINT_PATH_ENV,
        _SNAPPER_URL_ENV,
        _API_KEY_FILE_ENV,
        _TOKEN_FILE_ENV,
        _MAX_TOOL_ROUNDS_ENV,
    }
)
_DEFAULT_ENDPOINT_PATH: Final[str] = "/v1/chat/completions"
_ENDPOINT_VERSION_PATTERN: Final[str] = r"/v[1-9][0-9]{0,2}(?:[a-z][a-z0-9]{0,15})?/"
_ENDPOINT_COMPATIBILITY_PATTERN: Final[str] = r"(?:[a-z][a-z0-9_-]{0,31}/)?chat/completions"
_ENDPOINT_PATH_PATTERN: Final[re.Pattern[str]] = re.compile(
    _ENDPOINT_VERSION_PATTERN + _ENDPOINT_COMPATIBILITY_PATTERN
)
_API_KEY_PATH: Final[Path] = Path("/run/secrets/delegate/api_key")
_DELEGATE_TOKEN_PATH: Final[Path] = Path("/run/secrets/delegate/delegate_token")
_LOG_FILE_ENV: Final[str] = "SNAPPER_LOG_FILE"
_LOG_FILE_PATH: Final[Path] = Path("/app/data/log/delegate/model/delegate.log")
_MODEL_ALIAS_PATTERN: Final[re.Pattern[str]] = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_JWT_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"
)
_KEY_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(?:api|key|pk|rk|secret|sk|token)[_-][A-Za-z0-9_.-]{12,}",
    re.IGNORECASE,
)
_BASE64_PATTERN: Final[re.Pattern[str]] = re.compile(r"[A-Za-z0-9]{32,}")
_FQDN_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(?=.{1,253}\Z)(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+[A-Za-z]{2,63}"
)
_ORIGIN_HOST_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(?=.{1,253}\Z)[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
    r"(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*\.?"
)
_CONFIG_ERROR_MESSAGE: Final[str] = "Runner-only delegate configuration is incomplete or invalid"


class RunnerOnlyConfigurationError(ValueError):
    """Report a refused PID1 configuration without retaining input values."""

    def __init__(self) -> None:
        """Initialize the value-independent configuration error."""
        super().__init__(_CONFIG_ERROR_MESSAGE)


class RunnerOnlyConfiguration(BaseModel):
    """Represent the exact one-model configuration accepted by the PID1."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        validate_default=True,
    )

    model_alias: str = Field(min_length=1, max_length=128)
    base_url: str = Field(min_length=1)
    endpoint_path: str = Field(default=_DEFAULT_ENDPOINT_PATH, min_length=1, max_length=128)
    snapper_base_url: str = Field(min_length=1)
    api_key_file: str = Field(min_length=1)
    delegate_token_file: str = Field(min_length=1)
    max_tool_rounds: int = Field(ge=1, le=8)

    @field_validator("model_alias")
    @classmethod
    def _validate_model_alias(cls, value: str) -> str:
        """Require one route token rather than a list or inline secret."""
        looks_like_credential = (
            _JWT_PATTERN.fullmatch(value) is not None
            or _KEY_PATTERN.fullmatch(value) is not None
            or _BASE64_PATTERN.fullmatch(value) is not None
        )
        if _MODEL_ALIAS_PATTERN.fullmatch(value) is None or looks_like_credential:
            raise ValueError("invalid model alias")
        return value

    @field_validator("base_url")
    @classmethod
    def _validate_base_url(cls, value: str) -> str:
        """Require a credential-free HTTPS origin on TCP port 443."""
        if value != value.strip() or any(character.isspace() for character in value):
            raise ValueError("invalid endpoint origin")
        try:
            parsed = urlsplit(value)
            port = parsed.port
        except ValueError as error:
            raise ValueError("invalid endpoint origin") from error
        hostname = parsed.hostname
        canonical_authority = hostname if port is None else f"{hostname}:{port}"
        valid = (
            parsed.scheme == "https"
            and hostname is not None
            and _FQDN_PATTERN.fullmatch(hostname) is not None
            and parsed.username is None
            and parsed.password is None
            and parsed.netloc.lower() == canonical_authority
            and parsed.path in ("", "/")
            and parsed.query == ""
            and parsed.fragment == ""
            and port in (None, 443)
        )
        if not valid:
            raise ValueError("invalid endpoint origin")
        return value.rstrip("/")

    @field_validator("endpoint_path")
    @classmethod
    def _validate_endpoint_path(cls, value: str) -> str:
        """Require a bounded provider-neutral chat-completions endpoint path."""
        if _ENDPOINT_PATH_PATTERN.fullmatch(value) is None:
            raise ValueError("invalid endpoint path")
        return value

    @field_validator("snapper_base_url")
    @classmethod
    def _validate_snapper_base_url(cls, value: str) -> str:
        """Require a credential-free HTTP or HTTPS Snapper origin."""
        invalid_characters = any(character.isspace() for character in value) or any(
            delimiter in value for delimiter in ("?", "#")
        )
        if value != value.strip() or invalid_characters:
            raise ValueError("invalid Snapper origin")
        try:
            parsed = urlsplit(value)
            port = parsed.port
        except ValueError as error:
            raise ValueError("invalid Snapper origin") from error
        hostname = parsed.hostname
        valid_hostname = hostname is not None and (
            ":" in hostname or _ORIGIN_HOST_PATTERN.fullmatch(hostname) is not None
        )
        authority_hostname = (
            f"[{hostname}]" if hostname is not None and ":" in hostname else hostname
        )
        canonical_authority = authority_hostname if port is None else f"{authority_hostname}:{port}"
        valid = (
            parsed.scheme in {"http", "https"}
            and valid_hostname
            and parsed.username is None
            and parsed.password is None
            and (port is None or port > 0)
            and parsed.netloc.lower() == canonical_authority
            and parsed.path in ("", "/")
            and parsed.query == ""
            and parsed.fragment == ""
        )
        if not valid:
            raise ValueError("invalid Snapper origin")
        return value.rstrip("/")

    @field_validator("api_key_file")
    @classmethod
    def _validate_api_key_file(cls, value: str) -> str:
        """Require the fixed API-key reference path, never an inline value."""
        if value != str(_API_KEY_PATH):
            raise ValueError("invalid API-key reference")
        return value

    @field_validator("delegate_token_file")
    @classmethod
    def _validate_delegate_token_file(cls, value: str) -> str:
        """Require the fixed delegate-token reference path."""
        if value != str(_DELEGATE_TOKEN_PATH):
            raise ValueError("invalid delegate-token reference")
        return value


def _file_reference_is_ready(path: Path) -> bool:
    """Return whether a secret reference resolves to a readable non-empty file."""
    try:
        if not path.is_file():
            return False
        with path.open("rb") as handle:
            return bool(handle.read(1))
    except OSError:
        return False


def _parse_round_count(raw_value: str) -> int:
    """Parse a canonical decimal round count without coercive whitespace."""
    if len(raw_value) != 1 or raw_value not in "12345678":
        raise RunnerOnlyConfigurationError
    return int(raw_value)


def load_runner_configuration(environ: Mapping[str, str]) -> RunnerOnlyConfiguration:
    """Load and verify the exact one-model PID1 configuration.

    Args:
        environ: Environment mapping supplied to the runner-only process.

    Returns:
        Fully validated reference-only runner configuration.

    Raises:
        RunnerOnlyConfigurationError: If required values or referenced files
            are absent, malformed, ambiguous, or not ready.
    """
    unexpected_names = {
        name for name in environ if name.startswith(_ENV_PREFIX) and name not in _CONFIG_ENV_NAMES
    }
    if unexpected_names:
        raise RunnerOnlyConfigurationError
    try:
        payload: dict[str, object] = {
            "model_alias": environ[_MODEL_ALIAS_ENV],
            "base_url": environ[_BASE_URL_ENV],
            "endpoint_path": environ.get(_ENDPOINT_PATH_ENV, _DEFAULT_ENDPOINT_PATH),
            "snapper_base_url": environ[_SNAPPER_URL_ENV],
            "api_key_file": environ[_API_KEY_FILE_ENV],
            "delegate_token_file": environ[_TOKEN_FILE_ENV],
            "max_tool_rounds": _parse_round_count(environ[_MAX_TOOL_ROUNDS_ENV]),
        }
        configuration = RunnerOnlyConfiguration.model_validate(payload)
    except (KeyError, RunnerOnlyConfigurationError, ValidationError):
        raise RunnerOnlyConfigurationError from None
    if not _file_reference_is_ready(Path(configuration.api_key_file)):
        raise RunnerOnlyConfigurationError
    if not _file_reference_is_ready(Path(configuration.delegate_token_file)):
        raise RunnerOnlyConfigurationError
    return configuration


def _configure_file_logging(environ: Mapping[str, str]) -> None:
    """Attach the one writable model logfile when its exact path is present."""
    configured_path = environ.get(_LOG_FILE_ENV)
    if configured_path != str(_LOG_FILE_PATH):
        return
    try:
        logger.add(
            configured_path,
            level="INFO",
            backtrace=False,
            diagnose=False,
            enqueue=False,
        )
    except (OSError, ValueError):
        logger.info("Runner-only delegate logfile is unavailable; using standard output")


async def _idle_unconfigured() -> None:
    """Wait without doing work until PID1 receives a termination signal."""
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    installed = False
    try:
        loop.add_signal_handler(signal.SIGTERM, stop_event.set)
        installed = True
    except (NotImplementedError, RuntimeError):
        pass
    try:
        await stop_event.wait()
    finally:
        if installed:
            loop.remove_signal_handler(signal.SIGTERM)


async def run_pid1(environ: Mapping[str, str] | None = None) -> None:
    """Run one configured delegate or remain safely idle.

    Args:
        environ: Optional injected environment mapping for tests.
    """
    source = os.environ if environ is None else environ
    try:
        configuration = load_runner_configuration(source)
    except RunnerOnlyConfigurationError:
        logger.info("Runner-only delegate is unconfigured and remains idle")
        await _idle_unconfigured()
        return
    runner = DelegateRunner(
        model_alias=configuration.model_alias,
        base_url=configuration.base_url,
        endpoint_path=configuration.endpoint_path,
        snapper_base_url=configuration.snapper_base_url,
        api_key_file=configuration.api_key_file,
        delegate_token_file=configuration.delegate_token_file,
        max_tool_rounds=configuration.max_tool_rounds,
    )
    await runner.start()


def main() -> int:
    """Run the runner-only delegate as the container's PID1.

    Returns:
        Zero after a clean runner or unconfigured-idle shutdown.
    """
    _configure_file_logging(os.environ)
    asyncio.run(run_pid1())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
