"""Protect delegate process configuration from inline credential values.

Delegate parameters cross persistence and subprocess argument boundaries. This
module classifies delegate workloads exclusively from their authoritative
registry metadata and enforces a structural reference-only configuration
shape before either boundary can serialize the parameters.
"""

import re
from collections.abc import Mapping
from pathlib import PurePosixPath
from typing import Final
from urllib.parse import urlsplit

from snapper.application.process_manager.models import ProcessRegistryEntry
from snapper.application.process_manager.process_parameters import DelegateProcessParameters
from snapper.application.process_manager.registry import get_registered_processes

_DELEGATE_TAGS: Final[frozenset[str]] = frozenset({"delegate", "runner"})
_DELEGATE_PARAMETER_KEYS: Final[frozenset[str]] = frozenset(
    {
        "model_alias",
        "base_url",
        "api_key_file",
        "delegate_token_file",
        "max_tool_rounds",
    }
)
_ROUTE_ALIAS_PATTERN: Final[re.Pattern[str]] = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_PATH_SEGMENT_PATTERN: Final[re.Pattern[str]] = re.compile(r"[A-Za-z0-9._-]+")
_JWT_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"
)
_KEY_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(?:api|key|pk|rk|secret|sk|token)[_-][A-Za-z0-9_+=./-]{12,}", re.IGNORECASE
)
_BASE64_PATTERN: Final[re.Pattern[str]] = re.compile(r"[A-Za-z0-9+/]{32,}={0,2}")
_GENERIC_ERROR_MESSAGE: Final[str] = (
    "Delegate process configuration violates the reference-only boundary"
)


class DelegateConfigReferenceError(ValueError):
    """Raised when delegate parameters are not structurally reference-only."""

    def __init__(self) -> None:
        """Initialize the error with a value-independent message."""
        super().__init__(_GENERIC_ERROR_MESSAGE)


def _is_delegate_entry(entry: ProcessRegistryEntry | None) -> bool:
    """Return whether registry metadata authoritatively identifies a delegate."""
    return (
        entry is not None
        and _DELEGATE_TAGS.issubset(entry.tags)
        and entry.parameters_model is DelegateProcessParameters
    )


def _entry_for_config(name: str, template_name: str | None) -> ProcessRegistryEntry | None:
    """Resolve the registry metadata that owns a process configuration."""
    registry_name = template_name if template_name is not None else name
    return get_registered_processes().get(registry_name)


def _is_route_alias(value: object) -> bool:
    """Return whether a value is a non-empty route-alias token."""
    return (
        isinstance(value, str)
        and _ROUTE_ALIAS_PATTERN.fullmatch(value) is not None
        and not _looks_like_inline_credential(value)
    )


def _is_https_reference(value: object) -> bool:
    """Return whether a value is an HTTPS endpoint without credential channels."""
    if not isinstance(value, str) or not value or value != value.strip():
        return False
    if "?" in value or "#" in value or any(character.isspace() for character in value):
        return False
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        return False
    return (
        parsed.scheme == "https"
        and parsed.hostname is not None
        and parsed.username is None
        and parsed.password is None
        and (port is None or port > 0)
    )


def _looks_like_inline_credential(value: str) -> bool:
    """Return whether a non-path token has a common credential structure."""
    candidate = value[1:] if value.startswith("/") and "/" not in value[1:] else value
    return (
        candidate.lower().startswith(("bearer ", "basic "))
        or _JWT_PATTERN.fullmatch(candidate) is not None
        or _KEY_PATTERN.fullmatch(candidate) is not None
        or _BASE64_PATTERN.fullmatch(candidate) is not None
    )


def _is_file_reference(value: object) -> bool:
    """Return whether a value has an unambiguous absolute-file-path shape."""
    if not isinstance(value, str) or not value or value != value.strip():
        return False
    if _looks_like_inline_credential(value):
        return False
    path = PurePosixPath(value)
    raw_segments = value[1:].split("/") if value.startswith("/") else []
    return (
        path.is_absolute()
        and value != "/"
        and not value.endswith("/")
        and "\x00" not in value
        and ".." not in path.parts
        and bool(raw_segments)
        and all(_PATH_SEGMENT_PATTERN.fullmatch(segment) is not None for segment in raw_segments)
        and not _looks_like_inline_credential(path.name)
    )


def _is_bounded_round_count(value: object) -> bool:
    """Return whether a value is a strict integer within the delegate bound."""
    return isinstance(value, int) and not isinstance(value, bool) and 1 <= value <= 8


def _validate_reference_shape(parameters: Mapping[str, object]) -> None:
    """Validate the exact structural shape of delegate parameters."""
    valid = (
        frozenset(parameters) == _DELEGATE_PARAMETER_KEYS
        and _is_route_alias(parameters.get("model_alias"))
        and _is_https_reference(parameters.get("base_url"))
        and _is_file_reference(parameters.get("api_key_file"))
        and _is_file_reference(parameters.get("delegate_token_file"))
        and _is_bounded_round_count(parameters.get("max_tool_rounds"))
    )
    if not valid:
        raise DelegateConfigReferenceError


def validate_delegate_config_references(
    name: str,
    template_name: str | None,
    parameters: Mapping[str, object],
) -> None:
    """Enforce reference-only parameters for a registry-identified delegate.

    Args:
        name: Configured process name used by registry-native workloads.
        template_name: Source registry template for configured process copies.
        parameters: Process parameters about to cross a serialization boundary.

    Raises:
        DelegateConfigReferenceError: When an authoritative delegate entry has
            parameters outside the reference-only structural contract.
    """
    entry = _entry_for_config(name, template_name)
    if _is_delegate_entry(entry):
        _validate_reference_shape(parameters)
