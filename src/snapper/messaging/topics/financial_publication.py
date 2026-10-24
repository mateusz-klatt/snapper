"""Validate immutable accounting destinations independently of the mutable catalog.

Exchange evidence is canonical lowercase text and can contain dots. Full
destinations therefore parse the environment and UUIDs from the right.
Subscription validation establishes routing shape only, never account access.
"""

from typing import Literal

from pydantic import TypeAdapter
from pydantic import ValidationError

from snapper.core.execution_unit_snapshot import EvidenceExchange
from snapper.core.execution_unit_snapshot import EvidenceUUID

_EXCHANGE = TypeAdapter(EvidenceExchange)
_UUID = TypeAdapter(EvidenceUUID)
_ENVIRONMENT = TypeAdapter(Literal["production", "demo", "paper"])
_LEGACY = ("accounting", "contract_position")
_NATIVE = ("accounting", "native_position", "v2")


def _scope_segments(topic: str) -> list[str] | None:
    """Separate a recognized exact version prefix from its original scope.

    Args:
        topic: Full destination without a subscription terminator.

    Returns:
        Remaining scope segments, or None for an unsupported publication family.
    """
    segments = topic.split(".")
    for prefix in (_LEGACY, _NATIVE):
        if tuple(segments[: len(prefix)]) == prefix:
            return segments[len(prefix) :]
    return None


def _canonical_uuid(value: str) -> bool:
    """Require the original spelling to equal the actual evidence UUID codec.

    Args:
        value: Original wallet or instrument topic segment.

    Returns:
        Whether the UUID is canonical, without restricting its version.
    """
    try:
        return _UUID.validate_python(value, strict=True) == value
    except ValidationError:
        return False


def _valid_exchange(exchange: str) -> bool:
    """Use the source evidence codec without consulting present catalog rows.

    Args:
        exchange: Complete original exchange spelling, including any dots.

    Returns:
        Whether the original exchange has the canonical evidence spelling.
    """
    try:
        return _EXCHANGE.validate_python(exchange, strict=True) == exchange
    except ValidationError:
        return False


def _valid_environment(exchange: str, environment: str) -> bool:
    """Preserve the actual live versus paper account scope relation.

    Args:
        exchange: Canonical exchange from the original source scope.
        environment: Original production, demo or paper environment segment.

    Returns:
        Whether the environment is supported and agrees with paper exchange identity.
    """
    try:
        _ENVIRONMENT.validate_python(environment, strict=True)
    except ValidationError:
        return False
    return (exchange == "paper") == (environment == "paper")


def _validate_scope(segments: list[str]) -> tuple[bool, str]:
    """Validate one complete destination using its right-anchored immutable IDs.

    Args:
        segments: Original exchange segments followed by environment, wallet and instrument.

    Returns:
        A validity result with a fixed structural refusal reason when invalid.
    """
    if len(segments) < 4:
        return False, "Accounting topic requires exchange, environment, wallet and instrument"
    exchange = ".".join(segments[:-3])
    if not _valid_exchange(exchange):
        return False, "Accounting exchange must use canonical evidence spelling"
    if not _valid_environment(exchange, segments[-3]):
        return False, "Accounting environment must agree with its exchange scope"
    if not all(_canonical_uuid(value) for value in segments[-2:]):
        return False, "Accounting wallet and instrument must be canonical UUIDs"
    return True, ""


def validate_financial_publication_topic(topic: str) -> tuple[bool, str]:
    """Accept only the two explicit immutable publication topic families.

    Args:
        topic: Actual complete ZMQ destination, including its original source scope.

    Returns:
        Whether the destination has the exact supported family and scope shape.
    """
    segments = _scope_segments(topic)
    if segments is None:
        return False, "Unsupported accounting publication family or version"
    return _validate_scope(segments)


def _validate_scope_prefix(segments: list[str]) -> tuple[bool, str]:
    """Validate explicit exchange, environment and wallet subscription boundaries.

    Args:
        segments: Original trailing segments after a complete version prefix.

    Returns:
        Whether this explicit source prefix follows the immutable topic hierarchy.
    """
    if not segments:
        return True, ""
    if len(segments) == 1:
        valid = _valid_exchange(segments[0])
        return valid, "" if valid else "Invalid accounting exchange prefix"
    if _valid_environment(".".join(segments[:-1]), segments[-1]):
        valid = _valid_exchange(".".join(segments[:-1]))
        return valid, "" if valid else "Invalid accounting exchange prefix"
    if len(segments) >= 3 and _valid_environment(".".join(segments[:-2]), segments[-2]):
        exchange = ".".join(segments[:-2])
        if _valid_exchange(exchange) and _canonical_uuid(segments[-1]):
            return True, ""
        return False, "Invalid accounting exchange or wallet prefix"
    return _validate_scope(segments)


def validate_financial_publication_prefix(pattern: str) -> tuple[bool, str]:
    """Accept explicit internal subscription roots without changing browser access.

    Args:
        pattern: Original accounting prefix ending in one dot.

    Returns:
        Whether the prefix belongs to a supported publication family and scope.
    """
    if not pattern.endswith("."):
        return False, "Accounting subscription prefix must end with a dot"
    bare = pattern[:-1]
    if any(not segment for segment in bare.split(".")):
        return False, "Accounting subscription segments cannot be empty"
    if bare in {"accounting", "accounting.native_position"}:
        return True, ""
    segments = _scope_segments(bare)
    if segments is None:
        return False, "Unsupported accounting publication family or version"
    return _validate_scope_prefix(segments)
