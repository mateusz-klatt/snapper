"""Idempotency key storage constraints shared by order entry points."""

from typing import Annotated

from pydantic import AfterValidator

IDEMPOTENCY_KEY_MAX_LENGTH = 64


def validate_idempotency_key(value: str) -> str:
    """Validate a supplied key before an internal submission accesses persistence.

    Args:
        value: Caller-supplied key to store in the execution-plan column.

    Returns:
        The unchanged key when its length fits the storage contract.

    Raises:
        ValueError: If the key exceeds the shared storage limit.
    """
    if len(value) > IDEMPOTENCY_KEY_MAX_LENGTH:
        raise ValueError(
            f"idempotency_key must contain at most {IDEMPOTENCY_KEY_MAX_LENGTH} characters"
        )
    return value


IdempotencyKey = Annotated[str, AfterValidator(validate_idempotency_key)]
