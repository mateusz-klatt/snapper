"""Declare host-side environment inputs for the optional delegate profile."""

from typing import Final

ENV_VARS: Final[frozenset[str]] = frozenset(
    {
        "SNAPPER_DELEGATE_BASE_URL",
        "SNAPPER_DELEGATE_MAX_TOOL_ROUNDS",
        "SNAPPER_DELEGATE_MODEL_ALIAS",
    }
)
"""Variables interpolated into the runner-only service by Compose."""
