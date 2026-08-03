"""Declare host-side environment inputs for the optional delegate profile."""

from typing import Final

RUNNER_ENV_VARS: Final[frozenset[str]] = frozenset(
    {
        "SNAPPER_DELEGATE_BASE_URL",
        "SNAPPER_DELEGATE_ENDPOINT_PATH",
        "SNAPPER_DELEGATE_MAX_TOOL_ROUNDS",
        "SNAPPER_DELEGATE_MODEL_ALIAS",
        "SNAPPER_DELEGATE_SNAPPER_URL",
    }
)
"""Variables interpolated into the generic runner-only service by Compose."""

MODEL_PROFILE_ENV_VARS: Final[frozenset[str]] = frozenset(
    {
        "SNAPPER_DELEGATE_GEMINI_BASE_URL",
        "SNAPPER_DELEGATE_GEMINI_MODEL",
        "SNAPPER_DELEGATE_KIMI_BASE_URL",
        "SNAPPER_DELEGATE_KIMI_MODEL",
    }
)
"""Host-side model and origin overrides for per-model delegate services."""

ENV_VARS: Final[frozenset[str]] = RUNNER_ENV_VARS | MODEL_PROFILE_ENV_VARS
"""All host-side inputs owned by the optional delegate Compose profile."""
