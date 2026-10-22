"""Environment contract for optional backend-owned Android push configuration."""

CONFIG_FILE_ENV: str = "SNAPPER_ANDROID_PUSH_CONFIG_FILE"
ENV_VARS: frozenset[str] = frozenset({CONFIG_FILE_ENV})
