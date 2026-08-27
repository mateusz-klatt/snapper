#!/bin/sh
set -eu

seed_kimi_region() {
    if [ ! -f "$HOME/.kimi-code/region" ]; then
        mkdir -p "$HOME/.kimi-code"
        printf 'global\n' > "$HOME/.kimi-code/region"
    fi
}

seed_kimi_region

if [ "${AGENT_CONSOLE_MODE:-idle}" = "delegate" ]; then
    export SNAPPER_PID1_STRICT=1
    exec python -m snapper_delegate.pid1
fi

exec sleep infinity
