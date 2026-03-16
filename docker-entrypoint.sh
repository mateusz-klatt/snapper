#!/bin/sh
set -e

chown snapper:snapper /app/data 2>/dev/null || true
exec setpriv --reuid=snapper --regid=snapper --init-groups -- snapper "$@"
