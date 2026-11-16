#!/usr/bin/env bash
set -euo pipefail
set +x
umask 077

reason=${1:-monitor_unit_failed}
[[ "$reason" =~ ^[a-z_]{1,64}$ ]] || { echo "ALERT-UNDELIVERED: invalid reason" >&2; exit 1; }
archive_root=${SNAPPER_ARCHIVE_ROOT:-/mnt/e/snapper-archive}
alert_helper="$archive_root/bin/_alert.sh"
log_file="$archive_root/logs/snapshot-freshness-alert.log"
export SNAPPER_ALERT_EMAIL=${SNAPPER_ALERT_EMAIL:-root}
command -v mail >/dev/null || { echo "ALERT-UNDELIVERED: mail unavailable" >&2; exit 1; }
[[ -r "$alert_helper" ]] || { echo "ALERT-UNDELIVERED: helper unavailable" >&2; exit 1; }
mkdir -p "$archive_root/logs"
printf '=== snapshot-freshness %s ===\nREFUSE %s\n' "$(date -u +%FT%TZ)" "$reason" >> "$log_file"
source "$alert_helper"
delivery=$(snapper_alert "Snapper market snapshot freshness FAILED" "$log_file")
if [[ -n "$delivery" ]]; then
    printf '%s\n' "$delivery" >> "$log_file"
fi
if [[ "$delivery" == *ALERT-UNDELIVERED* ]]; then
    echo "ALERT-UNDELIVERED: existing mail adapter failed" >&2
    exit 1
fi
echo "ALERT submitted: existing host mail adapter"
