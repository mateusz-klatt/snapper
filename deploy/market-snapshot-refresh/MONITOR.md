# Independent freshness monitor templates

These are source templates only. Nothing in this directory installs, enables or
starts a scheduler. Keep activation separate from source review and quality gates.

The monitor calls the existing read-only freshness endpoint. It does not refresh
prices, access exchange credentials, change a database or authorize an order. A
stopped refresh service does not stop this independent check.

Run the reviewed source with
`python -m snapper.cli.market_snapshot_freshness_monitor`. The required arguments
are `--endpoint`, `--token-file`, repeated `--instrument-public-id`, repeated
`--fx-exchange`, `--scope-digest` and `--alert-hook`. The explicit instrument and FX
scope must match the refresh runner. The digest binds all resolved metadata,
including excluded FX candidates; obtain it from reviewed scope evidence rather
than accepting a changed digest automatically.

The endpoint must use HTTPS and the exact
`/api/metrics/market-snapshots/freshness` path. The monitor refuses redirects,
compressed responses and bodies larger than one mebibyte. It uses a ten-second
total HTTP deadline and rechecks original timestamps after response cleanup using
the existing inclusive 300-second age and five-second future-skew limits. Its
Python API accepts a smaller positive `timeout_seconds` value for callers with a
remaining end-to-end budget; values above ten seconds are refused.

The token file must already exist, be a private regular POSIX file with no group
or other permission bits, and contain only the existing Bearer access token.
Final-component symlinks, FIFOs, oversized input and multiline tokens are refused.
The monitor rereads this file on each invocation. The operator's existing token
lifecycle must keep it current; this command does not mint or renew credentials.
Missing credentials and expired tokens produce failures, not an unauthenticated
fallback. No credential value is passed as a command argument or logged.

`--alert-hook` names an absolute executable adapter accepting one fixed diagnostic
code. It is executed directly, without a shell, with a five-second deadline. The
existing host `_alert.sh` is a **sourced function library**, not that executable.
The supplied `snapshot-freshness-alert.sh` template sources it and calls
`snapper_alert` using an existing mail route to `root`. It makes missing mail,
missing helper and reported delivery failure observable. The CLI remains nonzero
when alert delivery fails. Do not point `--alert-hook` directly at `_alert.sh`.

Before any separately authorized installation, replace every `REPLACE_` value,
review executable ownership and permissions, supply the existing environment,
and verify the existing alert channel and private-token rotation. The templates
have no default instrument or accepted digest. The timer runs every 60 seconds;
there is no dependency on the refresh unit and no shared refresh or retention
lock. Systemd serializes this one service instance. The `OnFailure` service also
covers process termination and startup failure; an ordinary CLI failure can
therefore produce both the direct alert and the unit-failure alert. Alert delivery
must not be treated as a successful freshness observation.

Rollback is disabling the monitor timer under the production owner's workflow.
These templates neither install a cron entry nor replace the existing ticks
retention or archive alert configuration.
