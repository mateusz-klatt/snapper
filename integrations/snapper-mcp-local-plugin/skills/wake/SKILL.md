---
name: wake
description: Arm the local Snapper review-request watch monitor for THIS session only
disable-model-invocation: true
---

Arm the LOCALHOST Snapper watch monitor for THIS session. Arm only one session
at a time — every armed session receives the same requests. Do it now, in
order:

1. Check whether this machine is already watching. A task list does not show
   background monitors, so check the processes:

   ```
   pgrep -af '[i]ndex\.js watch'
   ```

   Treat matches as candidates and verify the configuration path and backend
   they belong to. If a monitor already watches this local backend, report that
   and STOP: a second monitor can double-handle its consults. A process match
   alone does not establish that the current session owns that monitor.
2. Arm it with the **Monitor tool**, `persistent: true` and no timeout — NOT a
   backgrounded Bash command. The Monitor tool turns every stdout line into an
   event that wakes you; a backgrounded shell command only reports when the
   process exits, and `watch` is built never to exit, so it would deliver no
   wakeups at all:

   - command: `node __SNAPPER_REPO_ROOT__/integrations/snapper-mcp/dist/index.js watch --config="/absolute/path/to/snapper-mcp-local-plugin-data/env.json"`
   - description: `Snapper consult (ai_review.request) JSONL stream`
   - persistent: true

   **Do not use `$CLAUDE_PLUGIN_DATA` on this path.** It is set PER PLUGIN and
   in an agent shell resolves to whichever plugin exported it last — observed
   pointing at the copilot plugin's directory. Checking `test -f
   "$CLAUDE_PLUGIN_DATA/env.json"` does NOT save you: that directory holds its
   own `env.json` with the same `SNAPPER_BASE_URL` / `SNAPPER_ACCESS_TOKEN`
   keys but a different backend, so the check passes and the monitor silently
   connects somewhere else entirely.

   Resolve the directory by NAME instead, and confirm it is this plugin's:

   ```
   ls -d ~/.claude/plugins/data/snapper-mcp-local-*/
   ```

   Replace the command's example config path with that directory's absolute
   `env.json` path before arming the monitor. Never print the file — it holds a
   credential. (When the HOST arms the monitor, `${CLAUDE_PLUGIN_DATA}` in the
   manifest is expanded per plugin and is correct; this warning is only for the
   self-arm fallback, which runs in a shell.)

   Do not add `--topic`: it replaces the defaults, and dropping `ai_reviews.`
   silently stops every consult.
3. Confirm it survived startup before reporting success. On connect `watch`
   logs `subscribing to topics: ...`, and that list must include
   both `ai_reviews.` and `ai_research.`. If credentials cannot be resolved it exits within a couple of
   seconds naming every source it tried — read the monitor's output.

   This plugin targets **localhost:8000**, so also confirm the local backend is
   actually up (`make dev-backend` if not). A dead backend and a missing
   credential fail the same way from the outside: `watch` exits 1 within
   seconds.

Once armed, pending AI-review (consult) frames addressed to this delegate
stream as JSONL and wake the agent; answer each `ai_review.request` within its
deadline via the `submit_ai_review_decision` MCP tool. `signal` and
`decision_ack` frames are noise.

Sessions and subagents that never invoke this skill never arm the monitor, so
they do not connect, consume tokens, or double-handle a consult. On a session
resume the monitor is not restored; invoke this skill again to re-arm it. Stop
by ending the session or with TaskStop on the monitor task.
