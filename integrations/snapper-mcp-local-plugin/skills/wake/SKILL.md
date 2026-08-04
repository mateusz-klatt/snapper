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
   pgrep -af "index\.js watch"
   ```

   A match means a monitor is already running — report that and STOP: a second
   monitor double-handles every consult.
2. Arm it with the **Monitor tool**, `persistent: true` and no timeout — NOT a
   backgrounded Bash command. The Monitor tool turns every stdout line into an
   event that wakes you; a backgrounded shell command only reports when the
   process exits, and `watch` is built never to exit, so it would deliver no
   wakeups at all:

   - command: `node __SNAPPER_REPO_ROOT__/integrations/snapper-mcp/dist/index.js watch --config="$CLAUDE_PLUGIN_DATA/env.json"`
   - description: `Snapper consult (ai_review.request) JSONL stream`
   - persistent: true

   `CLAUDE_PLUGIN_DATA` is set PER PLUGIN, and in an agent shell it resolves to
   whichever plugin exported it last — observed pointing at an unrelated
   plugin's directory. The host expands it correctly when IT arms the monitor,
   but on this self-arm path you must check: run
   `test -f "$CLAUDE_PLUGIN_DATA/env.json"` first, and if it is missing, use
   this plugin's own seeded `env.json` under the Claude plugin data directory
   (the `snapper-mcp-local-*` one) by absolute path instead. Never print the
   file — it holds a credential.

   Do not add `--topic`: it replaces the defaults, and dropping `ai_reviews.`
   silently stops every consult.
3. Confirm it survived startup before reporting success. On connect `watch`
   logs `subscribing to topics: ...`, and that list must include
   `ai_reviews.`. If credentials cannot be resolved it exits within a couple of
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
