---
name: wake
description: Arm the local Snapper consult wake monitor for THIS session only
disable-model-invocation: true
---

Arm the Snapper consult watch monitor for THIS session. Do it now:

1. Call TaskList. If a persistent monitor is already streaming the Snapper
   watch (a task running `dist/index.js watch`), it is already armed — report
   that and STOP. Starting a second one would double-handle every consult.
2. Otherwise call the Monitor tool with:
   - command: `node __SNAPPER_REPO_ROOT__/integrations/snapper-mcp/dist/index.js watch --config="__SNAPPER_REPO_ROOT__/data/dev-pat.json"`
   - description: `Snapper consult (ai_review.request) JSONL stream`
   - persistent: true

Once armed, pending AI-review (consult) frames addressed to this delegate
stream as JSONL and wake the agent; answer each `ai_review.request` within its
deadline via the `submit_ai_review_decision` MCP tool. `signal` and
`decision_ack` frames are noise. Sessions and subagents that never invoke this
skill never arm the monitor, so they do not connect, consume tokens, or
double-handle a consult.

On a session resume the monitor is not restored; invoke this skill again to
re-arm it. Stop by ending the session or TaskStop on the monitor task.
