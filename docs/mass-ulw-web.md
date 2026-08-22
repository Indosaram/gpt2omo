# mass-ulw + ChatGPT Web delegation

This guide defines the integration contract between OMO mass-ulw DAG orchestration and `delegate_to_chatgpt_web`.

The central rule is simple: **mass-ulw schedules workflow phases; `delegate_to_chatgpt_web` remains the authority for Web-worker lifecycle and safety policy.** The DAG layer must not open browser tabs itself, bypass readiness, or implement a competing worker semaphore.

## Result handoff

Completion is not a prose claim in the browser. A Web worker supplies a structured
`result` object in its final `completion_check` call; the bridge atomically saves
the resulting `task_result` for its exact scope/generation before it can return
`ready: true`. The helper includes that artifact in both the worker's
`event:"terminal"` NDJSON record and its final aggregate JSON:

```json
{
  "task_result": {
    "summary": "...",
    "changed_files": ["..."],
    "verification": ["..."],
    "blockers": [],
    "final_message": "..."
  }
}
```

The DAG coordinator must consume this payload as the authoritative worker report.
It must not resume a completed retained browser session merely to ask for prose
output. A `COMPLETED` terminal record without `task_result` is a contract failure,
not a successful result handoff.

## Architecture

```text
OMO mass-ulw / tool.dag / JS SDK / Python eval
                    |
                    |  DelegateWebInvocation
                    |  { program, args, stdin }
                    v
          delegate_to_chatgpt_web
                    |
       +------------+-------------+
       |                          |
 max 2 in-flight workers    telemetry guards
 10 s second-tab stagger    active lockout + window cap
 readiness barrier          retained-session lifecycle
       |                          |
       +------------+-------------+
                    |
          ChatGPT Web workers
                    |
          dispatched NDJSON event
                    |
          terminal JSON result
                    |
       scope_id + resumable state
                    |
          serial --resume-scope
                    |
              fan-in / fix loop
```

The reusable Rust contract lives in `gpt2omo::mass_ulw_web`. Equivalent zero-dependency helpers for OMO JavaScript and Python eval live in:

- `examples/mass_ulw_web.mjs`
- `examples/mass_ulw_web.py`

All three expose the same conceptual primitives:

- `single`: one fresh Web worker, retained after terminal by default.
- `parallel_pair`: exactly two domain workers encoded into **one** `--batch-stdin` process invocation.
- `resume`: one serial follow-up in the exact retained ChatGPT Web conversation.
- `close_scope`: explicit final cleanup after orchestration approval.
- `retained_scope` / `fan_in_handoff`: result parsing and serial fan-in construction.

## Why one batch process for parallel fan-out

Do not start two independent `delegate_to_chatgpt_web` processes concurrently from two DAG nodes. Doing so introduces a race between process-local admission checks and defeats the purpose of a single anti-burst staging path.

For two domains, create one batch node with two tasks:

```json
{
  "tasks": [
    {
      "label": "backend",
      "task": "Own backend files only. Implement the API change and backend tests.",
      "workspace": "/repo"
    },
    {
      "label": "frontend",
      "task": "Own frontend files only. Implement the UI change and frontend tests.",
      "workspace": "/repo"
    }
  ]
}
```

The delegate CLI then performs all of the following in one authority domain:

1. Rejects a third worker. The supported maximum is **2**.
2. Checks current in-flight scopes before opening a new tab.
3. Checks bridge telemetry for an active rate-limit lockout and the sliding dispatch window.
4. Opens the first Web tab, waits **10 seconds**, then opens the second tab.
5. Runs the readiness bootstrap for both workers and dispatches actual task prompts only after the readiness barrier succeeds.
6. Waits for both workers to reach terminal state and returns one JSON result containing both retained scope IDs.

A mass-ulw fan-in node should depend on that batch node, so it cannot run while either worker is still active.

The 10-second tab-creation stagger is intentional anti-burst protection, not serial task
execution. Once both tabs are created and both readiness handshakes succeed, the helper
sends the actual prompts concurrently. A two-worker batch therefore has two distinct
browser scopes even though the second tab becomes visible slightly later.

### Observe batch fan-out without weakening the terminal barrier

`parallelPairInvocation` adds `--progress-json` to its helper invocation. After every
worker has passed readiness and received its actual task, the helper flushes one
newline-delimited JSON event with `event: "dispatched"`, `parallel_count`, and every
worker's distinct `scope_id` and `browser_page_id`.

That event proves the browser-level fan-out while the native DAG still displays one
running batch node. It is not terminal evidence: one completed browser worker plus one
active worker means the batch node is **RUNNING**, not blocked. Report the event through
`runInvocation(..., { onProgress })`, but do not start a dependent node, close a scope,
or terminate the helper when an observer's wait expires. The final Promise result remains
the only terminal batch result and is emitted after every worker becomes terminal.

`onProgress` also receives one `event: "terminal"` record for each worker as soon as its
authoritative terminal lifecycle is recorded. Treat that record as the worker's immediate
completion notification, but do not close the retained scope or release downstream DAG
dependencies until the aggregate terminal JSON arrives.

### Recovering an exited helper without touching the worker

If the native helper exits before producing terminal JSON but its earlier dispatched event
identified a browser-bound `scope_id`, do not replace or resume the worker. Attach a
read-only durable lifecycle observer instead:

```bash
delegate_to_chatgpt_web --mount-root / --observe-scope "$SCOPE_ID" --json --progress-json
```

The observer does not send a prompt, inspect or navigate the browser, or create a new
generation. It returns the authoritative terminal JSON and persisted `task_result` when
that exact scope becomes terminal. If the lifecycle is already terminal, use
`--report-scope "$SCOPE_ID"` to replay the same artifact immediately.

## Single retained session + resume loop

The normal coding loop is intentionally serial after the first worker is created:

```text
fresh single delegation
        |
        v
terminal result -> retained scope_id
        |
        v
local orchestrator verification
        |
        +---- green ----> final review/approval ----> --close-scope
        |
        +---- failed ---> --resume-scope SAME_SCOPE with exact failure feedback
                              |
                              v
                         terminal result
                              |
                              +---- repeat local verification
```

Example CLI flow:

```bash
printf '%s' 'Inspect the change, implement it, run tests, and finish through completion_check.' |
  delegate_to_chatgpt_web --workspace /repo --stdin --json > /tmp/web-1.json

# Extract the retained scope_id from /tmp/web-1.json, then run local verification.
# If verification fails, resume the same conversation instead of spawning another worker:
printf '%s' 'Local cargo test failed in auth::tests::expired_token. Fix that failure and rerun all required verification.' |
  delegate_to_chatgpt_web --resume-scope "$SCOPE_ID" --stdin --json > /tmp/web-2.json

# After final coordinator approval:
delegate_to_chatgpt_web --close-scope "$SCOPE_ID" --json
```

Do not use `--close-on-terminal` for a workflow that expects a resume loop. Terminal sessions are retained by default and are released with `--close-scope` only after approval.

## Two-domain fan-out + fan-in

The safe pattern is:

```text
                 +--> backend Web worker --+
DAG fanout batch |                           | terminal batch JSON
                 +--> frontend Web worker --+
                                              |
                                              v
                                  local integration verification
                                              |
                                              v
                              resume retained primary scope
                                    (serial fan-in pass)
                                              |
                                              v
                                     final verification
                                              |
                                              v
                              close both retained scopes
```

Choose domains with disjoint file ownership whenever possible. The two Web workers may edit the same workspace concurrently, so overlapping ownership should be avoided and reconciled only during the serial fan-in pass.

The fan-in prompt should include:

- the integration goal;
- terminal evidence from both parallel workers;
- the exact local build/test/lint failure, if any;
- an instruction to inspect the current workspace independently and finish through authoritative `completion_check`.

`DelegateWebResult::fan_in_handoff` and the JS/Python `fanInPrompt` / `fan_in_prompt` helpers build this handoff without opening a third worker.

## Native DAG / JavaScript SDK node pattern

`examples/mass_ulw_web.mjs` is deliberately independent of a specific OMO SDK release. It returns a stable process contract (`program`, `args`, `stdin`) that can be executed inside a `tool.dag` node callback or a JavaScript SDK node.

A mass-ulw graph should have these logical nodes:

```javascript
import {
  parallelPairInvocation,
  resumeInvocation,
  runInvocation,
  retainedScope,
  fanInPrompt,
  closeInvocation,
} from "./examples/mass_ulw_web.mjs";

const workspace = "/repo";
const bridgeUrl = "https://code.checka.cc";

// Node: web-fanout
const fanout = await runInvocation(
  parallelPairInvocation({
    bridgeUrl,
    tasks: [
      {
        label: "backend",
        workspace,
        task: "Own backend files only. Implement backend behavior and tests.",
      },
      {
        label: "frontend",
        workspace,
        task: "Own frontend files only. Implement frontend behavior and tests.",
      },
    ],
  }),
  { cwd: workspace },
);

// Node: local-verify, depends on web-fanout.
// Run repository build/test/lint here and capture exact failure text.
const localFailure = "cargo test: integration::round_trip failed ...";

// Node: web-fanin, depends on local-verify.
const primaryScope = retainedScope(fanout, "backend");
const fanin = await runInvocation(
  resumeInvocation({
    bridgeUrl,
    scopeId: primaryScope,
    task: fanInPrompt(
      fanout,
      "Reconcile both domains and make all integration verification green.",
      localFailure,
    ),
  }),
  { cwd: workspace },
);

// Node: final-verify, depends on web-fanin.
// After final approval, close both retained tabs in serial cleanup nodes.
await runInvocation(closeInvocation({ bridgeUrl, scopeId: primaryScope }));
await runInvocation(
  closeInvocation({ bridgeUrl, scopeId: retainedScope(fanout, "frontend") }),
);
```

Map each commented phase to the node-registration shape used by the installed OMO `tool.dag` / JS SDK. The **stable integration boundary is the invocation object**, not the SDK's outer callback field names. This keeps bridge orchestration compatible if the DAG SDK changes its node-registration syntax.

Recommended dependency graph:

```text
web-fanout -> local-verify -> web-fanin -> final-verify -> cleanup
```

For a single worker:

```text
web-initial -> local-verify -> [web-resume -> local-verify]* -> final-review -> cleanup
```

### Web-only source-changing phases

Native `dag` node categories describe OMO-side execution; `dependsOn` provides ordering
only and never carries a retained `scope_id` into a downstream prompt. Consequently, a
downstream node is an ordinary OMO worker unless its own invocation explicitly calls
`delegate_to_chatgpt_web`.

For a Web-owned workflow, use this rule:

- a **source-changing** phase is always a fresh helper batch or an exact
  `resumeInvocation` / `--resume-scope` Web invocation;
- a **local verification** phase may be an ordinary DAG node, but it only runs
  build/test/lint and returns structured feedback; and
- a **fan-in/fix** phase resumes one exact retained Web scope with that feedback. Do not
  express it as a generic `deep`, `quick`, or other coding node.

Because native DAG does not pass prior outputs into later prompts, use a staged run when
the next Web phase needs returned scope IDs: wait for the terminal batch JSON, extract
the exact retained IDs, then define the next DAG run with those IDs embedded in the
`resumeInvocation` inputs. Never substitute a newly spawned ordinary node merely because
the IDs are not automatically available.

For more than two independent Web tasks, schedule safe two-worker waves. Finish the
first batch and preserve its retained scopes before launching the next batch; do not
work around the bridge capacity limit with concurrent helper processes.

## Python `eval` pipeline

`examples/mass_ulw_web.py` can be imported directly by a Python eval node:

```python
import sys
sys.path.insert(0, "examples")

from mass_ulw_web import (
    close_invocation,
    fan_in_prompt,
    parallel_pair_invocation,
    retained_scope,
    resume_invocation,
    run_invocation,
)

workspace = "/repo"
bridge_url = "https://code.checka.cc"

fanout = run_invocation(
    parallel_pair_invocation(
        bridge_url=bridge_url,
        tasks=[
            {
                "label": "core",
                "workspace": workspace,
                "task": "Own core implementation files only.",
            },
            {
                "label": "tests",
                "workspace": workspace,
                "task": "Own test fixtures and test files only.",
            },
        ],
    ),
    cwd=workspace,
)

# A separate local verification node should produce this exact feedback.
verification_feedback = "cargo test: protocol::compatibility failed with ..."
primary = retained_scope(fanout, "core")

fanin = run_invocation(
    resume_invocation(
        bridge_url=bridge_url,
        scope_id=primary,
        task=fan_in_prompt(
            fanout,
            "Integrate core + tests and make the complete quality gate green.",
            verification_feedback,
        ),
    ),
    cwd=workspace,
)

# Only after final approval:
run_invocation(close_invocation(bridge_url=bridge_url, scope_id=primary))
run_invocation(
    close_invocation(
        bridge_url=bridge_url,
        scope_id=retained_scope(fanout, "tests"),
    )
)
```

The Python helper uses `subprocess.run` without a shell, and the JavaScript helper uses `spawn` with argv arrays. Task text is sent through stdin in both cases, avoiding shell-quoting and command-injection problems.

## Rate-limit and retry behavior

The wrapper layer intentionally has **no automatic retry loop** around fresh dispatch or resume. If telemetry reports an active ChatGPT Web lockout or the sliding-window cap is exhausted, `delegate_to_chatgpt_web` fails before opening a new worker.

The DAG should treat that as a scheduling/backoff condition rather than immediately retrying in a tight loop. In particular:

- do not replace a blocked resume with a new fresh worker;
- preserve the retained `scope_id` and feedback payload;
- retry the same resume phase only after the bridge lockout/window permits dispatch;
- never bypass the two-worker cap with separate subprocesses;
- do not add another 10-second sleep in the wrapper: the delegate CLI already owns the stagger.

This keeps mass-ulw orchestration aligned with bridge telemetry and prevents retry storms.

## Result contract

The wrapper consumes the existing `--json` delegate result. The fields required for orchestration are:

```json
{
  "ok": true,
  "ready": true,
  "terminal": true,
  "delegations": [
    {
      "label": "backend",
      "scope_id": "...",
      "terminal_state": "COMPLETED",
      "terminal_detail": "...",
      "session_retained": true,
      "resumable": true
    }
  ]
}
```

Do not resume until `terminal` is true and the chosen delegation reports both `session_retained: true` and `resumable: true`.

## Verification

The integration is covered by `tests/mass_ulw_web.rs`. The tests validate:

- fixed 2-worker / 10-second policy constants;
- single retained-session invocation construction;
- one-process two-domain `--batch-stdin` construction;
- rejection of a third worker before spawn;
- exact `--resume-scope` reuse without `--workspace` or batch flags;
- terminal result parsing and retained-scope selection;
- serial fan-in prompt construction with local failure feedback;
- explicit final `--close-scope` cleanup.

Repository quality gates:

```bash
cargo fmt -- --check
cargo check
cargo test
cargo clippy --all-targets -- -D warnings
git diff --check
```
