<div align="center">

```
 ██████╗ ██████╗ ████████╗ ██████╗  ██████╗ ███╗   ███╗ ██████╗ 
██╔════╝ ██╔══██╗╚══██╔══╝ ██╔═══██╗ ██╔═══██╗████╗ ████║██╔═══██╗
██║  ███╗ ██████╔╝   ██║   ╚════██║ ██║   ██║██╔████╔██║██║   ██║
██║   ██║ ██╔═══╝    ██║        ██║ ██║   ██║██║╚██╔╝██║██║   ██║
╚██████╔╝ ██║        ██║   ██   ██║ ╚██████╔╝██║ ╚═╝ ██║╚██████╔╝
 ╚═════╝  ╚═╝        ╚═╝    ╚████╔╝  ╚═════╝ ╚═╝     ╚═╝ ╚═════╝ 
                               ╚═══╝                             
```

**High-Performance, Capability-Sandboxed MCP Daemon & Web Delegation Harness: ChatGPT Web Workers → OMO Workspaces, in 100% Rust.**

[![Rust](https://img.shields.io/badge/Rust-1.80+-f74c00?style=flat-square&logo=rust&logoColor=white)](https://www.rust-lang.org/)
[![Tokio](https://img.shields.io/badge/Tokio-Async_Runtime-232f3e?style=flat-square&logo=rust&logoColor=white)](https://tokio.rs/)
[![Axum](https://img.shields.io/badge/Axum-HTTP_%26_SSE-4f46e5?style=flat-square)](https://github.com/tokio-rs/axum)
[![MCP](https://img.shields.io/badge/MCP-Protocol_v1.0-06b6d4?style=flat-square)](https://modelcontextprotocol.io/)
[![License](https://img.shields.io/badge/License-Apache_2.0-22c55e?style=flat-square)](LICENSE)
[![Zero GC](https://img.shields.io/badge/GC-Zero_Pause-a855f7?style=flat-square)]()
[![Platform](https://img.shields.io/badge/Platform-macOS_|_Linux-0ea5e9?style=flat-square)]()

---

### The Native Control Plane for LLM Coding Delegations

**gpt2omo** is a low-latency, secure Model Context Protocol (MCP) daemon and browser delegation coordinator written in pure Rust.<br/>
It acts as a capability-sandboxed I/O, code intelligence, execution, and verification harness—bridging orchestrators like **OMO** to isolated **ChatGPT Web** workers with authoritative state tracking, zero-escape filesystem guarantees, resumable session lifecycles, daemon-owned asynchronous commands, and an optional bounded advisory-model call.

</div>

---

## ⚡ Why gpt2omo?

Delegating coding tasks to external web-based LLMs presents critical coordination and security challenges:

- **Unsandboxed Host Access**: Raw agent executions risk directory traversal, credential leakage, and destructive commands.
- **Informal Lifecycle Claims**: Models frequently claim "Done!" or "Task finished" in prose without running builds, tests, or linters.
- **Race Conditions & Lost State**: Concurrent multi-worker tasks overwrite files and destroy workspace state.
- **Long-Running Command Timeouts**: Build/test processes can outlive an HTTP request, orphan descendants, or lose verification provenance after subsequent edits.
- **Ephemeral Context Loss**: Web conversations are traditionally throwaway, discarding valuable reasoning when follow-up iterations are required.

**gpt2omo solves this** by decoupling orchestration from execution, enforcing strict capability sandboxing, owning command lifecycles inside the daemon, and providing authoritative tool-backed verification gates.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       OMO ORCHESTRATOR / COORDINATOR                        │
│                (Task Decomposition & Worktree Selection)                    │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       │ 1. Fan-out Manifest (1-3 Workers)
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    GPT2OMO CONTROL PLANE & HELPER                        │
│                     (delegate_to_chatgpt_web CLI)                           │
│                                                                             │
│  ┌─────────────────────────┐   ┌─────────────────────────────────────────┐  │
│  │   Scope Multiplexer     │   │      Authoritative Readiness Gate       │  │
│  │ (UUID per-worker scope) │   │  (Fail-Closed MCP task_state evidence) │  │
│  └────────────┬────────────┘   └────────────────────┬────────────────────┘  │
│               │                                     │                       │
│               └─────────────────┬───────────────────┘                       │
│                                 ▼                                           │
│                    Orca Browser Tab Dispatcher                              │
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      CHATGPT WEB WORKERS (1 to 3 Max)                       │
│            - Sole coding agents for their delegated workspaces              │
│            - Optional query_subagent second opinion when configured         │
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │ JSON-RPC MCP Calls
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          GPT2OMO DAEMON (Rust)                           │
│  18 standard sandboxed tools + optional query_subagent                      │
│  - File I/O / Search / AST / LSP / Verification / Task lifecycle            │
│  - Daemon-owned CommandManager with bounded streaming output                 │
│  - Capability sandboxing and generation/revision-indexed evidence            │
│  - Optional bounded OpenAI-compatible advisory call                         │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## ✨ Key Features

- 🛡️ **Zero-Escape Capability Sandboxing**: Built on `cap-std` capability-based security. Filesystem operations are locked to the scoped workspace directory.
- 🤝 **Authoritative Readiness Handshake**: Workers must prove operational readiness by successfully calling MCP `task_state(scope_id=...)`. Purely textual "READY" responses are ignored.
- 🏁 **Revision-Fresh Terminal Verification**: `COMPLETED` comes only from `completion_check.ready=true`, and verification must match the current workspace revision and lifecycle generation.
- ⚙️ **Daemon-Owned Async Commands**: `run_command` never holds an MCP request longer than 15 seconds. Slow work detaches into the shared `CommandManager` and is recovered with `poll_command` or `list_commands`.
- 🧹 **Process-Tree Timeout Defense**: On Unix every command gets a process group. Cancellation/timeout sends `SIGTERM`, waits a bounded 1.5-second grace period, then sends `SIGKILL` to remaining descendants.
- 🔐 **Sanitized Child Environment**: Bridge/subagent credentials are stripped before spawning commands and child `PATH` is normalized.
- 🧠 **Bounded Output Memory**: stdout/stderr are continuously drained into capped ring buffers; one MCP response exposes at most 64 KiB combined output while retaining bounded recent history.
- 🔁 **Idempotent Command Retry**: Optional `client_request_id` is scoped to `(scope_id, generation)` and maps retries back to the original `command_id`.
- 🔄 **Resumable Session Lifecycle**: Terminal sessions can remain `IDLE_RETAINED` for bounded same-conversation follow-up.
- ⚡ **Strict Concurrency Limits**: Fresh fan-out has a hard maximum of three independent Web workers.
- 🧩 **18 Standard MCP Tools**: File I/O, structural search, LSP navigation, daemon-owned execution, git inspection, and task lifecycle tools.
- 🧠 **Optional Pattern B Advisory Tool**: `query_subagent` is advertised only when an endpoint is configured. It is a bounded second opinion, not another coding agent and never completion evidence.

---

## 🛠️ MCP Tool Suite

| Category | Tool Name | Description |
|---|---|---|
| **File I/O** | `read_file` | Read workspace files with SHA-256 hashing and line slicing. |
| | `patch_file` | Atomic write with optimistic SHA-256 precondition; increments `workspace_revision`. |
| | `list_files` | Workspace file tree traversal with `.gitignore` enforcement. |
| **Search & AST** | `search_text` | Fast literal text search across workspace source files. |
| | `ast_grep` | Structural code search using AST patterns. |
| **Language Server** | `lsp_diagnostics` | Compiler and linter diagnostics via installed language servers. |
| | `lsp_definition` | Go-to-definition symbol resolution. |
| | `lsp_references` | Project-wide symbol reference search. |
| | `lsp_symbols` | Document symbol outline. |
| **Execution** | `run_command` | Spawn a whitelisted daemon-owned command; returns immediately or detaches after 15 seconds. |
| | `poll_command` | Long-poll up to 15 seconds and consume bounded stdout/stderr deltas plus evidence status. |
| | `list_commands` | Recover active and recent command IDs/status for the current scope. |
| | `cancel_command` | Cancel a command and terminate its process group/descendants. |
| | `git_status_diff` | Git porcelain status, bounded diff, and whitespace validation. |
| **Lifecycle** | `task_plan` | Persistent implementation plan definition. |
| | `task_update` | Plan status updates (`in_progress`, `done`, `blocked`). |
| | `task_state` | State recovery/readiness registration and completed-command reconciliation. |
| | `completion_check` | Revision-matched authoritative completion gate. |
| **Advisory (opt-in)** | `query_subagent` | OpenAI-compatible bounded second opinion marked `trust: "untrusted_advisory"`. |

### Daemon-owned command lifecycle

`run_command` creates the command in the daemon before waiting for it. The command therefore survives the individual MCP/HTTP request. A command that finishes inside the 15-second synchronous window returns its final exit code and bounded output immediately. If it is still running at the clamp, the response has `status: "detached_running"` and a stable `command_id`.

Use `poll_command` with that ID to read stdout/stderr deltas and optionally wait up to 15 seconds per call. `list_commands` recovers IDs after a context interruption, and `cancel_command` terminates work that is no longer needed. Supplying the same `client_request_id` during a retry in the same `(scope_id, generation)` returns the existing command rather than starting a duplicate.

Each spawn captures `generation` and `workspace_revision`. A successful `patch_file` increments the revision. If a verification command was spawned against an earlier revision, its response changes to `evidence_status: "stale_revision"` and `completion_check` refuses to use it. Verification must be rerun after the latest mutation.

stdout and stderr are drained continuously so verbose compilers cannot deadlock on full pipes. The daemon retains a bounded recent ring for each stream and sends no more than 32 KiB per stream (64 KiB combined) in one response page.

On Unix the child is placed in its own process group with `setpgid`. Timeout and explicit cancellation send `SIGTERM` to that process group, wait a bounded grace period, and escalate to `SIGKILL`, preventing test runners, compilers, and `sleep` descendants from being orphaned.

### Optional `query_subagent` configuration

`query_subagent` is **disabled and omitted from `tools/list` unless `--subagent-endpoint` / `OMO_SUBAGENT_ENDPOINT` is set**. The daemon POSTs to `/v1/chat/completions` using the configured model.

```bash
./target/release/gpt2omo \
  --subagent-endpoint http://127.0.0.1:8000 \
  --subagent-api-key "$OMO_SUBAGENT_API_KEY" \
  --subagent-model deepseek-v4-flash-free
```

Equivalent environment variables are `OMO_SUBAGENT_ENDPOINT`, `OMO_SUBAGENT_API_KEY`, and `OMO_SUBAGENT_MODEL`. `--subagent-allow-remote` (or `OMO_SUBAGENT_ALLOW_REMOTE=true`) is required for non-loopback endpoints; local loopback endpoints are the safe default.

The advisory path is intentionally constrained:

- `scope_id` must resolve to an active, non-terminal `DelegationLifecycle` generation.
- `prompt` is mandatory, non-empty, and capped at 32 KiB UTF-8.
- `timeout_ms` defaults to 30 seconds and is clamped to 1–60 seconds.
- Each generation may make at most four advisory calls; calls also pass through a process-global concurrency semaphore.
- Successful upstream bodies are streamed into a 256 KiB raw-byte cap before JSON parsing.
- Upstream error bodies and internal headers are never returned to the caller.
- HTTP redirects are disabled. Remote endpoints require explicit opt-in.
- Returned advice includes token usage when provided, request latency, and `trust: "untrusted_advisory"`.

A Web worker remains solely responsible for inspecting source, making edits, running verification, and satisfying `completion_check`. Advisory text cannot substitute for repository evidence or authoritative lifecycle state.

---

## 🚀 Quick Start

### 1. Build Binaries

```bash
cargo build --release
```

Compiled binaries in `target/release/`:
- `gpt2omo`: The background MCP HTTP/SSE daemon.
- `delegate_to_chatgpt_web`: The multi-worker batch delegation and resume CLI.
- `gpt2omo-relay`: The bridge-to-orchestrator SSE event relay.
- `install_delegate_web`: Automated installer for OMO and OpenCode skills.

### 2. Start the Daemon

```bash
# Local development (default bind is 127.0.0.1:18800 and mount-root is .):
./target/release/gpt2omo

# Non-loopback bridge-control exposure (--token protects `/events`):
./target/release/gpt2omo --bind 0.0.0.0:18800 --token "$OMO_BRIDGE_TOKEN"
```

Or connect via Cloudflare Tunnel. MCP tool calls authenticate with the per-delegation `scope_id`; do not configure a static Authorization header in the ChatGPT MCP connector. If a local relay subscribes to `/events`, keep its control token configured:
```bash
# Start daemon with an optional relay-control token:
./target/release/gpt2omo --token "$OMO_BRIDGE_TOKEN"

# Forward loopback traffic through tunnel:
cloudflared tunnel --url http://127.0.0.1:18800
```

### 3. Dispatch a Web Delegation

```bash
cat <<'EOF' | ./target/release/delegate_to_chatgpt_web --bridge-url https://code.checka.cc --batch-stdin --json
{
  "tasks": [
    {
      "label": "auth-module",
      "task": "Implement OAuth2 PKCE flow and add integration tests.",
      "workspace": "/Users/indo/code/project/my-app"
    }
  ]
}
EOF
```

### 4. Resume a Retained Session

```bash
echo "Add unit tests for token expiration." | ./target/release/delegate_to_chatgpt_web \
  --bridge-url https://code.checka.cc \
  --resume-scope '<scope-id>' \
  --stdin \
  --json
```

---

## 🧪 Verification & Quality Gates

Run the complete test and verification suite:

```bash
cargo fmt -- --check
cargo check
cargo test
cargo clippy --all-targets -- -D warnings
git diff --check
```

---

## 📜 License

Licensed under the [Apache License, Version 2.0](LICENSE).
