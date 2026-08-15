<div align="center">

```
 ██████╗ ███╗   ███╗ ██████╗       ██████╗ ██████╗  ██╗██████╗   ██████╗ ███████╗
██╔═══██╗████╗ ████║██╔═══██╗      ██╔══██╗██╔══██╗██║██╔══██╗ ██╔════╝ ██╔════╝
██║   ██║██╔████╔██║██║   ██║█████╗██████╔╝██████╔╝██║██║  ██║ ██║  ███╗█████╗
██║   ██║██║╚██╔╝██║██║   ██║╚════╝██╔══██╗██╔══██╗██║██║  ██║ ██║   ██║██╔══╝
╚██████╔╝██║ ╚═╝ ██║╚██████╔╝      ██████╔╝██║  ██║██║██████╔╝ ╚██████╔╝███████╗
 ╚═════╝ ╚═╝     ╚═╝ ╚═════╝       ╚═════╝ ╚═╝  ╚═╝╚═╝╚═════╝   ╚═════╝ ╚══════╝
```

**High-Performance, Capability-Sandboxed MCP Daemon & Web Delegation Harness for OMO in 100% Rust.**

[![Rust](https://img.shields.io/badge/Rust-1.80+-f74c00?style=flat-square&logo=rust&logoColor=white)](https://www.rust-lang.org/)
[![Tokio](https://img.shields.io/badge/Tokio-Async_Runtime-232f3e?style=flat-square&logo=rust&logoColor=white)](https://tokio.rs/)
[![Axum](https://img.shields.io/badge/Axum-HTTP_%26_SSE-4f46e5?style=flat-square)](https://github.com/tokio-rs/axum)
[![MCP](https://img.shields.io/badge/MCP-Protocol_v1.0-06b6d4?style=flat-square)](https://modelcontextprotocol.io/)
[![License](https://img.shields.io/badge/License-Apache_2.0-22c55e?style=flat-square)](LICENSE)
[![Zero GC](https://img.shields.io/badge/GC-Zero_Pause-a855f7?style=flat-square)]()
[![Platform](https://img.shields.io/badge/Platform-macOS_|_Linux-0ea5e9?style=flat-square)]()

---

### The Native Control Plane for LLM Coding Delegations

**OMO Bridge** is a low-latency, secure Model Context Protocol (MCP) daemon and browser delegation coordinator written in pure Rust.<br/>
It acts as a capability-sandboxed I/O, code intelligence, execution, and verification harness—bridging orchestrators like **OMO** to isolated **ChatGPT Web** workers with authoritative state tracking, zero-escape filesystem guarantees, and resumable session lifecycles.

</div>

---

## ⚡ Why OMO Bridge?

Delegating coding tasks to external web-based LLMs presents critical coordination and security challenges:

- **Unsandboxed Host Access**: Raw agent executions risk directory traversal, credential leakage, and destructive commands.
- **Informal Lifecycle Claims**: Models frequently claim "Done!" or "Task finished" in prose without running builds, tests, or linters.
- **Race Conditions & Lost State**: Concurrent multi-worker tasks overwrite files and destroy workspace state.
- **Ephemeral Context Loss**: Web conversations are traditionally throwaway, discarding valuable reasoning when follow-up iterations are required.

**OMO Bridge solves this** by decoupling orchestration from execution, enforcing strict capability sandboxing, and providing authoritative cryptographic and tool-backed verification gates.

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
│                    OMO-BRIDGE CONTROL PLANE & HELPER                        │
│                     (delegate_to_chatgpt_web CLI)                           │
│                                                                             │
│  ┌─────────────────────────┐   ┌─────────────────────────────────────────┐  │
│  │   Scope Multiplexer     │   │      Authoritative Readiness Gate       │  │
│  │ (UUID per-worker scope) │   │  (Fail-Closed 180s MCP task_state check)│  │
│  └────────────┬────────────┘   └────────────────────┬────────────────────┘  │
│               │                                     │                       │
│               ▼                                     ▼                       │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                    Orca Browser Tab Dispatcher                        │  │
│  │        (Isolated Tab Creation -> Bootstrap -> Concurrent TASK)       │  │
│  └────────────────────────────────────┬──────────────────────────────────┘  │
└───────────────────────────────────────┼─────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      CHATGPT WEB WORKERS (1 to 3 Max)                       │
│                  - Autonomous Reasoning & Implementation                    │
│                  - No direct access to host shell or filesystem             │
└───────────────────────────────────────┬─────────────────────────────────────┘
                                        │
                                        │ JSON-RPC MCP Calls (SSE / POST)
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          OMO-BRIDGE DAEMON (Rust)                           │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │ 15 Sandboxed MCP Tools                                                │  │
│  │ - File I/O: read_file (SHA256), patch_file (atomic), list_files       │  │
│  │ - Search & AST: search_text, ast_grep                                 │  │
│  │ - Code Intelligence: lsp_diagnostics, lsp_definition, lsp_references  │  │
│  │ - Execution: run_command (whitelisted), git_status_diff               │  │
│  │ - Task State: task_plan, task_update, task_state, completion_check   │  │
│  └───────────────────────────────────┬───────────────────────────────────┘  │
│                                      │                                       │
│  ┌───────────────────────────────────┴───────────────────────────────────┐  │
│  │ Security & Isolation Kernel                                           │  │
│  │ - cap-std Capability Sandboxing (No symlink/relative escapes)         │  │
│  │ - Authoritative Evidence Ledger & Multi-Writer Lock (1st Writer Wins) │  │
│  │ - Idle Lease Manager (120m Default TTL, Generation-Indexed Resume)    │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## ✨ Key Features

- 🛡️ **Zero-Escape Capability Sandboxing**: Built on `cap-std` capability-based security. All filesystem operations are locked to the scoped workspace directory. Symlink attacks, `..` traversal, and credential exposure (`.env`, `.git/config`) are structurally denied.
- 🤝 **Authoritative Readiness Handshake**: Workers must prove operational readiness by successfully calling MCP `task_state(scope_id=...)`. Purely textual "READY" responses are ignored. If any worker is unready or times out, the entire batch fails closed and cleans up staged resources.
- 🏁 **Authoritative Terminal State Verification**: Tasks conclude only via server-side verification:
  - `COMPLETED`: Authoritative `completion_check.ready=true` requiring a finished task plan, clean git diff checks, and verified build/test runs after the latest mutation.
  - `BLOCKED`: Explicit server-side blocker recording via `task_update` / `task_state`.
  - `FAILED` / `LOST`: Monitored and reported immediately to callers.
- 🔄 **Resumable Session Lifecycle**: Terminal sessions are retained in an `IDLE_RETAINED` state (120-minute lease by default). Follow-ups seamlessly resume the exact ChatGPT conversation, reopen blocked tasks, or replace completed plans with fresh iterations.
- ⚡ **Strict Concurrency Limits**: Hard maximum of 3 concurrent workers per batch. Batches requesting 4 or more workers are rejected immediately, and parallel tracks must be independent.
- 🧩 **15 Standard MCP Tools**: Complete tool surface covering file I/O, AST structural search (`ast-grep`), Language Server Protocol (LSP) diagnostics/navigation, whitelisted execution, and task tracking.

---

## 🛠️ MCP Tool Suite

| Category | Tool Name | Description |
|---|---|---|
| **File I/O** | `read_file` | Read workspace files with SHA-256 hashing and line slicing. |
| | `patch_file` | Atomic write with optimistic SHA-256 precondition verification. |
| | `list_files` | Workspace file tree traversal with `.gitignore` enforcement. |
| **Search & AST** | `search_text` | Fast literal text search across workspace source files. |
| | `ast_grep` | Structural code search using AST patterns across 25 languages. |
| **Language Server** | `lsp_diagnostics` | Real-time compiler & linter diagnostics via active language servers. |
| | `lsp_definition` | Go-to-definition symbol resolution. |
| | `lsp_references` | Project-wide symbol reference search. |
| | `lsp_symbols` | Document symbol outline. |
| **Execution** | `run_command` | Whitelisted test and build runner (`cargo`, `npm`, `pytest`, etc.). |
| | `git_status_diff`| Unified git porcelain status, diffstat, and whitespace validation. |
| **Lifecycle** | `task_plan` | Persistent multi-step implementation plan definition. |
| | `task_update` | Atomic status updates (`in_progress`, `done`, `blocked`). |
| | `task_state` | State recovery and readiness handshake registration. |
| | `completion_check` | Cryptographic & test-backed verification gate for task completion. |

---

## 🚀 Quick Start

### 1. Build Binaries

```bash
cargo build --release
```

Compiled binaries in `target/release/`:
- `omo-bridge`: The background MCP HTTP/SSE daemon.
- `delegate_to_chatgpt_web`: The multi-worker batch delegation and resume CLI.
- `omo-relay`: The bridge-to-orchestrator SSE event relay.
- `install_delegate_web`: Automated installer for OMO and OpenCode skills.

### 2. Start the Daemon

```bash
./target/release/omo-bridge --bind 0.0.0.0:18800 --mount-root /
```

Or connect via Cloudflare Tunnel:
```bash
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
# Code formatting check
cargo fmt -- --check

# Strict Clippy linter
cargo clippy --all-targets -- -D warnings

# Unit & Integration tests
cargo test
cargo test --release

# Git whitespace & diff check
git diff --check
```

---

## 📜 License

Licensed under the [Apache License, Version 2.0](LICENSE).